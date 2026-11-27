"""PipelineArtifact runtime: WritePlan / TransitionPlan under D46 (#135).

Every test runs on a real DuckDB store holding the source, the sink, the
artifact table and the transition table.  After each run the PA invariants
are checked with the evaluators of ``dgm_validate_invariant``.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterator
from datetime import datetime, timedelta
from typing import Any

import duckdb
import pytest

from sqldim.core.query.dgm import DGMQuery
from sqldim.core.query.dgm.annotations import PipelineArtifact, WriteModeKind
from sqldim.core.query.dgm.model import (
    DGMModel,
    EdgeKind,
    EdgeType,
    ForeignKey,
    Holder,
    NodeKind,
    NodeTable,
    NodeType,
    PropType,
    Reference,
)
from sqldim.core.query.dgm.nl.supervisor._auth import Action, AuthContext, grants_for
from sqldim.core.query.dgm.nl.supervisor._invariant_core import Evaluator
from sqldim.core.query.dgm.nl.supervisor._invariant_pa import PA_EVALUATORS
from sqldim.core.query.dgm.planner import (
    DGMPlanner,
    ExportPlan,
    QueryTarget,
    SinkTarget,
    StaticCatalog,
)
from sqldim.core.query.dgm.runtime import (
    ArtifactConflict,
    ArtifactOutcome,
    ArtifactPlan,
    ArtifactRuntime,
    ArtifactState,
    InvalidRuntimeArgument,
    MaterialisationFailed,
    NotConfigured,
    PlanExecutor,
    RunCancelled,
    SinkWrite,
    StaticDatasets,
    TransitionCondition,
    TransitionKind,
    TransitionNotRecorded,
    TransitionPlan,
    Unauthorized,
    UnknownIdentifier,
    UnsupportedOperation,
    WriteOperation,
    WritePlan,
    plan_artifact_run,
    transition_key,
)

# ---------------------------------------------------------------------------
# Fixture: a store with source, sink, artifact and transition tables
# ---------------------------------------------------------------------------

W0 = datetime(2024, 1, 1)
W1 = datetime(2024, 1, 2)
KEY = ("2024-01-01",)

STORE_SQL = [
    "CREATE TABLE events(id INTEGER, ts TIMESTAMP, amount DOUBLE)",
    "INSERT INTO events VALUES (1, '2024-01-01 08:00', 10), (2, '2024-01-01 09:00', 20)",
    "CREATE TABLE daily(id INTEGER, ts TIMESTAMP, amount DOUBLE CHECK (amount < 100))",
    "CREATE TABLE windows(wid VARCHAR, state VARCHAR, window_start TIMESTAMP, "
    "window_end TIMESTAMP)",
    "CREATE TABLE window_transitions(tid VARCHAR, wid VARCHAR, prev_tid VARCHAR, "
    "from_state VARCHAR, kind VARCHAR, to_state VARCHAR, occurred_at TIMESTAMP)",
]
DATASETS = StaticDatasets(
    {"events": "raw", "daily": "mart", "windows": "ops", "window_transitions": "log"}
)
CATALOG = StaticCatalog(base=frozenset({"events"}))

STATES = PropType.enum("Missing", "In-flight", "Complete", "Stale", "Failed")
WINDOW = NodeType(
    "Window",
    NodeKind.FACT,
    {"state": STATES, "window_start": PropType("Timestamp"),
     "window_end": PropType("Timestamp")},
    NodeTable(
        "windows",
        ("wid",),
        {"state": "state", "window_start": "window_start", "window_end": "window_end"},
    ),
)  # fmt: skip
TRANSITION = NodeType(
    "WindowTransition",
    NodeKind.FACT,
    {},
    NodeTable(
        "window_transitions",
        ("tid",),
        {
            "from_state": "from_state",
            "kind": "kind",
            "to_state": "to_state",
            "occurred_at": "occurred_at",
        },
    ),
)
TRANSITION_OF = EdgeType(
    "WindowTransition",
    "Window",
    "transition_of",
    EdgeKind.BRIDGE,
    mapping=ForeignKey(Holder.SOURCE, ("wid",), Reference.ENTITY_KEY),
)
FOLLOWED_BY = EdgeType(
    "WindowTransition",
    "WindowTransition",
    "followed_by",
    EdgeKind.BRIDGE,
    mapping=ForeignKey(Holder.TARGET, ("prev_tid",), Reference.ENTITY_KEY),
)


def model(mode: WriteModeKind = WriteModeKind.BACKFILL, **kw: Any) -> DGMModel:
    ann = PipelineArtifact(
        "Window", "daily", 3600, 90, mode, transitions="WindowTransition"
    )
    edges = kw.pop("edges", (TRANSITION_OF, FOLLOWED_BY))
    return DGMModel(
        (WINDOW, TRANSITION), edges, (ann,), frozenset({"PipelineArtifact"})
    )


def grants(
    read: tuple[str, ...] = ("raw", "ops", "log"),
    write: tuple[str, ...] = ("mart", "ops", "log"),
) -> AuthContext:
    g = set(grants_for([d for d in read if d not in write], {Action.READ}))
    g |= set(grants_for(write, {Action.READ, Action.WRITE}))
    return AuthContext("alice", "acme", frozenset(g))


class Clock:
    """A clock one minute later at every call; ``fail_at`` calls raise."""

    def __init__(self, fail_at: tuple[int, ...] = ()) -> None:
        self.calls = 0
        self.fail_at = fail_at

    def __call__(self) -> datetime:
        self.calls += 1
        if self.calls in self.fail_at:
            raise RuntimeError("clock unavailable")
        return datetime(2024, 1, 2, 1) + timedelta(minutes=self.calls)


@pytest.fixture()
def con() -> Iterator[duckdb.DuckDBPyConnection]:
    c = duckdb.connect()
    for statement in STORE_SQL:
        c.execute(statement)
    yield c
    c.close()


def runtime(
    con: Any,
    *,
    auth: Any = None,
    mode: WriteModeKind = WriteModeKind.BACKFILL,
    clock: Clock | None = None,
    sinks: dict[SinkTarget, Any] | None = None,
    m: DGMModel | None = None,
) -> ArtifactRuntime:
    return ArtifactRuntime(
        auth=grants() if auth is None else auth,
        model=m or model(mode),
        datasets=DATASETS,
        catalog=CATALOG,
        connection=con,
        clock=clock or Clock(),
        write_id_sinks=sinks or {},
    )


def query() -> ExportPlan:
    planner = DGMPlanner(None, None, None, None, QueryTarget.SQL_DUCKDB)  # type: ignore[arg-type]
    return planner.plan_query(DGMQuery().anchor("events", "e"))


def write(
    op: WriteOperation = WriteOperation.APPEND,
    sink: SinkTarget = SinkTarget.DUCKDB,
    window_column: str | None = "ts",
) -> WritePlan:
    return WritePlan(sink, "daily", op, window_column)


def plan_for(rt: ArtifactRuntime, **kw: Any) -> ArtifactPlan:
    snap = rt.snapshot("Window", KEY)
    return plan_artifact_run(
        snap,
        window_start=W0,
        window_end=W1,
        query=kw.pop("q", None) or query(),
        write=kw.pop("w", None) or write(),
    )


def counts(con: Any) -> tuple[int, int, int]:
    return tuple(  # type: ignore[return-value]
        con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
        for t in ("daily", "windows", "window_transitions")
    )


def chain(con: Any) -> list[tuple[str, str, str]]:
    return con.execute(
        "SELECT from_state, kind, to_state FROM window_transitions ORDER BY occurred_at"
    ).fetchall()


def state(con: Any) -> str | None:
    row = con.execute("SELECT state FROM windows").fetchone()
    return row[0] if row else None


def assert_pa_holds(con: Any, m: DGMModel | None = None) -> None:
    for name in ("PA1", "PA2", "PA4"):
        ev = Evaluator(con, m or model(), 10, now=datetime(2030, 1, 1))
        PA_EVALUATORS[name](ev)
        assert ev.result.holds, (name, ev.result.violations)


def seed_complete_then_stale(con: Any) -> None:
    """History Missing → In-flight → Complete → Stale, with its old rows."""
    con.execute(
        "INSERT INTO windows VALUES ('2024-01-01', 'Stale', '2024-01-01', '2024-01-02')"
    )
    con.execute(
        "INSERT INTO window_transitions VALUES "
        "('a', '2024-01-01', NULL, 'Missing', 'scheduled', 'In-flight', '2024-01-01 23:00'), "
        "('b', '2024-01-01', 'a', 'In-flight', 'succeeded', 'Complete', '2024-01-01 23:10'), "
        "('c', '2024-01-01', 'b', 'Complete', 'expired', 'Stale', '2024-01-02 00:30')"
    )
    con.execute("INSERT INTO daily VALUES (9, '2024-01-01 07:00', 1)")


# ---------------------------------------------------------------------------
# Plan types (no side effects)
# ---------------------------------------------------------------------------


class TestPlanTypes:
    def test_planning_writes_nothing(self, con: Any) -> None:
        rt = runtime(con)
        p = plan_for(rt)
        assert counts(con) == (0, 0, 0)
        assert p.on_dispatch.kind is TransitionKind.SCHEDULED
        assert p.on_failure.to_state is ArtifactState.FAILED

    def test_keys_follow_adr_0005(self, con: Any) -> None:
        p = plan_for(runtime(con))
        for tp in (p.on_dispatch, p.on_success, p.on_failure):
            assert tp.idempotency_key == transition_key(
                KEY, tp.from_state, tp.kind, p.plan_id
            )
        assert len({p.on_dispatch.idempotency_key, p.on_success.idempotency_key,
                    p.on_failure.idempotency_key}) == 3  # fmt: skip
        assert p.on_success.condition is TransitionCondition.ON_SUCCESS
        assert p.write_id != p.plan_id

    def test_transition_outside_delta(self) -> None:
        with pytest.raises(InvalidRuntimeArgument, match="δ"):
            TransitionPlan(
                KEY,
                ArtifactState.MISSING,
                TransitionKind.SUCCEEDED,
                ArtifactState.COMPLETE,
                TransitionCondition.ON_SUCCESS,
                "k",
            )

    def test_condition_must_match_kind(self) -> None:
        with pytest.raises(InvalidRuntimeArgument):
            TransitionPlan(
                KEY,
                ArtifactState.IN_FLIGHT,
                TransitionKind.FAILED,
                ArtifactState.FAILED,
                TransitionCondition.ON_SUCCESS,
                "k",
            )

    def test_merge_needs_window_column(self) -> None:
        with pytest.raises(InvalidRuntimeArgument):
            WritePlan(SinkTarget.DUCKDB, "daily", WriteOperation.MERGE)

    def test_no_run_starts_from_complete(self, con: Any) -> None:
        rt = runtime(con)
        rt.execute(plan_for(rt))
        with pytest.raises(InvalidRuntimeArgument, match="Complete"):
            plan_for(rt)

    def test_window_query_with_text_sink_is_refused(self, con: Any) -> None:
        q = dataclasses.replace(query(), sink_target=SinkTarget.DUCKDB)
        with pytest.raises(InvalidRuntimeArgument, match="WritePlan"):
            plan_for(runtime(con), q=q)

    def test_window_must_be_ordered(self, con: Any) -> None:
        snap = runtime(con).snapshot("Window", KEY)
        with pytest.raises(InvalidRuntimeArgument):
            plan_artifact_run(
                snap, window_start=W1, window_end=W0, query=query(), write=write()
            )


# ---------------------------------------------------------------------------
# Authorization (D44): nothing touched without the grants
# ---------------------------------------------------------------------------


class TestAuthorization:
    def test_no_auth_context(self, con: Any) -> None:
        with pytest.raises(Unauthorized):
            runtime(con, auth=object())
        assert counts(con) == (0, 0, 0)

    @pytest.mark.parametrize(
        "write_on",
        [
            ("ops", "log"),  # no WRITE on the sink
            ("mart", "ops"),  # no WRITE on the transitions dataset
            ("mart", "log"),  # no WRITE on the artifact row's dataset
        ],
    )
    def test_missing_write(self, con: Any, write_on: tuple[str, ...]) -> None:
        p = plan_for(runtime(con))
        denied = runtime(
            con, auth=grants(read=("raw", "mart", "ops", "log"), write=write_on)
        )
        with pytest.raises(Unauthorized):
            denied.execute(p)
        assert counts(con) == (0, 0, 0)

    def test_missing_read_on_the_source(self, con: Any) -> None:
        p = plan_for(runtime(con))
        with pytest.raises(Unauthorized):
            runtime(con, auth=grants(read=("ops", "log"))).execute(p)
        assert counts(con) == (0, 0, 0)

    def test_snapshot_needs_read(self, con: Any) -> None:
        with pytest.raises(Unauthorized):
            runtime(con, auth=grants(read=("raw",), write=("mart",))).snapshot(
                "Window", KEY
            )

    def test_unknown_relation(self, con: Any) -> None:
        p = plan_for(runtime(con))
        p = dataclasses.replace(
            p, write=WritePlan(SinkTarget.DUCKDB, "nowhere", WriteOperation.APPEND)
        )
        with pytest.raises(UnknownIdentifier):
            runtime(con).execute(p)
        assert counts(con) == (0, 0, 0)

    def test_unknown_artifact_type(self, con: Any) -> None:
        with pytest.raises(UnknownIdentifier):
            runtime(con).snapshot("Nope", KEY)


# ---------------------------------------------------------------------------
# D46 on the transactional store
# ---------------------------------------------------------------------------


class TestTransactional:
    def test_success_records_and_writes(self, con: Any) -> None:
        rt = runtime(con)
        out = rt.execute(plan_for(rt))
        assert isinstance(out, ArtifactOutcome)
        assert (out.state, out.rows_written, out.replayed) == (
            ArtifactState.COMPLETE,
            2,
            False,
        )
        assert counts(con) == (2, 1, 2)
        assert chain(con) == [
            ("Missing", "scheduled", "In-flight"),
            ("In-flight", "succeeded", "Complete"),
        ]
        assert state(con) == "Complete"
        assert_pa_holds(con)

    def test_idempotent_retry(self, con: Any) -> None:
        rt = runtime(con)
        p = plan_for(rt)
        rt.execute(p)
        again = rt.execute(p)
        assert again.replayed and again.rows_written is None
        assert counts(con) == (2, 1, 2)
        assert_pa_holds(con)

    def test_failure_mid_write_rolls_back_and_records_failed(self, con: Any) -> None:
        con.execute(
            "INSERT INTO events VALUES (3, '2024-01-01 10:00', 500)"
        )  # CHECK fails
        rt = runtime(con)
        p = plan_for(rt)
        with pytest.raises(MaterialisationFailed) as err:
            rt.execute(p)
        assert err.value.code == "WRITE_FAILED"
        assert err.value.state is ArtifactState.FAILED
        assert counts(con) == (0, 1, 2)  # nothing partial in the sink
        assert state(con) == "Failed"
        assert_pa_holds(con)
        # the same plan replays its failure; it is never a success
        with pytest.raises(MaterialisationFailed) as again:
            rt.execute(p)
        assert again.value.replayed
        assert counts(con) == (0, 1, 2)

    def test_retry_after_failure_is_a_new_plan(self, con: Any) -> None:
        con.execute("INSERT INTO events VALUES (3, '2024-01-01 10:00', 500)")
        rt = runtime(con)
        with pytest.raises(MaterialisationFailed):
            rt.execute(plan_for(rt))
        con.execute("DELETE FROM events WHERE id = 3")
        retry = plan_for(rt)
        assert retry.on_dispatch.kind is TransitionKind.RETRIED
        assert rt.execute(retry).state is ArtifactState.COMPLETE
        assert [k for _, k, _ in chain(con)] == [
            "scheduled",
            "failed",
            "retried",
            "succeeded",
        ]
        assert counts(con) == (2, 1, 4)
        assert_pa_holds(con)

    def test_record_failure_rolls_back_the_write(self, con: Any) -> None:
        rt = runtime(con, clock=Clock(fail_at=(2,)))  # succeeded cannot be appended
        with pytest.raises(MaterialisationFailed):
            rt.execute(plan_for(rt))
        assert counts(con) == (0, 1, 2)
        assert state(con) == "Failed"
        assert_pa_holds(con)

    def test_unrecorded_outcome_is_in_flight_then_retried(self, con: Any) -> None:
        rt = runtime(con, clock=Clock(fail_at=(2, 3)))
        p = plan_for(rt)
        with pytest.raises(TransitionNotRecorded) as err:
            rt.execute(p)
        assert err.value.state is ArtifactState.IN_FLIGHT
        assert counts(con) == (0, 1, 1)
        assert state(con) == "In-flight"
        assert_pa_holds(con)
        out = rt.execute(p)  # retry under the same write_id
        assert out.state is ArtifactState.COMPLETE and out.rows_written == 2
        assert counts(con) == (2, 1, 2)
        assert_pa_holds(con)

    def test_recover_appends_failed(self, con: Any) -> None:
        rt = runtime(con, clock=Clock(fail_at=(2, 3)))
        p = plan_for(rt)
        with pytest.raises(TransitionNotRecorded):
            rt.execute(p)
        with pytest.raises(MaterialisationFailed) as err:
            rt.recover(p)
        assert err.value.state is ArtifactState.FAILED
        assert counts(con) == (0, 1, 2)
        assert_pa_holds(con)

    def test_recover_without_dispatch(self, con: Any) -> None:
        rt = runtime(con)
        with pytest.raises(ArtifactConflict):
            rt.recover(plan_for(rt))
        assert counts(con) == (0, 0, 0)

    def test_cancel_before_dispatch(self, con: Any) -> None:
        rt = runtime(con)

        class Token:
            cancelled = True

        with pytest.raises(RunCancelled):
            rt.execute(plan_for(rt), cancel=Token())
        assert counts(con) == (0, 0, 0)

    def test_cancel_after_dispatch_records_failed(self, con: Any) -> None:
        rt = runtime(con)
        answers = iter([False, True])

        class Token:
            @property
            def cancelled(self) -> bool:
                return next(answers)

        with pytest.raises(MaterialisationFailed, match="cancelled"):
            rt.execute(plan_for(rt), cancel=Token())
        assert counts(con) == (0, 1, 2)
        assert state(con) == "Failed"
        assert_pa_holds(con)

    def test_row_outside_the_window_fails_the_write(self, con: Any) -> None:
        con.execute("INSERT INTO events VALUES (3, '2024-01-05 10:00', 1)")
        rt = runtime(con)
        with pytest.raises(MaterialisationFailed, match="outside"):
            rt.execute(plan_for(rt))
        assert counts(con) == (0, 1, 2)
        assert_pa_holds(con)

    def test_stale_plan_is_refused(self, con: Any) -> None:
        rt = runtime(con)
        assert plan_for(rt) == plan_for(rt)  # one attempt, one plan id
        first = plan_for(rt)
        rival = plan_for(rt, w=write(window_column=None))
        assert rival.plan_id != first.plan_id
        rt.execute(first)
        before = counts(con)
        with pytest.raises(ArtifactConflict):
            rt.execute(rival)  # made for Missing; the artifact is Complete now
        assert counts(con) == before
        assert_pa_holds(con)

    def test_broken_history_is_refused(self, con: Any) -> None:
        con.execute(
            "INSERT INTO windows VALUES ('2024-01-01', 'Complete', '2024-01-01', '2024-01-02')"
        )
        rt = runtime(con)
        with pytest.raises(ArtifactConflict, match="PA4"):
            rt.snapshot("Window", KEY)

    def test_another_window_in_the_row(self, con: Any) -> None:
        rt = runtime(con)
        p = plan_for(rt)
        con.execute(
            "INSERT INTO windows VALUES ('2024-01-01', 'Missing', '2023-01-01', '2023-01-02')"
        )
        with pytest.raises(ArtifactConflict, match="window"):
            rt.execute(p)
        assert counts(con) == (0, 1, 0)


# ---------------------------------------------------------------------------
# REFRESH and ADAPTIVE (write_mode → operation)
# ---------------------------------------------------------------------------


class TestWriteModes:
    def test_refresh_merge_swaps_the_window(self, con: Any) -> None:
        seed_complete_then_stale(con)
        rt = runtime(con, mode=WriteModeKind.REFRESH)
        p = plan_for(rt, w=write(WriteOperation.MERGE))
        assert p.on_dispatch.kind is TransitionKind.RESCHEDULED
        assert p.on_failure.to_state is ArtifactState.STALE
        assert rt.execute(p).rows_written == 2
        assert sorted(r[0] for r in con.execute("SELECT id FROM daily").fetchall()) == [
            1,
            2,
        ]
        assert_pa_holds(con, model(WriteModeKind.REFRESH))

    def test_refresh_failure_keeps_the_prior_result(self, con: Any) -> None:
        seed_complete_then_stale(con)
        con.execute("INSERT INTO events VALUES (3, '2024-01-01 10:00', 500)")
        rt = runtime(con, mode=WriteModeKind.REFRESH)
        with pytest.raises(MaterialisationFailed) as err:
            rt.execute(plan_for(rt, w=write(WriteOperation.MERGE)))
        assert err.value.state is ArtifactState.STALE
        assert con.execute("SELECT id FROM daily").fetchall() == [(9,)]
        assert state(con) == "Stale"
        assert_pa_holds(con, model(WriteModeKind.REFRESH))

    def test_operation_must_match_write_mode(self, con: Any) -> None:
        rt = runtime(con, mode=WriteModeKind.REFRESH)
        with pytest.raises(InvalidRuntimeArgument, match="REFRESH"):
            rt.execute(plan_for(rt))  # APPEND under REFRESH
        assert counts(con) == (0, 0, 0)

    def test_adaptive_from_stale_merges(self, con: Any) -> None:
        seed_complete_then_stale(con)
        rt = runtime(con, mode=WriteModeKind.ADAPTIVE)
        with pytest.raises(InvalidRuntimeArgument):
            rt.execute(plan_for(rt))
        assert (
            rt.execute(plan_for(rt, w=write(WriteOperation.MERGE))).state
            is ArtifactState.COMPLETE
        )


# ---------------------------------------------------------------------------
# Sinks without a transaction with the store
# ---------------------------------------------------------------------------


class CommitLogSink:
    """A write-id sink over a DuckDB table and a commit log (test double)."""

    def __init__(self, *, fail_before: bool = False, fail_after: bool = False) -> None:
        self.log: set[str] = set()
        self.fail_before = fail_before
        self.fail_after = fail_after
        self.writes = 0

    def write(self, con: Any, request: SinkWrite) -> int:
        self.writes += 1
        if self.fail_before:
            raise OSError("object store unavailable")
        n = con.execute(
            f"INSERT INTO daily BY NAME SELECT * FROM ({request.query_sql})"
        ).fetchone()[0]
        self.log.add(request.write_id)
        if self.fail_after:
            raise TimeoutError("ack lost")
        return int(n)

    def committed(self, relation: str, write_id: str) -> bool:
        return write_id in self.log


class TestWriteIdSink:
    def test_sink_without_guarantee_is_unsupported(self, con: Any) -> None:
        rt = runtime(con)
        with pytest.raises(UnsupportedOperation, match="D46"):
            rt.execute(plan_for(rt, w=write(sink=SinkTarget.PARQUET)))
        assert counts(con) == (0, 0, 0)

    def test_success(self, con: Any) -> None:
        sink = CommitLogSink()
        rt = runtime(con, sinks={SinkTarget.PARQUET: sink})
        out = rt.execute(plan_for(rt, w=write(sink=SinkTarget.PARQUET)))
        assert out.rows_written == 2 and sink.writes == 1
        assert_pa_holds(con)

    def test_ambiguous_failure_after_commit_is_success(self, con: Any) -> None:
        sink = CommitLogSink(fail_after=True)
        rt = runtime(con, sinks={SinkTarget.PARQUET: sink})
        out = rt.execute(plan_for(rt, w=write(sink=SinkTarget.PARQUET)))
        assert out.recovered and out.state is ArtifactState.COMPLETE
        assert counts(con) == (2, 1, 2)
        assert_pa_holds(con)

    def test_failure_before_commit(self, con: Any) -> None:
        sink = CommitLogSink(fail_before=True)
        rt = runtime(con, sinks={SinkTarget.PARQUET: sink})
        with pytest.raises(MaterialisationFailed):
            rt.execute(plan_for(rt, w=write(sink=SinkTarget.PARQUET)))
        assert state(con) == "Failed"
        assert_pa_holds(con)

    def test_recovery_finds_the_write_id(self, con: Any) -> None:
        sink = CommitLogSink()
        rt = runtime(con, sinks={SinkTarget.PARQUET: sink}, clock=Clock(fail_at=(2,)))
        p = plan_for(rt, w=write(sink=SinkTarget.PARQUET))
        with pytest.raises(TransitionNotRecorded):
            rt.execute(p)  # written, but succeeded not appended
        assert state(con) == "In-flight"
        out = rt.recover(p)
        assert out.recovered and sink.writes == 1
        assert state(con) == "Complete"
        assert_pa_holds(con)

    def test_retry_does_not_repeat_a_committed_write(self, con: Any) -> None:
        sink = CommitLogSink()
        rt = runtime(con, sinks={SinkTarget.PARQUET: sink}, clock=Clock(fail_at=(2,)))
        p = plan_for(rt, w=write(sink=SinkTarget.PARQUET))
        with pytest.raises(TransitionNotRecorded):
            rt.execute(p)
        assert rt.execute(p).recovered
        assert sink.writes == 1 and counts(con) == (2, 1, 2)


# ---------------------------------------------------------------------------
# Model mappings the runtime cannot append to
# ---------------------------------------------------------------------------


class TestLayout:
    def test_followed_by_held_by_the_source_is_unsupported(self, con: Any) -> None:
        other = dataclasses.replace(
            FOLLOWED_BY,
            mapping=ForeignKey(Holder.SOURCE, ("prev_tid",), Reference.ENTITY_KEY),
        )
        rt = runtime(con, m=model(edges=(TRANSITION_OF, other)))
        with pytest.raises(UnsupportedOperation):
            rt.snapshot("Window", KEY)


# ---------------------------------------------------------------------------
# PlanExecutor integration
# ---------------------------------------------------------------------------


class _Backend:
    target = QueryTarget.SQL_DUCKDB

    def execute(self, sql: str, datasets: frozenset[str]) -> Any:
        raise AssertionError("not called")

    def interrupt(self) -> None:
        pass


def plan_executor(
    rt: ArtifactRuntime | None, auth: AuthContext | None = None
) -> PlanExecutor:
    return PlanExecutor(
        auth=auth or grants(),
        datasets=DATASETS,
        catalog=CATALOG,
        backend=_Backend(),
        job_sinks={},
        stream_batch_size=10,
        artifacts=rt,
    )


class TestPlanExecutor:
    def test_materialise(self, con: Any) -> None:
        rt = runtime(con)
        out = plan_executor(rt).materialise(plan_for(rt))
        assert out.state is ArtifactState.COMPLETE
        assert_pa_holds(con)

    def test_without_runtime_nothing_is_written(self, con: Any) -> None:
        p = plan_for(runtime(con))
        with pytest.raises(NotConfigured):
            plan_executor(None).materialise(p)
        assert counts(con) == (0, 0, 0)

    def test_runtime_of_another_principal(self, con: Any) -> None:
        other = AuthContext("bob", "acme", grants().grants)
        with pytest.raises(NotConfigured):
            plan_executor(runtime(con, auth=other))

    def test_run_refuses_a_sink_plan(self, con: Any) -> None:
        q = dataclasses.replace(query(), sink_target=SinkTarget.DUCKDB)
        with pytest.raises(InvalidRuntimeArgument, match="materialise"):
            plan_executor(None).run(q, "PROCEED")
        assert counts(con) == (0, 0, 0)

"""Planner Rule 10 emits typed PipelineArtifact plans (§6.2, §2.4; #171).

The pure tests build snapshots by hand.  The runtime tests plan with Rule 10
on a real DuckDB store, execute or expire with :class:`ArtifactRuntime`, and
check PA1, PA2 and PA4 with the ``dgm_validate_invariant`` evaluators.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from typing import Any

import duckdb
import pytest

from sqldim.core.query.dgm.annotations import (
    GrainKind,
    PipelineArtifact,
    WriteModeKind,
)
from sqldim.core.query.dgm.graph import GraphStatistics
from sqldim.core.query.dgm.annotations import AnnotationSigma
from sqldim.core.query.dgm.planner import (
    ArtifactTarget,
    DGMPlanner,
    IcebergCatalog,
    QueryTarget,
    SinkTarget,
)
from sqldim.core.query.dgm.planner._artifact_plan import (
    ArtifactPlan,
    ArtifactSnapshot,
    ArtifactState,
    ExpiryPlan,
    InvalidArtifactPlan,
    NotEligible,
    TransitionCondition,
    TransitionKind,
    UnsupportedArtifactPlan,
    WriteOperation,
)
from sqldim.core.query.dgm.planner._rule10 import backfill_eligible
from sqldim.core.query.dgm.runtime import (
    ArtifactConflict,
    ExpiryNotDue,
    InvalidRuntimeArgument,
    TransitionNotRecorded,
    Unauthorized,
)
from tests.query.test_dgm_runtime_artifact import (
    KEY,
    W0,
    W1,
    assert_pa_holds,
    chain,
    STORE_SQL,
    counts,
    grants,
    model,
    query,
    runtime,
    state,
)


@pytest.fixture()
def con() -> Iterator[duckdb.DuckDBPyConnection]:
    c = duckdb.connect()
    for statement in STORE_SQL:
        c.execute(statement)
    yield c
    c.close()


NOW = datetime(2024, 1, 2, 1)
TARGET = ArtifactTarget("daily", "ts")


def planner(
    sink: SinkTarget | None = SinkTarget.DUCKDB, catalog: IcebergCatalog | None = None
) -> DGMPlanner:
    return DGMPlanner(
        cost_model=None,
        statistics=GraphStatistics(node_count=1, edge_count=0),
        annotations=AnnotationSigma([]),
        rules=None,
        query_target=QueryTarget.SQL_DUCKDB,
        sink_target=sink,
        iceberg_catalog=catalog,
    )


def artifact(
    mode: WriteModeKind = WriteModeKind.BACKFILL, ttl: int = 3600, horizon: int = 90
) -> PipelineArtifact:
    return PipelineArtifact(
        "Window", "daily", ttl, horizon, mode, transitions="WindowTransition"
    )


def snap(
    st: ArtifactState = ArtifactState.MISSING,
    *,
    window: tuple[datetime, datetime] | None = None,
    last: str | None = None,
    at: datetime | None = None,
    reached: bool = False,
) -> ArtifactSnapshot:
    return ArtifactSnapshot("Window", KEY, st, last, reached, window, at)


def rule10(
    a: PipelineArtifact | None = None,
    s: ArtifactSnapshot | None = None,
    *,
    sink: SinkTarget | None = SinkTarget.DUCKDB,
    now: datetime = NOW,
    window: tuple[datetime, datetime] | None = (W0, W1),
    catalog: IcebergCatalog | None = None,
) -> ArtifactPlan | ExpiryPlan:
    return planner(sink, catalog).apply_rule_10(
        a or artifact(),
        s or snap(),
        now=now,
        target=TARGET,
        query=query(),
        window=window,
    )


# ---------------------------------------------------------------------------
# Operation by write_mode and state
# ---------------------------------------------------------------------------


class TestWriteMode:
    def test_backfill_from_missing_appends(self) -> None:
        p = rule10()
        assert isinstance(p, ArtifactPlan)
        assert p.write.operation is WriteOperation.APPEND
        assert p.write.sink is SinkTarget.DUCKDB
        assert (p.write.relation, p.write.window_column) == ("daily", "ts")
        assert p.on_dispatch.kind is TransitionKind.SCHEDULED
        assert p.on_failure.to_state is ArtifactState.FAILED

    @pytest.mark.parametrize(
        ("st", "reached", "op", "failure"),
        [
            (ArtifactState.MISSING, False, WriteOperation.APPEND, ArtifactState.FAILED),
            (ArtifactState.FAILED, False, WriteOperation.APPEND, ArtifactState.FAILED),
            (ArtifactState.STALE, True, WriteOperation.MERGE, ArtifactState.STALE),
        ],
    )
    def test_adaptive_follows_the_state(
        self,
        st: ArtifactState,
        reached: bool,
        op: WriteOperation,
        failure: ArtifactState,
    ) -> None:
        s = snap(st, window=(W0, W1), last="t", at=W1, reached=reached)
        p = rule10(artifact(WriteModeKind.ADAPTIVE), s)
        assert isinstance(p, ArtifactPlan)
        assert p.write.operation is op and p.on_failure.to_state is failure

    def test_refresh_merges_from_stale(self) -> None:
        s = snap(ArtifactState.STALE, window=(W0, W1), last="t", at=W1, reached=True)
        p = rule10(artifact(WriteModeKind.REFRESH), s)
        assert isinstance(p, ArtifactPlan)
        assert p.write.operation is WriteOperation.MERGE
        assert p.on_dispatch.kind is TransitionKind.RESCHEDULED

    def test_refresh_is_not_bounded_by_the_horizon(self) -> None:
        s = snap(ArtifactState.STALE, window=(W0, W1), last="t", at=W1, reached=True)
        late = W1 + timedelta(days=365)
        assert isinstance(
            rule10(artifact(WriteModeKind.REFRESH), s, now=late), ArtifactPlan
        )

    def test_unknown_write_mode_is_invalid(self) -> None:
        bad = dataclasses.replace(artifact(), write_mode=GrainKind.EVENT)  # type: ignore[arg-type]
        with pytest.raises(InvalidArtifactPlan, match="write_mode"):
            rule10(bad)

    def test_no_sink_is_invalid(self) -> None:
        with pytest.raises(InvalidArtifactPlan, match="no default sink"):
            rule10(sink=None)

    def test_merge_into_delta_is_unsupported(self) -> None:
        s = snap(ArtifactState.STALE, window=(W0, W1), last="t", at=W1, reached=True)
        with pytest.raises(UnsupportedArtifactPlan, match="DELTA"):
            rule10(artifact(WriteModeKind.REFRESH), s, sink=SinkTarget.DELTA)

    def test_append_into_delta_is_planned(self) -> None:
        p = rule10(sink=SinkTarget.DELTA)
        assert isinstance(p, ArtifactPlan) and p.write.sink is SinkTarget.DELTA

    def test_iceberg_without_catalog_is_unsupported(self) -> None:
        with pytest.raises(UnsupportedArtifactPlan, match="REST catalog"):
            rule10(sink=SinkTarget.ICEBERG)

    def test_append_into_iceberg_on_any_version(self) -> None:
        p = rule10(sink=SinkTarget.ICEBERG, catalog=IcebergCatalog("ice", "1.5.0"))
        assert isinstance(p, ArtifactPlan) and p.write.sink is SinkTarget.ICEBERG

    @pytest.mark.parametrize(
        ("version", "mor", "match"),
        [("1.5.0", True, "capability table"), ("2.0", False, "merge-on-read")],
    )
    def test_merge_into_iceberg_is_checked(
        self, version: str, mor: bool, match: str
    ) -> None:
        s = snap(ArtifactState.STALE, window=(W0, W1), last="t", at=W1, reached=True)
        with pytest.raises(UnsupportedArtifactPlan, match=match):
            rule10(artifact(WriteModeKind.REFRESH), s, sink=SinkTarget.ICEBERG,
                   catalog=IcebergCatalog("ice", version, mor))  # fmt: skip

    def test_merge_into_iceberg_is_planned_when_documented(self) -> None:
        s = snap(ArtifactState.STALE, window=(W0, W1), last="t", at=W1, reached=True)
        p = rule10(artifact(WriteModeKind.REFRESH), s, sink=SinkTarget.ICEBERG,
                   catalog=IcebergCatalog("ice", "2.0.1", True))  # fmt: skip
        assert isinstance(p, ArtifactPlan) and p.write.operation.name == "MERGE"

    def test_in_flight_is_not_eligible(self) -> None:
        s = snap(ArtifactState.IN_FLIGHT, window=(W0, W1), last="t", at=W1)
        with pytest.raises(NotEligible, match="In-flight"):
            rule10(s=s)


# ---------------------------------------------------------------------------
# Backfill eligibility (§2.4): now − H ≤ window_end ≤ now
# ---------------------------------------------------------------------------


class TestBackfillHorizon:
    def test_lower_bound_is_inclusive(self) -> None:
        now = W1 + timedelta(days=90)
        assert isinstance(rule10(now=now), ArtifactPlan)

    def test_outside_the_horizon_is_not_eligible(self) -> None:
        now = W1 + timedelta(days=90, seconds=1)
        with pytest.raises(NotEligible, match="backfill"):
            rule10(now=now)

    def test_window_not_closed_is_not_eligible(self) -> None:
        with pytest.raises(NotEligible, match="backfill"):
            rule10(now=W1 - timedelta(seconds=1))

    def test_window_ending_now_is_eligible(self) -> None:
        assert isinstance(rule10(now=W1), ArtifactPlan)

    def test_failed_window_is_bounded_too(self) -> None:
        s = snap(ArtifactState.FAILED, window=(W0, W1), last="t", at=W1)
        with pytest.raises(NotEligible):
            rule10(s=s, now=W1 + timedelta(days=91))

    def test_zero_horizon_only_admits_window_end_now(self) -> None:
        assert isinstance(rule10(artifact(horizon=0), now=W1), ArtifactPlan)
        with pytest.raises(NotEligible):
            rule10(artifact(horizon=0), now=W1 + timedelta(seconds=1))

    def test_aware_instants_are_compared_as_utc(self) -> None:
        brt = timezone(timedelta(hours=-3))
        assert backfill_eligible(W1, datetime(2024, 1, 1, 21, tzinfo=brt), 0)
        assert not backfill_eligible(W1, datetime(2024, 1, 1, 20, tzinfo=brt), 0)

    def test_negative_horizon_is_invalid(self) -> None:
        with pytest.raises(InvalidArtifactPlan, match="backfill_horizon"):
            rule10(artifact(horizon=-1))


# ---------------------------------------------------------------------------
# Window and snapshot consistency
# ---------------------------------------------------------------------------


class TestWindow:
    def test_missing_without_window_is_invalid(self) -> None:
        with pytest.raises(InvalidArtifactPlan, match="window"):
            rule10(window=None)

    def test_window_must_be_the_stored_one(self) -> None:
        s = snap(ArtifactState.FAILED, window=(W0, W1), last="t", at=W1)
        with pytest.raises(InvalidArtifactPlan, match="not the artifact's window"):
            rule10(s=s, window=(W0, W1 + timedelta(hours=1)))

    def test_stored_window_is_used(self) -> None:
        s = snap(ArtifactState.FAILED, window=(W0, W1), last="t", at=W1)
        p = rule10(s=s, window=None)
        assert isinstance(p, ArtifactPlan) and (p.window_start, p.window_end) == (
            W0,
            W1,
        )

    def test_snapshot_of_another_type_is_invalid(self) -> None:
        other = dataclasses.replace(snap(), artifact_type="Other")
        with pytest.raises(InvalidArtifactPlan, match="Other"):
            rule10(s=other)

    def test_annotation_must_be_a_pipeline_artifact(self) -> None:
        with pytest.raises(InvalidArtifactPlan):
            planner().apply_rule_10(
                object(),  # type: ignore[arg-type]
                snap(),
                now=NOW,
                target=TARGET,
                query=query(),
                window=(W0, W1),
            )

    def test_target_must_be_typed(self) -> None:
        with pytest.raises(InvalidArtifactPlan, match="ArtifactTarget"):
            planner().apply_rule_10(
                artifact(),
                snap(),
                now=NOW,
                target="daily",  # type: ignore[arg-type]
                query=query(),
                window=(W0, W1),
            )


# ---------------------------------------------------------------------------
# TTL expiry (§2.4, ADR 0005 ON_EXPIRY)
# ---------------------------------------------------------------------------


class TestExpiryPlan:
    def complete(self) -> ArtifactSnapshot:
        return snap(
            ArtifactState.COMPLETE, window=(W0, W1), last="b", at=W1, reached=True
        )

    def test_elapsed_ttl_plans_the_expired_transition(self) -> None:
        p = rule10(s=self.complete(), now=W1 + timedelta(seconds=3601))
        assert isinstance(p, ExpiryPlan)
        tp = p.transition
        assert (tp.from_state, tp.kind, tp.to_state) == (
            ArtifactState.COMPLETE,
            TransitionKind.EXPIRED,
            ArtifactState.STALE,
        )
        assert tp.condition is TransitionCondition.ON_EXPIRY
        assert (p.based_on, p.completed_at, p.ttl) == ("b", W1, 3600)

    def test_ttl_boundary_is_strict(self) -> None:
        with pytest.raises(NotEligible, match="ttl"):
            rule10(s=self.complete(), now=W1 + timedelta(seconds=3600))

    def test_within_ttl_is_not_eligible(self) -> None:
        with pytest.raises(NotEligible):
            rule10(s=self.complete(), now=W1)

    def test_complete_without_its_instant_is_invalid(self) -> None:
        s = dataclasses.replace(self.complete(), last_occurred_at=None)
        with pytest.raises(InvalidArtifactPlan, match="occurred_at"):
            rule10(s=s, now=W1 + timedelta(days=1))

    def test_expiry_needs_no_sink(self) -> None:
        p = rule10(s=self.complete(), now=W1 + timedelta(days=1), sink=None)
        assert isinstance(p, ExpiryPlan)

    def test_negative_ttl_is_invalid(self) -> None:
        with pytest.raises(InvalidArtifactPlan, match="ttl"):
            rule10(artifact(ttl=-1), self.complete())

    def test_expired_is_not_on_dispatch(self) -> None:
        from sqldim.core.query.dgm.planner._artifact_plan import TransitionPlan

        with pytest.raises(InvalidArtifactPlan):
            TransitionPlan(
                KEY,
                ArtifactState.COMPLETE,
                TransitionKind.EXPIRED,
                ArtifactState.STALE,
                TransitionCondition.ON_DISPATCH,
                "k",
            )


# ---------------------------------------------------------------------------
# Rule 10 plans executed by the runtime
# ---------------------------------------------------------------------------


class SetClock:
    """A settable clock one second later at every call; raises when ``fail``."""

    def __init__(self, now: datetime) -> None:
        self.now = now
        self.last: datetime | None = None
        self.fail = False

    def __call__(self) -> datetime:
        if self.fail:
            raise RuntimeError("clock unavailable")
        self.last, self.now = self.now, self.now + timedelta(seconds=1)
        return self.last


def plan_now(
    rt: Any, clock: SetClock, mode: WriteModeKind
) -> ArtifactPlan | ExpiryPlan:
    return planner().apply_rule_10(
        artifact(mode),
        rt.snapshot("Window", KEY),
        now=clock.now,
        target=TARGET,
        query=query(),
        window=(W0, W1),
    )


def complete_run(
    con: Any, mode: WriteModeKind = WriteModeKind.ADAPTIVE
) -> tuple[Any, SetClock]:
    clock = SetClock(NOW)
    rt = runtime(con, mode=mode, clock=clock)
    p = plan_now(rt, clock, mode)
    assert isinstance(p, ArtifactPlan)
    rt.execute(p)
    assert state(con) == "Complete"
    return rt, clock


class TestRuntime:
    def test_planning_writes_nothing(self, con: Any) -> None:
        clock = SetClock(NOW)
        p = plan_now(runtime(con, clock=clock), clock, WriteModeKind.BACKFILL)
        assert isinstance(p, ArtifactPlan) and counts(con) == (0, 0, 0)

    def test_lifecycle_backfill_expire_refresh(self, con: Any) -> None:
        rt, clock = complete_run(con)
        clock.now = NOW + timedelta(hours=2)
        exp = plan_now(rt, clock, WriteModeKind.ADAPTIVE)
        assert isinstance(exp, ExpiryPlan)
        assert counts(con) == (2, 1, 2)  # planning the expiry wrote nothing
        out = rt.expire(exp)
        assert out.state is ArtifactState.STALE and out.occurred_at == clock.last
        assert state(con) == "Stale"
        assert_pa_holds(con, model(WriteModeKind.ADAPTIVE))
        clock.now += timedelta(minutes=1)
        refresh = plan_now(rt, clock, WriteModeKind.ADAPTIVE)
        assert isinstance(refresh, ArtifactPlan)
        assert refresh.write.operation is WriteOperation.MERGE
        rt.execute(refresh)
        assert chain(con) == [
            ("Missing", "scheduled", "In-flight"),
            ("In-flight", "succeeded", "Complete"),
            ("Complete", "expired", "Stale"),
            ("Stale", "rescheduled", "In-flight"),
            ("In-flight", "succeeded", "Complete"),
        ]
        assert counts(con)[0] == 2  # the MERGE replaced the window's rows
        assert_pa_holds(con, model(WriteModeKind.ADAPTIVE))

    def test_expire_is_idempotent(self, con: Any) -> None:
        rt, clock = complete_run(con)
        clock.now = NOW + timedelta(hours=2)
        exp = plan_now(rt, clock, WriteModeKind.ADAPTIVE)
        assert isinstance(exp, ExpiryPlan)
        first = rt.expire(exp)
        clock.now += timedelta(hours=1)
        again = rt.expire(exp)
        assert again.replayed and again.occurred_at == first.occurred_at
        assert counts(con) == (2, 1, 3)
        assert_pa_holds(con, model(WriteModeKind.ADAPTIVE))

    def test_expire_before_ttl_records_nothing(self, con: Any) -> None:
        rt, clock = complete_run(con)
        clock.now = NOW + timedelta(hours=2)
        exp = plan_now(rt, clock, WriteModeKind.ADAPTIVE)
        assert isinstance(exp, ExpiryPlan)
        clock.now = NOW  # the runtime's own instant decides ON_EXPIRY
        with pytest.raises(ExpiryNotDue):
            rt.expire(exp)
        assert state(con) == "Complete" and counts(con) == (2, 1, 2)
        assert_pa_holds(con, model(WriteModeKind.ADAPTIVE))

    def test_expire_after_another_transition_is_a_conflict(self, con: Any) -> None:
        rt, clock = complete_run(con)
        clock.now = NOW + timedelta(hours=2)
        exp = plan_now(rt, clock, WriteModeKind.ADAPTIVE)
        assert isinstance(exp, ExpiryPlan)
        other = dataclasses.replace(exp, based_on="not-the-last")
        with pytest.raises(ArtifactConflict):
            rt.expire(other)
        moved = dataclasses.replace(exp, completed_at=NOW)
        with pytest.raises(ArtifactConflict):
            rt.expire(moved)
        assert state(con) == "Complete"

    def test_expire_of_a_non_complete_artifact_is_a_conflict(self, con: Any) -> None:
        clock = SetClock(NOW)
        rt = runtime(con, clock=clock)
        exp = ExpiryPlan("Window", KEY, "b", W1, 3600)
        with pytest.raises(ArtifactConflict, match="Missing"):
            rt.expire(exp)
        assert counts(con) == (0, 0, 0)

    def test_expire_with_another_ttl_is_invalid(self, con: Any) -> None:
        rt, clock = complete_run(con)
        clock.now = NOW + timedelta(days=1)
        exp = plan_now(rt, clock, WriteModeKind.ADAPTIVE)
        assert isinstance(exp, ExpiryPlan)
        with pytest.raises(InvalidRuntimeArgument, match="ttl"):
            rt.expire(dataclasses.replace(exp, ttl=60))
        assert state(con) == "Complete"

    def test_expire_needs_write(self, con: Any) -> None:
        rt, clock = complete_run(con)
        clock.now = NOW + timedelta(days=1)
        exp = plan_now(rt, clock, WriteModeKind.ADAPTIVE)
        reader = runtime(
            con,
            auth=grants(read=("raw", "ops", "log", "mart"), write=("mart",)),
            mode=WriteModeKind.ADAPTIVE,
            clock=clock,
        )
        with pytest.raises(Unauthorized):
            reader.expire(exp)  # type: ignore[arg-type]
        assert state(con) == "Complete" and counts(con) == (2, 1, 2)

    def test_expire_takes_only_an_expiry_plan(self, con: Any) -> None:
        clock = SetClock(NOW)
        rt = runtime(con, clock=clock)
        p = plan_now(rt, clock, WriteModeKind.BACKFILL)
        with pytest.raises(InvalidRuntimeArgument, match="ExpiryPlan"):
            rt.expire(p)  # type: ignore[arg-type]
        assert counts(con) == (0, 0, 0)

    def test_unreadable_clock_records_nothing(self, con: Any) -> None:
        rt, clock = complete_run(con)
        clock.now = NOW + timedelta(days=1)
        exp = plan_now(rt, clock, WriteModeKind.ADAPTIVE)
        assert isinstance(exp, ExpiryPlan)
        clock.fail = True
        with pytest.raises(TransitionNotRecorded) as info:
            rt.expire(exp)
        assert info.value.state is ArtifactState.COMPLETE
        assert state(con) == "Complete"
        assert_pa_holds(con, model(WriteModeKind.ADAPTIVE))

    def test_backfill_outside_horizon_plans_nothing(self, con: Any) -> None:
        clock = SetClock(W1 + timedelta(days=91))
        rt = runtime(con, clock=clock)
        with pytest.raises(NotEligible):
            plan_now(rt, clock, WriteModeKind.BACKFILL)
        assert counts(con) == (0, 0, 0)

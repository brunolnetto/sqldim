"""The authorised PipelineArtifact runtime of §2.4 (D46, ADR 0002, ADR 0005).

:class:`ArtifactRuntime` executes an :class:`ArtifactPlan` by the four steps
of D46 and records the transitions as append-only facts:

1. *Validate* — before any data is touched: the principal's grants (§12.10:
   ``WRITE`` on the sink and on the datasets holding the artifact's
   transitions and its row, ``READ`` on the window query's datasets), the
   artifact layout from the declared model, the operation against
   ``write_mode``, and the sink's atomicity guarantee.  Then the artifact is
   read: it must be in the plan's ``dispatch_from`` state with the plan's
   ``based_on`` as last transition, so the transitions stay rows of δ and the
   chain stays single (PA1, PA2).
2. *Dispatch* — append the dispatch transition and set ``P(a).state`` in one
   transaction (PA4).
3. *Write* and 4. *Record* — atomic with respect to failure:

   * ``SinkTarget.DUCKDB``: the sink relation lives in the store that holds
     the artifact and its transitions, so the write, the ``succeeded``
     transition and the state update are one transaction.  On any failure the
     transaction is rolled back and ``failed`` is appended.
   * any other sink: only through a host-registered :class:`WriteIdSink`,
     whose commit records ``write_id`` where :meth:`WriteIdSink.committed`
     finds it.  A sink without one is ``UNSUPPORTED`` and nothing is written;
     there is no fallback sink, mode or backend.

Idempotency: every append uses the transition's ``idempotency_key`` as its
entity key.  Executing a plan again returns the recorded outcome when its
terminal transition exists, and otherwise retries steps 3–4 under the same
``write_id`` — a write that already committed is not repeated.
:meth:`ArtifactRuntime.recover` decides an unresolved run without writing.

A failed write raises :class:`MaterialisationFailed` with the recorded state;
a transition that could not be appended raises :class:`TransitionNotRecorded`
with the state the store holds.  Neither is ever reported as a success.
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from sqldim.core.query.dgm.annotations import WriteModeKind
from sqldim.core.query.dgm.model import DGMModel
from sqldim.core.query.dgm.nl.supervisor._auth import Action, AuthContext
from sqldim.core.query.dgm.nl.supervisor._invariant_core import col, ident
from sqldim.core.query.dgm.planner._ir_identity import RelationCatalog
from sqldim.core.query.dgm.planner._targets import SinkTarget
from sqldim.core.query.dgm.runtime._artifact_plan import (
    ArtifactConflict,
    ArtifactKey,
    ArtifactPlan,
    ArtifactSnapshot,
    ArtifactState,
    MaterialisationFailed,
    RunCancelled,
    TransitionNotRecorded,
    TransitionPlan,
    WriteOperation,
    instant,
)
from sqldim.core.query.dgm.runtime._artifact_store import (
    ArtifactLayout,
    History,
    append_transition,
    insert_artifact,
    read_history,
    resolve_layout,
)
from sqldim.core.query.dgm.runtime._errors import (
    InvalidRuntimeArgument,
    NotConfigured,
    Unauthorized,
    UnknownIdentifier,
    UnsupportedOperation,
)

__all__ = [
    "SinkWrite",
    "WriteIdSink",
    "ArtifactOutcome",
    "ArtifactRuntime",
]


class _Cancel(Protocol):
    @property
    def cancelled(self) -> bool: ...


class _Resolver(Protocol):
    def dataset_of(self, relation: str) -> str | None: ...


@dataclass(frozen=True)
class SinkWrite:
    """One write handed to a :class:`WriteIdSink`."""

    relation: str
    operation: WriteOperation
    window_column: str | None
    window_start: datetime
    window_end: datetime
    query_sql: str
    write_id: str


class WriteIdSink(Protocol):
    """A non-transactional sink usable for materialisation (D46).

    ``write`` runs ``query_sql`` on the given connection and commits its rows
    to ``relation`` atomically — a MERGE replaces the window's rows in the
    same commit — recording ``write_id`` in the commit metadata.  It returns
    the rows written.  ``committed`` answers whether a commit with that
    ``write_id`` exists; it must not guess.
    """

    def write(self, con: Any, request: SinkWrite) -> int: ...

    def committed(self, relation: str, write_id: str) -> bool: ...


@dataclass(frozen=True)
class ArtifactOutcome:
    """A run whose ``succeeded`` transition is recorded.

    ``rows_written`` is ``None`` when this call wrote nothing: the outcome was
    already recorded (``replayed``) or the write had committed before
    (``recovered``).
    """

    plan_id: str
    write_id: str
    state: ArtifactState
    rows_written: int | None
    replayed: bool = False
    recovered: bool = False


_WRITE_MODE_OPERATION: dict[WriteModeKind, WriteOperation] = {
    WriteModeKind.BACKFILL: WriteOperation.APPEND,
    WriteModeKind.REFRESH: WriteOperation.MERGE,
}
_ADAPTIVE: dict[ArtifactState, WriteOperation] = {
    ArtifactState.MISSING: WriteOperation.APPEND,
    ArtifactState.FAILED: WriteOperation.APPEND,
    ArtifactState.STALE: WriteOperation.MERGE,
}


def _operation_for(mode: WriteModeKind, state: ArtifactState) -> WriteOperation:
    """§2.4 ``write_mode``: BACKFILL → APPEND, REFRESH → MERGE, ADAPTIVE by state."""
    if mode is WriteModeKind.ADAPTIVE:
        return _ADAPTIVE[state]
    return _WRITE_MODE_OPERATION[mode]


def _dispatched(plan: ArtifactPlan, h: History) -> bool:
    """The plan's dispatch is recorded and, unresolved, is the last transition."""
    key = plan.on_dispatch.idempotency_key
    if h.get(key) is None:
        return False
    if h.last is None or h.last.key != key:
        raise ArtifactConflict("the plan's dispatch is not the last transition")
    return True


class _WindowViolation(Exception):
    """Rows of the window query lie outside the window."""


class ArtifactRuntime:
    """The PipelineArtifact executor, bound to one principal (§12.10, D44)."""

    def __init__(
        self,
        *,
        auth: AuthContext,
        model: DGMModel,
        datasets: _Resolver,
        catalog: RelationCatalog,
        connection: Any,
        clock: Callable[[], datetime],
        write_id_sinks: Mapping[SinkTarget, WriteIdSink],
    ) -> None:
        if not isinstance(auth, AuthContext):
            raise Unauthorized()
        for name, dep in (
            ("model", model),
            ("datasets", datasets),
            ("catalog", catalog),
            ("connection", connection),
            ("clock", clock),
            ("write_id_sinks", write_id_sinks),
        ):
            if dep is None:
                raise NotConfigured(f"ArtifactRuntime needs {name}")
        if not isinstance(model, DGMModel):
            raise NotConfigured("ArtifactRuntime needs the declared DGMModel")
        self.auth = auth
        self._model = model
        self._datasets = datasets
        self._catalog = catalog
        self._con = connection
        self._clock = clock
        self._sinks = dict(write_id_sinks)
        self._lock = threading.Lock()

    # -- validation (no data touched) -----------------------------------------

    def _dataset(self, relation: str) -> str:
        ds = self._datasets.dataset_of(relation)
        if not ds:
            raise UnknownIdentifier(
                f"relation {relation!r} belongs to no known dataset"
            )
        return ds

    def _require(self, relations: set[str], action: Action) -> None:
        datasets = {self._dataset(r) for r in relations}
        if not all(self.auth.allows(ds, {action}) for ds in datasets):
            raise Unauthorized()

    def _store_relations(self, lay: ArtifactLayout) -> set[str]:
        return {lay.artifact.relation, lay.transitions.relation}

    def _admit(self, plan: object) -> tuple[ArtifactPlan, ArtifactLayout]:
        if not isinstance(plan, ArtifactPlan):
            raise InvalidRuntimeArgument(f"expected an ArtifactPlan, got {plan!r}")
        lay = resolve_layout(self._model, plan.artifact_type)
        self._require(self._store_relations(lay) | {plan.write.relation}, Action.WRITE)
        reads = {d.relation for d in plan.query.deps(self._catalog)}
        self._require(reads, Action.READ)
        expected = _operation_for(lay.annotation.write_mode, plan.dispatch_from)
        if plan.write.operation is not expected:
            raise InvalidRuntimeArgument(
                f"write_mode {lay.annotation.write_mode.value} from "
                f"{plan.dispatch_from.value} writes {expected.value}, the plan "
                f"{plan.write.operation.value}"
            )
        self._mode(plan)
        return plan, lay

    def _mode(self, plan: ArtifactPlan) -> WriteIdSink | None:
        """``None`` for the transactional store, else the write-id sink."""
        if plan.write.sink is SinkTarget.DUCKDB:
            return None
        sink = self._sinks.get(plan.write.sink)
        if sink is None:
            raise UnsupportedOperation(
                f"sink {plan.write.sink.value} offers neither one transaction with "
                "the transition table nor a write_id commit record; it is not "
                "used for materialisation (D46)"
            )
        return sink

    # -- snapshot ------------------------------------------------------------

    def snapshot(
        self, artifact_type: str, artifact_key: ArtifactKey
    ) -> ArtifactSnapshot:
        """What a planner reads of one artifact (``READ`` on its datasets)."""
        lay = resolve_layout(self._model, artifact_type)
        self._require(self._store_relations(lay), Action.READ)
        with self._lock:
            h = read_history(self._con, lay, artifact_key)
        window = (h.row[1], h.row[2]) if h.row else None
        last = h.last.key if h.last else None
        return ArtifactSnapshot(
            artifact_type, artifact_key, h.state, last, h.reached_complete, window
        )

    # -- execute -------------------------------------------------------------

    def execute(
        self, plan: ArtifactPlan, *, cancel: _Cancel | None = None
    ) -> ArtifactOutcome:
        """Run *plan* by D46; see the module docstring."""
        plan, lay = self._admit(plan)
        sink = self._mode(plan)
        with self._lock:
            h = read_history(self._con, lay, plan.artifact_key)
            done = self._replay(plan, h)
            if done is not None:
                return done
            if not _dispatched(plan, h):
                self._check_current(plan, h)
                if cancel is not None and cancel.cancelled:
                    raise RunCancelled("cancelled before dispatch; nothing was written")
                h = self._dispatch(plan, lay, h)
            return self._write_and_record(plan, lay, h, sink, cancel)

    def recover(self, plan: ArtifactPlan) -> ArtifactOutcome:
        """Decide a dispatched, unresolved run without writing (D46 recovery).

        In the transactional store an unrecorded ``succeeded`` means the write
        did not commit, so ``failed`` is appended.  With a write-id sink the
        commit record decides.
        """
        plan, lay = self._admit(plan)
        sink = self._mode(plan)
        with self._lock:
            h = read_history(self._con, lay, plan.artifact_key)
            done = self._replay(plan, h)
            if done is not None:
                return done
            if not _dispatched(plan, h):
                raise ArtifactConflict(
                    "the plan was never dispatched; nothing to recover"
                )
            if sink is not None and self._committed(plan, sink):
                return self._record_success(plan, lay, h, None, recovered=True)
            raise self._record_failure(
                plan, lay, h, "recovered: the write did not commit"
            )

    # -- steps ---------------------------------------------------------------

    def _replay(self, plan: ArtifactPlan, h: History) -> ArtifactOutcome | None:
        if h.get(plan.on_success.idempotency_key) is not None:
            return ArtifactOutcome(
                plan.plan_id, plan.write_id, h.state, None, replayed=True
            )
        if h.get(plan.on_failure.idempotency_key) is not None:
            raise MaterialisationFailed(
                "the run failed earlier; plan a retry from the recorded state",
                state=h.state,
                plan_id=plan.plan_id,
                write_id=plan.write_id,
                replayed=True,
            )
        return None

    def _check_current(self, plan: ArtifactPlan, h: History) -> None:
        last = h.last.key if h.last else None
        if h.state is not plan.dispatch_from or last != plan.based_on:
            raise ArtifactConflict(
                f"the artifact is {h.state.value} after {last!r}; the plan was made "
                f"for {plan.dispatch_from.value} after {plan.based_on!r}"
            )
        failure = ArtifactState.STALE if h.reached_complete else ArtifactState.FAILED
        if plan.failure_to is not failure:
            raise ArtifactConflict(f"a failure must go to {failure.value} (δ, §2.4)")
        if h.row is not None and (h.row[1], h.row[2]) != (
            plan.window_start,
            plan.window_end,
        ):
            raise ArtifactConflict("the artifact row holds another window")

    def _now(self) -> datetime:
        return instant(self._clock(), "clock()")

    def _dispatch(self, plan: ArtifactPlan, lay: ArtifactLayout, h: History) -> History:
        try:
            self._con.begin()
            if h.row is None:
                insert_artifact(
                    self._con,
                    lay,
                    plan.artifact_key,
                    plan.window_start,
                    plan.window_end,
                )
            append_transition(self._con, lay, plan.on_dispatch, h.last, self._now())
            self._con.commit()
        except Exception as exc:
            self._con.rollback()
            if isinstance(exc, ArtifactConflict):
                raise
            raise TransitionNotRecorded(
                f"the dispatch transition was not recorded; nothing was written: {exc}",
                state=h.state,
                plan_id=plan.plan_id,
            ) from exc
        return read_history(self._con, lay, plan.artifact_key)

    def _write_and_record(
        self,
        plan: ArtifactPlan,
        lay: ArtifactLayout,
        h: History,
        sink: WriteIdSink | None,
        cancel: _Cancel | None,
    ) -> ArtifactOutcome:
        if sink is None:
            return self._transactional(plan, lay, h, cancel)
        return self._via_write_id(plan, lay, h, sink, cancel)

    def _transactional(
        self,
        plan: ArtifactPlan,
        lay: ArtifactLayout,
        h: History,
        cancel: _Cancel | None,
    ) -> ArtifactOutcome:
        try:
            self._con.begin()
            if cancel is not None and cancel.cancelled:
                raise RunCancelled("cancelled after dispatch")
            rows = self._write_duckdb(plan)
            append_transition(self._con, lay, plan.on_success, h.last, self._now())
            self._con.commit()
        except Exception as exc:
            self._con.rollback()
            raise self._record_failure(plan, lay, h, str(exc)) from exc
        return ArtifactOutcome(
            plan.plan_id, plan.write_id, ArtifactState.COMPLETE, rows
        )

    def _via_write_id(
        self,
        plan: ArtifactPlan,
        lay: ArtifactLayout,
        h: History,
        sink: WriteIdSink,
        cancel: _Cancel | None,
    ) -> ArtifactOutcome:
        if self._committed(plan, sink):
            return self._record_success(plan, lay, h, None, recovered=True)
        if cancel is not None and cancel.cancelled:
            raise self._record_failure(plan, lay, h, "cancelled after dispatch")
        try:
            self._check_window(plan)
            rows = sink.write(self._con, self._sink_write(plan))
        except Exception as exc:
            if self._committed(plan, sink):
                return self._record_success(plan, lay, h, None, recovered=True)
            raise self._record_failure(plan, lay, h, str(exc)) from exc
        return self._record_success(plan, lay, h, rows)

    def _committed(self, plan: ArtifactPlan, sink: WriteIdSink) -> bool:
        try:
            found = sink.committed(plan.write.relation, plan.write_id)
        except Exception as exc:
            raise TransitionNotRecorded(
                f"the sink's commit record could not be read; the run stays "
                f"unresolved: {exc}",
                state=ArtifactState.IN_FLIGHT,
                plan_id=plan.plan_id,
            ) from exc
        if not isinstance(found, bool):
            raise TransitionNotRecorded(
                f"the sink's commit record answered {found!r}, not a bool",
                state=ArtifactState.IN_FLIGHT,
                plan_id=plan.plan_id,
            )
        return found

    def _record_success(
        self,
        plan: ArtifactPlan,
        lay: ArtifactLayout,
        h: History,
        rows: int | None,
        *,
        recovered: bool = False,
    ) -> ArtifactOutcome:
        self._append_alone(plan, lay, h, plan.on_success, "the write committed")
        return ArtifactOutcome(
            plan.plan_id,
            plan.write_id,
            ArtifactState.COMPLETE,
            rows,
            recovered=recovered,
        )

    def _record_failure(
        self, plan: ArtifactPlan, lay: ArtifactLayout, h: History, why: str
    ) -> Exception:
        self._append_alone(plan, lay, h, plan.on_failure, f"the write failed ({why})")
        return MaterialisationFailed(
            f"the write did not commit: {why}",
            state=plan.on_failure.to_state,
            plan_id=plan.plan_id,
            write_id=plan.write_id,
        )

    def _append_alone(
        self,
        plan: ArtifactPlan,
        lay: ArtifactLayout,
        h: History,
        tp: TransitionPlan,
        context: str,
    ) -> None:
        try:
            self._con.begin()
            append_transition(self._con, lay, tp, h.last, self._now())
            self._con.commit()
        except Exception as exc:
            self._con.rollback()
            raise TransitionNotRecorded(
                f"{context}, but its {tp.kind.value} transition was not recorded; "
                f"the run is unresolved (In-flight): {exc}",
                state=ArtifactState.IN_FLIGHT,
                plan_id=plan.plan_id,
            ) from exc

    # -- the write -----------------------------------------------------------

    def _sink_write(self, plan: ArtifactPlan) -> SinkWrite:
        w = plan.write
        return SinkWrite(
            w.relation,
            w.operation,
            w.window_column,
            plan.window_start,
            plan.window_end,
            plan.query.query_text,
            plan.write_id,
        )

    def _check_window(self, plan: ArtifactPlan) -> None:
        wc = plan.write.window_column
        if wc is None:
            return
        c = col("w", wc)
        outside = self._con.execute(
            f"SELECT count(*) FROM ({plan.query.query_text}) AS w "
            f"WHERE NOT coalesce({c} >= ? AND {c} < ?, FALSE)",
            [plan.window_start, plan.window_end],
        ).fetchone()[0]
        if outside:
            raise _WindowViolation(
                f"{outside} rows of the window query lie outside "
                f"[{plan.window_start}, {plan.window_end})"
            )

    def _write_duckdb(self, plan: ArtifactPlan) -> int:
        """Step 3 inside the open transaction; the MERGE swap is delete + insert."""
        self._check_window(plan)
        w = plan.write
        sink = ident(w.relation)
        if w.operation is WriteOperation.MERGE:
            assert w.window_column is not None
            c = ident(w.window_column)
            self._con.execute(
                f"DELETE FROM {sink} WHERE {c} >= ? AND {c} < ?",
                [plan.window_start, plan.window_end],
            )
        row = self._con.execute(
            f"INSERT INTO {sink} BY NAME SELECT * FROM ({plan.query.query_text}) AS w"
        ).fetchone()
        return int(row[0]) if row else 0

"""Typed PipelineArtifact plans of §2.4 and ADR 0005: ``WritePlan`` and ``TransitionPlan``.

Planning a PipelineArtifact run produces one :class:`ArtifactPlan`: the read
query that computes the window, the :class:`WritePlan` that puts its rows in
the sink, and the three :class:`TransitionPlan` s the runtime may append —
the dispatch transition (``ON_DISPATCH``), ``succeeded`` (``ON_SUCCESS``) and
``failed`` (``ON_FAILURE``).  Building a plan has no side effect (ADR 0005).

Every derived value is computed, never supplied, so a plan cannot carry an
inconsistent key:

* ``plan_id = H(artifact type, artifact_key, window, dag_hash(query),
  query target, write plan, based_on, dispatch from_state, failure to_state)``;
  ``based_on`` is the key of the artifact's last transition when the plan was
  made, so a plan is one attempt and a new attempt has a new id;
* ``idempotency_key = H(artifact_key, from_state, kind, plan_id)`` (ADR 0005);
* ``write_id = H(plan_id, artifact_key, window_start, window_end)`` (D46).

``ArtifactState`` and ``TransitionKind`` carry the spelling of §2.4, which is
what the transition table stores.  The legacy ``PipelineStateKind`` of
``annotations`` is not used here.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum

from sqldim.core.query.dgm.nl.supervisor._invariant_pa import DELTA
from sqldim.core.query.dgm.planner._canon import canonical_hash
from sqldim.core.query.dgm.planner._ir_identity import dag_hash
from sqldim.core.query.dgm.planner._targets import ExportPlan, QueryTarget, SinkTarget
from sqldim.core.query.dgm.runtime._errors import (
    InvalidRuntimeArgument,
    RuntimeAPIError,
)

__all__ = [
    "ArtifactState",
    "TransitionKind",
    "TransitionCondition",
    "WriteOperation",
    "ArtifactKey",
    "TransitionPlan",
    "WritePlan",
    "ArtifactSnapshot",
    "ArtifactPlan",
    "plan_artifact_run",
    "transition_key",
    "ArtifactConflict",
    "MaterialisationFailed",
    "TransitionNotRecorded",
    "RunCancelled",
]


class ArtifactState(str, Enum):
    MISSING = "Missing"
    IN_FLIGHT = "In-flight"
    COMPLETE = "Complete"
    STALE = "Stale"
    FAILED = "Failed"


class TransitionKind(str, Enum):
    SCHEDULED = "scheduled"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    EXPIRED = "expired"
    RESCHEDULED = "rescheduled"
    RETRIED = "retried"


class TransitionCondition(str, Enum):
    ON_DISPATCH = "ON_DISPATCH"
    ON_SUCCESS = "ON_SUCCESS"
    ON_FAILURE = "ON_FAILURE"


class WriteOperation(str, Enum):
    """``write_mode`` of §2.4 as an operation: BACKFILL → APPEND, REFRESH → MERGE."""

    APPEND = "APPEND"
    MERGE = "MERGE"


ArtifactKey = tuple[str | int, ...]

#: The dispatch row of δ for each state a run can start from.
DISPATCH: dict[ArtifactState, TransitionKind] = {
    ArtifactState.MISSING: TransitionKind.SCHEDULED,
    ArtifactState.STALE: TransitionKind.RESCHEDULED,
    ArtifactState.FAILED: TransitionKind.RETRIED,
}

_CONDITION: dict[TransitionKind, TransitionCondition] = {
    TransitionKind.SCHEDULED: TransitionCondition.ON_DISPATCH,
    TransitionKind.RESCHEDULED: TransitionCondition.ON_DISPATCH,
    TransitionKind.RETRIED: TransitionCondition.ON_DISPATCH,
    TransitionKind.SUCCEEDED: TransitionCondition.ON_SUCCESS,
    TransitionKind.FAILED: TransitionCondition.ON_FAILURE,
}


# ---------------------------------------------------------------------------
# Errors of the artifact runtime
# ---------------------------------------------------------------------------


class ArtifactConflict(RuntimeAPIError):
    """The artifact is not in the state the plan was made for, or its stored
    history breaks PA2/PA4; nothing is written."""

    code = "CONFLICT"


class MaterialisationFailed(RuntimeAPIError):
    """The write did not commit; the ``failed`` transition is recorded.

    ``state`` is the recorded state (Failed, or Stale on a refresh).
    ``replayed`` is true when the failure was recorded by an earlier call.
    """

    code = "WRITE_FAILED"

    def __init__(
        self,
        message: str,
        *,
        state: ArtifactState,
        plan_id: str,
        write_id: str,
        replayed: bool = False,
    ) -> None:
        super().__init__(message)
        self.state = state
        self.plan_id = plan_id
        self.write_id = write_id
        self.replayed = replayed


class TransitionNotRecorded(RuntimeAPIError):
    """A transition could not be appended; ``state`` is what the store holds.

    After a dispatch the state is In-flight: the run is unresolved, never a
    success.  Executing the same plan again retries it, and
    ``ArtifactRuntime.recover`` decides it without writing (D46).
    """

    code = "NOT_RECORDED"

    def __init__(self, message: str, *, state: ArtifactState, plan_id: str) -> None:
        super().__init__(message)
        self.state = state
        self.plan_id = plan_id


class RunCancelled(RuntimeAPIError):
    """Cancelled before dispatch: nothing was written or recorded."""

    code = "CANCELLED"


# ---------------------------------------------------------------------------
# Plan values
# ---------------------------------------------------------------------------


def instant(value: object, what: str) -> datetime:
    """A timestamp as a naive UTC instant; an aware one is converted (§2.4)."""
    if not isinstance(value, datetime):
        raise InvalidRuntimeArgument(f"{what} must be a datetime, got {value!r}")
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _key(value: object) -> ArtifactKey:
    if (
        not isinstance(value, tuple)
        or not value
        or not all(isinstance(v, (str, int)) and not isinstance(v, bool) for v in value)
    ):
        raise InvalidRuntimeArgument(
            f"artifact_key must be a non-empty tuple of str/int, got {value!r}"
        )
    return value


def transition_key(
    artifact_key: ArtifactKey,
    from_state: ArtifactState,
    kind: TransitionKind,
    plan_id: str,
) -> str:
    """``idempotency_key = H(artifact_key, from_state, kind, plan id)`` (ADR 0005)."""
    return canonical_hash(
        "transition", artifact_key, from_state.value, kind.value, plan_id
    )


@dataclass(frozen=True)
class TransitionPlan:
    """One proposed transition fact (ADR 0005), a row of δ."""

    artifact_key: ArtifactKey
    from_state: ArtifactState
    kind: TransitionKind
    to_state: ArtifactState
    condition: TransitionCondition
    idempotency_key: str

    def __post_init__(self) -> None:
        triple = (self.from_state.value, self.kind.value, self.to_state.value)
        if triple not in DELTA:
            raise InvalidRuntimeArgument(f"{triple} is not a row of δ (§2.4)")
        if _CONDITION.get(self.kind) is not self.condition:
            raise InvalidRuntimeArgument(
                f"a {self.kind.value} transition is not appended {self.condition.value}"
            )


@dataclass(frozen=True)
class WritePlan:
    """Where and how the window's rows are written (§2.4 ``write_mode``).

    ``relation`` is the sink relation.  ``window_column`` names the column of
    the rows that places them in the window; MERGE needs it, because it
    replaces exactly the rows of ``[window_start, window_end)``.  When it is
    set, a row outside the window fails the write instead of being dropped.
    """

    sink: SinkTarget
    relation: str
    operation: WriteOperation
    window_column: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.sink, SinkTarget):
            raise InvalidRuntimeArgument("WritePlan.sink must be a SinkTarget")
        if not isinstance(self.relation, str) or not self.relation:
            raise InvalidRuntimeArgument("WritePlan.relation must be non-empty")
        if not isinstance(self.operation, WriteOperation):
            raise InvalidRuntimeArgument("WritePlan.operation must be a WriteOperation")
        if self.operation is WriteOperation.MERGE and not self.window_column:
            raise InvalidRuntimeArgument("a MERGE write needs its window_column")

    def identity(self) -> tuple[str, str, str, str | None]:
        return (
            self.sink.value,
            self.relation,
            self.operation.value,
            self.window_column,
        )


@dataclass(frozen=True)
class ArtifactSnapshot:
    """What the planner reads of one artifact before planning a run."""

    artifact_type: str
    artifact_key: ArtifactKey
    state: ArtifactState
    last_transition: str | None
    reached_complete: bool
    window: tuple[datetime, datetime] | None


def _check_query(query: object) -> ExportPlan:
    if not isinstance(query, ExportPlan) or query.ir is None:
        raise InvalidRuntimeArgument("the window query must be an ExportPlan with IR")
    if query.sink_target is not None or query.write_plan is not None:
        raise InvalidRuntimeArgument(
            "the window query carries its own sink or text write plan; the write "
            "is the ArtifactPlan's WritePlan"
        )
    if query.query_target is not QueryTarget.SQL_DUCKDB:
        raise InvalidRuntimeArgument(
            f"the window query is {query.query_target.value}; the artifact runtime "
            "reads with SQL_DUCKDB"
        )
    return query


@dataclass(frozen=True)
class ArtifactPlan:
    """One planned run of one artifact window (§2.4, ADR 0005, D46)."""

    artifact_type: str
    artifact_key: ArtifactKey
    window_start: datetime
    window_end: datetime
    query: ExportPlan
    write: WritePlan
    dispatch_from: ArtifactState
    failure_to: ArtifactState
    based_on: str | None

    def __post_init__(self) -> None:
        if not isinstance(self.artifact_type, str) or not self.artifact_type:
            raise InvalidRuntimeArgument("artifact_type must be non-empty")
        _key(self.artifact_key)
        start = instant(self.window_start, "window_start")
        end = instant(self.window_end, "window_end")
        if not start < end:
            raise InvalidRuntimeArgument("window_start must precede window_end")
        object.__setattr__(self, "window_start", start)
        object.__setattr__(self, "window_end", end)
        _check_query(self.query)
        if not isinstance(self.write, WritePlan):
            raise InvalidRuntimeArgument("write must be a WritePlan")
        if self.dispatch_from not in DISPATCH:
            raise InvalidRuntimeArgument(
                f"a run cannot start from {self.dispatch_from!r} (δ, §2.4)"
            )
        if self.failure_to not in (ArtifactState.FAILED, ArtifactState.STALE):
            raise InvalidRuntimeArgument("failure_to must be Failed or Stale (δ)")

    @property
    def plan_id(self) -> str:
        assert self.query.ir is not None
        return canonical_hash(
            "artifact-plan",
            self.artifact_type,
            self.artifact_key,
            self.window_start.isoformat(),
            self.window_end.isoformat(),
            dag_hash(self.query.ir),
            self.query.query_target.value,
            self.write.identity(),
            self.based_on,
            self.dispatch_from.value,
            self.failure_to.value,
        )

    @property
    def write_id(self) -> str:
        """``write_id = H(plan id, artifact_key, window_start, window_end)`` (D46)."""
        return canonical_hash(
            "write",
            self.plan_id,
            self.artifact_key,
            self.window_start.isoformat(),
            self.window_end.isoformat(),
        )

    def _transition(
        self, from_state: ArtifactState, kind: TransitionKind, to_state: ArtifactState
    ) -> TransitionPlan:
        return TransitionPlan(
            self.artifact_key,
            from_state,
            kind,
            to_state,
            _CONDITION[kind],
            transition_key(self.artifact_key, from_state, kind, self.plan_id),
        )

    @property
    def on_dispatch(self) -> TransitionPlan:
        return self._transition(
            self.dispatch_from, DISPATCH[self.dispatch_from], ArtifactState.IN_FLIGHT
        )

    @property
    def on_success(self) -> TransitionPlan:
        return self._transition(
            ArtifactState.IN_FLIGHT, TransitionKind.SUCCEEDED, ArtifactState.COMPLETE
        )

    @property
    def on_failure(self) -> TransitionPlan:
        return self._transition(
            ArtifactState.IN_FLIGHT, TransitionKind.FAILED, self.failure_to
        )


def plan_artifact_run(
    snapshot: ArtifactSnapshot,
    *,
    window_start: datetime,
    window_end: datetime,
    query: ExportPlan,
    write: WritePlan,
) -> ArtifactPlan:
    """Plan one run of the artifact read in *snapshot*; writes nothing.

    The dispatch transition is the row of δ leaving the current state, and a
    failure goes to Stale when an earlier transition reached Complete
    (refresh), to Failed otherwise (backfill).  A state that no run leaves —
    In-flight, Complete — is :class:`InvalidRuntimeArgument`.
    """
    if not isinstance(snapshot, ArtifactSnapshot):
        raise InvalidRuntimeArgument("snapshot must be an ArtifactSnapshot")
    if snapshot.state not in DISPATCH:
        raise InvalidRuntimeArgument(
            f"artifact {snapshot.artifact_key!r} is {snapshot.state.value}; no run "
            "starts from it (δ, §2.4)"
        )
    return ArtifactPlan(
        snapshot.artifact_type,
        snapshot.artifact_key,
        window_start,
        window_end,
        query,
        write,
        snapshot.state,
        ArtifactState.STALE if snapshot.reached_complete else ArtifactState.FAILED,
        snapshot.last_transition,
    )

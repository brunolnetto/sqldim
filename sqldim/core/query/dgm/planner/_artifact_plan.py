"""Typed PipelineArtifact plans of §2.4 and ADR 0005: ``WritePlan`` and ``TransitionPlan``.

Planning a PipelineArtifact run produces one :class:`ArtifactPlan`: the read
query that computes the window, the :class:`WritePlan` that puts its rows in
the sink, and the three :class:`TransitionPlan` s the runtime may append —
the dispatch transition (``ON_DISPATCH``), ``succeeded`` (``ON_SUCCESS``) and
``failed`` (``ON_FAILURE``).  Planning the expiry of a Complete artifact
produces one :class:`ExpiryPlan`: the ``expired`` transition (``ON_EXPIRY``)
and no write.  Building a plan has no side effect (ADR 0005).

Every derived value is computed, never supplied, so a plan cannot carry an
inconsistent key:

* ``plan_id = H(artifact type, artifact_key, window, dag_hash(query),
  query target, write plan, based_on, dispatch from_state, failure to_state)``;
  ``based_on`` is the key of the artifact's last transition when the plan was
  made, so a plan is one attempt and a new attempt has a new id;
* ``idempotency_key = H(artifact_key, from_state, kind, plan_id)`` (ADR 0005);
* ``write_id = H(plan_id, artifact_key, window_start, window_end)`` (D46).

These types live in the planner, which emits them (Rule 10); the runtime
imports them from here.  The planner imports nothing from ``runtime`` or
``nl``, so the dependency points one way: ``runtime`` → ``nl`` → ``planner``.
δ is defined here for the same reason; the PA evaluators read it from here.

``ArtifactState`` and ``TransitionKind`` carry the spelling of §2.4, which is
what the transition table stores.  The legacy ``PipelineStateKind`` of
``annotations`` is not used here.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum

from sqldim.core.query.dgm.planner._canon import canonical_hash
from sqldim.core.query.dgm.planner._ir_identity import dag_hash
from sqldim.core.query.dgm.planner._targets import ExportPlan, QueryTarget, SinkTarget
from sqldim.exceptions import SemanticError

__all__ = [
    "DELTA",
    "ArtifactState",
    "TransitionKind",
    "TransitionCondition",
    "WriteOperation",
    "ArtifactKey",
    "TransitionPlan",
    "WritePlan",
    "ArtifactSnapshot",
    "ArtifactPlan",
    "ExpiryPlan",
    "plan_artifact_run",
    "transition_key",
    "ArtifactPlanError",
    "InvalidArtifactPlan",
    "NotEligible",
    "UnsupportedArtifactPlan",
]

DELTA: frozenset[tuple[str, str, str]] = frozenset(
    {
        ("Missing", "scheduled", "In-flight"),
        ("In-flight", "succeeded", "Complete"),
        ("In-flight", "failed", "Failed"),
        ("In-flight", "failed", "Stale"),
        ("Complete", "expired", "Stale"),
        ("Stale", "rescheduled", "In-flight"),
        ("Failed", "retried", "In-flight"),
    }
)
"""The admissible ``(from_state, kind, to_state)`` triples δ of §2.4."""


# ---------------------------------------------------------------------------
# Errors of artifact planning
# ---------------------------------------------------------------------------


class ArtifactPlanError(SemanticError):
    """Base class; ``code`` names the failure kind."""

    code: str = "ARTIFACT_PLAN"


class InvalidArtifactPlan(ArtifactPlanError, ValueError):
    """An argument that violates the plan's contract."""

    code = "INVALID"


class NotEligible(ArtifactPlanError):
    """Rule 10 plans nothing for this artifact now, and says why.

    The artifact is In-flight, Complete within its ``ttl``, or a gap outside
    ``backfill_horizon`` or not closed yet (§2.4).  It is never planned with
    another window or mode instead.
    """

    code = "NOT_ELIGIBLE"


class UnsupportedArtifactPlan(ArtifactPlanError):
    """The plan is defined, but not for this sink and operation."""

    code = "UNSUPPORTED"


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


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
    ON_EXPIRY = "ON_EXPIRY"


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
    TransitionKind.EXPIRED: TransitionCondition.ON_EXPIRY,
}


# ---------------------------------------------------------------------------
# Plan values
# ---------------------------------------------------------------------------


def instant(value: object, what: str) -> datetime:
    """A timestamp as a naive UTC instant; an aware one is converted (§2.4)."""
    if not isinstance(value, datetime):
        raise InvalidArtifactPlan(f"{what} must be a datetime, got {value!r}")
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _key(value: object) -> ArtifactKey:
    if (
        not isinstance(value, tuple)
        or not value
        or not all(isinstance(v, (str, int)) and not isinstance(v, bool) for v in value)
    ):
        raise InvalidArtifactPlan(
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
            raise InvalidArtifactPlan(f"{triple} is not a row of δ (§2.4)")
        if _CONDITION.get(self.kind) is not self.condition:
            raise InvalidArtifactPlan(
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
            raise InvalidArtifactPlan("WritePlan.sink must be a SinkTarget")
        if not isinstance(self.relation, str) or not self.relation:
            raise InvalidArtifactPlan("WritePlan.relation must be non-empty")
        if not isinstance(self.operation, WriteOperation):
            raise InvalidArtifactPlan("WritePlan.operation must be a WriteOperation")
        if self.operation is WriteOperation.MERGE and not self.window_column:
            raise InvalidArtifactPlan("a MERGE write needs its window_column")

    def identity(self) -> tuple[str, str, str, str | None]:
        return (
            self.sink.value,
            self.relation,
            self.operation.value,
            self.window_column,
        )


@dataclass(frozen=True)
class ArtifactSnapshot:
    """What the planner reads of one artifact before planning a run.

    ``last_occurred_at`` is the ``occurred_at`` of ``last_transition``; for a
    Complete artifact it is the instant of the transition into Complete, from
    which ``ttl`` is measured.
    """

    artifact_type: str
    artifact_key: ArtifactKey
    state: ArtifactState
    last_transition: str | None
    reached_complete: bool
    window: tuple[datetime, datetime] | None
    last_occurred_at: datetime | None = None


def _check_query(query: object) -> ExportPlan:
    if not isinstance(query, ExportPlan) or query.ir is None:
        raise InvalidArtifactPlan("the window query must be an ExportPlan with IR")
    if query.sink_target is not None or query.write_plan is not None:
        raise InvalidArtifactPlan(
            "the window query carries its own sink or text write plan; the write "
            "is the ArtifactPlan's WritePlan"
        )
    if query.query_target is not QueryTarget.SQL_DUCKDB:
        raise InvalidArtifactPlan(
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
            raise InvalidArtifactPlan("artifact_type must be non-empty")
        _key(self.artifact_key)
        start = instant(self.window_start, "window_start")
        end = instant(self.window_end, "window_end")
        if not start < end:
            raise InvalidArtifactPlan("window_start must precede window_end")
        object.__setattr__(self, "window_start", start)
        object.__setattr__(self, "window_end", end)
        _check_query(self.query)
        if not isinstance(self.write, WritePlan):
            raise InvalidArtifactPlan("write must be a WritePlan")
        if self.dispatch_from not in DISPATCH:
            raise InvalidArtifactPlan(
                f"a run cannot start from {self.dispatch_from!r} (δ, §2.4)"
            )
        if self.failure_to not in (ArtifactState.FAILED, ArtifactState.STALE):
            raise InvalidArtifactPlan("failure_to must be Failed or Stale (δ)")

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


@dataclass(frozen=True)
class ExpiryPlan:
    """The ``expired`` transition of one Complete artifact (§2.4, ADR 0005).

    ``based_on`` is the key of the transition into Complete and
    ``completed_at`` its ``occurred_at``; ``ttl`` is the declared ``ttl`` in
    seconds.  The plan carries no write: the refresh is a later run from
    Stale.  The runtime appends :attr:`transition` only when
    :meth:`due` holds at its own evaluation instant (``ON_EXPIRY``).
    """

    artifact_type: str
    artifact_key: ArtifactKey
    based_on: str
    completed_at: datetime
    ttl: int

    def __post_init__(self) -> None:
        if not isinstance(self.artifact_type, str) or not self.artifact_type:
            raise InvalidArtifactPlan("artifact_type must be non-empty")
        _key(self.artifact_key)
        if not isinstance(self.based_on, str) or not self.based_on:
            raise InvalidArtifactPlan(
                "an expiry is based on the transition into Complete"
            )
        object.__setattr__(
            self, "completed_at", instant(self.completed_at, "completed_at")
        )
        if not isinstance(self.ttl, int) or isinstance(self.ttl, bool) or self.ttl < 0:
            raise InvalidArtifactPlan(f"ttl must be seconds ≥ 0, got {self.ttl!r}")

    @property
    def plan_id(self) -> str:
        return canonical_hash(
            "artifact-expiry",
            self.artifact_type,
            self.artifact_key,
            self.based_on,
            self.completed_at.isoformat(),
            self.ttl,
        )

    @property
    def transition(self) -> TransitionPlan:
        kind = TransitionKind.EXPIRED
        return TransitionPlan(
            self.artifact_key,
            ArtifactState.COMPLETE,
            kind,
            ArtifactState.STALE,
            _CONDITION[kind],
            transition_key(
                self.artifact_key, ArtifactState.COMPLETE, kind, self.plan_id
            ),
        )

    def due(self, now: datetime) -> bool:
        """``now − completed_at > ttl``: the ``ON_EXPIRY`` condition (§2.4)."""
        elapsed = instant(now, "now") - self.completed_at
        return elapsed.total_seconds() > self.ttl


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
    In-flight, Complete — is :class:`InvalidArtifactPlan`.
    """
    if not isinstance(snapshot, ArtifactSnapshot):
        raise InvalidArtifactPlan("snapshot must be an ArtifactSnapshot")
    if snapshot.state not in DISPATCH:
        raise InvalidArtifactPlan(
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

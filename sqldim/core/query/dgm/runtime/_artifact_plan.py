"""PipelineArtifact plans as the runtime sees them (§2.4, ADR 0005, D46).

The plan types are emitted by the planner (Rule 10) and defined in
:mod:`sqldim.core.query.dgm.planner._artifact_plan`; they are re-exported
here so the runtime and its callers keep one import path.  Building or
validating a plan raises the planner's :class:`InvalidArtifactPlan`.

This module adds the errors of *executing* a plan.
"""

from __future__ import annotations

from datetime import datetime

from sqldim.core.query.dgm.planner._artifact_plan import (
    DISPATCH,
    ArtifactKey,
    ArtifactPlan,
    ArtifactPlanError,
    ArtifactSnapshot,
    ArtifactState,
    ExpiryPlan,
    InvalidArtifactPlan,
    NotEligible,
    TransitionCondition,
    TransitionKind,
    TransitionPlan,
    UnsupportedArtifactPlan,
    WriteOperation,
    WritePlan,
    plan_artifact_run,
    transition_key,
)
from sqldim.core.query.dgm.planner._artifact_plan import instant as _instant
from sqldim.core.query.dgm.runtime._errors import (
    InvalidRuntimeArgument,
    RuntimeAPIError,
)

__all__ = [
    "DISPATCH",
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
    "ArtifactConflict",
    "MaterialisationFailed",
    "TransitionNotRecorded",
    "RunCancelled",
    "ExpiryNotDue",
]


def instant(value: object, what: str) -> datetime:
    """A timestamp as a naive UTC instant, or :class:`InvalidRuntimeArgument`."""
    try:
        return _instant(value, what)
    except InvalidArtifactPlan as exc:
        raise InvalidRuntimeArgument(str(exc)) from exc


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


class ExpiryNotDue(RuntimeAPIError):
    """``ON_EXPIRY`` does not hold at the runtime's instant: ``ttl`` has not
    elapsed since the transition into Complete.  Nothing is recorded."""

    code = "NOT_DUE"

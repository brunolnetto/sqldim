"""DGM Supervisor — normative contract types (§12.2, §12.11).

Every symbol here mirrors §12.2 of the specification.  The checkpointed
:class:`SupervisorState` holds only plan and result data; identity, grants,
data access and the knowledge-base handle live in :class:`ToolContext`, which
is never checkpointed and never shown to a model (§12.10).
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, model_validator

from sqldim.core.query.dgm.nl.supervisor._auth import AuthContext, DataAccess
from sqldim.core.query.dgm.planner._gate import BudgetDecisionKind, ExecutionBudget
from sqldim.core.query.dgm.planner._targets import CostEstimate

__all__ = [
    "BudgetDecisionKind",
    "CallStatus",
    "CancelToken",
    "Outcome",
    "RESULT_STATUSES",
    "SupervisorPlan",
    "SupervisorState",
    "ToolCallRequest",
    "ToolCallResult",
    "ToolCapabilitySchema",
    "ToolContext",
    "ToolError",
    "ToolErrorKind",
    "zero_cost",
]


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class CallStatus(str, Enum):
    """Status of one step (§12.11)."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    REJECTED = "REJECTED"
    SKIPPED = "SKIPPED"
    CANCELLED = "CANCELLED"


RESULT_STATUSES: frozenset[CallStatus] = frozenset(
    {
        CallStatus.SUCCEEDED,
        CallStatus.FAILED,
        CallStatus.REJECTED,
        CallStatus.SKIPPED,
        CallStatus.CANCELLED,
    }
)
"""Terminal statuses a :class:`ToolCallResult` may carry."""


class ToolErrorKind(str, Enum):
    """Typed failure reasons (§12.2)."""

    UNKNOWN_TOOL = "UNKNOWN_TOOL"
    INVALID_ARGUMENTS = "INVALID_ARGUMENTS"
    UNAUTHORIZED = "UNAUTHORIZED"
    NOT_IMPLEMENTED = "NOT_IMPLEMENTED"
    UNSUPPORTED = "UNSUPPORTED"
    BUDGET_REJECTED = "BUDGET_REJECTED"
    NEEDS_CLARIFICATION = "NEEDS_CLARIFICATION"
    TIMEOUT = "TIMEOUT"
    CANCELLED = "CANCELLED"
    TOOL_FAILURE = "TOOL_FAILURE"
    INVALID_OUTPUT = "INVALID_OUTPUT"
    DEPENDENCY_NOT_MET = "DEPENDENCY_NOT_MET"
    COMPOSITION_FAILURE = "COMPOSITION_FAILURE"


_MAY_HAVE_NO_ATTEMPT = frozenset(
    {CallStatus.SKIPPED, CallStatus.CANCELLED, CallStatus.REJECTED}
)
"""SKIPPED never runs; CANCELLED may be a step cancelled before dispatch;
REJECTED may be a step whose reservation never fits (§12.4, ADR 0008)."""

_RETRYABLE_KINDS = frozenset({ToolErrorKind.TIMEOUT, ToolErrorKind.TOOL_FAILURE})


class Outcome(str, Enum):
    """Terminal outcome of a supervisor run (§12.11)."""

    COMPLETE = "COMPLETE"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"
    ABORTED = "ABORTED"
    CANCELLED = "CANCELLED"


def zero_cost() -> CostEstimate:
    return CostEstimate(cpu_ops=0, io_ops=0)


# ---------------------------------------------------------------------------
# Capability schema
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ToolCapabilitySchema:
    """What a tool declares at startup (§12.2)."""

    name: str
    version: str
    description: str
    input_schema: dict[str, Any]
    output_schema: dict[str, Any]
    reads: tuple[str, ...]
    writes_kb: bool
    idempotent: bool
    composable_with: tuple[str, ...]
    available: bool


# ---------------------------------------------------------------------------
# Request, error and result
# ---------------------------------------------------------------------------


class ToolCallRequest(BaseModel):
    """One step of a plan (§12.2).

    ``datasets`` and ``idempotency_key`` are derived by the supervisor
    (:func:`~._validation.bind_plan`); values supplied by a planner are
    discarded.  ``None`` means "not yet derived", and the dispatcher refuses
    such a request.
    """

    call_id: str = Field(min_length=1)
    tool_name: str = Field(min_length=1)
    arguments: dict[str, Any] = Field(default_factory=dict)
    depends_on: list[str] = Field(default_factory=list)
    budget_fraction: float = Field(ge=0.0, le=1.0)
    datasets: list[str] | None = None
    idempotency_key: str | None = None

    model_config = {"frozen": True}


class ToolError(BaseModel):
    """Structured failure detail (§12.2)."""

    kind: ToolErrorKind
    message: str
    retryable: bool = False
    context: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _retryable_only_for_transient_kinds(self) -> "ToolError":
        if self.retryable and self.kind not in _RETRYABLE_KINDS:
            raise ValueError(
                f"{self.kind.value} is never retryable; only TIMEOUT and "
                "transient TOOL_FAILURE are"
            )
        return self

    model_config = {"frozen": True}


class ToolCallResult(BaseModel):
    """The typed end of one call (§12.2).

    Invariants (fail closed, D42): ``data`` is present iff ``status`` is
    SUCCEEDED; ``error`` is present iff it is not; ``attempts`` is ≥ 1 unless
    the call was SKIPPED, or cancelled or budget-rejected before dispatch.  ``demo`` marks output of the explicit demo mode
    (D47): such a result is never a contract SUCCEEDED.
    """

    call_id: str
    tool_name: str
    status: CallStatus
    data: Any = None
    decision: BudgetDecisionKind | None = None
    error: ToolError | None = None
    datasets: list[str] = Field(default_factory=list)
    attempts: int = 0
    cost_actual: CostEstimate = Field(default_factory=zero_cost)
    started_at: datetime | None = None
    finished_at: datetime | None = None
    demo: bool = False

    @model_validator(mode="after")
    def _fail_closed_invariants(self) -> "ToolCallResult":
        for problem in (_payload_problem(self), _attempts_problem(self)):
            if problem is not None:
                raise ValueError(problem)
        return self

    @property
    def is_contract_success(self) -> bool:
        """SUCCEEDED and not produced by demo mode (D47)."""
        return self.status is CallStatus.SUCCEEDED and not self.demo

    model_config = {"arbitrary_types_allowed": True, "frozen": True}

    @classmethod
    def failure(
        cls,
        request: ToolCallRequest,
        status: CallStatus,
        kind: ToolErrorKind,
        message: str,
        *,
        retryable: bool = False,
        context: dict[str, Any] | None = None,
        attempts: int = 1,
        decision: BudgetDecisionKind | None = None,
    ) -> "ToolCallResult":
        return cls(
            call_id=request.call_id,
            tool_name=request.tool_name,
            status=status,
            error=ToolError(
                kind=kind,
                message=message,
                retryable=retryable,
                context=dict(context or {}),
            ),
            datasets=list(request.datasets or []),
            attempts=0 if status is CallStatus.SKIPPED else attempts,
            decision=decision,
        )


def _payload_problem(r: ToolCallResult) -> str | None:
    """data iff SUCCEEDED, error iff not (D42)."""
    if r.status not in RESULT_STATUSES:
        return f"{r.status.value} is not a terminal result status"
    if r.status is CallStatus.SUCCEEDED:
        if r.data is None:
            return "a SUCCEEDED result must carry data"
        if r.error is not None:
            return "a SUCCEEDED result must not carry an error"
        return None
    if r.data is not None:
        return "only a SUCCEEDED result may carry data"
    if r.error is None:
        return f"a {r.status.value} result must carry an error"
    return None


def _attempts_problem(r: ToolCallResult) -> str | None:
    if r.status is CallStatus.SKIPPED and r.attempts != 0:
        return "a SKIPPED result has no attempts"
    if r.status not in _MAY_HAVE_NO_ATTEMPT and r.attempts < 1:
        return "an executed result has at least one attempt"
    return None


# ---------------------------------------------------------------------------
# Plan
# ---------------------------------------------------------------------------


class SupervisorPlan(BaseModel):
    """A versioned decomposition of the goal (§12.2).

    Construction does not validate P1–P6: an invalid plan must be
    representable so that the violation can be reported and replanned
    (§12.4).  Use :func:`~._validation.validate_plan`.
    """

    plan_id: str = Field(min_length=1)
    version: int = Field(ge=1)
    goal: str
    steps: list[ToolCallRequest] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning: str = ""

    @property
    def total_budget_fraction(self) -> float:
        return sum(s.budget_fraction for s in self.steps)

    def step(self, call_id: str) -> ToolCallRequest:
        for s in self.steps:
            if s.call_id == call_id:
                return s
        raise KeyError(call_id)

    model_config = {"frozen": True}


# ---------------------------------------------------------------------------
# Runtime context (never checkpointed, never shown to a model)
# ---------------------------------------------------------------------------


class CancelToken:
    """Cooperative cancellation flag shared by the run and its calls."""

    def __init__(self) -> None:
        self._event = threading.Event()

    def cancel(self) -> None:
        self._event.set()

    @property
    def cancelled(self) -> bool:
        return self._event.is_set()


@dataclass(frozen=True)
class ToolContext:
    """What a tool receives besides its request (§12.2).

    ``kb`` is ``None`` when the deployment has no knowledge base; the KB tools
    are then registered unavailable.  ``demo`` is true only when the caller
    selected demo mode for this run (D47).
    """

    auth: AuthContext
    budget: ExecutionBudget
    deadline: datetime
    cancel: CancelToken
    data: DataAccess
    kb: Any = None
    demo: bool = False


# ---------------------------------------------------------------------------
# LangGraph state (checkpointed)
# ---------------------------------------------------------------------------


class SupervisorState(BaseModel):
    """Checkpointed state of one supervisor run (§12.4, §12.11).

    Holds no identity, grant or credential: those stay in the runtime
    context (§12.10).
    """

    utterance: str
    plan: SupervisorPlan | None = None
    executed_plan: SupervisorPlan | None = None
    plan_violations: list[ToolError] = Field(default_factory=list)
    statuses: dict[str, CallStatus] = Field(default_factory=dict)
    results: dict[str, ToolCallResult] = Field(default_factory=dict)
    attempts: dict[str, int] = Field(default_factory=dict)
    replans: int = 0
    spent: int = 0
    route: str | None = None
    rejection: ToolCallResult | None = None
    abort_diagnostic: dict[str, Any] | None = None
    planning_error: ToolError | None = None
    outcome: Outcome | None = None
    coverage: float | None = None
    missing: list[dict[str, Any]] = Field(default_factory=list)
    synthesis: str | None = None
    demo: bool = False
    visited_nodes: list[str] = Field(default_factory=list)
    node_timings: dict[str, float] = Field(default_factory=dict)
    conversation_history: list[dict[str, str]] = Field(default_factory=list)

    model_config = {"arbitrary_types_allowed": True}

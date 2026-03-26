"""DGM pre-execution gate — ExecutionBudget, BudgetDecision, ExecutionGate (§6.3).

Sits between the planner and SQL emitter. Compares plan cost estimates against
a declared ExecutionBudget and returns a BudgetDecision that controls how (or
whether) the query is executed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqldim.core.query.dgm.planner._targets import (
        CostEstimate,
        ExportPlan,
        SinkTarget,
    )

__all__ = [
    "HARD_CEILING_FACTOR",
    "BudgetDecisionKind",
    "SampleMethod",
    "BudgetDecision",
    "ProceedDecision",
    "StreamDecision",
    "AsyncDecision",
    "RewriteDecision",
    "SampleDecision",
    "PaginateDecision",
    "ClarifyDecision",
    "RejectDecision",
    "ExecutionBudget",
    "ExecutionGate",
]

#: Multiplier applied to ``max_estimated_cost`` to get the hard ceiling.
HARD_CEILING_FACTOR: float = 2.0

#: Default page size used by PAGINATE when no override is given.
_DEFAULT_PAGE_SIZE: int = 1_000


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class BudgetDecisionKind(Enum):
    """The eight possible outcomes of the pre-execution gate (§6.3)."""

    PROCEED = "PROCEED"
    STREAM = "STREAM"
    ASYNC = "ASYNC"
    REWRITE = "REWRITE"
    SAMPLE = "SAMPLE"
    PAGINATE = "PAGINATE"
    CLARIFY = "CLARIFY"
    REJECT = "REJECT"


class SampleMethod(Enum):
    """Sampling strategy for :class:`SampleDecision` (§6.3 Strategy 3)."""

    RANDOM = "RANDOM"
    STRATIFIED = "STRATIFIED"
    RESERVOIR = "RESERVOIR"


# ---------------------------------------------------------------------------
# BudgetDecision base + variants
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BudgetDecision:
    """Abstract base for all pre-execution gate decisions."""

    kind: BudgetDecisionKind


@dataclass(frozen=True)
class ProceedDecision(BudgetDecision):
    """Within budget — execute synchronously."""

    kind: BudgetDecisionKind = field(default=BudgetDecisionKind.PROCEED, init=False)


@dataclass(frozen=True)
class StreamDecision(BudgetDecision):
    """Above streaming threshold — deliver results progressively."""

    kind: BudgetDecisionKind = field(default=BudgetDecisionKind.STREAM, init=False)


@dataclass(frozen=True)
class AsyncDecision(BudgetDecision):
    """Above async threshold — execute async with callback and materialisation."""

    kind: BudgetDecisionKind = field(default=BudgetDecisionKind.ASYNC, init=False)
    sink: "SinkTarget"
    callback: str
    ttl: timedelta


@dataclass(frozen=True)
class RewriteDecision(BudgetDecision):
    """Cost-reduced rewrite with transparency note."""

    kind: BudgetDecisionKind = field(default=BudgetDecisionKind.REWRITE, init=False)
    plan: "ExportPlan"
    warning: str


@dataclass(frozen=True)
class SampleDecision(BudgetDecision):
    """Approximate result with Hoeffding confidence interval."""

    kind: BudgetDecisionKind = field(default=BudgetDecisionKind.SAMPLE, init=False)
    fraction: float
    method: SampleMethod
    error_bound: float


@dataclass(frozen=True)
class PaginateDecision(BudgetDecision):
    """Rewrite to paginate result set."""

    kind: BudgetDecisionKind = field(default=BudgetDecisionKind.PAGINATE, init=False)
    page_size: int


@dataclass(frozen=True)
class ClarifyDecision(BudgetDecision):
    """Ask user to narrow; surface lattice refinements."""

    kind: BudgetDecisionKind = field(default=BudgetDecisionKind.CLARIFY, init=False)
    options: list["ExportPlan"]
    cost_per_option: list["CostEstimate"]


@dataclass(frozen=True)
class RejectDecision(BudgetDecision):
    """Hard ceiling exceeded — refuse execution."""

    kind: BudgetDecisionKind = field(default=BudgetDecisionKind.REJECT, init=False)
    reason: str


# ---------------------------------------------------------------------------
# ExecutionBudget
# ---------------------------------------------------------------------------


@dataclass(eq=True)
class ExecutionBudget:
    """Declared execution constraints for the pre-execution gate (§6.3).

    Parameters
    ----------
    max_estimated_cost:
        Hard ceiling (multiplied by HARD_CEILING_FACTOR inside the gate).
    max_result_rows:
        Row count ceiling; triggers PAGINATE when exceeded.
    max_wall_time:
        Synchronous execution timeout.
    max_precompute_time:
        Per-PreComputation timeout.
    streaming_threshold:
        Above this → STREAM.
    async_threshold:
        Above this (and below hard ceiling) → ASYNC.
    """

    max_estimated_cost: "CostEstimate"
    max_result_rows: int
    max_wall_time: timedelta
    max_precompute_time: timedelta
    streaming_threshold: "CostEstimate"
    async_threshold: "CostEstimate"


# ---------------------------------------------------------------------------
# ExecutionGate
# ---------------------------------------------------------------------------


class ExecutionGate:
    """Pre-execution gate implementing the §6.3 decision procedure.

    The gate is stateless — ``gate()`` is a pure function over its arguments.
    """

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def gate(self, plan: "ExportPlan", budget: ExecutionBudget) -> BudgetDecision:
        """Apply the §6.3 decision procedure to *plan* against *budget*."""
        if plan.cost_estimate is None:
            return ProceedDecision()
        return self._decide(plan, budget)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _should_paginate(self, plan: "ExportPlan", budget: ExecutionBudget) -> bool:
        """Return ``True`` when estimated row count exceeds the budget limit."""
        estimated_rows = getattr(plan, "estimated_rows", 0)
        return estimated_rows > budget.max_result_rows

    def _decide_by_cost(
        self, est: "CostEstimate", plan: "ExportPlan", budget: ExecutionBudget
    ) -> BudgetDecision | None:
        """Check cost thresholds and return a decision, or ``None`` to continue."""
        hard_ceiling = self._scale_cost(budget.max_estimated_cost, HARD_CEILING_FACTOR)
        if self._cost_exceeds(est, hard_ceiling):
            return RejectDecision(reason=self._reject_reason(plan))
        if self._cost_exceeds(est, budget.async_threshold):
            return self._async_decision()
        if self._cost_exceeds(est, budget.streaming_threshold):
            return StreamDecision()
        return None

    def _decide(self, plan: "ExportPlan", budget: ExecutionBudget) -> BudgetDecision:
        est = plan.cost_estimate
        assert est is not None  # gate() guard ensures this
        cost_decision = self._decide_by_cost(est, plan, budget)
        if cost_decision is not None:
            return cost_decision
        if self._should_paginate(plan, budget):
            return PaginateDecision(page_size=_DEFAULT_PAGE_SIZE)
        return ProceedDecision()

    def _async_decision(self) -> AsyncDecision:
        from sqldim.core.query.dgm.planner._targets import SinkTarget

        return AsyncDecision(
            sink=SinkTarget.PARQUET,
            callback="",
            ttl=timedelta(hours=24),
        )

    def _reject_reason(self, plan: "ExportPlan") -> str:
        est = plan.cost_estimate
        return (
            f"Query exceeds hard cost ceiling "
            f"(cpu_ops={est.cpu_ops if est else 'n/a'}). "
            "Narrow the query scope to reduce cost."
        )

    # ------------------------------------------------------------------
    # Cost arithmetic helpers
    # ------------------------------------------------------------------

    def _cost_total(self, cost: "CostEstimate") -> int:
        return cost.cpu_ops + cost.io_ops

    def _cost_exceeds(self, cost: "CostEstimate", ceiling: "CostEstimate") -> bool:
        return self._cost_total(cost) > self._cost_total(ceiling)

    def _scale_cost(self, cost: "CostEstimate", factor: float) -> "CostEstimate":
        from sqldim.core.query.dgm.planner._targets import CostEstimate

        return CostEstimate(
            cpu_ops=int(cost.cpu_ops * factor),
            io_ops=int(cost.io_ops * factor),
        )

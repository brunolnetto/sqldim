"""Plan steps and costs: PreComputation, ApproximateVariant, CostEstimate (§6.2, §6.3).

A leaf module: it imports no other planner module at load time, so the plan
IR and its identity can depend on it without an import cycle.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

__all__ = ["PreComputation", "ApproximateVariant", "CostEstimate"]


# ---------------------------------------------------------------------------
# PreComputation
# ---------------------------------------------------------------------------


def _check_duration(value: timedelta | None, what: str) -> None:
    if value is None:
        return
    if not isinstance(value, timedelta) or value < timedelta(0):
        raise ValueError(f"{what} must be a non-negative timedelta, got {value!r}")


def _check_reads(value: "frozenset[str] | None") -> None:
    if value is None:
        return
    if not isinstance(value, frozenset) or not all(isinstance(r, str) for r in value):
        raise ValueError(f"reads must be a frozenset of relation names, got {value!r}")
    from sqldim.core.query.dgm.planner._ir import check_relation_name

    for r in value:
        check_relation_name(r)


@dataclass(frozen=True)
class ApproximateVariant:
    """The declared approximate variant of a PreComputation (§6.3 Step 1b).

    Used only under ``Substitution.APPROX_ALGORITHM`` or when the caller picks
    it from a ``CLARIFY`` option.  Every field is required: an approximation
    whose time, cost or accuracy is not stated is not an approximation the
    gate can offer.

    Parameters
    ----------
    name, query, kind:
        The replacement PreComputation step.
    estimated_time:
        Estimated duration of the approximate step.
    cost_estimate:
        Cost of the approximate step, in the units of the plan's cost.
    accuracy:
        The accuracy statement of the approximate algorithm (§6.3, e.g.
        ``"Monte Carlo approximation (k=200 samples)"``); it becomes the
        warning of the REWRITE or CLARIFY option.
    reads:
        Relations the approximate step reads, for ``deps(plan)`` (§8.14);
        ``None`` when undeclared, and then the approximated plan has no
        ``deps``.  It is never copied from the exact step.
    """

    name: str
    query: str
    estimated_time: timedelta
    cost_estimate: "CostEstimate"
    accuracy: str
    kind: str = "sql"
    reads: frozenset[str] | None = None

    def __post_init__(self) -> None:
        if self.estimated_time is None:
            raise ValueError("an approximate variant needs an estimated_time")
        _check_duration(self.estimated_time, "estimated_time")
        _check_reads(self.reads)
        if not isinstance(self.accuracy, str) or not self.accuracy.strip():
            raise ValueError("an approximate variant needs an accuracy statement")


@dataclass(frozen=True)
class PreComputation:
    """A named pre-computation step emitted before the main query.

    A step is part of the plan IR (``PlanIR.pre_compute``): its ``name``,
    ``kind``, ``query``, ``reads`` and ``accuracy`` enter the plan's canonical
    identity, so an exact plan and its approximation never share a key.  Its
    estimates and declared approximation do not: they describe the cost of
    the question, not the question.

    Parameters
    ----------
    name:
        Identifier for the pre-computation (e.g. ``"gt_bfs"``).
    query:
        SQL or Python snippet that materialises the result.
    kind:
        ``"sql"`` (default) or ``"py"`` (Python pre-computation).
    estimated_time:
        Estimated duration; ``None`` when unknown.  The gate (§6.3 Step 1b)
        never starts a PreComputation whose time is unknown.
    cost_estimate:
        Cost of this step, as included in the plan's cost; needed to price
        the plan with the step replaced by its approximation.
    approximation:
        The declared approximate variant, if the algorithm has one.
    reads:
        Relations the step reads, for ``deps(plan)`` (§8.14); ``None`` when
        undeclared, and then the plan has no ``deps``.
    accuracy:
        ``None`` for an exact step; for a step that replaced an exact one by
        its approximate variant, the variant's accuracy statement.
    """

    name: str
    query: str
    kind: str = "sql"
    estimated_time: timedelta | None = None
    cost_estimate: "CostEstimate | None" = None
    approximation: ApproximateVariant | None = None
    reads: frozenset[str] | None = None
    accuracy: str | None = None

    def __post_init__(self) -> None:
        _check_duration(self.estimated_time, "estimated_time")
        _check_reads(self.reads)


# ---------------------------------------------------------------------------
# CostEstimate
# ---------------------------------------------------------------------------


@dataclass(eq=True)
class CostEstimate:
    """Mechanical cost estimate for an ExportPlan.

    Parameters
    ----------
    cpu_ops:
        Estimated CPU operations (dimensionless).
    io_ops:
        Estimated I/O operations (dimensionless).
    note:
        Optional auditable note (no natural language).
    """

    cpu_ops: int
    io_ops: int
    note: str = ""

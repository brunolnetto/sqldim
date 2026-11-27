"""A :class:`PrecomputeProvider` that reads the planner's declarations (§6.3 Step 1b).

:class:`DeclaredPrecomputeProvider` answers the gate from what each
:class:`PreComputation` declares — its ``estimated_time``, ``cost_estimate``
and ``approximation`` — and invents nothing: an undeclared time is unknown
(the gate rejects), and an approximation exists only when every slow step
declares one and the plan's cost can be recomputed from declared costs.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from datetime import timedelta

from sqldim.core.query.dgm.planner._gate import Approximation
from sqldim.core.query.dgm.planner._targets import (
    CostEstimate,
    ExportPlan,
    PreComputation,
)

__all__ = ["DeclaredPrecomputeProvider"]


def _approximated(pc: PreComputation) -> PreComputation:
    v = pc.approximation
    assert v is not None
    return PreComputation(
        name=v.name,
        query=v.query,
        kind=v.kind,
        estimated_time=v.estimated_time,
        cost_estimate=v.cost_estimate,
    )


def _repriced(cost: CostEstimate, slow: Sequence[PreComputation]) -> CostEstimate:
    cpu, io = cost.cpu_ops, cost.io_ops
    for pc in slow:
        exact, approx = pc.cost_estimate, pc.approximation
        assert exact is not None and approx is not None
        cpu += approx.cost_estimate.cpu_ops - exact.cpu_ops
        io += approx.cost_estimate.io_ops - exact.io_ops
    if cpu < 0 or io < 0:
        raise ValueError(
            "declared PreComputation costs exceed the plan's cost estimate; "
            "the approximated plan cannot be priced"
        )
    notes = [n for n in (cost.note, "APPROX_ALGORITHM") if n]
    return CostEstimate(cpu_ops=cpu, io_ops=io, note="; ".join(notes))


def _declared(slow: Sequence[PreComputation]) -> bool:
    return bool(slow) and all(
        pc.approximation is not None and pc.cost_estimate is not None for pc in slow
    )


def _replace_steps(
    steps: Sequence[PreComputation], slow: Sequence[PreComputation]
) -> list[PreComputation]:
    slow_ids = {id(pc) for pc in slow}
    if len(slow_ids) != sum(1 for pc in steps if id(pc) in slow_ids):
        raise ValueError("slow PreComputations must be steps of the plan")
    return [_approximated(pc) if id(pc) in slow_ids else pc for pc in steps]


class DeclaredPrecomputeProvider:
    """Step 1b provider over the declarations carried by each PreComputation."""

    def estimated_time(self, pc: PreComputation) -> timedelta | None:
        return pc.estimated_time

    def approximate(
        self, plan: ExportPlan, slow: Sequence[PreComputation]
    ) -> Approximation | None:
        """*plan* with every step in *slow* replaced by its declared variant.

        ``None`` when a slow step has no approximation or no declared cost,
        or the plan has no cost estimate: the gate then rejects the plan.
        """
        if plan.cost_estimate is None or not _declared(slow):
            return None
        steps = _replace_steps(plan.pre_compute, slow)
        approx_plan = replace(
            plan,
            pre_compute=steps,
            cost_estimate=_repriced(plan.cost_estimate, slow),
            alternatives=[],
        )
        warning = "; ".join(
            f"{pc.name}: {pc.approximation.accuracy}"  # type: ignore[union-attr]
            for pc in slow
        )
        return Approximation(plan=approx_plan, warning=warning)

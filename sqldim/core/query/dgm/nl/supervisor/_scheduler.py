"""DGM Supervisor — bounded concurrent dispatch (§12.4, §12.11, ADR 0008).

The scheduler runs the steps of one validated plan version until a *stable
point*: every step has a terminal status, or a run-level event (replan,
abort, cancellation) has happened and every call that was RUNNING has
finished.  The graph is checkpointed only at stable points, so no checkpoint
holds a RUNNING call.

Rules (ADR 0008):

* *Limit.*  At most ``policy.max_concurrent_calls`` calls are RUNNING; free
  slots are filled with runnable steps in plan order.  ``1`` is strictly
  sequential.
* *Writes.*  Two steps whose tool writes to the knowledge base never run at
  the same time.
* *Budget reservation.*  A step is dispatched only if
  ``spent + reserved + its child budget ≤ B.max_estimated_cost``.  When
  nothing is running and no runnable step fits, waiting could never end: the
  first such step is REJECTED with ``BUDGET_REJECTED`` before dispatch and
  routed like any rejection (§12.4).
* *Routing.*  The §12.4 table is applied to each result as it arrives.  A
  retry re-dispatches only the failed call, with the same idempotency key.
  A replan closes the barrier: nothing new is dispatched and the RUNNING
  calls are drained.  Abort and cancellation set the cancel token of every
  RUNNING call, mark every PENDING step CANCELLED and wait for the running
  calls to acknowledge; a result that arrives after the signal is recorded
  CANCELLED.
"""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from sqldim.core.query.dgm.nl.supervisor._dispatch import child_budget, execute_tool
from sqldim.core.query.dgm.nl.supervisor._schemas import SupervisorPolicy
from sqldim.core.query.dgm.nl.supervisor._types import (
    CallStatus,
    CancelToken,
    Outcome,
    SupervisorPlan,
    ToolCallRequest,
    ToolCallResult,
    ToolContext,
    ToolErrorKind,
)
from sqldim.core.query.dgm.planner._gate import BudgetDecisionKind

if TYPE_CHECKING:
    from sqldim.core.query.dgm.nl.supervisor._tools import BaseTool

__all__ = ["RunEvent", "Scheduler", "cost_total"]

_POLL_SECONDS = 0.02
"""How often the run's cancel token is checked while calls are running."""


def cost_total(cost: Any) -> int:
    return int(cost.cpu_ops) + int(cost.io_ops)


@dataclass
class RunEvent:
    """Why the scheduler stopped at a stable point."""

    route: str = "continue"  # continue | replan | abort
    outcome: Outcome | None = None
    rejection: ToolCallResult | None = None
    diagnostic: dict[str, Any] | None = None


@dataclass
class _Running:
    call_id: str
    reservation: int
    token: CancelToken
    writes_kb: bool


@dataclass
class Scheduler:
    """Dispatch one plan version; see the module docstring for the rules."""

    plan: SupervisorPlan
    registry: Mapping[str, "BaseTool"]
    ctx: ToolContext
    policy: SupervisorPolicy
    statuses: dict[str, CallStatus]
    results: dict[str, ToolCallResult]
    attempts: dict[str, int]
    spent: int
    replans: int
    run_cancel: CancelToken
    event: RunEvent = field(default_factory=RunEvent, init=False)
    _running: dict["asyncio.Task[ToolCallResult]", _Running] = field(
        default_factory=dict, init=False
    )
    _retries: list[str] = field(default_factory=list, init=False)
    _barrier: bool = field(default=False, init=False)
    _stopping: bool = field(default=False, init=False)
    _rejected: list[ToolCallResult] = field(default_factory=list, init=False)

    # -- budget ------------------------------------------------------------

    @property
    def ceiling(self) -> int:
        return cost_total(self.ctx.budget.max_estimated_cost)

    @property
    def reserved(self) -> int:
        return sum(r.reservation for r in self._running.values())

    def _reservation(self, step: ToolCallRequest) -> int:
        child = child_budget(self.ctx.budget, step.budget_fraction, self.ctx.deadline)
        return cost_total(child.max_estimated_cost)

    def _fits(self, step: ToolCallRequest) -> bool:
        return self.spent + self.reserved + self._reservation(step) <= self.ceiling

    # -- selection ---------------------------------------------------------

    def _writes_kb(self, step: ToolCallRequest) -> bool:
        tool = self.registry.get(step.tool_name)
        return bool(tool and tool.capability_schema.writes_kb)

    def _kb_busy(self) -> bool:
        return any(r.writes_kb for r in self._running.values())

    def _deps_succeeded(self, step: ToolCallRequest) -> bool:
        return all(self.statuses[d] is CallStatus.SUCCEEDED for d in step.depends_on)

    def _skip_unreachable(self) -> None:
        """PENDING steps with a dependency that ended otherwise become SKIPPED."""
        alive = (CallStatus.SUCCEEDED, CallStatus.PENDING, CallStatus.RUNNING)
        changed = True
        while changed:
            changed = False
            for step in self.plan.steps:
                if self.statuses[step.call_id] is not CallStatus.PENDING:
                    continue
                if all(self.statuses[d] in alive for d in step.depends_on):
                    continue
                self._record(
                    ToolCallResult.failure(
                        step,
                        CallStatus.SKIPPED,
                        ToolErrorKind.DEPENDENCY_NOT_MET,
                        "a dependency did not succeed",
                        context={
                            "depends_on": {
                                d: self.statuses[d].value for d in step.depends_on
                            }
                        },
                    )
                )
                changed = True

    def _runnable(self) -> list[ToolCallRequest]:
        return [
            s
            for s in self.plan.steps
            if self.statuses[s.call_id] is CallStatus.PENDING
            and self._deps_succeeded(s)
        ]

    def _can_start(self, step: ToolCallRequest) -> bool:
        if len(self._running) >= self.policy.max_concurrent_calls:
            return False
        if self._writes_kb(step) and self._kb_busy():
            return False
        return self._fits(step)

    def _fill(self) -> None:
        for call_id in list(self._retries):
            step = self.plan.step(call_id)
            if self._can_start(step):
                self._retries.remove(call_id)
                self._dispatch(step)
        if self._barrier or self._stopping:
            return
        for step in self._runnable():
            if self._can_start(step):
                self._dispatch(step)

    # -- dispatch and results ----------------------------------------------

    def _dispatch(self, step: ToolCallRequest) -> None:
        attempt = self.attempts.get(step.call_id, 0) + 1
        self.attempts[step.call_id] = attempt
        self.statuses[step.call_id] = CallStatus.RUNNING
        token = CancelToken()
        call_ctx = dataclasses.replace(self.ctx, cancel=token)
        task = asyncio.ensure_future(
            execute_tool(step, self.registry, call_ctx, attempt=attempt)
        )
        self._running[task] = _Running(
            step.call_id, self._reservation(step), token, self._writes_kb(step)
        )

    def _record(self, result: ToolCallResult) -> None:
        self.results[result.call_id] = result
        self.statuses[result.call_id] = result.status

    def _plan_index(self, call_id: str) -> int:
        return [s.call_id for s in self.plan.steps].index(call_id)

    def _finish(self, task: "asyncio.Task[ToolCallResult]") -> None:
        slot = self._running.pop(task)
        result = task.result()
        self.spent += cost_total(result.cost_actual)
        if result.error is not None and result.error.kind is ToolErrorKind.TIMEOUT:
            slot.token.cancel()  # §12.11: on expiry the call's token is set
        if self._stopping:
            result = self._acknowledged(result)
        self._record(result)
        if not self._stopping:
            self._route(result)

    def _acknowledged(self, result: ToolCallResult) -> ToolCallResult:
        """A call that ends after abort/cancellation was signalled."""
        if result.status is CallStatus.CANCELLED:
            return result
        step = self.plan.step(result.call_id)
        return ToolCallResult.failure(
            step,
            CallStatus.CANCELLED,
            ToolErrorKind.CANCELLED,
            "the run stopped while this call was running",
            context={"acknowledged_status": result.status.value},
            attempts=result.attempts,
        ).model_copy(update={"cost_actual": result.cost_actual})

    # -- routing (§12.4) ---------------------------------------------------

    def _retryable(self, result: ToolCallResult) -> bool:
        tool = self.registry.get(result.tool_name)
        return (
            result.status is CallStatus.FAILED
            and result.error is not None
            and result.error.retryable
            and bool(tool and tool.capability_schema.idempotent)
            and result.attempts < self.policy.max_attempts
        )

    def _route(self, result: ToolCallResult) -> None:
        if result.status is CallStatus.SUCCEEDED:
            return
        if self._retryable(result):
            self.statuses[result.call_id] = CallStatus.RUNNING
            self._retries.append(result.call_id)
            return
        if result.status is CallStatus.REJECTED:
            self._rejected.append(result)
            if self.replans < self.policy.max_replans:
                self._barrier = True
            else:
                self._stop(result, Outcome.ABORTED)
            return
        if result.status is CallStatus.CANCELLED or self.run_cancel.cancelled:
            self._stop(result, Outcome.CANCELLED)

    def _stop(self, cause: ToolCallResult | None, outcome: Outcome) -> None:
        """Abort or cancel: signal running calls, cancel pending steps."""
        self._stopping = True
        self.event = RunEvent(
            route="abort", outcome=outcome, diagnostic=self._diagnostic(cause)
        )
        for slot in self._running.values():
            slot.token.cancel()
        self._drop_retries()
        for step in self.plan.steps:
            if self.statuses[step.call_id] is CallStatus.PENDING:
                self._record(
                    ToolCallResult.failure(
                        step,
                        CallStatus.CANCELLED,
                        ToolErrorKind.CANCELLED,
                        "the run stopped before this step started",
                        attempts=0,
                    )
                )

    def _drop_retries(self) -> None:
        """Retries that will not run: the failure they retry stands."""
        for call_id in self._retries:
            self.statuses[call_id] = self.results[call_id].status
        self._retries.clear()

    def _diagnostic(self, cause: ToolCallResult | None) -> dict[str, Any]:
        if cause is None:
            return {"reason": "the run was cancelled"}
        step = self.plan.step(cause.call_id)
        return {
            "call_id": step.call_id,
            "tool_name": step.tool_name,
            "arguments": step.arguments,
            "budget_fraction": step.budget_fraction,
            "child_budget": self._reservation(step),
            "decision": cause.decision.value if cause.decision else None,
            "error": cause.error.model_dump(mode="json") if cause.error else None,
        }

    def _reject_unfundable(self) -> bool:
        """Nothing runs and no runnable step fits: reject the first (§12.4)."""
        runnable = self._runnable()
        if not runnable:
            return False
        step = runnable[0]
        result = ToolCallResult.failure(
            step,
            CallStatus.REJECTED,
            ToolErrorKind.BUDGET_REJECTED,
            "the step's child budget does not fit in what remains of the run's budget",
            context={
                "spent": self.spent,
                "reservation": self._reservation(step),
                "ceiling": self.ceiling,
            },
            attempts=0,
            decision=BudgetDecisionKind.REJECT,
        )
        self._record(result)
        self._route(result)
        return True

    # -- main loop ---------------------------------------------------------

    async def _wait(self) -> None:
        done, _ = await asyncio.wait(
            set(self._running),
            timeout=_POLL_SECONDS,
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in sorted(
            done, key=lambda t: self._plan_index(self._running[t].call_id)
        ):
            self._finish(task)

    def _idle_step(self) -> bool:
        """Nothing is running; return ``True`` to keep going."""
        if self._retries:
            # A retry whose reservation cannot fit now never will.
            self._drop_retries()
            return True
        if self._stopping or self._barrier:
            return False
        return self._reject_unfundable()

    async def run(self) -> RunEvent:
        while True:
            if self.run_cancel.cancelled and not self._stopping:
                self._stop(None, Outcome.CANCELLED)
            if not self._stopping:
                self._skip_unreachable()
            self._fill()
            if self._running:
                await self._wait()
            elif not self._idle_step():
                break
        if self._barrier and not self._stopping:
            rejection = min(self._rejected, key=lambda r: self._plan_index(r.call_id))
            self.event = RunEvent(route="replan", rejection=rejection)
        return self.event

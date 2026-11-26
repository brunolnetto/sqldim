"""ADR 0008 / §12.4: bounded concurrent dispatch, driven deterministically.

Every call of a :class:`GatedTool` blocks until the test opens its gate, so
the order of starts and finishes is fixed by the test, not by timing.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

import pytest

from sqldim.core.query.dgm.nl.supervisor._auth import Action
from sqldim.core.query.dgm.nl.supervisor._scheduler import RunEvent, Scheduler
from sqldim.core.query.dgm.nl.supervisor._schemas import SupervisorPolicy
from sqldim.core.query.dgm.nl.supervisor._tools import BaseTool, _capability
from sqldim.core.query.dgm.nl.supervisor._types import (
    CallStatus,
    CancelToken,
    Outcome,
    SupervisorPlan,
    ToolCallRequest,
    ToolCallResult,
    ToolCapabilitySchema,
    ToolContext,
    ToolErrorKind,
)
from sqldim.core.query.dgm.nl.supervisor._validation import validate_plan
from sqldim.core.query.dgm.planner._gate import BudgetDecisionKind
from sqldim.core.query.dgm.planner._targets import CostEstimate
from tests.supervisor._fakes import (
    FakeTool,
    auth,
    fail,
    plan,
    query_turn,
    registry,
    step,
    succeed,
    tool_context,
)

_GRANTS = {"sales": {Action.READ, Action.WRITE_KB}}
_FINDING = {"finding": {"question_key": "k", "datasets": ["sales"], "content": "x"}}


class Log:
    """What the gated tools observed."""

    def __init__(self) -> None:
        self.started: list[str] = []
        self.finished: list[str] = []
        self.running: set[str] = set()
        self.max_running = 0
        self.max_writes = 0
        self.writing: set[str] = set()


class GatedTool(BaseTool):
    """Blocks each call until its gate opens (or its cancel token is set)."""

    def __init__(
        self,
        name: str,
        log: Log,
        gates: dict[str, asyncio.Event],
        *,
        outcomes: dict[str, Callable[..., Any]] | None = None,
        cost: dict[str, int] | None = None,
        honour_cancel: bool = True,
    ) -> None:
        self._schema: ToolCapabilitySchema = _capability(
            name, available=True, version="test"
        )
        self.log, self.gates = log, gates
        self.outcomes = outcomes or {}
        self.cost = cost or {}
        self.honour_cancel = honour_cancel
        self.calls: list[ToolCallRequest] = []

    @property
    def capability_schema(self) -> ToolCapabilitySchema:
        return self._schema

    def cost_estimate(self, arguments: dict[str, Any], ctx: ToolContext) -> Any:
        return CostEstimate(cpu_ops=1, io_ops=0)

    def _enter(self, cid: str) -> None:
        log = self.log
        log.started.append(cid)
        log.running.add(cid)
        log.max_running = max(log.max_running, len(log.running))
        if self._schema.writes_kb:
            log.writing.add(cid)
            log.max_writes = max(log.max_writes, len(log.writing))

    def _leave(self, cid: str) -> None:
        self.log.running.discard(cid)
        self.log.writing.discard(cid)
        self.log.finished.append(cid)

    async def execute(
        self, request: ToolCallRequest, ctx: ToolContext
    ) -> ToolCallResult:
        cid = request.call_id
        self.calls.append(request)
        self._enter(cid)
        try:
            gate = self.gates.setdefault(cid, asyncio.Event())
            while not gate.is_set():
                if self.honour_cancel and ctx.cancel.cancelled:
                    return ToolCallResult.failure(
                        request,
                        CallStatus.CANCELLED,
                        ToolErrorKind.CANCELLED,
                        "acknowledged",
                    )
                await asyncio.sleep(0.001)
            gate.clear()  # a retry waits for the gate again
            attempt = sum(1 for c in self.calls if c.call_id == cid)
            default = succeed(self._data())
            result = self.outcomes.get(cid, default)(request, ctx, attempt)
            if result.status is CallStatus.SUCCEEDED and cid in self.cost:
                result = result.model_copy(
                    update={
                        "cost_actual": CostEstimate(cpu_ops=self.cost[cid], io_ops=0)
                    }
                )
            return result
        finally:
            self._leave(cid)

    def _data(self) -> dict[str, Any]:
        if self._schema.name == "dgm_store_finding":
            return {"finding_id": "f"}
        if self._schema.name == "dgm_explore_from":
            return {"suggestions": []}
        return query_turn()


class Harness:
    def __init__(self, *tools: BaseTool, grants: Any = None) -> None:
        self.auth = auth(grants or _GRANTS)
        self.registry = registry(*tools)
        self.cancel = CancelToken()

    def scheduler(
        self,
        the_plan: SupervisorPlan,
        *,
        limit: int = 4,
        spent: int = 0,
        replans: int = 0,
    ) -> Scheduler:
        check = validate_plan(the_plan, self.registry, self.auth)
        assert not check.violations, check.violations
        return Scheduler(
            plan=check.plan,
            registry=self.registry,
            ctx=tool_context(self.auth),
            policy=SupervisorPolicy(max_concurrent_calls=limit),
            statuses={s.call_id: CallStatus.PENDING for s in check.plan.steps},
            results={},
            attempts={},
            spent=spent,
            replans=replans,
            run_cancel=self.cancel,
        )


async def until(predicate: Callable[[], bool], what: str) -> None:
    for _ in range(4000):
        if predicate():
            return
        await asyncio.sleep(0.001)
    raise AssertionError(f"timed out waiting for {what}")


def q(cid: str, fraction: float = 0.1, **kw: Any) -> ToolCallRequest:
    return step(cid, fraction=fraction, **kw)


def explore(cid: str, source: str, fraction: float = 0.1) -> ToolCallRequest:
    """A step that consumes *source* (P4: dgm_query → dgm_explore_from)."""
    return step(
        cid,
        "dgm_explore_from",
        {"source": source, "limit": 5},
        depends_on=[source],
        fraction=fraction,
    )


def run(coro: Any) -> Any:
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# Limit and order
# ---------------------------------------------------------------------------


class TestLimit:
    @pytest.mark.parametrize("limit", [1, 2, 4])
    def test_never_more_than_the_limit(self, limit: int) -> None:
        log, gates = Log(), {}
        h = Harness(GatedTool("dgm_query", log, gates))
        ids = [f"q{i}" for i in range(6)]
        sched = h.scheduler(plan(*(q(c) for c in ids)), limit=limit)

        async def drive() -> RunEvent:
            task = asyncio.ensure_future(sched.run())
            for i, cid in enumerate(ids):
                expected = min(limit, len(ids) - i)
                await until(lambda: len(log.running) == expected, f"{expected} running")
                await asyncio.sleep(0.03)  # give the scheduler a chance to overfill
                assert len(log.running) == expected and cid in log.running
                gates[cid].set()
                await until(lambda: cid in log.finished, f"{cid} done")
            return await task

        event = run(drive())
        assert event.route == "continue"
        assert log.max_running == limit
        assert log.started == ids  # free slots filled in plan order
        assert all(sched.statuses[c] is CallStatus.SUCCEEDED for c in ids)

    def test_limit_one_is_strictly_sequential(self) -> None:
        log, gates = Log(), {}
        h = Harness(GatedTool("dgm_query", log, gates))
        sched = h.scheduler(plan(q("a"), q("b"), q("c")), limit=1)

        async def drive() -> None:
            task = asyncio.ensure_future(sched.run())
            for cid in "abc":
                await until(lambda: cid in log.started, cid)
                assert log.started[-1] == cid and log.running == {cid}
                gates[cid].set()
            await task

        run(drive())
        assert log.finished == ["a", "b", "c"]

    def test_dependent_waits_for_its_dependency(self) -> None:
        log, gates = Log(), {}
        h = Harness(
            GatedTool("dgm_query", log, gates),
            GatedTool("dgm_explore_from", log, gates),
        )
        sched = h.scheduler(plan(q("a"), explore("b", "a"), q("c")))

        async def drive() -> None:
            task = asyncio.ensure_future(sched.run())
            await until(lambda: set(log.started) == {"a", "c"}, "a and c")
            assert "b" not in log.started
            gates["a"].set()
            await until(lambda: "b" in log.started, "b")
            gates["b"].set()
            gates["c"].set()
            await task

        run(drive())
        assert log.started.index("b") > log.finished.index("a")


class TestKnowledgeBaseWrites:
    def test_two_kb_writes_never_overlap(self) -> None:
        log, gates = Log(), {}
        h = Harness(
            GatedTool("dgm_store_finding", log, gates),
            GatedTool("dgm_query", log, gates),
        )
        the_plan = plan(
            q("w1", tool="dgm_store_finding", arguments=_FINDING),
            q("w2", tool="dgm_store_finding", arguments=_FINDING),
            q("r1"),
        )
        sched = h.scheduler(the_plan)

        async def drive() -> None:
            task = asyncio.ensure_future(sched.run())
            await until(lambda: set(log.started) == {"w1", "r1"}, "w1 and r1")
            assert "w2" not in log.started  # a free slot, but the KB is busy
            gates["w1"].set()
            await until(lambda: "w2" in log.started, "w2")
            gates["w2"].set()
            gates["r1"].set()
            await task

        run(drive())
        assert log.max_writes == 1
        assert all(s is CallStatus.SUCCEEDED for s in sched.statuses.values())


# ---------------------------------------------------------------------------
# Budget reservation
# ---------------------------------------------------------------------------


class TestBudgetReservation:
    def test_step_waits_until_its_reservation_fits(self) -> None:
        # ceiling 1000; spent 300; a reserves 500 → b (300) must wait.
        log, gates = Log(), {}
        h = Harness(GatedTool("dgm_query", log, gates, cost={"a": 100}))
        sched = h.scheduler(plan(q("a", 0.5), q("b", 0.3)), spent=300)

        async def drive() -> None:
            task = asyncio.ensure_future(sched.run())
            await until(lambda: "a" in log.started, "a")
            await asyncio.sleep(0.05)
            assert log.started == ["a"]
            gates["a"].set()
            await until(lambda: "b" in log.started, "b")
            gates["b"].set()
            await task

        run(drive())
        assert log.max_running == 1
        assert sched.spent == 400
        assert sched.statuses["b"] is CallStatus.SUCCEEDED

    def test_no_overcommit_under_concurrency(self) -> None:
        log, gates = Log(), {}
        h = Harness(GatedTool("dgm_query", log, gates))
        sched = h.scheduler(plan(q("a", 0.4), q("b", 0.4), q("c", 0.2)), spent=200)

        async def drive() -> None:
            task = asyncio.ensure_future(sched.run())
            await until(lambda: set(log.started) == {"a", "b"}, "a and b")
            await asyncio.sleep(0.05)
            assert "c" not in log.started  # 200 + 800 + 200 > 1000
            gates["a"].set()
            await until(lambda: "c" in log.started, "c")
            gates["b"].set()
            gates["c"].set()
            await task

        run(drive())

    def test_unfundable_step_is_rejected_before_dispatch(self) -> None:
        log, gates = Log(), {}
        h = Harness(
            GatedTool("dgm_query", log, gates, cost={"a": 900}),
            GatedTool("dgm_explore_from", log, gates),
        )
        sched = h.scheduler(plan(q("a", 0.5), explore("b", "a", 0.5)))

        async def drive() -> RunEvent:
            task = asyncio.ensure_future(sched.run())
            await until(lambda: "a" in log.started, "a")
            gates["a"].set()
            return await task

        event = run(drive())
        assert log.started == ["a"]  # b never reached the tool
        b = sched.results["b"]
        assert b.status is CallStatus.REJECTED
        assert b.error is not None and b.error.kind is ToolErrorKind.BUDGET_REJECTED
        assert b.decision is BudgetDecisionKind.REJECT
        assert b.attempts == 0
        assert event.route == "replan" and event.rejection == b

    def test_unfundable_step_aborts_when_replans_are_exhausted(self) -> None:
        tool = FakeTool("dgm_query")
        h = Harness(tool)
        sched = h.scheduler(plan(q("a", 0.5)), spent=600, replans=2)
        event = run(sched.run())
        assert tool.calls == []
        assert event.route == "abort" and event.outcome is Outcome.ABORTED
        assert event.diagnostic is not None and event.diagnostic["call_id"] == "a"


# ---------------------------------------------------------------------------
# Recovery barrier, retry, abort and cancellation
# ---------------------------------------------------------------------------


def _reject(request: ToolCallRequest, ctx: ToolContext, attempt: int) -> Any:
    return ToolCallResult.failure(
        request,
        CallStatus.REJECTED,
        ToolErrorKind.BUDGET_REJECTED,
        "too expensive",
        decision=BudgetDecisionKind.REJECT,
        attempts=attempt,
    )


class TestReplanBarrier:
    def test_barrier_stops_dispatch_and_drains_running_calls(self) -> None:
        log, gates = Log(), {}
        h = Harness(GatedTool("dgm_query", log, gates, outcomes={"a": _reject}))
        sched = h.scheduler(plan(q("a"), q("b"), q("c")), limit=2)

        async def drive() -> RunEvent:
            task = asyncio.ensure_future(sched.run())
            await until(lambda: set(log.started) == {"a", "b"}, "a and b")
            gates["a"].set()
            await until(lambda: "a" in log.finished, "a rejected")
            await asyncio.sleep(0.05)
            assert "c" not in log.started  # a slot is free but the barrier is up
            assert not task.done()  # b is still running
            gates["b"].set()
            return await task

        event = run(drive())
        assert event.route == "replan"
        assert event.rejection is not None and event.rejection.call_id == "a"
        assert sched.statuses == {
            "a": CallStatus.REJECTED,
            "b": CallStatus.SUCCEEDED,
            "c": CallStatus.PENDING,
        }


class TestRetry:
    def test_retry_redispatches_only_the_failed_call(self) -> None:
        log, gates = Log(), {}
        flaky = fail(ToolErrorKind.TIMEOUT, retryable=True)
        outcomes = {
            "a": lambda r, c, n: (flaky if n == 1 else succeed(query_turn()))(r, c, n)
        }
        tool = GatedTool("dgm_query", log, gates, outcomes=outcomes)
        h = Harness(tool)
        sched = h.scheduler(plan(q("a"), q("b")))

        async def drive() -> None:
            task = asyncio.ensure_future(sched.run())
            await until(lambda: set(log.started) == {"a", "b"}, "a and b")
            gates["b"].set()
            gates["a"].set()
            await until(lambda: log.started.count("a") == 2, "a retried")
            gates["a"].set()
            await task

        run(drive())
        assert [c.call_id for c in tool.calls].count("b") == 1
        keys = {c.idempotency_key for c in tool.calls if c.call_id == "a"}
        assert len(keys) == 1
        assert sched.attempts == {"a": 2, "b": 1}
        assert sched.statuses["a"] is CallStatus.SUCCEEDED


class TestAbortAndCancel:
    def test_abort_signals_running_calls_and_waits_for_acknowledgement(self) -> None:
        log, gates = Log(), {}
        h = Harness(GatedTool("dgm_query", log, gates, outcomes={"a": _reject}))
        sched = h.scheduler(plan(q("a"), q("b"), q("c")), limit=2, replans=2)

        async def drive() -> RunEvent:
            task = asyncio.ensure_future(sched.run())
            await until(lambda: set(log.started) == {"a", "b"}, "a and b")
            gates["a"].set()  # b is never released: it must acknowledge
            return await task

        event = run(drive())
        assert event.route == "abort" and event.outcome is Outcome.ABORTED
        assert event.diagnostic is not None and event.diagnostic["call_id"] == "a"
        assert "b" in log.finished  # acknowledged, not abandoned
        assert sched.statuses == {
            "a": CallStatus.REJECTED,
            "b": CallStatus.CANCELLED,
            "c": CallStatus.CANCELLED,
        }
        assert sched.results["c"].attempts == 0
        assert "c" not in log.started

    def test_result_arriving_after_the_signal_is_cancelled(self) -> None:
        log, gates = Log(), {}
        tool = GatedTool(
            "dgm_query", log, gates, outcomes={"a": _reject}, honour_cancel=False
        )
        h = Harness(tool)
        sched = h.scheduler(plan(q("a"), q("b")), replans=2)

        async def drive() -> RunEvent:
            task = asyncio.ensure_future(sched.run())
            await until(lambda: set(log.started) == {"a", "b"}, "a and b")
            gates["a"].set()
            await until(lambda: "a" in log.finished, "a")
            await asyncio.sleep(0.05)
            assert not task.done()  # waits for b to acknowledge
            gates["b"].set()
            return await task

        run(drive())
        b = sched.results["b"]
        assert b.status is CallStatus.CANCELLED and b.data is None
        assert b.error is not None
        assert b.error.context["acknowledged_status"] == "SUCCEEDED"

    def test_run_cancellation_cancels_running_and_pending(self) -> None:
        log, gates = Log(), {}
        h = Harness(GatedTool("dgm_query", log, gates))
        sched = h.scheduler(plan(q("a"), q("b"), q("c")), limit=2)

        async def drive() -> RunEvent:
            task = asyncio.ensure_future(sched.run())
            await until(lambda: set(log.started) == {"a", "b"}, "a and b")
            gates["a"].set()
            await until(lambda: "a" in log.finished, "a")
            await until(lambda: "c" in log.started, "c")
            h.cancel.cancel()
            return await task

        event = run(drive())
        assert event.outcome is Outcome.CANCELLED
        assert sched.statuses == {
            "a": CallStatus.SUCCEEDED,  # finished before the cancellation
            "b": CallStatus.CANCELLED,
            "c": CallStatus.CANCELLED,
        }

    def test_failed_dependency_skips_dependents_without_running_them(self) -> None:
        tool = FakeTool("dgm_query", fail(ToolErrorKind.TOOL_FAILURE))
        downstream = FakeTool("dgm_explore_from", succeed({"suggestions": []}))
        writer = FakeTool("dgm_store_finding", succeed({"finding_id": "f"}))
        h = Harness(tool, downstream, writer)
        store = step("c", "dgm_store_finding", _FINDING, depends_on=["b"], fraction=0.1)
        sched = h.scheduler(plan(q("a"), explore("b", "a"), store))
        event = run(sched.run())
        assert event.route == "continue"
        assert [c.call_id for c in tool.calls] == ["a"]
        assert downstream.calls == [] and writer.calls == []
        assert sched.statuses["b"] is CallStatus.SKIPPED
        assert sched.statuses["c"] is CallStatus.SKIPPED
        assert sched.results["c"].error is not None
        assert sched.results["c"].error.kind is ToolErrorKind.DEPENDENCY_NOT_MET


# ---------------------------------------------------------------------------
# Through the graph
# ---------------------------------------------------------------------------


class _CostingTool(FakeTool):
    """A FakeTool whose results carry a fixed ``cost_actual``."""

    def __init__(self, name: str, *behaviours: Any, cost: int, **kw: Any) -> None:
        super().__init__(name, *behaviours, **kw)
        self._cost = cost

    async def execute(
        self, request: ToolCallRequest, ctx: ToolContext
    ) -> ToolCallResult:
        result = await super().execute(request, ctx)
        return result.model_copy(
            update={"cost_actual": CostEstimate(cpu_ops=self._cost, io_ops=0)}
        )


def _from(view: Any, *steps: ToolCallRequest) -> SupervisorPlan:
    return plan(*steps, plan_id=view.plan_id, version=view.version)


class TestGraph:
    @pytest.mark.parametrize("slow", ["a", "b"])
    def test_synthesis_is_in_plan_order_whatever_finishes_first(
        self, slow: str
    ) -> None:
        from sqldim.core.query.dgm.nl.supervisor._graph import run_supervisor
        from tests.supervisor._fakes import ScriptedPlanner, runtime

        class Timed(FakeTool):
            async def execute(
                self, request: ToolCallRequest, ctx: ToolContext
            ) -> ToolCallResult:
                if request.call_id == slow:
                    await asyncio.sleep(0.1)
                rows = [{"call": request.call_id}]
                self.calls.append(request)
                return succeed(query_turn(rows))(request, ctx, 1)

        planner = ScriptedPlanner(lambda v: _from(v, q("a"), q("b")))
        state = run_supervisor("q", runtime(registry(Timed("dgm_query")), planner))
        assert state.outcome is Outcome.COMPLETE
        text = state.synthesis or ""
        assert text.index("[a]") < text.index("[b]")

    def test_spent_accumulates_across_replans(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._graph import run_supervisor
        from tests.supervisor._fakes import ScriptedPlanner, runtime

        tool = _CostingTool(
            "dgm_query",
            fail(ToolErrorKind.BUDGET_REJECTED, status=CallStatus.REJECTED),
            succeed(query_turn()),
            cost=300,
        )
        smaller = q("a2", 0.5, arguments={"dataset": "sales", "utterance": "less"})
        planner = ScriptedPlanner(
            lambda v: _from(v, q("a", 0.5)), lambda v: _from(v, smaller)
        )
        state = run_supervisor("q", runtime(registry(tool), planner))
        assert state.outcome is Outcome.COMPLETE
        # The rejected call's cost is not forgotten when the replan drops it.
        assert planner.views[1].remaining_fraction == pytest.approx(0.7)
        assert state.spent == 600

    def test_policy_limit_reaches_the_scheduler(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._graph import run_supervisor
        from tests.supervisor._fakes import ScriptedPlanner, runtime

        running: list[int] = []
        live: set[str] = set()

        class Probe(FakeTool):
            async def execute(
                self, request: ToolCallRequest, ctx: ToolContext
            ) -> ToolCallResult:
                live.add(request.call_id)
                running.append(len(live))
                await asyncio.sleep(0.02)
                live.discard(request.call_id)
                return succeed(query_turn())(request, ctx, 1)

        planner = ScriptedPlanner(lambda v: _from(v, q("a"), q("b"), q("c")))
        rt = runtime(
            registry(Probe("dgm_query")),
            planner,
            policy=SupervisorPolicy(max_concurrent_calls=1),
        )
        state = run_supervisor("q", rt)
        assert state.outcome is Outcome.COMPLETE
        assert max(running) == 1

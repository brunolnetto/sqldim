"""DGM Supervisor — the §12.4 graph, sequential dispatch.

::

    plan → validate_plan ──invalid──→ replan ─→ validate_plan
                │ valid
                ▼
           select_step ──none runnable──→ synthesize → respond → END
                │
                ▼
           execute_tool → route_result ─┬─ continue ─→ select_step
                                        ├─ retry ────→ execute_tool
                                        ├─ replan ───→ replan
                                        └─ abort ────→ respond → END

Steps run one at a time in plan order (``MAX_CONCURRENT_CALLS = 1``); the
concurrent scheduler of ADR 0008 builds on this graph.

Identity, grants, data access and the registry live in
:class:`SupervisorRuntime`, which is closed over by the nodes and never
written to the checkpointed :class:`SupervisorState` (§12.10).
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import time
import uuid
from collections.abc import Callable, Coroutine, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, TypeVar

from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, StateGraph

from sqldim.core.query.dgm.nl.supervisor._auth import (
    AuthContext,
    DataAccess,
    DatasetOpener,
)
from sqldim.core.query.dgm.nl.supervisor._dispatch import execute_tool
from sqldim.core.query.dgm.nl.supervisor._planner import (
    KeywordDemoPlanner,
    LLMPlanner,
    Planner,
    PlannerView,
)
from sqldim.core.query.dgm.nl.supervisor._schemas import (
    DEFAULT_POLICY,
    SupervisorPolicy,
)
from sqldim.core.query.dgm.nl.supervisor._tools import BaseTool, build_registry
from sqldim.core.query.dgm.nl.supervisor._types import (
    CallStatus,
    CancelToken,
    Outcome,
    SupervisorState,
    ToolCallRequest,
    ToolCallResult,
    ToolContext,
    ToolError,
    ToolErrorKind,
)
from sqldim.core.query.dgm.nl.supervisor._validation import (
    check_replan,
    validate_plan,
)
from sqldim.core.query.dgm.planner._gate import ExecutionBudget

SUPERVISOR_NODE_NAMES: list[str] = [
    "plan",
    "validate_plan",
    "replan",
    "select_step",
    "execute_tool",
    "route_result",
    "synthesize",
    "respond",
]

__all__ = [
    "SUPERVISOR_NODE_NAMES",
    "SupervisorRuntime",
    "build_supervisor_graph",
    "make_runtime",
    "run_supervisor",
]

_T = TypeVar("_T")


# ---------------------------------------------------------------------------
# Runtime context
# ---------------------------------------------------------------------------


@dataclass
class SupervisorRuntime:
    """Per-run dependencies; never checkpointed, never shown to a model."""

    auth: AuthContext
    budget: ExecutionBudget
    registry: Mapping[str, BaseTool]
    planner: Planner | None
    data: DataAccess
    deadline: datetime
    cancel: CancelToken = field(default_factory=CancelToken)
    kb: Any = None
    demo: bool = False
    policy: SupervisorPolicy = DEFAULT_POLICY

    def __post_init__(self) -> None:
        if self.planner is not None and self.planner.demo and not self.demo:
            raise ValueError("the keyword planner runs only in demo mode (D47)")
        demo_tools = [
            name
            for name, tool in self.registry.items()
            if tool.capability_schema.version.endswith("+demo")
        ]
        if demo_tools and not self.demo:
            raise ValueError(f"demo tools {demo_tools} run only in demo mode (D47)")

    def tool_context(self) -> ToolContext:
        return ToolContext(
            auth=self.auth,
            budget=self.budget,
            deadline=self.deadline,
            cancel=self.cancel,
            data=self.data,
            kb=self.kb,
            demo=self.demo,
        )

    def available_tools(self) -> tuple[Any, ...]:
        return tuple(
            t.capability_schema
            for _, t in sorted(self.registry.items())
            if t.capability_schema.available
        )


def make_runtime(
    auth: AuthContext,
    opener: DatasetOpener,
    budget: ExecutionBudget,
    *,
    model: Any = None,
    demo: bool = False,
    timeout: timedelta = timedelta(minutes=5),
    policy: SupervisorPolicy = DEFAULT_POLICY,
) -> SupervisorRuntime:
    """Assemble a runtime.  ``demo`` must be the caller's explicit choice."""
    # Demo mode always plans with the keyword heuristic; the model, if any,
    # then only drives the demo dgm_query.  Outside demo mode a model is the
    # only planner, and without one the run fails closed.
    planner: Planner | None
    if demo:
        planner = KeywordDemoPlanner()
    elif model is not None:
        planner = LLMPlanner(model)
    else:
        planner = None
    return SupervisorRuntime(
        auth=auth,
        budget=budget,
        registry=build_registry(demo=demo, model=model),
        planner=planner,
        data=DataAccess(auth, opener),
        deadline=datetime.now(timezone.utc) + timeout,
        demo=demo,
        policy=policy,
    )


def _run_sync(coro: Coroutine[Any, Any, _T]) -> _T:
    """Run *coro* to completion from synchronous graph code."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(asyncio.run, coro).result()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _error(kind: ToolErrorKind, message: str, **context: Any) -> ToolError:
    return ToolError(kind=kind, message=message, context=context)


def _cost_total(cost: Any) -> int:
    return int(cost.cpu_ops) + int(cost.io_ops)


def _remaining_fraction(state: SupervisorState, runtime: SupervisorRuntime) -> float:
    """``remaining / B.max_estimated_cost`` with nothing reserved (§12.5)."""
    ceiling = _cost_total(runtime.budget.max_estimated_cost)
    if ceiling <= 0:
        return 0.0
    spent = sum(_cost_total(r.cost_actual) for r in state.results.values())
    return max(0.0, 1.0 - spent / ceiling)


def _view(
    state: SupervisorState,
    runtime: SupervisorRuntime,
    *,
    plan_id: str,
    version: int,
) -> PlannerView:
    return PlannerView(
        goal=state.utterance,
        plan_id=plan_id,
        version=version,
        datasets=tuple(runtime.auth.readable_datasets()),
        tools=runtime.available_tools(),
        history=tuple(state.conversation_history),
        previous=state.plan if version > 1 else None,
        results=tuple(state.results.values()),
        violations=tuple(state.plan_violations),
        rejection=state.rejection,
        remaining_fraction=_remaining_fraction(state, runtime),
    )


def _propose(
    state: SupervisorState,
    runtime: SupervisorRuntime,
    *,
    plan_id: str,
    version: int,
) -> dict[str, Any]:
    """Ask the planner; a planner failure ends the run FAILED, never re-routed."""
    assert runtime.planner is not None
    try:
        proposed = runtime.planner.propose(
            _view(state, runtime, plan_id=plan_id, version=version)
        )
    except Exception as exc:  # noqa: BLE001 — reported, never replaced
        return {
            "planning_error": _error(
                ToolErrorKind.TOOL_FAILURE,
                "the planner failed",
                exception=type(exc).__name__,
                detail=str(exc),
            ),
            "outcome": Outcome.FAILED,
        }
    plan = proposed.model_copy(update={"plan_id": plan_id, "version": version})
    return {"plan": plan, "planning_error": None}


# ---------------------------------------------------------------------------
# Nodes
# ---------------------------------------------------------------------------


def _plan_node(
    runtime: SupervisorRuntime,
) -> Callable[[SupervisorState], dict[str, Any]]:
    def plan(state: SupervisorState) -> dict[str, Any]:
        base: dict[str, Any] = {"demo": runtime.demo}
        if runtime.planner is None:
            return {
                **base,
                "planning_error": _error(
                    ToolErrorKind.NOT_IMPLEMENTED,
                    "no planner is configured: pass a model, or select demo mode",
                ),
                "outcome": Outcome.FAILED,
            }
        if not runtime.available_tools():
            return {
                **base,
                "planning_error": _error(
                    ToolErrorKind.NOT_IMPLEMENTED,
                    "no supervisor tool is available in this deployment",
                    missing={
                        n: getattr(t, "missing", None)
                        for n, t in sorted(runtime.registry.items())
                    },
                ),
                "outcome": Outcome.FAILED,
            }
        return {**base, **_propose(state, runtime, plan_id=uuid.uuid4().hex, version=1)}

    return plan


def _validate_node(
    runtime: SupervisorRuntime,
) -> Callable[[SupervisorState], dict[str, Any]]:
    def validate(state: SupervisorState) -> dict[str, Any]:
        assert state.plan is not None
        check = validate_plan(state.plan, runtime.registry, runtime.auth)
        violations = list(check.violations)
        if state.executed_plan is not None:
            violations += check_replan(
                state.plan,
                state.executed_plan,
                state.results,
                state.rejection,
                _remaining_fraction(state, runtime),
            )
        if violations:
            return {"plan": check.plan, "plan_violations": violations}
        statuses = {
            s.call_id: (
                CallStatus.SUCCEEDED
                if s.call_id in state.results
                and state.results[s.call_id].status is CallStatus.SUCCEEDED
                else CallStatus.PENDING
            )
            for s in check.plan.steps
        }
        kept = {
            cid: r
            for cid, r in state.results.items()
            if cid in statuses and r.status is CallStatus.SUCCEEDED
        }
        return {
            "plan": check.plan,
            "executed_plan": check.plan,
            "plan_violations": [],
            "statuses": statuses,
            "results": kept,
            "rejection": None,
        }

    return validate


def _route_after_validate(state: SupervisorState) -> str:
    if not state.plan_violations:
        return "valid"
    return "invalid"


def _replan_node(
    runtime: SupervisorRuntime,
) -> Callable[[SupervisorState], dict[str, Any]]:
    def replan(state: SupervisorState) -> dict[str, Any]:
        assert state.plan is not None and runtime.planner is not None
        if state.replans >= runtime.policy.max_replans:
            return {
                "outcome": Outcome.ABORTED,
                "abort_diagnostic": {
                    "reason": "replan limit reached",
                    "replans": state.replans,
                    "violations": [
                        v.model_dump(mode="json") for v in state.plan_violations
                    ],
                    "rejection": (
                        state.rejection.model_dump(mode="json")
                        if state.rejection
                        else None
                    ),
                },
            }
        update = _propose(
            state,
            runtime,
            plan_id=state.plan.plan_id,
            version=state.plan.version + 1,
        )
        update["replans"] = state.replans + 1
        return update

    return replan


def _route_after_replan(state: SupervisorState) -> str:
    return "stop" if state.outcome is not None else "validate"


def _route_after_plan(state: SupervisorState) -> str:
    return "stop" if state.outcome is not None else "validate"


def _select_node(
    runtime: SupervisorRuntime,
) -> Callable[[SupervisorState], dict[str, Any]]:
    def select_step(state: SupervisorState) -> dict[str, Any]:
        assert state.plan is not None
        statuses = dict(state.statuses)
        results = dict(state.results)
        if runtime.cancel.cancelled:
            for step in state.plan.steps:
                if statuses[step.call_id] is CallStatus.PENDING:
                    statuses[step.call_id] = CallStatus.CANCELLED
                    results[step.call_id] = ToolCallResult.failure(
                        step,
                        CallStatus.CANCELLED,
                        ToolErrorKind.CANCELLED,
                        "the run was cancelled before this step started",
                        attempts=0,
                    )
            return {
                "statuses": statuses,
                "results": results,
                "next_call": None,
                "outcome": Outcome.CANCELLED,
            }
        next_call: str | None = None
        for step in state.plan.steps:
            if statuses[step.call_id] is not CallStatus.PENDING:
                continue
            dep_states = [statuses[d] for d in step.depends_on]
            if any(
                s not in (CallStatus.SUCCEEDED, CallStatus.PENDING) for s in dep_states
            ):
                statuses[step.call_id] = CallStatus.SKIPPED
                results[step.call_id] = ToolCallResult.failure(
                    step,
                    CallStatus.SKIPPED,
                    ToolErrorKind.DEPENDENCY_NOT_MET,
                    "a dependency did not succeed",
                    context={
                        "depends_on": {d: statuses[d].value for d in step.depends_on}
                    },
                )
                continue
            if next_call is None and all(s is CallStatus.SUCCEEDED for s in dep_states):
                next_call = step.call_id
        return {"statuses": statuses, "results": results, "next_call": next_call}

    return select_step


def _route_after_select(state: SupervisorState) -> str:
    if state.outcome is Outcome.CANCELLED:
        return "stop"
    return "run" if state.next_call is not None else "done"


def _execute_node(
    runtime: SupervisorRuntime,
) -> Callable[[SupervisorState], dict[str, Any]]:
    def execute(state: SupervisorState) -> dict[str, Any]:
        assert state.plan is not None and state.next_call is not None
        step = state.plan.step(state.next_call)
        attempt = state.attempts.get(step.call_id, 0) + 1
        result = _run_sync(
            execute_tool(
                step, runtime.registry, runtime.tool_context(), attempt=attempt
            )
        )
        return {
            "attempts": {**state.attempts, step.call_id: attempt},
            "results": {**state.results, step.call_id: result},
            "statuses": {**state.statuses, step.call_id: result.status},
        }

    return execute


def _route_node(
    runtime: SupervisorRuntime,
) -> Callable[[SupervisorState], dict[str, Any]]:
    """Apply the §12.4 routing table, top to bottom."""

    def route_result(state: SupervisorState) -> dict[str, Any]:
        assert state.plan is not None and state.next_call is not None
        result = state.results[state.next_call]
        tool = runtime.registry.get(result.tool_name)
        idempotent = bool(tool and tool.capability_schema.idempotent)
        if result.status is CallStatus.SUCCEEDED:
            return {"route": "continue"}
        if (
            result.status is CallStatus.FAILED
            and result.error is not None
            and result.error.retryable
            and idempotent
            and result.attempts < runtime.policy.max_attempts
        ):
            return {
                "route": "retry",
                "statuses": {**state.statuses, result.call_id: CallStatus.RUNNING},
            }
        if result.status is CallStatus.REJECTED:
            if state.replans < runtime.policy.max_replans:
                return {"route": "replan", "rejection": result}
            return _abort(state, result)
        if result.status is CallStatus.CANCELLED or runtime.cancel.cancelled:
            return {**_abort(state, result), "outcome": Outcome.CANCELLED}
        return {"route": "continue"}

    return route_result


def _abort(state: SupervisorState, result: ToolCallResult) -> dict[str, Any]:
    assert state.plan is not None
    step = state.plan.step(result.call_id)
    return {
        "route": "abort",
        "outcome": Outcome.ABORTED,
        "abort_diagnostic": {
            "call_id": step.call_id,
            "tool_name": step.tool_name,
            "arguments": step.arguments,
            "budget_fraction": step.budget_fraction,
            "decision": result.decision.value if result.decision else None,
            "error": result.error.model_dump(mode="json") if result.error else None,
        },
    }


def _route_after_result(state: SupervisorState) -> str:
    return state.route or "continue"


def _missing_entry(state: SupervisorState, s: ToolCallRequest) -> dict[str, Any]:
    r = state.results.get(s.call_id)
    error = r.error if r is not None else None
    return {
        "call_id": s.call_id,
        "tool_name": s.tool_name,
        "status": (state.statuses.get(s.call_id) or CallStatus.PENDING).value,
        "error": error.kind.value if error else None,
        "message": error.message if error else None,
    }


def _synthesize_node(state: SupervisorState) -> dict[str, Any]:
    """Coverage and outcome from SUCCEEDED results only (§12.11)."""
    assert state.plan is not None
    steps = state.plan.steps
    done = [s for s in steps if state.statuses.get(s.call_id) is CallStatus.SUCCEEDED]
    missing = [_missing_entry(state, s) for s in steps if s not in done]
    if not done:
        outcome = Outcome.FAILED
    elif missing:
        outcome = Outcome.PARTIAL
    else:
        outcome = Outcome.COMPLETE
    coverage = len(done) / len(steps) if steps else 0.0
    return {"outcome": outcome, "coverage": coverage, "missing": missing}


# ---------------------------------------------------------------------------
# Response rendering
# ---------------------------------------------------------------------------


def _render_rows(rows: list[dict[str, Any]], limit: int = 5) -> list[str]:
    if not rows:
        return ["  (no rows)"]
    lines = [f"  {row}" for row in rows[:limit]]
    if len(rows) > limit:
        lines.append(f"  … {len(rows) - limit} more rows")
    return lines


def _render_result(step: ToolCallRequest, result: ToolCallResult) -> list[str]:
    data = result.data if isinstance(result.data, dict) else {}
    head = f"[{step.call_id}] {step.tool_name}"
    if step.datasets:
        head += f" on {', '.join(step.datasets)}"
    lines = [head]
    if data.get("explanation"):
        lines.append(f"  {data['explanation']}")
    if "rows" in data and data["rows"] is not None:
        lines += _render_rows(list(data["rows"]))
    return lines


def _header_lines(state: SupervisorState, outcome: Outcome) -> list[str]:
    lines: list[str] = []
    if state.demo:
        lines.append("[demo mode — heuristic output, not a contract result (D47)]")
    lines.append(f"Outcome: {outcome.value}")
    if state.planning_error is not None:
        err = state.planning_error
        lines.append(f"{err.kind.value}: {err.message}")
    if state.plan is not None and state.plan.reasoning:
        lines.append(state.plan.reasoning)
    if state.abort_diagnostic is not None:
        lines.append(f"Diagnostic: {state.abort_diagnostic}")
    return lines


def _result_lines(state: SupervisorState, outcome: Outcome) -> list[str]:
    if outcome in (Outcome.ABORTED, Outcome.CANCELLED):
        return [
            f"  {cid} ({r.tool_name}): {r.status.value}"
            + (f" {r.error.kind.value}" if r.error else "")
            for cid, r in state.results.items()
        ]
    if state.plan is None or outcome not in (Outcome.COMPLETE, Outcome.PARTIAL):
        return []
    lines: list[str] = []
    if outcome is Outcome.PARTIAL:
        total = len(state.plan.steps)
        lines.append(
            f"Coverage {total - len(state.missing)}/{total}; "
            "this is not a complete answer."
        )
    for step in state.plan.steps:
        result = state.results.get(step.call_id)
        if result is not None and result.status is CallStatus.SUCCEEDED:
            lines += _render_result(step, result)
    return lines


def _missing_lines(state: SupervisorState) -> list[str]:
    lines = []
    for miss in state.missing:
        reason = miss["error"] or miss["status"]
        detail = f" — {miss['message']}" if miss.get("message") else ""
        lines.append(
            f"Not available: {miss['call_id']} ({miss['tool_name']}): {reason}{detail}"
        )
    return lines


def _respond_node(state: SupervisorState) -> dict[str, Any]:
    outcome = state.outcome or Outcome.FAILED
    lines = (
        _header_lines(state, outcome)
        + _result_lines(state, outcome)
        + _missing_lines(state)
    )
    response = "\n".join(lines)
    history = list(state.conversation_history)
    history.append({"role": "user", "content": state.utterance})
    history.append({"role": "assistant", "content": response})
    return {"outcome": outcome, "synthesis": response, "conversation_history": history}


# ---------------------------------------------------------------------------
# Tracing and assembly
# ---------------------------------------------------------------------------


def _traced_node(
    name: str,
    fn: Callable[[SupervisorState], dict[str, Any]],
) -> Callable[[SupervisorState], dict[str, Any]]:
    def _wrapper(state: SupervisorState) -> dict[str, Any]:
        t0 = time.perf_counter()
        result = fn(state)
        elapsed_ms = round((time.perf_counter() - t0) * 1000, 1)
        result["visited_nodes"] = list(state.visited_nodes) + [name]
        result["node_timings"] = {**state.node_timings, name: elapsed_ms}
        return result

    _wrapper.__name__ = name
    return _wrapper


def build_supervisor_graph(
    runtime: SupervisorRuntime,
    checkpointer: Any = None,
) -> Any:
    """Compile the §12.4 supervisor graph bound to *runtime*."""
    graph: Any = StateGraph(SupervisorState)
    nodes: dict[str, Callable[[SupervisorState], dict[str, Any]]] = {
        "plan": _plan_node(runtime),
        "validate_plan": _validate_node(runtime),
        "replan": _replan_node(runtime),
        "select_step": _select_node(runtime),
        "execute_tool": _execute_node(runtime),
        "route_result": _route_node(runtime),
        "synthesize": _synthesize_node,
        "respond": _respond_node,
    }
    for name, fn in nodes.items():
        graph.add_node(name, _traced_node(name, fn))

    graph.set_entry_point("plan")
    graph.add_conditional_edges(
        "plan", _route_after_plan, {"validate": "validate_plan", "stop": "respond"}
    )
    graph.add_conditional_edges(
        "validate_plan",
        _route_after_validate,
        {"valid": "select_step", "invalid": "replan"},
    )
    graph.add_conditional_edges(
        "replan", _route_after_replan, {"validate": "validate_plan", "stop": "respond"}
    )
    graph.add_conditional_edges(
        "select_step",
        _route_after_select,
        {"run": "execute_tool", "done": "synthesize", "stop": "respond"},
    )
    graph.add_edge("execute_tool", "route_result")
    graph.add_conditional_edges(
        "route_result",
        _route_after_result,
        {
            "continue": "select_step",
            "retry": "execute_tool",
            "replan": "replan",
            "abort": "respond",
        },
    )
    graph.add_edge("synthesize", "respond")
    graph.add_edge("respond", END)
    return graph.compile(checkpointer=checkpointer or MemorySaver())


def run_supervisor(
    utterance: str,
    runtime: SupervisorRuntime,
    *,
    thread_id: str = "supervisor",
    history: list[dict[str, str]] | None = None,
    checkpointer: Any = None,
) -> SupervisorState:
    """Run one supervisor turn; state is keyed by ``(tenant_id, thread_id)``."""
    graph = build_supervisor_graph(runtime, checkpointer=checkpointer)
    raw = graph.invoke(
        SupervisorState(
            utterance=utterance, conversation_history=list(history or [])
        ).model_dump(),
        config={
            "configurable": {"thread_id": f"{runtime.auth.tenant_id}:{thread_id}"},
            "recursion_limit": 200,
        },
    )
    return SupervisorState.model_validate(raw)

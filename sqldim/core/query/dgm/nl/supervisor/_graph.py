"""DGM Supervisor Agent — LangGraph state graph (§12.2).

Six-node supervisor graph:
  plan          → decompose user question into ordered tool calls
  reflect       → (conditional) if confidence < threshold, prepend orientation
  select_tool   → pick next tool from plan
  execute_tool  → invoke the selected tool
  synthesize    → combine results into coherent answer
  respond       → format final response

Routing:
  route_after_plan      — low confidence? → reflect; else → select_tool
  route_after_execution — more steps? → select_tool; done? → synthesize

The reflect node handles ill-defined or ambiguous questions by front-loading
two orientation steps (dgm_what_can_i_answer + dgm_map_analytical_space)
before the original plan steps, so the synthesizer always has context about
the analytical space even when the user's intent was unclear.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, StateGraph

from sqldim.core.query.dgm.nl.supervisor._types import (
    SupervisorPlan,
    SupervisorState,
    ToolCallRequest,
    ToolCallResult,
)

SUPERVISOR_NODE_NAMES: list[str] = [
    "plan",
    "reflect",
    "select_tool",
    "execute_tool",
    "synthesize",
    "respond",
]

__all__ = ["SUPERVISOR_NODE_NAMES", "build_supervisor_graph"]


# ---------------------------------------------------------------------------
# Routing
# ---------------------------------------------------------------------------


def route_after_execution(state: SupervisorState) -> str:
    """Route after tool execution: more steps or synthesize."""
    if state.plan is None:
        return "done"
    if state.current_step >= len(state.plan.steps):
        return "done"
    return "more"


_REFLECT_CONFIDENCE_THRESHOLD = 0.5


def route_after_plan(state: SupervisorState) -> str:
    """Route after planning: reflect on low-confidence plans, else proceed.

    Questions whose ``plan.confidence`` is below the threshold are redirected
    to the ``reflect`` node, which prepends orientation steps so the
    synthesizer can produce a meaningful response even for vague inputs.
    """
    if state.plan and state.plan.confidence < _REFLECT_CONFIDENCE_THRESHOLD:
        return "reflect"
    return "skip"


# ---------------------------------------------------------------------------
# Stub node implementations (no LLM needed)
# ---------------------------------------------------------------------------


def _stub_plan(state: SupervisorState) -> dict[str, Any]:
    """Stub planner: creates a single dgm_query step."""
    plan = SupervisorPlan(
        goal=state.utterance,
        steps=[
            ToolCallRequest(
                tool_name="dgm_query",
                arguments={"question": state.utterance},
                budget_fraction=1.0,
            ),
        ],
    )
    return {"plan": plan, "current_step": 0}


def _stub_select_tool(state: SupervisorState) -> dict[str, Any]:
    """Select the next tool from the plan."""
    return {}


def _stub_execute_tool(state: SupervisorState) -> dict[str, Any]:
    """Execute the current tool (stub: immediate success)."""
    from sqldim.core.query.dgm.nl.supervisor._tools import TOOL_REGISTRY

    if state.plan is None or state.current_step >= len(state.plan.steps):
        return {}

    step = state.plan.steps[state.current_step]
    tool = TOOL_REGISTRY.get(step.tool_name)
    if tool is not None:
        result = tool.execute(step.arguments, context=None)
    else:
        result = ToolCallResult(
            tool_name=step.tool_name,
            success=False,
            error=f"Unknown tool: {step.tool_name}",
        )

    return {
        "results": list(state.results) + [result],
        "current_step": state.current_step + 1,
    }


def _stub_synthesize(
    state: SupervisorState, *, has_model: bool = False
) -> dict[str, Any]:
    """Combine all results into a natural-language response.

    When *has_model* is ``True`` (an LLM is connected via ``context.model``),
    stub messages omit the "no LLM connected" hint.
    """
    parts: list[str] = []
    is_reflected = "reflect" in state.visited_nodes

    for r in state.results:
        if not r.success:
            parts.append(f"\u26a0 {r.tool_name}: {r.error}")
        elif r.tool_name == "dgm_what_can_i_answer":
            data = r.data if isinstance(r.data, dict) else {}
            datasets = data.get("datasets", [])
            questions = data.get("questions", [])
            if len(datasets) == 1:
                ds = datasets[0]
                if questions:
                    q_list = "\n".join(f"  • {q}" for q in questions)
                    parts.append(f"Questions for '{ds}' ({len(questions)}):\n{q_list}")
                elif has_model:
                    parts.append(f"Dataset '{ds}' is available.")
                else:
                    parts.append(
                        f"Dataset '{ds}' is available "
                        "(pass --model-provider + --model "
                        "to generate suggested questions)."
                    )
            elif datasets:
                ds_list = "\n".join(f"  • {d}" for d in sorted(datasets))
                parts.append(f"Datasets I can see ({len(datasets)}):\n{ds_list}")
            else:
                parts.append("No datasets are currently available.")
        elif r.tool_name == "dgm_map_analytical_space":
            data = r.data if isinstance(r.data, dict) else {}
            tools = data.get("tools", [])
            if tools:
                tl_list = "\n".join(
                    f"  \u2022 {t['name']} \u2014 {t['description']}" for t in tools
                )
                parts.append(f"Tools available ({len(tools)}):\n{tl_list}")
            else:
                parts.append("No tools registered.")
        elif r.tool_name == "dgm_query":
            data = r.data if isinstance(r.data, dict) else {}
            dataset = data.get("dataset") or "?"
            question = data.get("question")
            question_hint = f" for '{question}'" if question else ""
            explanation = data.get("explanation")
            query_result = data.get("query_result")
            if explanation:
                parts.append(explanation)
            if query_result and isinstance(query_result, dict):
                count = query_result.get("count", 0)
                cols = query_result.get("columns", [])
                rows = query_result.get("rows", [])
                if rows:
                    rows_text = "\n".join(f"  {row}" for row in rows[:5])
                    suffix = f"\n  ... ({count - 5} more rows)" if count > 5 else ""
                    parts.append(
                        f"Query on '{dataset}': {count} rows,"
                        f" columns: {', '.join(cols)}\n{rows_text}{suffix}"
                    )
                else:
                    parts.append(f"Query on '{dataset}' returned no rows.")
            elif not explanation:
                if has_model:
                    parts.append(f"DGM query on '{dataset}'{question_hint} completed.")
                else:
                    parts.append(
                        f"DGM query on '{dataset}'{question_hint} completed"
                        " (stub — pass --model-provider + --model for real results)."
                    )
        else:
            data = r.data if isinstance(r.data, dict) else {}
            summary = ", ".join(f"{k}: {v}" for k, v in data.items() if k != "stub")
            parts.append(
                f"[{r.tool_name}]{': ' + summary if summary else ' completed.'}"
            )

    body = "\n\n".join(parts) if parts else "No results produced."

    # For vague / reflected inputs, wrap the orientation data in a helpful intro
    if is_reflected:
        intro = (
            "I'm the DGM Supervisor — I can run analytical queries across "
            "your datasets using the tools listed below. "
            "Ask me a question about any of these datasets and I'll "
            "decompose it into the right tool calls.\n"
        )
        body = intro + "\n" + body

    return {"synthesis": body}


def _stub_respond(state: SupervisorState) -> dict[str, Any]:
    """Format the final response."""
    return {}


def _llm_synthesize(state: SupervisorState, model: Any) -> dict[str, Any]:
    """Use the LLM to synthesize tool results into a coherent answer."""
    try:
        from pydantic_ai import Agent  # lazy import

        parts: list[str] = []
        for r in state.results:
            if not r.success:
                parts.append(f"[{r.tool_name}] ERROR: {r.error}")
                continue
            data = r.data if isinstance(r.data, dict) else {}
            if r.tool_name == "dgm_query":
                ds = data.get("dataset", "?")
                explanation = data.get("explanation") or ""
                qr = data.get("query_result")
                if explanation:
                    parts.append(f"[dgm_query / {ds}]\n{explanation}")
                if qr and isinstance(qr, dict):
                    rows = qr.get("rows", [])
                    count = qr.get("count", len(rows))
                    cols = qr.get("columns", [])
                    if rows:
                        preview = "\n".join(str(row) for row in rows[:5])
                        parts.append(
                            f"Query result on '{ds}' ({count} rows,"
                            f" columns: {', '.join(cols)}):\n{preview}"
                        )
                    else:
                        parts.append(f"Query on '{ds}' returned no rows.")
                elif not explanation:
                    parts.append(f"[dgm_query / {ds}] query completed (no data).")
            elif r.tool_name == "dgm_what_can_i_answer":
                datasets = data.get("datasets", [])
                questions = data.get("questions", [])
                if datasets:
                    parts.append(
                        f"Available datasets ({len(datasets)}): {', '.join(datasets)}"
                    )
                if questions:
                    parts.append(
                        "Sample questions:\n"
                        + "\n".join(f"  \u2022 {q}" for q in questions[:5])
                    )
            elif r.tool_name == "dgm_map_analytical_space":
                tools = data.get("tools", [])
                if tools:
                    tool_list = "\n".join(
                        f"  \u2022 {t['name']}: {t['description']}" for t in tools
                    )
                    parts.append(f"Available tools ({len(tools)}):\n{tool_list}")
            else:
                summary = {
                    k: v for k, v in data.items() if k != "stub" and v is not None
                }
                if summary:
                    parts.append(f"[{r.tool_name}] {summary}")

        if not parts:
            return _stub_synthesize(state, has_model=True)

        tool_context = "\n\n".join(parts)
        history_lines = [
            f"{m.get('role', '').capitalize()}: {m.get('content', '')}"
            for m in (state.conversation_history or [])
            if m.get("role") and m.get("content")
        ]
        prompt_parts = [f"Question: {state.utterance}"]
        if history_lines:
            prompt_parts.append("Prior conversation:\n" + "\n".join(history_lines))
        prompt_parts.append(f"Tool results:\n{tool_context}")
        prompt_parts.append("Provide a concise, data-driven answer.")

        agent = Agent(
            model=model,
            system_prompt=(
                "You are an expert data analyst. Given the tool execution results, "
                "answer the user's question concisely and precisely. "
                "Reference specific numbers and findings. "
                "Keep the response under 200 words unless the data demands more detail."
            ),
        )
        llm_result = agent.run_sync(
            "\n\n".join(prompt_parts),
            model_settings={"timeout": 30.0},
        )
        return {"synthesis": llm_result.output}
    except Exception:  # noqa: BLE001
        return _stub_synthesize(state, has_model=True)


# ---------------------------------------------------------------------------
# Tracing wrapper (consistent with NL pipeline pattern)
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


# ---------------------------------------------------------------------------
# Graph factory
# ---------------------------------------------------------------------------


def build_supervisor_graph(
    checkpointer: Any = None,
    *,
    context: Any = None,
) -> Any:
    """Construct and compile the six-node supervisor LangGraph (§12.2).

    The LLM model (if any) is carried via ``context.model`` as a pydantic-ai
    model instance.  When ``context.model`` is ``None`` the planner falls
    back to keyword/phrase routing.
    """
    if checkpointer is None:
        checkpointer = MemorySaver()

    graph: StateGraph = StateGraph(SupervisorState)

    # Build context-bound node functions via closures so tools receive the
    # runtime context (available datasets, budget, model) at call time.
    def _plan_node(state: SupervisorState) -> dict[str, Any]:
        from sqldim.core.query.dgm.nl.supervisor._planner import decompose_question

        # Inject conversation history into planner context for follow-ups
        history = state.conversation_history
        if context is not None and history:
            context.memory = list(history)

        plan = decompose_question(state.utterance, context)
        return {"plan": plan, "current_step": 0}

    def _execute_node(state: SupervisorState) -> dict[str, Any]:
        from sqldim.core.query.dgm.nl.supervisor._tools import (
            TOOL_REGISTRY as _REGISTRY,
        )

        if state.plan is None or state.current_step >= len(
            state.plan.steps
        ):  # pragma: no cover
            return {}  # pragma: no cover
        step = state.plan.steps[state.current_step]
        tool = _REGISTRY.get(step.tool_name)
        if tool is not None:
            result = tool.execute(step.arguments, context=context)
        else:  # pragma: no cover  — keyword planner only generates registered tools
            result = ToolCallResult(
                tool_name=step.tool_name,
                success=False,
                error=f"Unknown tool: {step.tool_name}",
            )
        return {
            "results": list(state.results) + [result],
            "current_step": state.current_step + 1,
        }

    def _reflect_node(state: SupervisorState) -> dict[str, Any]:
        """Prepend orientation steps to the plan when confidence is too low.

        The reflect node rescales the original plan's budget fractions to
        fit within the 0.5 budget remaining after two 0.25-fraction
        orientation steps (dgm_what_can_i_answer + dgm_map_analytical_space).
        The total budget fraction of the enriched plan stays at 1.0.
        """
        orientation_budget: float = 0.5  # shared by two 0.25 steps
        original_steps = list(state.plan.steps) if state.plan else []
        original_total = sum(s.budget_fraction for s in original_steps) or 1.0
        scale = (1.0 - orientation_budget) / original_total
        rescaled = [
            s.model_copy(
                update={"budget_fraction": round(s.budget_fraction * scale, 6)}
            )
            for s in original_steps
        ]
        orientation = [
            ToolCallRequest(
                tool_name="dgm_what_can_i_answer",
                arguments={"dataset": "__all__"},
                budget_fraction=0.25,
            ),
            ToolCallRequest(
                tool_name="dgm_map_analytical_space",
                arguments={"dataset": "__all__"},
                budget_fraction=0.25,
            ),
        ]
        new_plan = SupervisorPlan(
            goal=state.plan.goal if state.plan else state.utterance,
            steps=orientation + rescaled,
            confidence=state.plan.confidence if state.plan else 0.0,
        )
        return {"plan": new_plan, "current_step": 0}

    def _synthesize_node(state: SupervisorState) -> dict[str, Any]:
        has_model = context is not None and getattr(context, "model", None) is not None
        if has_model:
            result = _llm_synthesize(state, context.model)
        else:
            result = _stub_synthesize(state, has_model=False)

        # Append this exchange to conversation history for multi-turn memory
        history = list(state.conversation_history)
        history.append({"role": "user", "content": state.utterance})
        if result.get("synthesis"):
            history.append({"role": "assistant", "content": result["synthesis"]})
        result["conversation_history"] = history
        return result

    # Register nodes (stub implementations; overridable via context/model)
    node_impls: dict[str, Callable[..., dict[str, Any]]] = {
        "plan": _plan_node,
        "reflect": _reflect_node,
        "select_tool": _stub_select_tool,
        "execute_tool": _execute_node,
        "synthesize": _synthesize_node,
        "respond": _stub_respond,
    }

    for name, fn in node_impls.items():
        graph.add_node(name, _traced_node(name, fn))

    # Edges
    graph.add_conditional_edges(
        "plan",
        route_after_plan,
        {
            "reflect": "reflect",
            "skip": "select_tool",
        },
    )
    graph.add_edge("reflect", "select_tool")
    graph.add_edge("select_tool", "execute_tool")

    graph.add_conditional_edges(
        "execute_tool",
        route_after_execution,
        {
            "more": "select_tool",
            "done": "synthesize",
        },
    )

    graph.add_edge("synthesize", "respond")
    graph.add_edge("respond", END)

    graph.set_entry_point("plan")

    return graph.compile(checkpointer=checkpointer)

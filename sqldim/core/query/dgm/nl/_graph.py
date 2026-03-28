"""DGM NL Interface — LangGraph state graph (§11.10).

Defines the eleven-node execution graph for the NL interface:
  Five LLM nodes (specialist Pydantic-AI agents):
    entity_resolution, temporal_classification, compositional_detection,
    candidate_ranking, explanation_rendering
  Six non-LLM nodes (pure DGM machinery):
    candidate_generation, confirmation_loop, dag_construction,
    budget_gate, execution, clarification

Routing functions:
  route_after_confirmation — confirmed / refine / more_options
  route_budget_decision    — maps BudgetDecisionKind to graph edge labels

Public factory:
  build_nl_graph() → compiled LangGraph (checkpointed, resumable)

Observability:
  Every node is wrapped with a Logfire span that records the node name,
  the utterance being processed, and the state fields that changed.
  Call ``logfire.configure()`` before invoking the graph to ship traces.
"""

from __future__ import annotations

from collections.abc import Callable
import time
from typing import Any

import logfire
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, StateGraph

from sqldim.core.query.dgm.nl._agent_types import NLInterfaceState
from sqldim.core.query.dgm.nl._node_impls import (
    LLM_NODE_NAMES,
    OPERATIONAL_NODE_NAMES,
    make_llm_nodes,
    make_operational_nodes,
)
from sqldim.core.query.dgm.planner._gate import (
    AsyncDecision,
    BudgetDecisionKind,
    ClarifyDecision,
    PaginateDecision,
    ProceedDecision,
    RejectDecision,
    RewriteDecision,
    SampleDecision,
    StreamDecision,
)

_DECISION_KIND_MAP: dict[type, str] = {
    ProceedDecision: BudgetDecisionKind.PROCEED.value,
    StreamDecision: BudgetDecisionKind.STREAM.value,
    AsyncDecision: BudgetDecisionKind.ASYNC.value,
    RewriteDecision: BudgetDecisionKind.REWRITE.value,
    SampleDecision: BudgetDecisionKind.SAMPLE.value,
    PaginateDecision: BudgetDecisionKind.PAGINATE.value,
    ClarifyDecision: BudgetDecisionKind.CLARIFY.value,
    RejectDecision: BudgetDecisionKind.REJECT.value,
}

__all__ = [
    "route_after_confirmation",
    "route_budget_decision",
    "build_nl_graph",
]


# ---------------------------------------------------------------------------
# Routing functions
# ---------------------------------------------------------------------------


def route_after_confirmation(state: NLInterfaceState) -> str:
    """Decide next node after the human confirmation checkpoint (§11.5).

    Returns
    -------
    "confirmed"    — user accepted a candidate → proceed to DAG construction
    "more_options" — user wants additional candidates → recommender next-step
    "refine"       — user wants to narrow the current candidate set
    """
    if state.confirmed:
        return "confirmed"
    if state.want_more_options:
        return "more_options"
    return "refine"


def route_budget_decision(state: NLInterfaceState) -> str:
    """Map the gate's BudgetDecision variant to a graph edge label (§6.3 + §11.9).

    Raises ValueError when budget_decision is None — the gate must always
    produce a decision before this router is called.
    """
    decision = state.budget_decision
    if decision is None:
        raise ValueError("budget_decision must be set before routing")
    label = _DECISION_KIND_MAP.get(type(decision))
    if label is None:
        raise ValueError(f"unknown BudgetDecision type: {type(decision)}")
    return label


# ---------------------------------------------------------------------------
# Logfire tracing wrapper
# ---------------------------------------------------------------------------


def _state_diff(before: NLInterfaceState, after: dict[str, Any]) -> dict[str, Any]:
    """Return only the keys in *after* whose values differ from *before*."""
    return {k: v for k, v in after.items() if getattr(before, k, object()) != v}


def _traced_node(
    name: str,
    fn: Callable[[NLInterfaceState], dict[str, Any]],
) -> Callable[[NLInterfaceState], dict[str, Any]]:
    """Wrap a node function with a Logfire span that records state changes."""

    def _wrapper(state: NLInterfaceState) -> dict[str, Any]:
        t0 = time.perf_counter()
        with logfire.span(
            "dgm.nl.{node}",
            node=name,
            utterance=state.utterance,
            confirmed=state.confirmed,
            candidates_count=len(state.candidates),
        ):
            result = fn(state)
            changed = _state_diff(state, result)
            if changed:
                logfire.info(
                    "dgm.nl.{node}.state_changed",
                    node=name,
                    changed_fields=list(changed.keys()),
                )
        elapsed_ms = round((time.perf_counter() - t0) * 1000, 1)
        # Record this hop + timing for eval tracing (always appends, never replaces).
        result["visited_nodes"] = list(state.visited_nodes) + [name]
        result["node_timings"] = {**state.node_timings, name: elapsed_ms}
        return result

    _wrapper.__name__ = name
    return _wrapper


# ---------------------------------------------------------------------------
# Stub node functions (replaced by specialist agents at runtime)
# ---------------------------------------------------------------------------


def _noop(state: NLInterfaceState) -> dict[str, Any]:
    """Passthrough stub used for non-LLM nodes not yet wired to DGM machinery."""
    return {}


# ---------------------------------------------------------------------------
# Graph factory
# ---------------------------------------------------------------------------


def build_nl_graph(
    checkpointer: Any = None,
    *,
    context: Any = None,
    model: Any = None,
) -> Any:
    """Construct and compile the eleven-node NL interface LangGraph (§11.10).

    The returned graph is a compiled ``CompiledStateGraph``.

    When *both* ``context`` and ``model`` are provided the five LLM nodes
    invoke real Pydantic-AI specialist agents.  When either is ``None`` the
    LLM nodes fall back to passthrough stubs so the graph can still be
    invoked safely in tests and schema-introspection mode.

    Every node is wrapped with a Logfire span.  Call ``logfire.configure()``
    before invoking the graph to enable trace export.

    Parameters
    ----------
    checkpointer:
        LangGraph checkpointer for cross-turn persistence.  Defaults to
        ``MemorySaver`` (in-process; swap for Redis/SQL in production).
    context:
        Optional :class:`~sqldim.core.query.dgm.nl._agent_types.DGMContext`
        carrying the EntityRegistry, ExecutionBudget, and optional live
        graph/BDD/recommender.  Injected into specialist agents as deps.
    model:
        Pydantic-AI model name or model instance to pass to specialist
        agent factories.  ``None`` disables LLM nodes (stubs only).
    """
    if checkpointer is None:
        checkpointer = MemorySaver()

    graph: StateGraph = StateGraph(NLInterfaceState)

    def _node(
        name: str, fn: Callable[[NLInterfaceState], dict[str, Any]] = _noop
    ) -> None:
        graph.add_node(name, _traced_node(name, fn))  # type: ignore[call-overload]

    # ── LLM nodes (Pydantic-AI specialist agents) ─────────────────────────
    if context is not None and model is not None:
        for name, fn in make_llm_nodes(context, model).items():
            _node(name, fn)
    else:
        for name in LLM_NODE_NAMES:
            _node(name)

    # ── Non-LLM nodes (pure DGM machinery) ────────────────────────────────
    if context is not None:
        for name, fn in make_operational_nodes(context, model=model).items():
            _node(name, fn)
    else:
        for name in OPERATIONAL_NODE_NAMES:
            _node(name)

    # ── Linear pipeline edges ─────────────────────────────────────────────
    graph.add_edge("entity_resolution", "temporal_classification")
    graph.add_edge("temporal_classification", "compositional_detection")
    graph.add_edge("compositional_detection", "candidate_generation")
    graph.add_edge("candidate_generation", "candidate_ranking")
    graph.add_edge("candidate_ranking", "confirmation_loop")

    # ── Conditional: after confirmation (refinement cycle) ────────────────
    graph.add_conditional_edges(
        "confirmation_loop",
        route_after_confirmation,
        {
            "confirmed": "dag_construction",
            "refine": "candidate_generation",  # cycle: narrow candidate set
            "more_options": "candidate_generation",  # cycle: recommender next-step
        },
    )

    graph.add_edge("dag_construction", "budget_gate")

    # ── Conditional: budget gate (8-way branch) ───────────────────────────
    graph.add_conditional_edges(
        "budget_gate",
        route_budget_decision,
        {
            "PROCEED": "execution",
            "STREAM": "execution",
            "ASYNC": "explanation_rendering",
            "REWRITE": "dag_construction",  # rewrite and re-gate
            "SAMPLE": "execution",
            "PAGINATE": "execution",
            "CLARIFY": "clarification",
            "REJECT": "explanation_rendering",
        },
    )

    graph.add_edge("execution", "explanation_rendering")
    graph.add_edge("explanation_rendering", END)
    graph.add_edge("clarification", "confirmation_loop")  # cycle

    graph.set_entry_point("entity_resolution")

    return graph.compile(checkpointer=checkpointer)

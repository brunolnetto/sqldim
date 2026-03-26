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
from typing import Any

import logfire
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, StateGraph

from sqldim.core.query.dgm.nl._agent_types import NLInterfaceState, QueryCandidate
from sqldim.core.query.dgm.nl._agents import (
    make_compositional_agent,
    make_entity_agent,
    make_explanation_agent,
    make_ranking_agent,
    make_temporal_agent,
)
from sqldim.core.query.dgm.planner._gate import (
    AsyncDecision,
    BudgetDecisionKind,
    ClarifyDecision,
    ExecutionGate,
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
            # Record this hop for eval tracing (always appends, never replaces).
            result["visited_nodes"] = list(state.visited_nodes) + [name]
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
        # Create agents once and close over them in node functions.
        _entity_agent = make_entity_agent(model)
        _temporal_agent = make_temporal_agent(model)
        _compositional_agent = make_compositional_agent(model)
        _ranking_agent = make_ranking_agent(model)
        _explanation_agent = make_explanation_agent(model)

        def _entity_node(state: NLInterfaceState) -> dict[str, Any]:
            try:
                r = _entity_agent.run_sync(state.utterance, deps=context)
                return {"entity_result": r.output}
            except Exception:  # noqa: BLE001
                return {}

        def _temporal_node(state: NLInterfaceState) -> dict[str, Any]:
            try:
                r = _temporal_agent.run_sync(state.utterance, deps=context)
                return {"temporal_result": r.output}
            except Exception:  # noqa: BLE001
                return {}

        def _compositional_node(state: NLInterfaceState) -> dict[str, Any]:
            try:
                r = _compositional_agent.run_sync(state.utterance, deps=context)
                return {"compositional": r.output}
            except Exception:  # noqa: BLE001
                return {}

        def _ranking_node(state: NLInterfaceState) -> dict[str, Any]:
            if not state.candidates:
                return {}
            try:
                prompt = f"{state.utterance}\n\nCandidates:\n" + "\n".join(
                    f"{i}. {c.description}" for i, c in enumerate(state.candidates)
                )
                r = _ranking_agent.run_sync(prompt, deps=context)
                return {"ranking_result": r.output}
            except Exception:  # noqa: BLE001
                return {}

        def _explanation_node(state: NLInterfaceState) -> dict[str, Any]:
            try:
                r = _explanation_agent.run_sync(state.utterance, deps=context)
                return {"explanation": r.output.explanation}
            except Exception:  # noqa: BLE001
                return {}

        _node("entity_resolution", _entity_node)
        _node("temporal_classification", _temporal_node)
        _node("compositional_detection", _compositional_node)
        _node("candidate_ranking", _ranking_node)
        _node("explanation_rendering", _explanation_node)
    else:
        _node("entity_resolution")
        _node("temporal_classification")
        _node("compositional_detection")
        _node("candidate_ranking")
        _node("explanation_rendering")

    # ── Non-LLM nodes (pure DGM machinery) ────────────────────────────────
    if context is not None:
        # ------------------------------------------------------------------
        # candidate_generation — build QueryCandidates from resolved vocab
        # ------------------------------------------------------------------
        def _candidate_generation_node(
            state: NLInterfaceState,
        ) -> dict[str, Any]:
            registry = context.entity_registry  # type: ignore[union-attr]

            # Collect table names referenced by resolved proprefs.
            tables: set[str] = set()
            if state.entity_result:
                for pr in state.entity_result.resolved:
                    tables.add(pr.alias)

            # Fallback: all registered node aliases.
            if not tables:
                tables = set(registry.node_terms.values())

            candidates: list[QueryCandidate] = []
            for i, table in enumerate(sorted(tables)):
                props = [
                    propref
                    for propref in registry.prop_terms.values()
                    if propref.startswith(f"{table}.")
                ]
                col_preview = ", ".join(p.split(".", 1)[1] for p in props[:3])
                suffix = "..." if len(props) > 3 else ""
                desc = (
                    f"SELECT from {table} ({col_preview}{suffix})"
                    if props
                    else f"SELECT from {table}"
                )
                candidates.append(
                    QueryCandidate(
                        dag_node_id=i,
                        description=desc,
                        band_coverage=["B1"],
                        cost_estimate=float(len(props)),
                    )
                )
            return {"candidates": candidates}

        # ------------------------------------------------------------------
        # confirmation_loop — auto-confirm top candidate in CLI mode
        # ------------------------------------------------------------------
        def _confirmation_loop_node(
            state: NLInterfaceState,
        ) -> dict[str, Any]:
            if state.confirmed:
                return {}
            if state.ranking_result and state.ranking_result.ranked:
                return {"confirmed": True, "user_selection": 0}
            if state.candidates:
                return {"confirmed": True, "user_selection": 0}
            return {"confirmed": True}

        # ------------------------------------------------------------------
        # dag_construction — build a SQL query from the confirmed candidate
        # ------------------------------------------------------------------
        def _dag_construction_node(
            state: NLInterfaceState,
        ) -> dict[str, Any]:
            from sqldim.core.query.dgm.planner._targets import (
                ExportPlan,
                QueryTarget,
            )

            registry = context.entity_registry  # type: ignore[union-attr]

            # Resolve selected candidate.
            selected: QueryCandidate | None = None
            idx = state.user_selection or 0
            if state.ranking_result and state.ranking_result.ranked:
                if 0 <= idx < len(state.ranking_result.ranked):
                    selected = state.ranking_result.ranked[idx]
            if selected is None and state.candidates:
                if 0 <= idx < len(state.candidates):
                    selected = state.candidates[idx]
                elif state.candidates:
                    selected = state.candidates[0]

            if selected is None:
                sql = "SELECT 1 AS placeholder"
            else:
                # Extract table name from description "SELECT from <table> ..."
                desc = selected.description
                table: str | None = None
                if "SELECT from " in desc:
                    rest = desc.split("SELECT from ", 1)[1].strip()
                    table = rest.split()[0].rstrip(" (")

                if table and table in registry.node_terms.values():
                    props = [
                        propref
                        for propref in registry.prop_terms.values()
                        if propref.startswith(f"{table}.")
                    ]
                    cols = (
                        ", ".join(p.split(".", 1)[1] for p in props[:10])
                        if props
                        else "*"
                    )
                    sql = f"SELECT {cols} FROM {table} LIMIT 100"
                else:
                    sql = "SELECT 1 AS placeholder"

            plan = ExportPlan(
                query_target=QueryTarget.SQL_DUCKDB,
                query_text=sql,
            )
            # Store both the raw SQL string (q_current) and the plan object.
            context.q_current = plan  # type: ignore[union-attr]
            return {"q_current": sql}

        # ------------------------------------------------------------------
        # budget_gate — apply ExecutionGate against declared budget
        # ------------------------------------------------------------------
        def _budget_gate_node(
            state: NLInterfaceState,
        ) -> dict[str, Any]:
            from sqldim.core.query.dgm.planner._targets import (
                CostEstimate,
                ExportPlan,
                QueryTarget,
            )

            sql = state.q_current or "SELECT 1"
            # Heuristic cost: number of characters as a cpu_ops proxy.
            cpu_ops = max(100, len(sql) * 10)
            plan = ExportPlan(
                query_target=QueryTarget.SQL_DUCKDB,
                query_text=sql,
                cost_estimate=CostEstimate(cpu_ops=cpu_ops, io_ops=100),
            )
            gate = ExecutionGate()
            decision = gate.gate(plan, context.budget)  # type: ignore[union-attr]
            return {"budget_decision": decision}

        # ------------------------------------------------------------------
        # execution — run SQL against DuckDB and capture results
        # ------------------------------------------------------------------
        def _execution_node(
            state: NLInterfaceState,
        ) -> dict[str, Any]:
            sql = state.q_current
            con = context.con  # type: ignore[union-attr]
            if not sql or con is None:
                return {"result": None}
            try:
                cursor = con.execute(sql)
                columns = (
                    [col[0] for col in cursor.description] if cursor.description else []
                )
                rows = cursor.fetchall()
                return {
                    "result": {
                        "columns": columns,
                        "rows": [[str(v) for v in row] for row in rows[:100]],
                        "count": len(rows),
                    }
                }
            except Exception:  # noqa: BLE001
                return {"result": None}

        # ------------------------------------------------------------------
        # clarification — fall-through: auto-confirm best candidate
        # ------------------------------------------------------------------
        def _clarification_node(
            state: NLInterfaceState,
        ) -> dict[str, Any]:
            # No interactive HIL in CLI mode; confirm the first candidate.
            if state.candidates:
                return {
                    "confirmed": True,
                    "user_selection": 0,
                    "want_more_options": False,
                }
            return {"confirmed": True, "want_more_options": False}

        _node("candidate_generation", _candidate_generation_node)
        _node("confirmation_loop", _confirmation_loop_node)
        _node("dag_construction", _dag_construction_node)
        _node("budget_gate", _budget_gate_node)
        _node("execution", _execution_node)
        _node("clarification", _clarification_node)
    else:
        _node("candidate_generation")
        _node("confirmation_loop")
        _node("dag_construction")
        _node("budget_gate")
        _node("execution")
        _node("clarification")

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

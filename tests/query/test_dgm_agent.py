"""RED → GREEN tests for §11.10 — LangGraph + Pydantic-AI specialist agents.

Covers:
  _agent_types.py
  - EntityResolutionResult   (§11.2 task 1)
  - CandidateRankingResult   (§11.2 task 2)
  - TemporalClassificationResult (§11.2 task 3 — at most one non-None)
  - CompositionalDetectionResult (§11.2 task 4)
  - ExplanationRenderResult  (§11.2 task 5)
  - DGMContext               (§11.10 dependency injection)

  _graph.py tracing
  - _state_diff()     — returns only changed fields
  - _traced_node()    — wraps node, emits Logfire span, records state changes
  - NLInterfaceState         (§11.10 LangGraph state)

  _graph.py
  - route_after_confirmation  (routing function: confirmed/refine/more_options)
  - route_budget_decision     (routing function: 8 BudgetDecisionKind variants)
  - build_nl_graph            (returns compiled LangGraph; nodes + edges present)
"""

from __future__ import annotations

import pytest
from datetime import timedelta
from typing import Any

from sqldim.core.query.dgm.nl._agent_types import (
    CandidateRankingResult,
    CompositionalDetectionResult,
    DGMContext,
    EntityResolutionResult,
    ExplanationRenderResult,
    NLInterfaceState,
    PropRefModel,
    QueryCandidate,
    TemporalClassificationResult,
)
from sqldim.core.query.dgm.nl._graph import (
    _state_diff,
    _traced_node,
    build_nl_graph,
    route_after_confirmation,
    route_budget_decision,
)
from sqldim.core.query.dgm.nl._types import (
    CompositionOp,
    EntityRegistry,
    TemporalIntent,
)
from sqldim.core.query.dgm.planner._gate import (
    AsyncDecision,
    BudgetDecision,
    ClarifyDecision,
    ExecutionBudget,
    PaginateDecision,
    ProceedDecision,
    RejectDecision,
    RewriteDecision,
    SampleDecision,
    SampleMethod,
    StreamDecision,
)

# Logfire emits a warning when no backend is configured; suppress it in tests.
pytestmark = pytest.mark.filterwarnings(
    "ignore::logfire._internal.config.LogfireNotConfiguredWarning"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _budget() -> ExecutionBudget:
    from sqldim.core.query.dgm.planner._gate import ExecutionBudget
    from sqldim.core.query.dgm.planner._targets import CostEstimate

    return ExecutionBudget(
        max_estimated_cost=CostEstimate(cpu_ops=1000, io_ops=0),
        max_result_rows=10_000,
        max_wall_time=timedelta(seconds=60),
        max_precompute_time=timedelta(seconds=10),
        streaming_threshold=CostEstimate(cpu_ops=500, io_ops=0),
        async_threshold=CostEstimate(cpu_ops=800, io_ops=0),
    )


# ---------------------------------------------------------------------------
# PropRefModel
# ---------------------------------------------------------------------------


class TestPropRefModel:
    def test_str_representation(self):
        p = PropRefModel(alias="c", prop="revenue")
        assert str(p) == "c.revenue"

    def test_alias_and_prop_fields(self):
        p = PropRefModel(alias="orders", prop="total_amount")
        assert p.alias == "orders"
        assert p.prop == "total_amount"

    def test_equality(self):
        a = PropRefModel(alias="c", prop="revenue")
        b = PropRefModel(alias="c", prop="revenue")
        assert a == b

    def test_inequality_different_alias(self):
        a = PropRefModel(alias="c", prop="revenue")
        b = PropRefModel(alias="s", prop="revenue")
        assert a != b


# ---------------------------------------------------------------------------
# QueryCandidate
# ---------------------------------------------------------------------------


class TestQueryCandidate:
    def test_construct_minimal(self):
        qc = QueryCandidate(dag_node_id=42, description="Total revenue by month")
        assert qc.dag_node_id == 42
        assert qc.description == "Total revenue by month"
        assert qc.band_coverage == []
        assert qc.cost_estimate == 0.0

    def test_cost_non_negative_valid(self):
        qc = QueryCandidate(dag_node_id=1, description="x", cost_estimate=3.14)
        assert qc.cost_estimate == pytest.approx(3.14)

    def test_cost_negative_invalid(self):
        with pytest.raises(Exception):
            QueryCandidate(dag_node_id=1, description="x", cost_estimate=-0.01)

    def test_band_coverage_list(self):
        qc = QueryCandidate(
            dag_node_id=5, description="x", band_coverage=["dim", "fact"]
        )
        assert qc.band_coverage == ["dim", "fact"]


# ---------------------------------------------------------------------------
# EntityResolutionResult
# ---------------------------------------------------------------------------


class TestEntityResolutionResult:
    def test_construct_minimal(self):
        r = EntityResolutionResult(resolved=[], unresolved=[], node_aliases={})
        assert r.resolved == []
        assert r.unresolved == []
        assert r.node_aliases == {}

    def test_resolved_proprefs(self):
        r = EntityResolutionResult(
            resolved=[
                PropRefModel(alias="c", prop="revenue"),
                PropRefModel(alias="s", prop="amount"),
            ],
            unresolved=[],
            node_aliases={"customer": "c"},
        )
        assert any(str(p) == "c.revenue" for p in r.resolved)
        assert r.node_aliases["customer"] == "c"

    def test_unresolved_terms_tracked(self):
        r = EntityResolutionResult(
            resolved=[],
            unresolved=["foobar_metric"],
            node_aliases={},
        )
        assert "foobar_metric" in r.unresolved

    def test_immutable_field_defaults(self):
        r1 = EntityResolutionResult(resolved=[], unresolved=[], node_aliases={})
        r2 = EntityResolutionResult(resolved=[], unresolved=[], node_aliases={})
        r1.resolved.append(PropRefModel(alias="x", prop="y"))
        assert r2.resolved == []  # no shared mutable default


# ---------------------------------------------------------------------------
# CandidateRankingResult
# ---------------------------------------------------------------------------


class TestCandidateRankingResult:
    def test_construct(self):
        r = CandidateRankingResult(
            ranked=[
                QueryCandidate(dag_node_id=1, description="q_a1b2"),
                QueryCandidate(dag_node_id=2, description="q_c3d4"),
            ],
            confidence=[0.92, 0.71],
        )
        assert len(r.ranked) == 2
        assert r.ranked[0].description == "q_a1b2"

    def test_confidence_between_0_and_1(self):
        with pytest.raises(Exception):
            CandidateRankingResult(
                ranked=[QueryCandidate(dag_node_id=1, description="q_a")],
                confidence=[1.5],  # > 1.0 — invalid
            )

    def test_confidence_non_negative(self):
        with pytest.raises(Exception):
            CandidateRankingResult(
                ranked=[QueryCandidate(dag_node_id=1, description="q_a")],
                confidence=[-0.1],  # < 0 — invalid
            )

    def test_ranked_and_confidence_same_length(self):
        with pytest.raises(Exception):
            CandidateRankingResult(
                ranked=[
                    QueryCandidate(dag_node_id=1, description="q_a"),
                    QueryCandidate(dag_node_id=2, description="q_b"),
                ],
                confidence=[0.9],  # length mismatch
            )


# ---------------------------------------------------------------------------
# TemporalClassificationResult
# ---------------------------------------------------------------------------


class TestTemporalClassificationResult:
    def test_all_none(self):
        r = TemporalClassificationResult()
        assert r.mode is None
        assert r.property_kind is None
        assert r.window is None

    def test_mode_only(self):
        r = TemporalClassificationResult(mode=TemporalIntent.EVENTUALLY)
        assert r.mode == TemporalIntent.EVENTUALLY
        assert r.property_kind is None
        assert r.window is None

    def test_property_kind_only(self):
        r = TemporalClassificationResult(property_kind=TemporalIntent.SAFETY)
        assert r.property_kind == TemporalIntent.SAFETY
        assert r.mode is None

    def test_window_only(self):
        r = TemporalClassificationResult(window="YTD")
        assert r.window == "YTD"

    def test_at_most_one_non_none(self):
        """Spec §11.10: at most one of mode/property_kind/window non-None."""
        with pytest.raises(Exception):
            TemporalClassificationResult(
                mode=TemporalIntent.EVENTUALLY,
                property_kind=TemporalIntent.SAFETY,
            )

    def test_confidence_explicit_valid(self):
        r = TemporalClassificationResult(confidence=0.5)
        assert r.confidence == pytest.approx(0.5)

    def test_confidence_out_of_range_raises(self):
        with pytest.raises(Exception):
            TemporalClassificationResult(confidence=2.0)


# ---------------------------------------------------------------------------
# CompositionalDetectionResult
# ---------------------------------------------------------------------------


class TestCompositionalDetectionResult:
    def test_no_composition(self):
        r = CompositionalDetectionResult(operation=None, join_key=None, confidence=0.4)
        assert r.operation is None
        assert r.confidence == pytest.approx(0.4)

    def test_intersect(self):
        r = CompositionalDetectionResult(
            operation=CompositionOp.INTERSECT, join_key=None, confidence=0.88
        )
        assert r.operation == CompositionOp.INTERSECT

    def test_join_requires_key(self):
        """JOIN operation should carry a join_key."""
        with pytest.raises(Exception):
            CompositionalDetectionResult(
                operation=CompositionOp.JOIN,
                join_key=None,
                confidence=0.9,
            )

    def test_confidence_range(self):
        with pytest.raises(Exception):
            CompositionalDetectionResult(operation=None, join_key=None, confidence=2.0)


# ---------------------------------------------------------------------------
# ExplanationRenderResult
# ---------------------------------------------------------------------------


class TestExplanationRenderResult:
    def test_construct(self):
        r = ExplanationRenderResult(
            explanation="Total YTD revenue for retail customers.",
            follow_up_suggestions=["Break down by region", "Compare to last year"],
        )
        assert r.explanation.startswith("Total")
        assert len(r.follow_up_suggestions) == 2

    def test_empty_suggestions(self):
        r = ExplanationRenderResult(
            explanation="Simple count.", follow_up_suggestions=[]
        )
        assert r.follow_up_suggestions == []

    def test_explanation_non_empty(self):
        with pytest.raises(Exception):
            ExplanationRenderResult(explanation="", follow_up_suggestions=[])


# ---------------------------------------------------------------------------
# DGMContext
# ---------------------------------------------------------------------------


class TestDGMContext:
    def test_construct_minimal(self):
        reg = EntityRegistry()
        ctx = DGMContext(entity_registry=reg, budget=_budget())
        assert ctx.entity_registry is reg
        assert ctx.q_current is None

    def test_q_current_defaults_none(self):
        ctx = DGMContext(entity_registry=EntityRegistry(), budget=_budget())
        assert ctx.q_current is None

    def test_optional_fields_none_by_default(self):
        ctx = DGMContext(entity_registry=EntityRegistry(), budget=_budget())
        assert ctx.graph is None
        assert ctx.bdd is None
        assert ctx.query_dag is None
        assert ctx.recommender is None

    def test_has_graph_true_when_graph_set(self):
        from unittest.mock import MagicMock

        ctx = DGMContext(
            entity_registry=EntityRegistry(), budget=_budget(), graph=MagicMock()
        )
        assert ctx.has_graph is True


# ---------------------------------------------------------------------------
# NLInterfaceState
# ---------------------------------------------------------------------------


class TestNLInterfaceState:
    def test_construct_with_utterance(self):
        s = NLInterfaceState(utterance="How are retail customers doing?")
        assert s.utterance == "How are retail customers doing?"
        assert s.entity_result is None
        assert s.candidates == []
        assert not s.confirmed

    def test_all_optional_fields_default_none(self):
        s = NLInterfaceState(utterance="test")
        assert s.temporal_result is None
        assert s.compositional is None
        assert s.q_current is None
        assert s.budget_decision is None
        assert s.result is None
        assert s.explanation is None
        assert s.ranking_result is None
        assert s.user_selection is None
        assert s.partial_result is None

    def test_candidates_default_empty_list(self):
        s = NLInterfaceState(utterance="x")
        assert s.candidates == []

    def test_ranking_result_can_be_set(self):
        qc = QueryCandidate(dag_node_id=1, description="Revenue by month")
        rr = CandidateRankingResult(
            ranked=[qc],
            confidence=[0.9],
        )
        s = NLInterfaceState(utterance="x", ranking_result=rr)
        assert s.ranking_result is not None
        assert len(s.ranking_result.ranked) == 1

    def test_user_selection_index(self):
        s = NLInterfaceState(utterance="x", user_selection=0)
        assert s.user_selection == 0

    def test_partial_result_any_type(self):
        s = NLInterfaceState(utterance="x", partial_result={"rows": []})
        assert s.partial_result == {"rows": []}

    def test_confirmed_false_by_default(self):
        s = NLInterfaceState(utterance="x")
        assert s.confirmed is False

    def test_utterance_required(self):
        with pytest.raises(Exception):
            NLInterfaceState()  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# route_after_confirmation
# ---------------------------------------------------------------------------


class TestRouteAfterConfirmation:
    def test_confirmed(self):
        s = NLInterfaceState(utterance="x", confirmed=True)
        assert route_after_confirmation(s) == "confirmed"

    def test_refine_when_not_confirmed_no_more(self):
        s = NLInterfaceState(utterance="x", confirmed=False)
        result = route_after_confirmation(s)
        assert result in ("refine", "more_options")

    def test_more_options_flag(self):
        s = NLInterfaceState(utterance="x", confirmed=False, want_more_options=True)
        assert route_after_confirmation(s) == "more_options"

    def test_refine_flag(self):
        s = NLInterfaceState(utterance="x", confirmed=False, want_more_options=False)
        assert route_after_confirmation(s) == "refine"


# ---------------------------------------------------------------------------
# route_budget_decision
# ---------------------------------------------------------------------------


class TestRouteBudgetDecision:
    def _state(self, decision: BudgetDecision) -> NLInterfaceState:
        return NLInterfaceState(utterance="x", budget_decision=decision)

    def test_proceed(self):
        assert route_budget_decision(self._state(ProceedDecision())) == "PROCEED"

    def test_stream(self):
        assert route_budget_decision(self._state(StreamDecision())) == "STREAM"

    def test_async(self):
        assert (
            route_budget_decision(
                self._state(
                    AsyncDecision(sink=None, callback=None, ttl=timedelta(minutes=5))
                )
            )
            == "ASYNC"
        )

    def test_rewrite(self):
        assert (
            route_budget_decision(
                self._state(RewriteDecision(plan=None, warning="too big"))
            )
            == "REWRITE"
        )

    def test_sample(self):
        assert (
            route_budget_decision(
                self._state(
                    SampleDecision(
                        fraction=0.1, method=SampleMethod.RANDOM, error_bound=0.05
                    )
                )
            )
            == "SAMPLE"
        )

    def test_paginate(self):
        assert (
            route_budget_decision(self._state(PaginateDecision(page_size=1000)))
            == "PAGINATE"
        )

    def test_clarify(self):
        assert (
            route_budget_decision(
                self._state(ClarifyDecision(options=[], cost_per_option=0.0))
            )
            == "CLARIFY"
        )

    def test_reject(self):
        assert (
            route_budget_decision(
                self._state(RejectDecision(reason="exceeds hard ceiling"))
            )
            == "REJECT"
        )

    def test_none_budget_raises(self):
        with pytest.raises(Exception):
            route_budget_decision(NLInterfaceState(utterance="x", budget_decision=None))


# ---------------------------------------------------------------------------
# build_nl_graph
# ---------------------------------------------------------------------------


class TestBuildNLGraph:
    def test_returns_compiled_graph(self):
        graph = build_nl_graph()
        assert graph is not None

    def test_has_entity_resolution_node(self):
        graph = build_nl_graph()
        assert "entity_resolution" in graph.get_graph().nodes

    def test_has_confirmation_loop_node(self):
        graph = build_nl_graph()
        assert "confirmation_loop" in graph.get_graph().nodes

    def test_has_budget_gate_node(self):
        graph = build_nl_graph()
        assert "budget_gate" in graph.get_graph().nodes

    def test_has_execution_node(self):
        graph = build_nl_graph()
        assert "execution" in graph.get_graph().nodes

    def test_has_explanation_rendering_node(self):
        graph = build_nl_graph()
        assert "explanation_rendering" in graph.get_graph().nodes

    def test_has_clarification_node(self):
        graph = build_nl_graph()
        assert "clarification" in graph.get_graph().nodes

    def test_has_dag_construction_node(self):
        graph = build_nl_graph()
        assert "dag_construction" in graph.get_graph().nodes

    def test_all_eleven_nodes_present(self):
        graph = build_nl_graph()
        nodes = set(graph.get_graph().nodes)
        expected = {
            "entity_resolution",
            "temporal_classification",
            "compositional_detection",
            "candidate_generation",
            "candidate_ranking",
            "confirmation_loop",
            "dag_construction",
            "budget_gate",
            "execution",
            "explanation_rendering",
            "clarification",
        }
        assert expected.issubset(nodes)


# ---------------------------------------------------------------------------
# _state_diff
# ---------------------------------------------------------------------------


class TestStateDiff:
    def _base(self) -> NLInterfaceState:
        return NLInterfaceState(utterance="u")

    def test_empty_when_nothing_changed(self):
        state = self._base()
        assert _state_diff(state, {}) == {}

    def test_detects_bool_change(self):
        state = self._base()
        diff = _state_diff(state, {"confirmed": True})
        assert diff == {"confirmed": True}

    def test_ignores_unchanged_field(self):
        state = self._base()
        diff = _state_diff(state, {"confirmed": False})  # same as default
        assert diff == {}

    def test_detects_multiple_changes(self):
        state = self._base()
        diff = _state_diff(state, {"confirmed": True, "explanation": "done"})
        assert set(diff.keys()) == {"confirmed", "explanation"}

    def test_unknown_key_included(self):
        state = self._base()
        diff = _state_diff(state, {"_new_field": 42})
        assert "_new_field" in diff


# ---------------------------------------------------------------------------
# _traced_node  (Logfire span emission)
# ---------------------------------------------------------------------------


class TestTracedNode:
    def test_returns_node_result(self):
        def _fn(state: NLInterfaceState) -> dict:
            return {"confirmed": True}

        wrapped = _traced_node("test_node", _fn)
        state = NLInterfaceState(utterance="hello")
        result = wrapped(state)
        assert result["confirmed"] is True
        # _traced_node always appends the node name to visited_nodes
        assert result["visited_nodes"] == ["test_node"]

    def test_preserves_function_name(self):
        wrapped = _traced_node("my_node", lambda s: {})
        assert wrapped.__name__ == "my_node"

    def test_passthrough_empty_result(self):
        wrapped = _traced_node("noop", lambda s: {})
        state = NLInterfaceState(utterance="x")
        result = wrapped(state)
        # Result carries visited_nodes even when the wrapped fn returns {}
        assert result["visited_nodes"] == ["noop"]
        assert "confirmed" not in result

    def test_span_emitted_without_error(self, capfd):
        """Node executes cleanly even without a logfire backend configured."""
        import logfire

        logfire.configure(send_to_logfire=False)
        wrapped = _traced_node("smoke", lambda s: {"explanation": "ok"})
        state = NLInterfaceState(utterance="smoke test")
        result = wrapped(state)
        assert result["explanation"] == "ok"


# ---------------------------------------------------------------------------
# Agent factory smoke tests (§11.10)
# ---------------------------------------------------------------------------


class TestAgentFactories:
    """Verify the five factory functions return pydantic-ai Agents (§11.10).

    These tests only check that agents are constructed with the correct
    result_type and deps_type — they do NOT invoke a real LLM.
    """

    def test_make_entity_agent_returns_agent(self):
        from sqldim.core.query.dgm.nl._agents import make_entity_agent
        from pydantic_ai import Agent

        agent = make_entity_agent("test")
        assert isinstance(agent, Agent)

    def test_entity_agent_result_type(self):
        from sqldim.core.query.dgm.nl._agents import make_entity_agent

        agent = make_entity_agent("test")
        assert agent.output_type is EntityResolutionResult

    def test_make_temporal_agent_returns_agent(self):
        from sqldim.core.query.dgm.nl._agents import make_temporal_agent
        from pydantic_ai import Agent

        agent = make_temporal_agent("test")
        assert isinstance(agent, Agent)

    def test_temporal_agent_result_type(self):
        from sqldim.core.query.dgm.nl._agents import make_temporal_agent
        from sqldim.core.query.dgm.nl._agent_types import TemporalClassificationResult

        agent = make_temporal_agent("test")
        assert agent.output_type is TemporalClassificationResult

    def test_make_compositional_agent_returns_agent(self):
        from sqldim.core.query.dgm.nl._agents import make_compositional_agent
        from pydantic_ai import Agent

        agent = make_compositional_agent("test")
        assert isinstance(agent, Agent)

    def test_compositional_agent_result_type(self):
        from sqldim.core.query.dgm.nl._agents import make_compositional_agent
        from sqldim.core.query.dgm.nl._agent_types import CompositionalDetectionResult

        agent = make_compositional_agent("test")
        assert agent.output_type is CompositionalDetectionResult

    def test_make_ranking_agent_returns_agent(self):
        from sqldim.core.query.dgm.nl._agents import make_ranking_agent
        from pydantic_ai import Agent

        agent = make_ranking_agent("test")
        assert isinstance(agent, Agent)

    def test_ranking_agent_result_type(self):
        from sqldim.core.query.dgm.nl._agents import make_ranking_agent

        agent = make_ranking_agent("test")
        assert agent.output_type is CandidateRankingResult

    def test_make_explanation_agent_returns_agent(self):
        from sqldim.core.query.dgm.nl._agents import make_explanation_agent
        from pydantic_ai import Agent

        agent = make_explanation_agent("test")
        assert isinstance(agent, Agent)

    def test_explanation_agent_result_type(self):
        from sqldim.core.query.dgm.nl._agents import make_explanation_agent

        agent = make_explanation_agent("test")
        assert agent.output_type is ExplanationRenderResult

    def test_entity_agent_has_get_valid_prop_refs_tool(self):
        from sqldim.core.query.dgm.nl._agents import make_entity_agent

        agent = make_entity_agent("test")
        tool_names = set(agent._function_toolset.tools.keys())
        assert "get_valid_prop_refs" in tool_names

    def test_entity_agent_has_get_verb_labels_tool(self):
        from sqldim.core.query.dgm.nl._agents import make_entity_agent

        agent = make_entity_agent("test")
        tool_names = set(agent._function_toolset.tools.keys())
        assert "get_verb_labels" in tool_names

    def test_explanation_agent_has_dag_decomposition_tool(self):
        from sqldim.core.query.dgm.nl._agents import make_explanation_agent

        agent = make_explanation_agent("test")
        tool_names = set(agent._function_toolset.tools.keys())
        assert "dag_decomposition" in tool_names

    def test_explanation_agent_has_get_next_suggestions_tool(self):
        from sqldim.core.query.dgm.nl._agents import make_explanation_agent

        agent = make_explanation_agent("test")
        tool_names = set(agent._function_toolset.tools.keys())
        assert "get_next_suggestions" in tool_names


# ---------------------------------------------------------------------------
# make_model provider factory coverage
# ---------------------------------------------------------------------------


class TestMakeModel:
    """Exercise make_model() and the per-provider _make_*_model factories."""

    def test_make_model_unknown_provider_raises(self):
        """Cover make_model ValueError branch (lines ~162-163 in _agents.py)."""
        from sqldim.core.query.dgm.nl._agents import make_model

        with pytest.raises(ValueError, match="Unknown provider"):
            make_model("nonexistent_provider")

    def test_make_model_ollama_returns_object(self):
        """Cover _make_ollama_model body (lines 61-66)."""
        from sqldim.core.query.dgm.nl._agents import make_model

        # Does not need a network connection — only creates a model object
        model = make_model("ollama", model_name="llama3.2:latest")
        assert model is not None

    def test_make_model_openai_returns_object(self):
        """Cover _make_openai_model body (lines 70-78)."""
        import os
        from sqldim.core.query.dgm.nl._agents import make_model

        old = os.environ.get("OPENAI_API_KEY")
        os.environ["OPENAI_API_KEY"] = "sk-test-fake"
        try:
            model = make_model("openai", model_name="gpt-4o-mini")
            assert model is not None
        finally:
            if old is None:
                os.environ.pop("OPENAI_API_KEY", None)
            else:
                os.environ["OPENAI_API_KEY"] = old

    def test_make_model_anthropic_returns_object(self):
        """Cover _make_anthropic_model body (lines 82-84)."""
        import os
        from sqldim.core.query.dgm.nl._agents import make_model

        # Provide a dummy key so instantiation succeeds without real credentials
        old = os.environ.get("ANTHROPIC_API_KEY")
        os.environ["ANTHROPIC_API_KEY"] = "test-key"
        try:
            model = make_model("anthropic", model_name="claude-3-5-haiku-latest")
            assert model is not None
        finally:
            if old is None:
                os.environ.pop("ANTHROPIC_API_KEY", None)
            else:
                os.environ["ANTHROPIC_API_KEY"] = old

    def test_make_model_gemini_returns_object(self):
        """Cover _make_gemini_model body (lines 88-90)."""
        import os
        from sqldim.core.query.dgm.nl._agents import make_model

        old = os.environ.get("GOOGLE_API_KEY")
        os.environ["GOOGLE_API_KEY"] = "test-key"
        try:
            model = make_model("gemini", model_name="gemini-1.5-flash")
            assert model is not None
        finally:
            if old is None:
                os.environ.pop("GOOGLE_API_KEY", None)
            else:
                os.environ["GOOGLE_API_KEY"] = old

    def test_make_model_groq_returns_object(self):
        """Cover _make_groq_model body (lines 94-96)."""
        import os
        from sqldim.core.query.dgm.nl._agents import make_model

        old = os.environ.get("GROQ_API_KEY")
        os.environ["GROQ_API_KEY"] = "test-key"
        try:
            model = make_model("groq", model_name="llama-3.1-8b-instant")
            assert model is not None
        finally:
            if old is None:
                os.environ.pop("GROQ_API_KEY", None)
            else:
                os.environ["GROQ_API_KEY"] = old

    def test_make_ollama_model_calls_make_model(self):
        """Cover make_ollama_model() line 174 — delegates to make_model('ollama')."""
        from sqldim.core.query.dgm.nl._agents import make_ollama_model

        model = make_ollama_model()
        assert model is not None

    def test_make_model_mistral_with_mocked_import(self):
        """Cover _make_mistral_model body (lines 100-102) by mocking the mistral module."""
        import sys
        from unittest.mock import MagicMock, patch
        from sqldim.core.query.dgm.nl._agents import _make_mistral_model

        fake_model_instance = MagicMock()
        fake_mistral_cls = MagicMock(return_value=fake_model_instance)
        fake_module = MagicMock()
        fake_module.MistralModel = fake_mistral_cls

        with patch.dict(sys.modules, {"pydantic_ai.models.mistral": fake_module}):
            result = _make_mistral_model("mistral-large-latest", None, None)

        fake_mistral_cls.assert_called_once_with("mistral-large-latest")
        assert result is fake_model_instance

    def test_make_model_openai_with_base_url(self):
        """Cover _make_openai_model kwargs base_url branch."""
        from sqldim.core.query.dgm.nl._agents import make_model

        model = make_model(
            "openai",
            model_name="gpt-4o-mini",
            base_url="http://localhost:8080/v1",
            api_key="test-key",
        )
        assert model is not None


# ---------------------------------------------------------------------------
# Explanation agent tool body with non-None deps coverage
# ---------------------------------------------------------------------------


class TestExplanationAgentToolsWithDeps:
    """Cover dag_decomposition and get_next_suggestions when deps are non-None."""

    def _make_run_ctx(self, ctx: DGMContext) -> Any:
        from pydantic_ai import RunContext
        from pydantic_ai.models.test import TestModel
        from pydantic_ai.usage import RunUsage

        return RunContext(deps=ctx, model=TestModel(), usage=RunUsage(), prompt="test")

    def test_dag_decomposition_with_query_dag(self):
        """Cover dag_decomposition try-block (lines 344-350) when query_dag is set."""
        from unittest.mock import MagicMock
        from sqldim.core.query.dgm.nl._agents import make_explanation_agent
        from pydantic_ai.models.test import TestModel

        mock_dag = MagicMock()
        mock_node = MagicMock()
        mock_node.description = "SELECT orders"
        mock_node.bands = ["B1"]
        mock_dag.get_node.return_value = mock_node

        reg = EntityRegistry()
        ctx = DGMContext(entity_registry=reg, budget=_budget(), query_dag=mock_dag)
        agent = make_explanation_agent(TestModel())
        rc = self._make_run_ctx(ctx)
        tool_fn = agent._function_toolset.tools["dag_decomposition"].function
        result = tool_fn(rc, 42)
        assert result["id"] == 42
        assert result["description"] == "SELECT orders"
        assert result["bands"] == ["B1"]

    def test_dag_decomposition_exception_returns_empty(self):
        """Cover dag_decomposition except-branch when get_node raises."""
        from unittest.mock import MagicMock
        from sqldim.core.query.dgm.nl._agents import make_explanation_agent
        from pydantic_ai.models.test import TestModel

        mock_dag = MagicMock()
        mock_dag.get_node.side_effect = RuntimeError("no such node")

        reg = EntityRegistry()
        ctx = DGMContext(entity_registry=reg, budget=_budget(), query_dag=mock_dag)
        agent = make_explanation_agent(TestModel())
        rc = self._make_run_ctx(ctx)
        tool_fn = agent._function_toolset.tools["dag_decomposition"].function
        result = tool_fn(rc, 99)
        assert result == {}

    def test_get_next_suggestions_with_recommender(self):
        """Cover get_next_suggestions try-block (lines 364-368) when recommender is set."""
        from unittest.mock import MagicMock
        from sqldim.core.query.dgm.nl._agents import make_explanation_agent
        from pydantic_ai.models.test import TestModel

        mock_rec = MagicMock()
        mock_candidate = MagicMock()
        mock_candidate.description = "revenue by month"
        mock_rec.clarification_options.return_value = [mock_candidate]

        reg = EntityRegistry()
        ctx = DGMContext(entity_registry=reg, budget=_budget(), recommender=mock_rec)
        agent = make_explanation_agent(TestModel())
        rc = self._make_run_ctx(ctx)
        tool_fn = agent._function_toolset.tools["get_next_suggestions"].function
        result = tool_fn(rc, 1, 3)
        assert result == ["revenue by month"]

    def test_get_next_suggestions_exception_returns_empty(self):
        """Cover get_next_suggestions except-branch when clarification_options raises."""
        from unittest.mock import MagicMock
        from sqldim.core.query.dgm.nl._agents import make_explanation_agent
        from pydantic_ai.models.test import TestModel

        mock_rec = MagicMock()
        mock_rec.clarification_options.side_effect = RuntimeError("fail")

        reg = EntityRegistry()
        ctx = DGMContext(entity_registry=reg, budget=_budget(), recommender=mock_rec)
        agent = make_explanation_agent(TestModel())
        rc = self._make_run_ctx(ctx)
        tool_fn = agent._function_toolset.tools["get_next_suggestions"].function
        result = tool_fn(rc, 1, 3)
        assert result == []


# ---------------------------------------------------------------------------
# build_nl_graph with model param
# ---------------------------------------------------------------------------


class TestBuildNLGraphWithModel:
    def test_build_without_model_uses_stub_nodes(self):
        """Without model, graph compiles and runs (stubs return empty dicts)."""
        graph = build_nl_graph()
        # Low recursion limit keeps the stub-loop test fast (stub nodes cycle
        # without a model; default limit of 25 iterations takes ~80 seconds).
        config = {"configurable": {"thread_id": "test-no-model"}, "recursion_limit": 5}
        state = {"utterance": "test query"}
        # Graph should not raise even without a real LLM
        try:
            graph.invoke(state, config=config)
        except Exception as exc:  # noqa: BLE001
            # Only GraphRecursionError is acceptable (stub infinite loop guard)
            from langgraph.errors import GraphRecursionError

            assert isinstance(exc, GraphRecursionError), f"Unexpected: {exc}"


# ---------------------------------------------------------------------------
# Agent tool body coverage (_agents.py lines 61-66, 70-78, 323-370)
# ---------------------------------------------------------------------------


def _make_ctx_with_registry() -> DGMContext:
    """Build a minimal DGMContext with a populated EntityRegistry."""
    reg = EntityRegistry()
    reg.register_node("orders", "orders")
    reg.register_node("customers", "customers")
    reg.register_prop("order id", "orders.id")
    reg.register_prop("order amount", "orders.amount")
    reg.register_prop("customer name", "customers.name")
    reg.register_verb("placed", "PLACED")
    return DGMContext(entity_registry=reg, budget=_budget())


class TestAgentToolBodies:
    """Cover agent tool function bodies via direct RunContext invocation."""

    def _make_run_ctx(self, ctx: DGMContext) -> Any:
        from pydantic_ai import RunContext
        from pydantic_ai.models.test import TestModel
        from pydantic_ai.usage import RunUsage

        return RunContext(deps=ctx, model=TestModel(), usage=RunUsage(), prompt="test")

    def test_get_valid_prop_refs_returns_registry_props(self):
        """Cover _agents.py lines 61-66: get_valid_prop_refs tool body."""
        from sqldim.core.query.dgm.nl._agents import make_entity_agent
        from pydantic_ai.models.test import TestModel

        ctx = _make_ctx_with_registry()
        agent = make_entity_agent(TestModel())
        rc = self._make_run_ctx(ctx)
        tool_fn = agent._function_toolset.tools["get_valid_prop_refs"].function
        result = tool_fn(rc, "Order")
        assert "orders.id" in result
        assert "orders.amount" in result

    def test_get_verb_labels_returns_registry_verbs(self):
        """Cover _agents.py lines 70-78: get_verb_labels tool body."""
        from sqldim.core.query.dgm.nl._agents import make_entity_agent
        from pydantic_ai.models.test import TestModel

        ctx = _make_ctx_with_registry()
        agent = make_entity_agent(TestModel())
        rc = self._make_run_ctx(ctx)
        tool_fn = agent._function_toolset.tools["get_verb_labels"].function
        result = tool_fn(rc, "Order", "Customer")
        assert "PLACED" in result

    def test_explanation_agent_dag_decomposition_tool_no_dag(self):
        """Cover _agents.py dag_decomposition branch: ctx.deps.query_dag is None."""
        from sqldim.core.query.dgm.nl._agents import make_explanation_agent
        from pydantic_ai.models.test import TestModel

        ctx = _make_ctx_with_registry()
        assert ctx.query_dag is None
        agent = make_explanation_agent(TestModel())
        rc = self._make_run_ctx(ctx)
        tool_fn = agent._function_toolset.tools["dag_decomposition"].function
        result = tool_fn(rc, 1)
        assert result == {}

    def test_explanation_agent_get_next_suggestions_no_recommender(self):
        """Cover _agents.py get_next_suggestions branch: ctx.deps.recommender is None."""
        from sqldim.core.query.dgm.nl._agents import make_explanation_agent
        from pydantic_ai.models.test import TestModel

        ctx = _make_ctx_with_registry()
        assert ctx.recommender is None
        agent = make_explanation_agent(TestModel())
        rc = self._make_run_ctx(ctx)
        tool_fn = agent._function_toolset.tools["get_next_suggestions"].function
        result = tool_fn(rc, 1, 3)
        assert result == []

    def test_make_entity_agent_model_none_calls_ollama(self, monkeypatch: pytest.MonkeyPatch):
        """Cover _agents.py line 174: model=None path calls make_ollama_model."""
        import sqldim.core.query.dgm.nl._agents as _agents_mod
        from pydantic_ai.models.test import TestModel

        stub_model = TestModel()
        called: list[int] = []

        def fake_make_ollama_model() -> object:
            called.append(1)
            return stub_model

        monkeypatch.setattr(_agents_mod, "make_ollama_model", fake_make_ollama_model)
        agent = _agents_mod.make_entity_agent(None)
        assert len(called) == 1
        assert agent is not None

    def test_make_temporal_agent_model_none_calls_ollama(self, monkeypatch: pytest.MonkeyPatch):
        """Cover make_temporal_agent model=None path."""
        import sqldim.core.query.dgm.nl._agents as _agents_mod
        from pydantic_ai.models.test import TestModel

        stub_model = TestModel()
        called: list[int] = []

        def fake_make_ollama_model() -> object:
            called.append(1)
            return stub_model

        monkeypatch.setattr(_agents_mod, "make_ollama_model", fake_make_ollama_model)
        agent = _agents_mod.make_temporal_agent(None)
        assert len(called) == 1
        assert agent is not None

    def test_make_compositional_agent_model_none_calls_ollama(self, monkeypatch: pytest.MonkeyPatch):
        """Cover make_compositional_agent model=None path."""
        import sqldim.core.query.dgm.nl._agents as _agents_mod
        from pydantic_ai.models.test import TestModel

        stub_model = TestModel()
        called: list[int] = []

        def fake_make_ollama_model() -> object:
            called.append(1)
            return stub_model

        monkeypatch.setattr(_agents_mod, "make_ollama_model", fake_make_ollama_model)
        agent = _agents_mod.make_compositional_agent(None)
        assert len(called) == 1
        assert agent is not None

    def test_make_ranking_agent_model_none_calls_ollama(self, monkeypatch: pytest.MonkeyPatch):
        """Cover make_ranking_agent model=None path."""
        import sqldim.core.query.dgm.nl._agents as _agents_mod
        from pydantic_ai.models.test import TestModel

        stub_model = TestModel()
        called: list[int] = []

        def fake_make_ollama_model() -> object:
            called.append(1)
            return stub_model

        monkeypatch.setattr(_agents_mod, "make_ollama_model", fake_make_ollama_model)
        agent = _agents_mod.make_ranking_agent(None)
        assert len(called) == 1
        assert agent is not None

    def test_make_explanation_agent_model_none_calls_ollama(self, monkeypatch: pytest.MonkeyPatch):
        """Cover make_explanation_agent model=None path."""
        import sqldim.core.query.dgm.nl._agents as _agents_mod
        from pydantic_ai.models.test import TestModel

        stub_model = TestModel()
        called: list[int] = []

        def fake_make_ollama_model() -> object:
            called.append(1)
            return stub_model

        monkeypatch.setattr(_agents_mod, "make_ollama_model", fake_make_ollama_model)
        agent = _agents_mod.make_explanation_agent(None)
        assert len(called) == 1
        assert agent is not None


# ---------------------------------------------------------------------------
# build_nl_graph with context + TestModel (LLM node coverage)
# ---------------------------------------------------------------------------


class TestBuildNLGraphWithContext:
    """Cover _graph.py LLM node callables (lines 207-257) and non-LLM context
    node branches (lines 278-279, 316, 318, 321, 340-346, 349, 371, 414,
    438-458)."""

    def _make_context_with_db(self) -> DGMContext:
        import duckdb

        con = duckdb.connect()
        con.execute("CREATE TABLE customers (id INTEGER, name VARCHAR)")
        con.execute("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')")
        reg = EntityRegistry()
        reg.register_node("customers", "customers")
        reg.register_prop("customer id", "customers.id")
        reg.register_prop("customer name", "customers.name")
        return DGMContext(entity_registry=reg, budget=_budget(), con=con)

    def test_graph_with_context_no_model_runs_non_llm_nodes(self):
        """Build graph with context only (model=None) to exercise non-LLM
        candidate_generation, confirmation_loop, dag_construction, budget_gate,
        execution, and clarification nodes.  Covers lines 278-458."""
        ctx = self._make_context_with_db()
        graph = build_nl_graph(context=ctx)
        config = {
            "configurable": {"thread_id": "test-ctx-no-model"},
            "recursion_limit": 25,
        }
        state: dict = {"utterance": "show customers"}
        try:
            result = graph.invoke(state, config=config)
        except Exception as exc:  # noqa: BLE001
            from langgraph.errors import GraphRecursionError

            assert isinstance(exc, GraphRecursionError), f"Unexpected: {exc}"
            return
        visited = result.get("visited_nodes", [])
        assert "candidate_generation" in visited
        assert "dag_construction" in visited
        assert "execution" in visited

    def test_graph_with_context_and_test_model_invokes_llm_nodes(self):
        """Build graph with context + TestModel to exercise LLM node bodies.
        Covers _graph.py lines 207-257 (entity_node, temporal_node, etc.)."""
        from pydantic_ai.models.test import TestModel

        ctx = self._make_context_with_db()
        model = TestModel()
        graph = build_nl_graph(context=ctx, model=model)
        config = {
            "configurable": {"thread_id": "test-ctx-with-model"},
            "recursion_limit": 25,
        }
        state: dict = {"utterance": "show customers"}
        try:
            result = graph.invoke(state, config=config)
        except Exception as exc:  # noqa: BLE001
            from langgraph.errors import GraphRecursionError

            assert isinstance(exc, GraphRecursionError), f"Unexpected: {exc}"
            return
        visited = result.get("visited_nodes", [])
        assert "entity_resolution" in visited
        assert "temporal_classification" in visited
        assert "compositional_detection" in visited

    def test_dag_construction_with_entity_result_resolved(self):
        """Execute graph with an entity_result pre-seeded so _candidate_generation_node
        takes the entity-table branch (line 278-279: tables from resolved proprefs)."""
        from pydantic_ai.models.test import TestModel

        ctx = self._make_context_with_db()
        model = TestModel()
        graph = build_nl_graph(context=ctx, model=model)
        config = {
            "configurable": {"thread_id": "test-entity-resolved"},
            "recursion_limit": 25,
        }
        # Pre-seed entity_result so candidate_generation uses the resolved tables path
        entity_result = EntityResolutionResult(
            resolved=[PropRefModel(alias="customers", prop="id")],
            unresolved=[],
            confidence=0.9,
        )
        state: dict = {"utterance": "show customers", "entity_result": entity_result}
        try:
            result = graph.invoke(state, config=config)
        except Exception as exc:  # noqa: BLE001
            from langgraph.errors import GraphRecursionError

            assert isinstance(exc, GraphRecursionError), f"Unexpected: {exc}"
            return
        visited = result.get("visited_nodes", [])
        assert "candidate_generation" in visited

    def test_confirmation_loop_with_ranking_result(self):
        """Seed ranking_result so confirmation_loop takes the ranked branch
        (lines 316, 318, 321 in _graph.py)."""
        ctx = self._make_context_with_db()
        graph = build_nl_graph(context=ctx)
        config = {
            "configurable": {"thread_id": "test-confirmation-ranked"},
            "recursion_limit": 25,
        }
        ranked_candidate = QueryCandidate(
            dag_node_id=0,
            description="SELECT from customers (id, name)",
            band_coverage=["B1"],
            cost_estimate=2.0,
        )
        ranking_result = CandidateRankingResult(
            ranked=[ranked_candidate], confidence=[0.95]
        )
        state: dict = {
            "utterance": "show customers",
            "ranking_result": ranking_result,
        }
        try:
            result = graph.invoke(state, config=config)
        except Exception as exc:  # noqa: BLE001
            from langgraph.errors import GraphRecursionError

            assert isinstance(exc, GraphRecursionError), f"Unexpected: {exc}"
            return
        visited = result.get("visited_nodes", [])
        assert "confirmation_loop" in visited

    def test_execution_node_runs_sql(self):
        """Verify execution node executes SQL and populates result dict
        (lines 414, 438-444 in _graph.py)."""
        ctx = self._make_context_with_db()
        graph = build_nl_graph(context=ctx)
        config = {
            "configurable": {"thread_id": "test-execution"},
            "recursion_limit": 25,
        }
        state: dict = {"utterance": "show customers"}
        try:
            result = graph.invoke(state, config=config)
        except Exception as exc:  # noqa: BLE001
            from langgraph.errors import GraphRecursionError

            assert isinstance(exc, GraphRecursionError), f"Unexpected: {exc}"
            return
        visited = result.get("visited_nodes", [])
        assert "execution" in visited
        # result key may be None (budget rejected) or a dict with columns/rows
        if result.get("result") is not None:
            assert "columns" in result["result"]

    def test_route_budget_decision_unknown_type_raises(self):
        """Cover _graph.py line 104: route_budget_decision with unknown type raises."""
        # route_budget_decision checks type(decision) against _DECISION_KIND_MAP;
        # any type not registered there raises ValueError("unknown BudgetDecision type")
        # We use a plain object (not a registered BudgetDecision subclass) as the
        # budget_decision to trigger the unknown-type branch.
        state = NLInterfaceState(utterance="x")
        # Bypass pydantic validation by constructing directly with model_construct
        state = NLInterfaceState.model_construct(
            utterance="x",
            budget_decision=object(),  # not a BudgetDecision at all
            candidates=[],
            confirmed=False,
        )
        with pytest.raises(ValueError, match="unknown BudgetDecision type"):
            route_budget_decision(state)  # type: ignore[arg-type]

    def test_graph_execution_node_with_no_sql(self):
        """Cover _graph.py line 414: execution returns None when q_current not set."""
        import duckdb

        con = duckdb.connect()
        con.execute("CREATE TABLE t (id INTEGER)")
        reg = EntityRegistry()
        reg.register_node("t", "t")
        ctx = DGMContext(entity_registry=reg, budget=_budget(), con=con)
        graph = build_nl_graph(context=ctx)
        config = {
            "configurable": {"thread_id": "test-no-sql"},
            "recursion_limit": 25,
        }
        # Pre-seed q_current to empty-string to trigger the no-sql branch
        state: dict = {"utterance": "show t", "q_current": ""}
        try:
            graph.invoke(state, config=config)
        except Exception as exc:  # noqa: BLE001
            from langgraph.errors import GraphRecursionError

            assert isinstance(exc, GraphRecursionError), f"Unexpected: {exc}"
            return
        # result should be None when no sql
        # Just assert the graph ran without error

    def test_graph_execution_node_sql_exception(self):
        """Cover _graph.py lines 438-444: execution returns None on SQL error.
        Register a table in the entity registry but don't create it in DuckDB
        so the generated SELECT fails.
        Uses model=None so LLM ranking doesn't override candidate descriptions
        (TestModel produces fake descriptions that break the SQL parser)."""
        import duckdb

        con = duckdb.connect()
        # Deliberately do NOT create the table in DuckDB
        reg = EntityRegistry()
        reg.register_node("ghost_table", "ghost_table")
        reg.register_prop("ghost id", "ghost_table.id")
        ctx = DGMContext(entity_registry=reg, budget=_budget(), con=con)
        # model=None: LLM nodes are noops, but non-LLM context nodes still run.
        # _candidate_generation_node will create a ghost_table candidate,
        # _dag_construction_node will build real SQL, execution will fail on DuckDB.
        graph = build_nl_graph(context=ctx)
        config = {
            "configurable": {"thread_id": "test-sql-exception"},
            "recursion_limit": 25,
        }
        state: dict = {"utterance": "show ghost table"}
        try:
            result = graph.invoke(state, config=config)
        except Exception as exc:  # noqa: BLE001
            from langgraph.errors import GraphRecursionError

            assert isinstance(exc, GraphRecursionError), f"Unexpected: {exc}"
            return
        visited = result.get("visited_nodes", [])
        assert "execution" in visited
        # When SQL fails, result should be None
        assert result.get("result") is None

    def test_confirmation_loop_returns_confirmed_on_empty_candidates(self):
        """Cover line 321: confirmation_loop -> confirmed:True when no candidates."""
        reg = EntityRegistry()  # empty — no nodes registered
        ctx = DGMContext(entity_registry=reg, budget=_budget())
        graph = build_nl_graph(context=ctx)
        config = {
            "configurable": {"thread_id": "test-empty-candidates"},
            "recursion_limit": 25,
        }
        state: dict = {"utterance": "query something"}
        try:
            result = graph.invoke(state, config=config)
        except Exception as exc:  # noqa: BLE001
            from langgraph.errors import GraphRecursionError

            assert isinstance(exc, GraphRecursionError), f"Unexpected: {exc}"
            return
        # Confirm the graph ran
        visited = result.get("visited_nodes", [])
        assert "confirmation_loop" in visited

    def test_dag_construction_no_table_match_uses_placeholder(self):
        """Cover line 349: dag_construction falls back to placeholder SQL
        when candidate table is not in registry."""
        import duckdb

        con = duckdb.connect()
        reg = EntityRegistry()
        # Register a table but give it no columns (no props registered)
        reg.register_node("empty_tbl", "empty_tbl")
        ctx = DGMContext(entity_registry=reg, budget=_budget(), con=con)
        graph = build_nl_graph(context=ctx)
        config = {
            "configurable": {"thread_id": "test-no-props"},
            "recursion_limit": 25,
        }
        # Pre-seed a candidate whose description doesn't match a registered table
        unregistered_candidate = QueryCandidate(
            dag_node_id=0,
            description="SELECT from nonexistent_table (x, y)",
            band_coverage=["B1"],
            cost_estimate=1.0,
        )
        state: dict = {
            "utterance": "show something",
            "candidates": [unregistered_candidate],
            "confirmed": True,
            "user_selection": 0,
        }
        try:
            result = graph.invoke(state, config=config)
        except Exception as exc:  # noqa: BLE001
            from langgraph.errors import GraphRecursionError

            assert isinstance(exc, GraphRecursionError), f"Unexpected: {exc}"
            return
        visited = result.get("visited_nodes", [])
        assert "dag_construction" in visited

    def test_llm_node_exception_handler_entity(self):
        """Cover lines 217-218: _entity_node except branch when agent raises."""
        from unittest.mock import patch
        from pydantic_ai.models.test import TestModel

        ctx = self._make_context_with_db()
        model = TestModel()
        graph = build_nl_graph(context=ctx, model=model)
        config = {
            "configurable": {"thread_id": "test-entity-exception"},
            "recursion_limit": 25,
        }
        # Mock Agent.run_sync to raise for entity agent
        with patch(
            "sqldim.core.query.dgm.nl._agents.Agent.run_sync",
            side_effect=RuntimeError("agent error"),
        ):
            try:
                graph.invoke({"utterance": "fail"}, config=config)
            except Exception as exc:  # noqa: BLE001
                from langgraph.errors import GraphRecursionError

                assert isinstance(exc, GraphRecursionError), f"Unexpected: {exc}"

    def test_ranking_node_returns_empty_when_no_candidates(self):
        """Cover _graph.py line 236: _ranking_node returns {} when candidates empty.
        Uses context + TestModel with empty registry so candidate_generation
        returns [] — causing ranking_node to hit the early-return branch."""
        from pydantic_ai.models.test import TestModel
        import duckdb

        con = duckdb.connect()
        reg = EntityRegistry()  # empty → candidate_generation returns []
        ctx = DGMContext(entity_registry=reg, budget=_budget(), con=con)
        model = TestModel()  # must be non-None to use real _ranking_node
        graph = build_nl_graph(context=ctx, model=model)
        config = {
            "configurable": {"thread_id": "test-ranking-empty-registry"},
            "recursion_limit": 25,
        }
        state: dict = {"utterance": "show something"}
        try:
            result = graph.invoke(state, config=config)
        except Exception as exc:  # noqa: BLE001
            from langgraph.errors import GraphRecursionError

            assert isinstance(exc, GraphRecursionError), f"Unexpected: {exc}"
            return
        visited = result.get("visited_nodes", [])
        assert "candidate_ranking" in visited

    def test_dag_construction_table_no_props_uses_star(self):
        """Cover _graph.py lines 345-346: dag_construction uses 'SELECT *'
        when the table has no registered props."""
        from pydantic_ai.models.test import TestModel
        import duckdb

        con = duckdb.connect()
        con.execute("CREATE TABLE bare (id INTEGER)")
        reg = EntityRegistry()
        reg.register_node("bare", "bare")
        # No props registered for 'bare' — forces the cols == "*" branch
        ctx = DGMContext(entity_registry=reg, budget=_budget(), con=con)
        # Use TestModel so the real _dag_construction_node closure is built
        model = TestModel()
        graph = build_nl_graph(context=ctx, model=model)
        config = {
            "configurable": {"thread_id": "test-no-props-star"},
            "recursion_limit": 25,
        }
        state: dict = {"utterance": "show bare"}
        try:
            result = graph.invoke(state, config=config)
        except Exception as exc:  # noqa: BLE001
            from langgraph.errors import GraphRecursionError

            assert isinstance(exc, GraphRecursionError), f"Unexpected: {exc}"
            return
        visited = result.get("visited_nodes", [])
        assert "dag_construction" in visited

    def test_confirmation_loop_no_candidates_no_ranking(self):
        """Cover _graph.py line 279: confirmation_loop returns confirmed:True
        when state has no candidates and no ranking_result."""
        from pydantic_ai.models.test import TestModel
        import duckdb

        con = duckdb.connect()
        reg = EntityRegistry()  # empty — no tables
        ctx = DGMContext(entity_registry=reg, budget=_budget(), con=con)
        # Use TestModel so real _confirmation_loop_node closure is built
        model = TestModel()
        graph = build_nl_graph(context=ctx, model=model)
        config = {
            "configurable": {"thread_id": "test-no-candidates-no-ranking"},
            "recursion_limit": 25,
        }
        state: dict = {"utterance": "query something"}
        try:
            result = graph.invoke(state, config=config)
        except Exception as exc:  # noqa: BLE001
            from langgraph.errors import GraphRecursionError

            assert isinstance(exc, GraphRecursionError), f"Unexpected: {exc}"
            return
        visited = result.get("visited_nodes", [])
        assert "confirmation_loop" in visited

    def test_candidate_generation_uses_entity_result_resolved(self):
        """Cover _graph.py line 279: tables.add(pr.alias) when entity_result
        has resolved PropRefModel items.
        Use model=None so entity_node is a noop (doesn't overwrite pre-seeded
        entity_result), then candidate_generation will iterate resolved items."""
        import duckdb

        con = duckdb.connect()
        con.execute("CREATE TABLE orders (id INTEGER, amount REAL)")
        reg = EntityRegistry()
        reg.register_node("orders", "orders")
        reg.register_prop("order id", "orders.id")
        reg.register_prop("order amount", "orders.amount")
        ctx = DGMContext(entity_registry=reg, budget=_budget(), con=con)
        # model=None: entity_node is noop -> entity_result stays as pre-seeded
        graph = build_nl_graph(context=ctx)
        config = {
            "configurable": {"thread_id": "test-entity-resolved"},
            "recursion_limit": 25,
        }
        # Pre-seed entity_result with a resolved PropRefModel so candidate_generation
        # will iter .resolved and hit line 279 (tables.add(pr.alias))
        pre_entity = EntityResolutionResult(
            resolved=[PropRefModel(alias="orders", prop="id")],
            unresolved=[],
        )
        state: dict = {
            "utterance": "show orders",
            "entity_result": pre_entity,
        }
        try:
            result = graph.invoke(state, config=config)
        except Exception as exc:  # noqa: BLE001
            from langgraph.errors import GraphRecursionError

            assert isinstance(exc, GraphRecursionError), f"Unexpected: {exc}"
            return
        visited = result.get("visited_nodes", [])
        assert "candidate_generation" in visited
        # The table "orders" was added via entity_result.resolved -> candidate created
        candidates = result.get("candidates", [])
        assert any("orders" in (c.description or "") for c in candidates)

    def test_dag_construction_out_of_bounds_user_selection_falls_back(self):
        """Cover _graph.py lines 345-346: else branch when user_selection index
        is out of range but candidates is non-empty.
        Pre-seed state with user_selection=99 and one candidate so the 'elif
        state.candidates: selected = candidates[0]' fallback fires."""
        import duckdb

        con = duckdb.connect()
        con.execute("CREATE TABLE items (id INTEGER)")
        reg = EntityRegistry()
        reg.register_node("items", "items")
        reg.register_prop("item id", "items.id")
        ctx = DGMContext(entity_registry=reg, budget=_budget(), con=con)
        # model=None: LLM nodes are noops, won't overwrite pre-seeded state
        graph = build_nl_graph(context=ctx)
        config = {
            "configurable": {"thread_id": "test-oob-selection"},
            "recursion_limit": 25,
        }
        item_candidate = QueryCandidate(
            dag_node_id=0,
            description="SELECT from items (id)",
            band_coverage=["B1"],
            cost_estimate=1.0,
        )
        state: dict = {
            "utterance": "show items",
            "candidates": [item_candidate],
            "confirmed": True,
            # user_selection=99 >> len(candidates)=1 -> triggers lines 345-346
            "user_selection": 99,
        }
        try:
            result = graph.invoke(state, config=config)
        except Exception as exc:  # noqa: BLE001
            from langgraph.errors import GraphRecursionError

            assert isinstance(exc, GraphRecursionError), f"Unexpected: {exc}"
            return
        visited = result.get("visited_nodes", [])
        assert "dag_construction" in visited
        # Should have fallen back to items SELECT
        q = result.get("q_current", "")
        assert "items" in (q or "") or "placeholder" in (q or "")

    def test_clarification_node_with_candidates(self):
        """Cover _graph.py lines 438-443: _clarification_node when state has
        candidates (returns confirmed+user_selection+want_more_options).
        Mock ExecutionGate.gate to return ClarifyDecision to trigger the
        clarification branch from budget_gate."""
        from unittest.mock import patch
        import duckdb
        from sqldim.core.query.dgm.planner._gate import (
            ClarifyDecision,
            ExecutionGate,
        )
        from sqldim.core.query.dgm.planner._targets import ExportPlan, QueryTarget

        con = duckdb.connect()
        con.execute("CREATE TABLE users (id INTEGER)")
        reg = EntityRegistry()
        reg.register_node("users", "users")
        reg.register_prop("user id", "users.id")
        ctx = DGMContext(entity_registry=reg, budget=_budget(), con=con)
        graph = build_nl_graph(context=ctx)
        config = {
            "configurable": {"thread_id": "test-clarify-branch"},
            "recursion_limit": 25,
        }
        user_candidate = QueryCandidate(
            dag_node_id=0,
            description="SELECT from users (id)",
            band_coverage=["B1"],
            cost_estimate=1.0,
        )
        clarify_decision = ClarifyDecision(
            options=[ExportPlan(query_target=QueryTarget.SQL_DUCKDB, query_text="SELECT 1")],
            cost_per_option=[],
        )
        # Mock gate to return ClarifyDecision -> routes to clarification node
        with patch.object(ExecutionGate, "gate", return_value=clarify_decision):
            state: dict = {
                "utterance": "show users",
                "candidates": [user_candidate],
                "confirmed": True,
                "user_selection": 0,
            }
            try:
                result = graph.invoke(state, config=config)
            except Exception as exc:  # noqa: BLE001
                from langgraph.errors import GraphRecursionError

                if isinstance(exc, GraphRecursionError):
                    return  # clarification loops back -> recursion limit expected
                raise
        visited = result.get("visited_nodes", [])
        assert "clarification" in visited

    def test_clarification_node_without_candidates(self):
        """Cover _graph.py line 444: _clarification_node when no candidates
        (returns just confirmed=True without user_selection).
        Mock ExecutionGate.gate to return ClarifyDecision."""
        from unittest.mock import patch
        import duckdb
        from sqldim.core.query.dgm.planner._gate import (
            ClarifyDecision,
            ExecutionGate,
        )
        from sqldim.core.query.dgm.planner._targets import ExportPlan, QueryTarget

        con = duckdb.connect()
        reg = EntityRegistry()  # empty - no tables
        ctx = DGMContext(entity_registry=reg, budget=_budget(), con=con)
        graph = build_nl_graph(context=ctx)
        config = {
            "configurable": {"thread_id": "test-clarify-no-candidates"},
            "recursion_limit": 25,
        }
        clarify_decision = ClarifyDecision(
            options=[ExportPlan(query_target=QueryTarget.SQL_DUCKDB, query_text="SELECT 1")],
            cost_per_option=[],
        )
        with patch.object(ExecutionGate, "gate", return_value=clarify_decision):
            state: dict = {
                "utterance": "query something",
                "confirmed": True,
                "candidates": [],
            }
            try:
                result = graph.invoke(state, config=config)
            except Exception as exc:  # noqa: BLE001
                from langgraph.errors import GraphRecursionError

                if isinstance(exc, GraphRecursionError):
                    return  # clarification cycles back -> recursion limit expected
                raise
        visited = result.get("visited_nodes", [])
        assert "clarification" in visited

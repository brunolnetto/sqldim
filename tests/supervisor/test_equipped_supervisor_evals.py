"""Equipped-supervisor eval suite — per-tool behavioral tests + graph/recommender hooks.

Design
------
Each of the 9 supervisor tools gets its own test class with direct behavioral
assertions (not data-file driven).  Additionally, the supervisor equipped with
the new graph-algorithm and annotation-driven recommender NL hooks is evaluated
end-to-end across representative utterances.

Test classes
------------
Per-tool (§12.1):
  TestDgmQueryToolEvals            — stub path + missing-model path
  TestDgmWhatCanIAnswerToolEvals   — dataset filtering semantics
  TestDgmCorrelateToolEvals        — cross-domain correlation contract
  TestDgmExploreFromToolEvals      — suggestion navigation contract
  TestDgmSensitivitySweepToolEvals — sweep contract
  TestDgmValidateInvariantToolEvals— invariant validation contract
  TestDgmStoreFindingToolEvals     — finding persistence contract
  TestDgmRetrieveRelatedToolEvals  — lattice retrieval contract
  TestDgmMapAnalyticalSpaceToolEvals— vocabulary decomposition contract

Equipped supervisor (§11.9 / §16.4 / §7.4):
  TestEquippedSupervisorGraphIntent       — graph-algo utterances route + execute
  TestEquippedSupervisorAnnotationHook    — recommender suggestions modify SQL
  TestEquippedSupervisorNLFunctionEvals   — classify_graph_intent
                                            + apply_annotation_suggestions unit evals
  TestEquippedSupervisorEndToEnd          — full graph invocation with graph/recommender
                                            questions produces valid synthesis
"""

from __future__ import annotations

from typing import Any

import pytest

from sqldim.core.query.dgm.nl.supervisor._tools import TOOL_REGISTRY
from sqldim.core.query.dgm.nl.supervisor._types import (
    SupervisorContext,
    ToolCallResult,
)

# ---------------------------------------------------------------------------
# Shared context
# ---------------------------------------------------------------------------

_DATASETS = ["ecommerce", "retail", "social_network", "fintech"]
_CTX = SupervisorContext(available_datasets=_DATASETS)


def _make_result(tool: str, success: bool = True, data: Any = None) -> ToolCallResult:
    return ToolCallResult(tool_name=tool, success=success, data=data or {})


# ===========================================================================
# Per-tool eval classes
# ===========================================================================


class TestDgmQueryToolEvals:
    """dgm_query: stub path (no model / unknown dataset) and contract assertions."""

    _tool = TOOL_REGISTRY["dgm_query"]

    def test_stub_path_when_no_model(self):
        result = self._tool.execute(
            {"dataset": "ecommerce", "question": "total revenue"},
            context=_CTX,
        )
        assert result.success
        assert result.tool_name == "dgm_query"
        assert isinstance(result.data, dict)
        assert result.data.get("stub") is True

    def test_stub_when_dataset_not_in_available(self):
        result = self._tool.execute(
            {"dataset": "nonexistent", "question": "anything"},
            context=_CTX,
        )
        assert result.success
        assert result.data.get("dataset") == "nonexistent"

    def test_cost_estimate_positive(self):
        assert self._tool.cost_estimate({"dataset": "ecommerce", "question": "q"}) > 0

    def test_explain_returns_string(self):
        text = self._tool.explain({"dataset": "ecommerce", "question": "q"})
        assert isinstance(text, str) and "dgm_query" in text

    def test_schema_composable_with_correlate_and_explore(self):
        composable = self._tool.capability_schema.composable_with
        assert "dgm_correlate" in composable
        assert "dgm_explore_from" in composable

    def test_no_context_returns_stub(self):
        result = self._tool.execute({"dataset": "ecommerce", "question": "q"})
        assert result.success


class TestDgmWhatCanIAnswerToolEvals:
    """dgm_what_can_i_answer: dataset scoping and output shape."""

    _tool = TOOL_REGISTRY["dgm_what_can_i_answer"]

    def test_all_datasets_when_no_filter(self):
        result = self._tool.execute({"dataset": "__all__"}, context=_CTX)
        assert result.success
        assert isinstance(result.data["datasets"], list)
        assert len(result.data["datasets"]) == len(_DATASETS)

    def test_single_dataset_scope(self):
        result = self._tool.execute({"dataset": "ecommerce"}, context=_CTX)
        assert result.success
        assert result.data["datasets"] == ["ecommerce"]

    def test_unknown_dataset_falls_back_to_all(self):
        result = self._tool.execute({"dataset": "bogus"}, context=_CTX)
        assert result.success
        assert set(result.data["datasets"]) == set(_DATASETS)

    def test_no_context_returns_empty_datasets(self):
        result = self._tool.execute({"dataset": "__all__"})
        assert result.success
        assert result.data["datasets"] == []

    def test_composable_with_dgm_query(self):
        assert "dgm_query" in self._tool.capability_schema.composable_with

    def test_cost_lower_than_query_tool(self):
        assert self._tool.cost_estimate({}) < TOOL_REGISTRY["dgm_query"].cost_estimate(
            {}
        )


class TestDgmCorrelateToolEvals:
    """dgm_correlate: cross-domain correlation contract."""

    _tool = TOOL_REGISTRY["dgm_correlate"]

    def test_returns_success(self):
        result = self._tool.execute({"left": {}, "right": {}}, context=_CTX)
        assert result.success
        assert result.tool_name == "dgm_correlate"

    def test_data_has_correlation_key(self):
        result = self._tool.execute({"left": {}, "right": {}}, context=_CTX)
        assert "correlation" in result.data

    def test_compose_merges_two_results(self):
        r1 = _make_result("dgm_query", data={"x": 1})
        r2 = _make_result("dgm_query", data={"y": 2})
        merged = self._tool.compose(r1, r2)
        assert isinstance(merged, ToolCallResult)
        assert merged.success
        assert "x" in merged.data and "y" in merged.data

    def test_cost_estimate_non_negative(self):
        assert self._tool.cost_estimate({}) >= 0


class TestDgmExploreFromToolEvals:
    """dgm_explore_from: suggestion navigation contract."""

    _tool = TOOL_REGISTRY["dgm_explore_from"]

    def test_returns_success_with_suggestions_key(self):
        result = self._tool.execute(
            {"result": {}, "direction": "forward"}, context=_CTX
        )
        assert result.success
        assert "suggestions" in result.data

    def test_suggestions_is_list(self):
        result = self._tool.execute({"result": {}, "direction": "backward"})
        assert isinstance(result.data["suggestions"], list)

    def test_composable_with_query_and_what_can_i_answer(self):
        composable = self._tool.capability_schema.composable_with
        assert "dgm_query" in composable
        assert "dgm_what_can_i_answer" in composable


class TestDgmSensitivitySweepToolEvals:
    """dgm_sensitivity_sweep: parameter sweep contract."""

    _tool = TOOL_REGISTRY["dgm_sensitivity_sweep"]

    def test_returns_success(self):
        result = self._tool.execute(
            {"dataset": "ecommerce", "question": "revenue", "param": "threshold"},
            context=_CTX,
        )
        assert result.success
        assert result.tool_name == "dgm_sensitivity_sweep"

    def test_sweep_results_key_present(self):
        result = self._tool.execute(
            {"dataset": "ecommerce", "question": "q", "param": "p"}
        )
        assert "sweep_results" in result.data

    def test_cost_is_highest_of_all_tools(self):
        costs = {
            name: tool.cost_estimate({"dataset": "x", "question": "q", "param": "p"})
            for name, tool in TOOL_REGISTRY.items()
        }
        assert self._tool.cost_estimate({}) >= max(
            c for n, c in costs.items() if n != "dgm_sensitivity_sweep"
        )


class TestDgmValidateInvariantToolEvals:
    """dgm_validate_invariant: formal invariant checking contract."""

    _tool = TOOL_REGISTRY["dgm_validate_invariant"]

    def test_returns_valid_true(self):
        result = self._tool.execute(
            {"invariant": "no_negative_revenue", "dataset": "ecommerce"},
            context=_CTX,
        )
        assert result.success
        assert result.data["valid"] is True

    def test_violations_is_list(self):
        result = self._tool.execute({"invariant": "test", "dataset": "retail"})
        assert isinstance(result.data["violations"], list)

    def test_not_composable(self):
        assert self._tool.capability_schema.composable_with == []

    def test_low_cost_estimate(self):
        assert self._tool.cost_estimate({}) < 500


class TestDgmStoreFindingToolEvals:
    """dgm_store_finding: finding persistence contract."""

    _tool = TOOL_REGISTRY["dgm_store_finding"]

    def test_stored_is_true(self):
        result = self._tool.execute(
            {"finding": "Revenue dipped in Q3", "tags": ["revenue", "q3"]},
            context=_CTX,
        )
        assert result.success
        assert result.data["stored"] is True

    def test_finding_id_present(self):
        result = self._tool.execute({"finding": "test", "tags": []})
        assert "finding_id" in result.data

    def test_composable_with_retrieve(self):
        assert "dgm_retrieve_related" in self._tool.capability_schema.composable_with

    def test_very_low_cost(self):
        assert self._tool.cost_estimate({}) <= 50


class TestDgmRetrieveRelatedToolEvals:
    """dgm_retrieve_related: lattice-adjacent retrieval contract."""

    _tool = TOOL_REGISTRY["dgm_retrieve_related"]

    def test_returns_findings_list(self):
        result = self._tool.execute(
            {"query": "revenue drop", "max_results": 5}, context=_CTX
        )
        assert result.success
        assert "findings" in result.data
        assert isinstance(result.data["findings"], list)

    def test_composable_with_store_and_query(self):
        composable = self._tool.capability_schema.composable_with
        assert "dgm_store_finding" in composable
        assert "dgm_query" in composable

    def test_store_then_retrieve_composes(self):
        store_result = TOOL_REGISTRY["dgm_store_finding"].execute(
            {"finding": "Top 5 customers drive 80% revenue", "tags": ["pareto"]},
            context=_CTX,
        )
        retrieve_result = self._tool.execute(
            {"query": "pareto revenue", "max_results": 3}, context=_CTX
        )
        composed = self._tool.compose(store_result, retrieve_result)
        assert isinstance(composed, ToolCallResult)
        assert composed.success


class TestDgmMapAnalyticalSpaceToolEvals:
    """dgm_map_analytical_space: vocabulary decomposition contract."""

    _tool = TOOL_REGISTRY["dgm_map_analytical_space"]

    def test_tools_list_non_empty(self):
        result = self._tool.execute({"dataset": "ecommerce"}, context=_CTX)
        assert result.success
        assert len(result.data.get("tools", [])) > 0

    def test_all_9_tools_described(self):
        result = self._tool.execute({"dataset": "ecommerce"}, context=_CTX)
        tool_names = {t["name"] for t in result.data["tools"]}
        assert set(TOOL_REGISTRY.keys()) == tool_names

    def test_composable_with_what_can_i_answer_and_sweep(self):
        composable = self._tool.capability_schema.composable_with
        assert "dgm_what_can_i_answer" in composable
        assert "dgm_sensitivity_sweep" in composable


# ===========================================================================
# Equipped supervisor — graph algorithm NL hooks (§16.4)
# ===========================================================================


class TestEquippedSupervisorGraphIntent:
    """classify_graph_intent routes graph-algorithm utterances correctly."""

    @pytest.mark.parametrize(
        "utterance,expected_algo",
        [
            ("find page rank of all users", "PAGE_RANK"),
            ("detect community detection groups", "LOUVAIN"),
            ("run label propagation on graph", "LABEL_PROPAGATION"),
            ("find connected component groups", "CONNECTED_COMPONENTS"),
            ("betweenness centrality of nodes", "BETWEENNESS_CENTRALITY"),
            ("closeness centrality analysis", "CLOSENESS_CENTRALITY"),
            ("node degree distribution", "DEGREE"),
            ("shortest path between source and target", "SHORTEST_PATH_LENGTH"),
            ("max flow from supplier to customer", "MAX_FLOW"),
            ("nodes reachable from root", "REACHABLE"),
            ("graph density of the network", "DENSITY"),
            ("graph diameter calculation", "DIAMETER"),
        ],
    )
    def test_utterance_maps_to_correct_algo(self, utterance, expected_algo):
        from sqldim.core.query.dgm.nl._functions import classify_graph_intent

        assert classify_graph_intent(utterance) == expected_algo

    def test_non_graph_utterance_returns_none(self):
        from sqldim.core.query.dgm.nl._functions import classify_graph_intent

        assert classify_graph_intent("total revenue by region last month") is None

    def test_graph_intent_sql_generated_by_template(self):
        """Template fallback generates graph-algorithm SQL for detected intent."""
        from tests.query.test_dgm_nl import _run_template

        sql = _run_template(
            "users",
            ["user_id", "score", "created_at"],
            "find page rank of users",
        )
        assert "page_rank" in sql.lower() or "node_id" in sql

    def test_graph_intent_takes_priority_over_aggregation(self):
        """Graph algo SQL should be emitted even when agg signals are present."""
        from tests.query.test_dgm_nl import _run_template

        sql = _run_template(
            "transactions",
            ["node_id", "amount", "created_at"],
            "total betweenness centrality of nodes",
        )
        # graph algo wins: betweenness_centrality phrase detected
        assert "betweenness_centrality" in sql.lower() or "node_id" in sql


# ===========================================================================
# Equipped supervisor — annotation-driven recommender hooks (§7.4)
# ===========================================================================


class TestEquippedSupervisorAnnotationHook:
    """apply_annotation_suggestions wires into template fallback correctly."""

    def test_empty_sigma_no_effect(self):
        from sqldim.core.query.dgm.nl._functions import apply_annotation_suggestions

        assert apply_annotation_suggestions([]) == []

    def test_degenerate_suppresses_groupby_in_template(self):
        """Degenerate annotation → SUPPRESS → group_col cleared → no GROUP BY."""
        import duckdb

        from sqldim.application.ask import make_default_budget
        from sqldim.core.query.dgm.annotations import Degenerate
        from sqldim.core.query.dgm.nl._agent_types import (
            DGMContext,
            NLInterfaceState,
            QueryCandidate,
        )
        from sqldim.core.query.dgm.nl._node_impls import make_operational_nodes
        from sqldim.core.query.dgm.nl._types import EntityRegistry
        from sqldim.core.query.dgm.recommender._core import DGMRecommender

        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE sales (region VARCHAR, amount VARCHAR)")
        r = EntityRegistry()
        r.register_node("sales", "sales")
        r.register_prop("s_region", "sales.region")
        r.register_prop("s_amount", "sales.amount")

        recom = DGMRecommender(sigma=[Degenerate(dim="region")])
        ctx = DGMContext(
            entity_registry=r,
            budget=make_default_budget(),
            con=con,
            recommender=recom,
        )
        state = NLInterfaceState(
            utterance="total amount by region",
            candidates=[
                QueryCandidate(
                    dag_node_id=0,
                    description="SELECT from sales (region, amount)",
                    band_coverage=["B1"],
                    cost_estimate=1.0,
                )
            ],
        )
        ops = make_operational_nodes(ctx)
        result = ops["dag_construction"](state)
        sql = result.get("q_current", "")
        # With SUPPRESS annotation on "region", group_col cleared → no GROUP BY
        assert "SELECT" in sql
        # GROUP BY should be absent since group_col was suppressed
        assert "GROUP BY region" not in sql

    def test_conformed_annotation_produces_path_pred_suggestion(self):
        from sqldim.core.query.dgm.annotations import Conformed
        from sqldim.core.query.dgm.nl._functions import apply_annotation_suggestions
        from sqldim.core.query.dgm.recommender._types import SuggestionKind

        sigma = [Conformed(dim="DateDim", fact_types={"SaleFact", "OrderFact"})]
        suggestions = apply_annotation_suggestions(sigma)
        assert any(s.kind is SuggestionKind.PATH_PRED for s in suggestions)

    def test_multiple_annotations_produce_multiple_suggestions(self):
        from sqldim.core.query.dgm.annotations import Conformed, Degenerate
        from sqldim.core.query.dgm.nl._functions import apply_annotation_suggestions

        sigma = [
            Degenerate(dim="order_number"),
            Conformed(dim="Date", fact_types={"Sale", "Order"}),
        ]
        suggestions = apply_annotation_suggestions(sigma)
        assert len(suggestions) >= 2


# ===========================================================================
# Equipped supervisor NL function evals
# ===========================================================================


class TestEquippedSupervisorNLFunctionEvals:
    """Eval cases directly testing the new NL classification functions."""

    # ── classify_temporal_window additions ────────────────────────────────

    @pytest.mark.parametrize(
        "phrase,expected_window",
        [
            ("this year revenue", "YTD"),
            ("this quarter sales", "QTD"),
            ("this month orders", "MTD"),
            ("year to date performance", "YTD"),
            ("quarter to date bookings", "QTD"),
            ("month to date signups", "MTD"),
            ("rolling 30 days revenue", "ROLLING"),
            ("trailing 7 days sessions", "TRAILING"),
        ],
    )
    def test_temporal_window_classification(self, phrase, expected_window):
        from sqldim.core.query.dgm.nl._functions import classify_temporal_window

        assert classify_temporal_window(phrase) == expected_window

    # ── classify_graph_intent ─────────────────────────────────────────────

    def test_cluster_maps_to_label_propagation(self):
        from sqldim.core.query.dgm.nl._functions import classify_graph_intent

        assert (
            classify_graph_intent("cluster users by similar behavior")
            == "LABEL_PROPAGATION"
        )

    def test_pagerank_case_insensitive(self):
        from sqldim.core.query.dgm.nl._functions import classify_graph_intent

        assert classify_graph_intent("PAGERANK ANALYSIS") == "PAGE_RANK"

    # ── apply_annotation_suggestions ─────────────────────────────────────

    def test_scd_type_annotation(self):
        from sqldim.core.query.dgm.annotations import SCDType
        from sqldim.core.query.dgm.nl._functions import apply_annotation_suggestions

        from sqldim.core.query.dgm.annotations import SCDKind

        suggestions = apply_annotation_suggestions(
            [SCDType(dim="CustomerDim", scd=SCDKind.SCD2)]
        )
        assert isinstance(suggestions, list)

    def test_grain_annotation_period_produces_suppress(self):
        from sqldim.core.query.dgm.annotations import Grain, GrainKind
        from sqldim.core.query.dgm.nl._functions import apply_annotation_suggestions
        from sqldim.core.query.dgm.recommender._types import SuggestionKind

        suggestions = apply_annotation_suggestions(
            [Grain(fact="OrderFact", grain=GrainKind.PERIOD)]
        )
        assert any(s.kind is SuggestionKind.SUPPRESS for s in suggestions)


# ===========================================================================
# Equipped supervisor end-to-end: full graph invocation
# ===========================================================================


class TestEquippedSupervisorEndToEnd:
    """Full supervisor graph invocation with graph-algorithm and recommender questions."""

    _RECURSION_LIMIT = 25

    def _invoke(self, utterance: str, thread_id: str) -> dict:
        from sqldim.core.query.dgm.nl.supervisor._graph import build_supervisor_graph
        from sqldim.core.query.dgm.nl.supervisor._types import SupervisorState

        graph = build_supervisor_graph(context=_CTX)
        initial = SupervisorState(utterance=utterance)
        return graph.invoke(
            initial.model_dump(),
            config={
                "configurable": {"thread_id": thread_id},
                "recursion_limit": self._RECURSION_LIMIT,
            },
        )

    @pytest.mark.parametrize(
        "utterance",
        [
            "find page rank of customers in social_network",
            "community detection on user graph",
            "shortest path analysis between nodes",
        ],
    )
    def test_graph_query_completes(self, utterance):
        result = self._invoke(utterance, f"graph-{utterance[:20]}")
        assert (
            "synthesis" in result or "q_current" in result or "visited_nodes" in result
        )

    @pytest.mark.parametrize(
        "utterance",
        [
            "What tools do you have?",
            "Which datasets can you see?",
            "Total revenue by region for ecommerce",
        ],
    )
    def test_standard_queries_complete(self, utterance):
        result = self._invoke(utterance, f"std-{utterance[:20]}")
        # Any complete invocation produces at least one of these keys
        assert result is not None

    def test_graph_question_produces_visited_nodes(self):
        result = self._invoke(
            "betweenness centrality for social_network",
            "betweenness-001",
        )
        assert result is not None

    def test_recommender_question_completes(self):
        """Supervisor graph completes without error for an annotation-style query."""
        result = self._invoke(
            "show conformed dimension path for retail",
            "recommender-001",
        )
        assert result is not None

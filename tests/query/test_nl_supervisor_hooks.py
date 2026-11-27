"""NL hooks used by the supervisor: graph-algorithm intent and annotation-driven
recommender suggestions (§11.9 / §16.4 / §7.4).

The per-tool and end-to-end classes that used to live here asserted that the
supervisor's stub tools returned success; they were removed with the stubs
(D42, ADR 0012).  Contract tests for the tools live in ``test_dispatch.py``
and ``test_graph.py``.
"""

from __future__ import annotations

import pytest


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
        """A GroupBy SUPPRESS (MedallionLayer BRONZE) → group_col cleared."""
        import duckdb

        from sqldim.application.ask import make_default_budget
        from sqldim.core.query.dgm.annotations import (
            MedallionLayer,
            MedallionLayerKind,
        )
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

        # MedallionLayer(BRONZE) suppresses GroupBy; Degenerate suppresses
        # nothing (§7.4), so it no longer drives this hook.
        recom = DGMRecommender(
            sigma=[MedallionLayer("sales", MedallionLayerKind.BRONZE)]
        )
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

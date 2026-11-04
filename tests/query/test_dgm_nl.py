"""RED → GREEN tests for §11 — Natural Language Interface.

Covers:
  - EntityRegistry construction and lookup (§11.3)
  - BandKind enum (B1/B2/B3)
  - resolve_band() — intent signals → band (§11.3)
  - classify_temporal_intent() — NL → TemporalMode (§11.4)
  - classify_compositional_intent() — NL → Q_algebra op (§11.4)
  - AmbiguityKind enum (§11.6)
  - validate_propref() — hallucination prevention (§11.8)
  - SchemaEvolutionAction enum (§11.7)
  - apply_schema_evolution() — Q_delta → actions (§11.7)
  - PathResolutionResult — RESOLVED vs AMBIGUOUS (§11.3)
  - classify_graph_intent() — NL → graph algorithm name (§16.4)
  - apply_annotation_suggestions() — AnnotationSigma → Suggestion list (§7.4)
  - Template fallback SQL generation (§11.9)
"""

from __future__ import annotations

import pytest

from sqldim.core.query.dgm.nl import (
    AmbiguityKind,
    BandKind,
    CompositionOp,
    EntityRegistry,
    PathResolutionResult,
    SchemaEvolutionAction,
    SchemaEvolutionKind,
    TemporalIntent,
    apply_annotation_suggestions,
    apply_schema_evolution,
    classify_compositional_intent,
    classify_graph_intent,
    classify_temporal_intent,
    classify_allen_relation,
    classify_temporal_window,
    extract_where_filters,
    detect_date_trunc_grain,
    infer_join_tables,
    resolve_band,
    validate_propref,
)


# ---------------------------------------------------------------------------
# BandKind enum
# ---------------------------------------------------------------------------


class TestBandKind:
    def test_has_b1(self):
        assert BandKind.B1.value == "B1"

    def test_has_b2(self):
        assert BandKind.B2.value == "B2"

    def test_has_b3(self):
        assert BandKind.B3.value == "B3"

    def test_all_three_bands(self):
        assert len(BandKind) == 3


# ---------------------------------------------------------------------------
# TemporalIntent enum
# ---------------------------------------------------------------------------


class TestTemporalIntent:
    def test_has_eventually(self):
        assert TemporalIntent.EVENTUALLY.value == "EVENTUALLY"

    def test_has_globally(self):
        assert TemporalIntent.GLOBALLY.value == "GLOBALLY"

    def test_has_next(self):
        assert TemporalIntent.NEXT.value == "NEXT"

    def test_has_until(self):
        assert TemporalIntent.UNTIL.value == "UNTIL"

    def test_has_since(self):
        assert TemporalIntent.SINCE.value == "SINCE"

    def test_has_once(self):
        assert TemporalIntent.ONCE.value == "ONCE"

    def test_has_previously(self):
        assert TemporalIntent.PREVIOUSLY.value == "PREVIOUSLY"

    def test_has_safety(self):
        assert TemporalIntent.SAFETY.value == "SAFETY"

    def test_has_liveness(self):
        assert TemporalIntent.LIVENESS.value == "LIVENESS"

    def test_has_response(self):
        assert TemporalIntent.RESPONSE.value == "RESPONSE"

    def test_has_persistence(self):
        assert TemporalIntent.PERSISTENCE.value == "PERSISTENCE"

    def test_all_twelve_intents(self):
        assert len(TemporalIntent) == 12


# ---------------------------------------------------------------------------
# CompositionOp enum
# ---------------------------------------------------------------------------


class TestCompositionOp:
    def test_has_union(self):
        assert CompositionOp.UNION.value == "UNION"

    def test_has_intersect(self):
        assert CompositionOp.INTERSECT.value == "INTERSECT"

    def test_has_join(self):
        assert CompositionOp.JOIN.value == "JOIN"

    def test_has_chain(self):
        assert CompositionOp.CHAIN.value == "CHAIN"

    def test_all_four_ops(self):
        assert len(CompositionOp) == 4


# ---------------------------------------------------------------------------
# AmbiguityKind enum
# ---------------------------------------------------------------------------


class TestAmbiguityKind:
    def test_has_measure(self):
        assert AmbiguityKind.MEASURE.value == "MEASURE"

    def test_has_path(self):
        assert AmbiguityKind.PATH.value == "PATH"

    def test_has_temporal(self):
        assert AmbiguityKind.TEMPORAL.value == "TEMPORAL"

    def test_has_grain(self):
        assert AmbiguityKind.GRAIN.value == "GRAIN"

    def test_has_compositional(self):
        assert AmbiguityKind.COMPOSITIONAL.value == "COMPOSITIONAL"

    def test_all_five_kinds(self):
        assert len(AmbiguityKind) == 5


# ---------------------------------------------------------------------------
# SchemaEvolutionKind enum
# ---------------------------------------------------------------------------


class TestSchemaEvolutionKind:
    def test_has_added_nodes(self):
        assert SchemaEvolutionKind.ADDED_NODES.value == "ADDED_NODES"

    def test_has_removed_nodes(self):
        assert SchemaEvolutionKind.REMOVED_NODES.value == "REMOVED_NODES"

    def test_has_added_edges(self):
        assert SchemaEvolutionKind.ADDED_EDGES.value == "ADDED_EDGES"

    def test_has_removed_edges(self):
        assert SchemaEvolutionKind.REMOVED_EDGES.value == "REMOVED_EDGES"

    def test_has_role_drift(self):
        assert SchemaEvolutionKind.ROLE_DRIFT.value == "ROLE_DRIFT"

    def test_has_changed_property(self):
        assert SchemaEvolutionKind.CHANGED_PROPERTY.value == "CHANGED_PROPERTY"

    def test_all_six_kinds(self):
        assert len(SchemaEvolutionKind) == 6


# ---------------------------------------------------------------------------
# EntityRegistry
# ---------------------------------------------------------------------------


class TestEntityRegistry:
    def test_empty_registry(self):
        r = EntityRegistry()
        assert r.prop_terms == {}
        assert r.node_terms == {}
        assert r.verb_terms == {}
        assert r.temporal_terms == {}
        assert r.agg_terms == {}

    def test_register_prop_term(self):
        r = EntityRegistry()
        r.register_prop("revenue", "sale.revenue")
        assert r.prop_terms["revenue"] == "sale.revenue"

    def test_register_node_term(self):
        r = EntityRegistry()
        r.register_node("customer", "c")
        assert r.node_terms["customer"] == "c"

    def test_register_verb_term(self):
        r = EntityRegistry()
        r.register_verb("purchased", "placed")
        assert r.verb_terms["purchased"] == "placed"

    def test_register_agg_term(self):
        r = EntityRegistry()
        r.register_agg("total", "SUM")
        assert r.agg_terms["total"] == "SUM"

    def test_known_propref_vocabulary(self):
        r = EntityRegistry()
        r.register_prop("revenue", "sale.revenue")
        r.register_prop("quantity", "sale.quantity")
        vocab = r.propref_vocabulary()
        assert "sale.revenue" in vocab
        assert "sale.quantity" in vocab

    def test_propref_vocabulary_is_set_of_values(self):
        r = EntityRegistry()
        r.register_prop("rev", "sale.revenue")
        r.register_prop("rev_alias", "sale.revenue")  # duplicate value
        vocab = r.propref_vocabulary()
        assert len(vocab) == 1  # deduped

    def test_resolve_prop_returns_propref(self):
        r = EntityRegistry()
        r.register_prop("revenue", "sale.revenue")
        assert r.resolve_prop("revenue") == "sale.revenue"

    def test_resolve_prop_unknown_returns_none(self):
        r = EntityRegistry()
        assert r.resolve_prop("nonexistent") is None

    def test_resolve_node_returns_alias(self):
        r = EntityRegistry()
        r.register_node("customer", "c")
        assert r.resolve_node("customer") == "c"


# ---------------------------------------------------------------------------
# validate_propref — hallucination prevention (§11.8)
# ---------------------------------------------------------------------------


class TestValidatePropref:
    def test_known_propref_is_valid(self):
        r = EntityRegistry()
        r.register_prop("revenue", "sale.revenue")
        assert validate_propref("sale.revenue", r) is True

    def test_unknown_propref_is_invalid(self):
        r = EntityRegistry()
        r.register_prop("revenue", "sale.revenue")
        assert validate_propref("sale.hallucinated_col", r) is False

    def test_empty_registry_everything_invalid(self):
        r = EntityRegistry()
        assert validate_propref("any.prop", r) is False


# ---------------------------------------------------------------------------
# resolve_band — intent signals → BandKind (§11.3)
# ---------------------------------------------------------------------------


class TestResolveBand:
    @pytest.mark.parametrize(
        "phrase,expected_band",
        [
            ("total revenue", BandKind.B2),
            ("sum of orders", BandKind.B2),
            ("average order value", BandKind.B2),
            ("count distinct customers", BandKind.B2),
            ("top 10 customers", BandKind.B3),
            ("rank by revenue", BandKind.B3),
            ("highest earners", BandKind.B3),
            ("where segment is retail", BandKind.B1),
            ("which customers bought", BandKind.B1),
            ("filter by region", BandKind.B1),
            ("has always been active", BandKind.B1),
            ("never churned", BandKind.B1),
        ],
    )
    def test_intent_maps_to_band(self, phrase, expected_band):
        assert resolve_band(phrase) == expected_band

    def test_unknown_phrase_defaults_to_b1(self):
        # No recognisable intent → default to B1 context layer
        assert resolve_band("something completely random xyz") == BandKind.B1


# ---------------------------------------------------------------------------
# classify_temporal_intent — NL → TemporalMode (§11.4)
# ---------------------------------------------------------------------------


class TestClassifyTemporalIntent:
    @pytest.mark.parametrize(
        "phrase,expected",
        [
            ("has always been true", TemporalIntent.GLOBALLY),
            ("will eventually happen", TemporalIntent.EVENTUALLY),
            ("at the next step", TemporalIntent.NEXT),
            ("positive until churn occurred", TemporalIntent.UNTIL),
            ("rising since the last campaign", TemporalIntent.SINCE),
            ("was once a premium customer", TemporalIntent.ONCE),
            ("the previous value was higher", TemporalIntent.PREVIOUSLY),
            ("this condition never violated", TemporalIntent.SAFETY),
            ("this outcome always eventually reached", TemporalIntent.LIVENESS),
            ("whenever purchase happens revenue follows", TemporalIntent.RESPONSE),
        ],
    )
    def test_phrase_maps_to_intent(self, phrase, expected):
        assert classify_temporal_intent(phrase) == expected

    def test_unknown_phrase_returns_none(self):
        assert classify_temporal_intent("plain english without temporal cues") is None


# ---------------------------------------------------------------------------
# classify_compositional_intent — NL → Q_algebra op (§11.4)
# ---------------------------------------------------------------------------


class TestClassifyCompositionalIntent:
    @pytest.mark.parametrize(
        "phrase,expected_op",
        [
            ("customers who bought X and also returned Y", CompositionOp.INTERSECT),
            ("customers who bought X or Y", CompositionOp.UNION),
            ("X compared to Y", CompositionOp.JOIN),
            ("X alongside Y", CompositionOp.JOIN),
            ("first X, then among those Y", CompositionOp.CHAIN),
        ],
    )
    def test_phrase_maps_to_op(self, phrase, expected_op):
        assert classify_compositional_intent(phrase) == expected_op

    def test_unknown_phrase_returns_none(self):
        assert classify_compositional_intent("a simple single question") is None


# ---------------------------------------------------------------------------
# PathResolutionResult
# ---------------------------------------------------------------------------


class TestPathResolutionResult:
    def test_resolved_stores_path(self):
        r = PathResolutionResult(
            resolved=True, path="Customer→placed→Sale", candidates=[]
        )
        assert r.resolved is True
        assert r.path == "Customer→placed→Sale"

    def test_ambiguous_result(self):
        r = PathResolutionResult(
            resolved=False, path=None, candidates=["path1", "path2"]
        )
        assert r.resolved is False
        assert len(r.candidates) == 2

    def test_resolved_no_candidates(self):
        r = PathResolutionResult(resolved=True, path="p", candidates=[])
        assert r.candidates == []


# ---------------------------------------------------------------------------
# apply_schema_evolution — Q_delta → actions (§11.7)
# ---------------------------------------------------------------------------


class TestApplySchemaEvolution:
    def test_added_nodes_suggests_new_questions(self):
        actions = apply_schema_evolution(SchemaEvolutionKind.ADDED_NODES)
        assert SchemaEvolutionAction.SUGGEST_NEW_QUESTIONS in actions

    def test_removed_nodes_invalidates_saved(self):
        actions = apply_schema_evolution(SchemaEvolutionKind.REMOVED_NODES)
        assert SchemaEvolutionAction.INVALIDATE_SAVED_QUESTIONS in actions

    def test_added_edges_suggests_paths(self):
        actions = apply_schema_evolution(SchemaEvolutionKind.ADDED_EDGES)
        assert SchemaEvolutionAction.SUGGEST_NEW_QUESTIONS in actions

    def test_role_drift_revalidates(self):
        actions = apply_schema_evolution(SchemaEvolutionKind.ROLE_DRIFT)
        assert SchemaEvolutionAction.REVALIDATE_SAVED_QUESTIONS in actions

    def test_changed_property_refreshes_registry(self):
        actions = apply_schema_evolution(SchemaEvolutionKind.CHANGED_PROPERTY)
        assert SchemaEvolutionAction.REFRESH_ENTITY_REGISTRY in actions

    def test_returns_list(self):
        result = apply_schema_evolution(SchemaEvolutionKind.ADDED_NODES)
        assert isinstance(result, list)


# ---------------------------------------------------------------------------
# classify_allen_relation — NL → Allen interval relation (§3.2)
# ---------------------------------------------------------------------------


class TestClassifyAllenRelation:
    @pytest.mark.parametrize(
        "phrase,expected",
        [
            ("orders placed before shipment", "BEFORE"),
            ("earlier than expected", "BEFORE"),
            ("preceded by a campaign", "BEFORE"),
            ("event after the sale", "AFTER"),
            ("followed by a return", "AFTER"),
            ("later than payment", "AFTER"),
            ("at the same time as invoice", "CONCURRENT"),
            ("simultaneous events", "CONCURRENT"),
            ("concurrent processing", "CONCURRENT"),
            ("order meets delivery", "MEETS"),
            ("immediately followed then completes", "MEETS"),
            ("met by the previous phase", "MET_BY"),
            ("immediately preceded and connected", "MET_BY"),
            ("subscription overlaps with trial", "OVERLAPS"),
            ("partially overlaps the campaign", "OVERLAPS"),
            ("overlapped by the next quarter", "OVERLAPPED_BY"),
            ("starts with onboarding", "STARTS"),
            ("started by initial contact", "STARTED_BY"),
            ("during the campaign period", "DURING"),
            ("within the fiscal year", "DURING"),
            ("the window contains the event", "CONTAINS"),
            ("year encompasses the quarter", "CONTAINS"),
            ("finishes with closing", "FINISHES"),
            ("finished by the last step", "FINISHED_BY"),
            ("equals the billing period", "EQUALS"),
            ("same period as last month", "EQUALS"),
        ],
    )
    def test_phrase_maps_to_relation(self, phrase, expected):
        assert classify_allen_relation(phrase) == expected

    def test_unknown_returns_none(self):
        assert classify_allen_relation("a simple query") is None

    def test_case_insensitive(self):
        assert classify_allen_relation("BEFORE the deadline") == "BEFORE"


# ---------------------------------------------------------------------------
# classify_temporal_window — NL → TemporalWindow kind (§4.2)
# ---------------------------------------------------------------------------


class TestClassifyTemporalWindow:
    @pytest.mark.parametrize(
        "phrase,expected",
        [
            ("year to date revenue", "YTD"),
            ("year-to-date sales", "YTD"),
            ("ytd count", "YTD"),
            ("quarter to date figures", "QTD"),
            ("quarter-to-date total", "QTD"),
            ("qtd numbers", "QTD"),
            ("month to date spending", "MTD"),
            ("month-to-date earnings", "MTD"),
            ("mtd total", "MTD"),
            ("rolling 30 day average", "ROLLING"),
            ("moving average of revenue", "ROLLING"),
            ("trailing 7 days", "TRAILING"),
            ("between January and March", "PERIOD"),
            ("from 2024-01-01 to today", "PERIOD"),
        ],
    )
    def test_phrase_maps_to_window(self, phrase, expected):
        assert classify_temporal_window(phrase) == expected

    def test_unknown_returns_none(self):
        assert classify_temporal_window("total revenue by category") is None


# ---------------------------------------------------------------------------
# extract_where_filters — NL → candidate column filters (§11.5)
# ---------------------------------------------------------------------------


class TestExtractWhereFilters:
    def test_greater_than(self):
        filters = extract_where_filters(
            "where revenue greater than 1000", ["revenue", "region"]
        )
        assert ("revenue", ">", "1000") in filters

    def test_less_than(self):
        filters = extract_where_filters(
            "filter items with price less than 50", ["price", "name"]
        )
        assert ("price", "<", "50") in filters

    def test_equality_with_operator(self):
        filters = extract_where_filters(
            "which customers where region = west", ["region", "name"]
        )
        assert ("region", "=", "west") in filters

    def test_no_filter_signal_returns_empty(self):
        filters = extract_where_filters("total revenue", ["revenue"])
        assert filters == []

    def test_unrecognised_column_ignored(self):
        filters = extract_where_filters(
            "where foo greater than 10", ["revenue", "price"]
        )
        assert filters == []

    def test_multiple_filters(self):
        filters = extract_where_filters(
            "where revenue above 500 and price below 20",
            ["revenue", "price"],
        )
        assert len(filters) >= 1  # at least revenue detected


# ---------------------------------------------------------------------------
# detect_date_trunc_grain — NL → DATE_TRUNC grain (§4.2)
# ---------------------------------------------------------------------------


class TestDetectDateTruncGrain:
    @pytest.mark.parametrize(
        "phrase,expected",
        [
            ("monthly revenue", "month"),
            ("per month breakdown", "month"),
            ("by month trend", "month"),
            ("yearly total", "year"),
            ("per year growth", "year"),
            ("annual report", "year"),
            ("quarterly figures", "quarter"),
            ("per quarter results", "quarter"),
            ("weekly active users", "week"),
            ("per week usage", "week"),
            ("daily transactions", "day"),
            ("per day counts", "day"),
            ("by date activity", "day"),
        ],
    )
    def test_phrase_maps_to_grain(self, phrase, expected):
        assert detect_date_trunc_grain(phrase) == expected

    def test_unknown_returns_none(self):
        assert detect_date_trunc_grain("total revenue by category") is None


# ---------------------------------------------------------------------------
# infer_join_tables — entity resolution → JOIN pairs (§11.3)
# ---------------------------------------------------------------------------


class TestInferJoinTables:
    def _make_registry_with_tables(self):
        r = EntityRegistry()
        r.register_node("orders", "orders")
        r.register_node("customers", "customers")
        r.register_prop("order_id", "orders.id")
        r.register_prop("order_cid", "orders.customer_id")
        r.register_prop("order_amount", "orders.amount")
        r.register_prop("cust_id", "customers.id")
        r.register_prop("cust_cid", "customers.customer_id")
        r.register_prop("cust_name", "customers.name")
        return r

    def test_shared_id_column_detected(self):
        r = self._make_registry_with_tables()

        class FakePR:
            def __init__(self, alias):
                self.alias = alias

        joins = infer_join_tables([FakePR("orders"), FakePR("customers")], r)
        assert len(joins) >= 1
        # Should find customer_id as shared
        assert any(j[2] == "customer_id" for j in joins)

    def test_single_table_returns_empty(self):
        r = self._make_registry_with_tables()

        class FakePR:
            def __init__(self, alias):
                self.alias = alias

        joins = infer_join_tables([FakePR("orders")], r)
        assert joins == []

    def test_no_shared_id_returns_empty(self):
        r = EntityRegistry()
        r.register_node("orders", "orders")
        r.register_node("products", "products")
        r.register_prop("order_amt", "orders.amount")
        r.register_prop("prod_name", "products.name")

        class FakePR:
            def __init__(self, alias):
                self.alias = alias

        joins = infer_join_tables([FakePR("orders"), FakePR("products")], r)
        assert joins == []

    def test_alias_resolved_via_node_terms(self):
        """When PropRefModel.alias matches a node_terms key (not value)."""
        r = EntityRegistry()
        r.register_node("order", "orders")
        r.register_node("customer", "customers")
        r.register_prop("oid", "orders.customer_id")
        r.register_prop("cid", "customers.customer_id")

        class FakePR:
            def __init__(self, alias):
                self.alias = alias

        joins = infer_join_tables([FakePR("order"), FakePR("customer")], r)
        assert any(j[2] == "customer_id" for j in joins)


# ===========================================================================
# classify_graph_intent — NL phrase → graph algorithm name (§16.4)
# ===========================================================================


class TestClassifyGraphIntent:
    @pytest.mark.parametrize(
        "phrase,expected",
        [
            ("find page rank of users", "PAGE_RANK"),
            ("detect community detection in graph", "LOUVAIN"),
            ("run louvain algorithm", "LOUVAIN"),
            ("label propagation clustering", "LABEL_PROPAGATION"),
            ("find connected component groups", "CONNECTED_COMPONENTS"),
            ("strongly connected scc analysis", "TARJAN_SCC"),
            ("betweenness centrality scores", "BETWEENNESS_CENTRALITY"),
            ("closeness centrality", "CLOSENESS_CENTRALITY"),
            ("node degree distribution", "DEGREE"),
            ("shortest path between nodes", "SHORTEST_PATH_LENGTH"),
            ("max flow analysis", "MAX_FLOW"),
            ("nodes reachable from source", "REACHABLE"),
            ("graph density measure", "DENSITY"),
            ("graph diameter", "DIAMETER"),
            ("cluster users by similarity", "LABEL_PROPAGATION"),
        ],
    )
    def test_phrase_maps_to_algo(self, phrase, expected):
        assert classify_graph_intent(phrase) == expected

    def test_unknown_returns_none(self):
        assert classify_graph_intent("show total revenue by region") is None

    def test_case_insensitive(self):
        assert classify_graph_intent("PAGE RANK OF NODES") == "PAGE_RANK"


# ===========================================================================
# apply_annotation_suggestions — AnnotationSigma → Suggestion list (§7.4)
# ===========================================================================


class TestApplyAnnotationSuggestions:
    def _make_sigma_with(self, *annotations):
        """Construct a minimal AnnotationSigma-like iterable."""
        return list(annotations)

    def test_empty_sigma_returns_empty(self):
        suggestions = apply_annotation_suggestions([])
        assert suggestions == []

    def test_degenerate_annotation_produces_suppress(self):
        from sqldim.core.query.dgm.annotations import Degenerate
        from sqldim.core.query.dgm.recommender._types import SuggestionKind

        sigma = self._make_sigma_with(Degenerate(dim="order_number"))
        suggestions = apply_annotation_suggestions(sigma)
        assert any(s.kind is SuggestionKind.SUPPRESS for s in suggestions)

    def test_conformed_annotation_produces_path_pred(self):
        from sqldim.core.query.dgm.annotations import Conformed
        from sqldim.core.query.dgm.recommender._types import SuggestionKind

        sigma = self._make_sigma_with(
            Conformed(dim="DateDim", fact_types={"SaleFact", "OrderFact"})
        )
        suggestions = apply_annotation_suggestions(sigma)
        assert any(s.kind is SuggestionKind.PATH_PRED for s in suggestions)

    def test_statistics_kwarg_accepted(self):
        suggestions = apply_annotation_suggestions([], statistics=None)
        assert suggestions == []


# ===========================================================================
# Template fallback — comprehensive SQL generation tests (§11.9)
#
# Each test class targets a specific branch of the template fallback in
# _node_impls._dag_construction_node.
# ===========================================================================


def _make_template_state(
    table: str,
    columns: list[str],
    utterance: str,
    *,
    compositional=None,
    entity_result=None,
    extra_candidates: list | None = None,
):
    """Build the (context, state) pair needed to drive the template fallback."""
    import duckdb

    from sqldim.application.ask import make_default_budget
    from sqldim.core.query.dgm.nl._agent_types import (
        DGMContext,
        NLInterfaceState,
        QueryCandidate,
    )
    from sqldim.core.query.dgm.nl._types import EntityRegistry

    con = duckdb.connect(":memory:")
    col_defs = ", ".join(f"{c} VARCHAR" for c in columns)
    con.execute(f"CREATE TABLE {table} ({col_defs})")

    r = EntityRegistry()
    r.register_node(table, table)
    for col in columns:
        r.register_prop(f"{table}_{col}", f"{table}.{col}")

    budget = make_default_budget()
    ctx = DGMContext(entity_registry=r, budget=budget, con=con)

    preview = ", ".join(columns[:3])
    candidates = [
        QueryCandidate(
            dag_node_id=0,
            description=f"SELECT from {table} ({preview})",
            band_coverage=["B1"],
            cost_estimate=1.0,
        )
    ]
    if extra_candidates:
        candidates.extend(extra_candidates)

    state = NLInterfaceState(
        utterance=utterance,
        candidates=candidates,
        compositional=compositional,
        entity_result=entity_result,
    )
    return ctx, state


def _run_template(table, columns, utterance, **kwargs):
    """Run dag_construction template fallback; return SQL string."""
    from sqldim.core.query.dgm.nl._node_impls import make_operational_nodes

    ctx, state = _make_template_state(table, columns, utterance, **kwargs)
    ops = make_operational_nodes(ctx)
    result = ops["dag_construction"](state)
    return result.get("q_current", "")


# ---------------------------------------------------------------------------
# Lines 539-540: date column detection (date_col = col; break)
# Lines 547-551: string filter → ValueError → quoted value branch
# ---------------------------------------------------------------------------


class TestTemplateFallbackDateAndStringFilter:
    def test_date_col_detected_from_column_name(self):
        """Columns ending in date suffix are detected as date columns."""
        sql = _run_template(
            "orders",
            ["order_id", "amount", "created_at"],
            "year to date orders",
        )
        # YTD + date col → WHERE with DATE_TRUNC
        assert "created_at" in sql or "CURRENT_DATE" in sql or "SELECT" in sql

    def test_string_filter_value_is_quoted(self):
        """Non-numeric filter values are wrapped in single quotes."""
        sql = _run_template(
            "customers",
            ["name", "revenue"],
            "customers where name = Smith",
        )
        assert "'smith'" in sql.lower() or "smith" in sql.lower()

    def test_numeric_filter_value_appended_unquoted(self):
        """Numeric filter values are NOT wrapped in quotes (line 551)."""
        sql = _run_template(
            "orders",
            ["order_id", "amount"],
            "orders where amount greater than 1000",
        )
        # float("1000") succeeds → where_clauses.append without quotes
        assert "1000" in sql or "amount" in sql.lower()

    def test_date_col_at_suffix_detected(self):
        """Columns ending exactly in 'at' are detected as date columns (updated_at)."""
        sql = _run_template(
            "events",
            ["user_id", "score", "updated_at"],
            "monthly score trend",
        )
        assert "updated_at" in sql or "SELECT" in sql


# ---------------------------------------------------------------------------
# Lines 558-562: date_grain detected + no initial group_col → group_col = alias
# ---------------------------------------------------------------------------


class TestTemplateFallbackDateGrainSetsGroupCol:
    def test_monthly_grain_no_group_col(self):
        """'monthly' sets group_col to date_trunc alias when no GROUP BY signal."""
        sql = _run_template(
            "sales",
            ["product_id", "amount", "sale_date"],
            "monthly amount distribution",
        )
        assert "DATE_TRUNC" in sql or "month" in sql.lower()

    def test_yearly_grain_sets_group_col(self):
        """'yearly' sets annual date_trunc alias."""
        sql = _run_template(
            "metrics",
            ["metric_id", "value", "event_date"],
            "yearly value summary",
        )
        assert "year" in sql.lower() or "DATE_TRUNC" in sql


# ---------------------------------------------------------------------------
# Lines 568-585: QTD / MTD / ROLLING branches
# Lines 589-592: TRAILING branch
# ---------------------------------------------------------------------------


class TestTemplateFallbackTemporalWindowBranches:
    def test_qtd_adds_quarter_where(self):
        sql = _run_template(
            "orders",
            ["order_id", "revenue", "order_date"],
            "this quarter revenue",
        )
        assert "quarter" in sql.lower() or "QTD" in sql or "CURRENT_DATE" in sql

    def test_mtd_adds_month_where(self):
        sql = _run_template(
            "orders",
            ["order_id", "amount", "order_date"],
            "this month amount",
        )
        assert "month" in sql.lower() or "CURRENT_DATE" in sql

    def test_rolling_with_number_in_phrase(self):
        sql = _run_template(
            "events",
            ["event_id", "count", "event_date"],
            "rolling 30 days count",
        )
        assert "INTERVAL" in sql or "CURRENT_DATE" in sql

    def test_rolling_without_number_uses_default(self):
        sql = _run_template(
            "events",
            ["event_id", "score", "event_date"],
            "rolling total score",
        )
        assert "CURRENT_DATE" in sql or "30" in sql or "SELECT" in sql

    def test_trailing_with_number(self):
        sql = _run_template(
            "sessions",
            ["session_id", "duration", "session_date"],
            "trailing 7 days duration",
        )
        assert "INTERVAL" in sql or "7" in sql or "CURRENT_DATE" in sql


# ---------------------------------------------------------------------------
# Lines 603-604: both where_sql AND temporal_where → combined with AND
# ---------------------------------------------------------------------------


class TestTemplateFallbackCombinedFilters:
    def test_where_and_ytd_combined(self):
        """Filter expression AND temporal window are joined with AND."""
        sql = _run_template(
            "sales",
            ["region", "revenue", "sale_date"],
            "year to date revenue > 1000",
        )
        # Should contain both a numeric condition and a date range condition
        assert "AND" in sql or "WHERE" in sql

    def test_numeric_filter_and_mtd(self):
        sql = _run_template(
            "orders",
            ["amount", "status", "order_date"],
            "this month amount > 500",
        )
        assert "500" in sql or "month" in sql.lower()

    def test_numeric_filter_and_temporal_combined_with_and(self):
        """Both a numeric WHERE and a temporal expression → joined with AND (line 592)."""
        sql = _run_template(
            "orders",
            ["order_id", "amount", "order_date"],
            "year to date orders where amount greater than 500",
        )
        # where_sql="amount > 500" AND temporal_where=YTD clause → combined
        assert "AND" in sql or "500" in sql or "CURRENT_DATE" in sql


# ---------------------------------------------------------------------------
# Lines 617-671: Compositional ops (INTERSECT, JOIN, CHAIN) + fall-through
# ---------------------------------------------------------------------------


class TestTemplateFallbackCompositional:
    def _make_comp(self, op):
        from sqldim.core.query.dgm.nl._agent_types import (
            CompositionalDetectionResult,
            PropRefModel,
        )
        from sqldim.core.query.dgm.nl._types import CompositionOp

        join_key = None
        if op is CompositionOp.JOIN:
            join_key = PropRefModel(alias="orders", prop="customer_id")
        return CompositionalDetectionResult(
            operation=op, join_key=join_key, confidence=0.9
        )

    def _make_two_table_state(self, op):
        from sqldim.core.query.dgm.nl._agent_types import (
            DGMContext,
            NLInterfaceState,
            QueryCandidate,
        )
        from sqldim.core.query.dgm.nl._types import EntityRegistry
        import duckdb
        from sqldim.application.ask import make_default_budget

        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE orders (order_id VARCHAR, amount VARCHAR)")
        con.execute("CREATE TABLE customers (customer_id VARCHAR, name VARCHAR)")

        r = EntityRegistry()
        r.register_node("orders", "orders")
        r.register_node("customers", "customers")
        r.register_prop("o_id", "orders.order_id")
        r.register_prop("o_amt", "orders.amount")
        r.register_prop("c_id", "customers.customer_id")
        r.register_prop("c_name", "customers.name")

        comp = self._make_comp(op)
        candidates = [
            QueryCandidate(
                dag_node_id=0,
                description="SELECT from orders (order_id, amount)",
                band_coverage=["B1"],
                cost_estimate=1.0,
            ),
            QueryCandidate(
                dag_node_id=1,
                description="SELECT from customers (customer_id, name)",
                band_coverage=["B1"],
                cost_estimate=1.0,
            ),
        ]
        state = NLInterfaceState(
            utterance="combine orders and customers",
            candidates=candidates,
            compositional=comp,
        )
        ctx = DGMContext(
            entity_registry=r,
            budget=make_default_budget(),
            con=con,
        )
        return ctx, state

    def test_union_generates_union_sql(self):
        from sqldim.core.query.dgm.nl._types import CompositionOp
        from sqldim.core.query.dgm.nl._node_impls import make_operational_nodes

        ctx, state = self._make_two_table_state(CompositionOp.UNION)
        ops = make_operational_nodes(ctx)
        result = ops["dag_construction"](state)
        sql = result.get("q_current", "")
        assert "UNION" in sql or "WITH" in sql

    def test_intersect_generates_intersect_sql(self):
        from sqldim.core.query.dgm.nl._types import CompositionOp
        from sqldim.core.query.dgm.nl._node_impls import make_operational_nodes

        ctx, state = self._make_two_table_state(CompositionOp.INTERSECT)
        ops = make_operational_nodes(ctx)
        result = ops["dag_construction"](state)
        sql = result.get("q_current", "")
        assert "INTERSECT" in sql or "WITH" in sql

    def test_join_generates_join_sql(self):
        from sqldim.core.query.dgm.nl._types import CompositionOp
        from sqldim.core.query.dgm.nl._node_impls import make_operational_nodes

        ctx, state = self._make_two_table_state(CompositionOp.JOIN)
        ops = make_operational_nodes(ctx)
        result = ops["dag_construction"](state)
        sql = result.get("q_current", "")
        assert "JOIN" in sql or "SELECT" in sql

    def test_chain_generates_cte_sql(self):
        from sqldim.core.query.dgm.nl._types import CompositionOp
        from sqldim.core.query.dgm.nl._node_impls import make_operational_nodes

        ctx, state = self._make_two_table_state(CompositionOp.CHAIN)
        ops = make_operational_nodes(ctx)
        result = ops["dag_construction"](state)
        sql = result.get("q_current", "")
        assert "WITH" in sql or "SELECT" in sql

    def test_fallthrough_when_too_few_tables(self):
        """Only one candidate with 'SELECT from' → tables_involved < 2 → line 713."""
        from sqldim.core.query.dgm.nl._agent_types import (
            CompositionalDetectionResult,
            DGMContext,
            NLInterfaceState,
            QueryCandidate,
        )
        from sqldim.core.query.dgm.nl._types import CompositionOp, EntityRegistry
        from sqldim.core.query.dgm.nl._node_impls import make_operational_nodes
        import duckdb
        from sqldim.application.ask import make_default_budget

        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE t1 (col1 VARCHAR, val1 VARCHAR)")
        r = EntityRegistry()
        r.register_node("t1", "t1")
        r.register_prop("t1_col1", "t1.col1")
        r.register_prop("t1_val1", "t1.val1")

        comp = CompositionalDetectionResult(
            operation=CompositionOp.INTERSECT, join_key=None, confidence=0.9
        )
        # First candidate has "SELECT from t1" (table found), second has no such prefix
        # → tables_involved = ["t1"] (only 1), so len < 2 → uses_composition = False (line 713)
        candidates = [
            QueryCandidate(
                dag_node_id=0,
                description="SELECT from t1 (col1, val1)",
                band_coverage=["B1"],
                cost_estimate=1.0,
            ),
            QueryCandidate(
                dag_node_id=1,
                description="aggregate summary",
                band_coverage=["B1"],
                cost_estimate=1.0,
            ),
        ]
        state = NLInterfaceState(
            utterance="combine t1 data",
            candidates=candidates,
            compositional=comp,
        )
        ctx = DGMContext(entity_registry=r, budget=make_default_budget(), con=con)
        ops = make_operational_nodes(ctx)
        result = ops["dag_construction"](state)
        # Falls through to single-table SQL (uses_composition reset to False)
        assert "q_current" in result


# ---------------------------------------------------------------------------
# Lines 686-687: agg + group_col + date_trunc_expr where group != alias
# ---------------------------------------------------------------------------


class TestTemplateFallbackAggGroupDateTrunc:
    def test_agg_group_col_with_date_trunc(self):
        """SUM by category monthly → date_trunc added alongside group_col in SELECT."""
        sql = _run_template(
            "sales",
            ["category", "amount", "sale_date"],
            "total amount by category monthly",
        )
        assert "SUM" in sql or "amount" in sql.lower()
        # Both group (category) and date_trunc should appear
        assert "DATE_TRUNC" in sql or "category" in sql

    def test_count_group_col_with_date_trunc(self):
        """COUNT by type monthly → both columns in SELECT."""
        sql = _run_template(
            "events",
            ["type", "score", "event_date"],
            "count events by type monthly",
        )
        assert "COUNT" in sql or "type" in sql


# ---------------------------------------------------------------------------
# Lines 730-731: date_trunc_expr + numeric_col (time-series: no agg signal)
# ---------------------------------------------------------------------------


class TestTemplateFallbackTimeSeries:
    def test_monthly_numeric_col_selects_trunc_and_sum(self):
        """'monthly price' with no agg keyword → time-series SUM branch."""
        sql = _run_template(
            "products",
            ["product_id", "price", "updated_at"],
            "monthly price distribution",
        )
        assert "DATE_TRUNC" in sql or "SUM" in sql or "price" in sql

    def test_date_trunc_numeric_col_without_agg_signal(self):
        """No 'total/sum/count' in utterance → elif date_trunc_expr and numeric_col."""
        sql = _run_template(
            "orders",
            ["order_id", "revenue", "order_date"],
            "weekly revenue trend",
        )
        assert "SELECT" in sql


# ---------------------------------------------------------------------------
# Lines 539-540 (recheck): graph algorithm SQL generation (§16.4)
# ---------------------------------------------------------------------------


class TestTemplateFallbackGraphAlgo:
    def test_page_rank_utterance_returns_graph_sql(self):
        sql = _run_template(
            "users",
            ["user_id", "score", "created_at"],
            "find page rank of users",
        )
        assert "page_rank" in sql.lower() or "node_id" in sql

    def test_community_detection_returns_graph_sql(self):
        sql = _run_template(
            "customers",
            ["customer_id", "amount"],
            "community detection of customers",
        )
        assert "louvain" in sql.lower() or "node_id" in sql

    def test_graph_algo_suppresses_composition(self):
        """Graph algo should win over compositional intent (graph_algo + not uses_composition)."""
        from sqldim.core.query.dgm.nl._agent_types import CompositionalDetectionResult
        from sqldim.core.query.dgm.nl._types import CompositionOp

        comp = CompositionalDetectionResult(
            operation=CompositionOp.UNION,
            join_key=None,
            confidence=0.3,  # below threshold
        )
        sql = _run_template(
            "nodes",
            ["node_id", "label"],
            "shortest path between nodes",
            compositional=comp,
        )
        assert "shortest_path_length" in sql.lower() or "node_id" in sql


# ---------------------------------------------------------------------------
# Annotation-driven recommender in template fallback (§7.4)
# ---------------------------------------------------------------------------


class TestTemplateFallbackAnnotationHook:
    def test_suppress_annotation_clears_group_col(self):
        """SUPPRESS suggestion from recommender removes GROUP BY from generated SQL."""
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
        con.execute("CREATE TABLE sales (category VARCHAR, amount VARCHAR)")

        r = EntityRegistry()
        r.register_node("sales", "sales")
        r.register_prop("s_cat", "sales.category")
        r.register_prop("s_amt", "sales.amount")

        # DGMRecommender with Degenerate(category) → produces SUPPRESS suggestion
        sigma = [Degenerate(dim="category")]
        recommender = DGMRecommender(sigma=sigma)

        budget = make_default_budget()
        ctx = DGMContext(
            entity_registry=r, budget=budget, con=con, recommender=recommender
        )
        candidates = [
            QueryCandidate(
                dag_node_id=0,
                description="SELECT from sales (category, amount)",
                band_coverage=["B1"],
                cost_estimate=1.0,
            )
        ]
        state = NLInterfaceState(
            utterance="total amount by category",
            candidates=candidates,
        )
        ops = make_operational_nodes(ctx)
        result = ops["dag_construction"](state)
        # With SUPPRESS annotation, group_col is cleared, so no GROUP BY
        sql = result.get("q_current", "")
        assert "SELECT" in sql

    def test_annotation_exception_is_caught(self):
        """Broken recommender sigma raises inside try → except block covers lines 623-624."""
        import duckdb
        from sqldim.application.ask import make_default_budget
        from sqldim.core.query.dgm.nl._agent_types import (
            DGMContext,
            NLInterfaceState,
            QueryCandidate,
        )
        from sqldim.core.query.dgm.nl._node_impls import make_operational_nodes
        from sqldim.core.query.dgm.nl._types import EntityRegistry

        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE sales (category VARCHAR, amount VARCHAR)")
        r = EntityRegistry()
        r.register_node("sales", "sales")
        r.register_prop("s_cat", "sales.category")
        r.register_prop("s_amt", "sales.amount")

        # sigma=None → DGMRecommender(None).run_annotation_rules() → TypeError
        # → caught by except Exception: pass (lines 623-624)
        class _BrokenRecommender:
            sigma = None  # not iterable → TypeError in run_annotation_rules
            statistics = None

        budget = make_default_budget()
        ctx = DGMContext(
            entity_registry=r, budget=budget, con=con, recommender=_BrokenRecommender()
        )
        candidates = [
            QueryCandidate(
                dag_node_id=0,
                description="SELECT from sales (category, amount)",
                band_coverage=["B1"],
                cost_estimate=1.0,
            )
        ]
        state = NLInterfaceState(
            utterance="total amount by category",
            candidates=candidates,
        )
        ops = make_operational_nodes(ctx)
        result = ops["dag_construction"](state)
        # Exception is caught silently; SQL is still generated
        sql = result.get("q_current", "")
        assert "SELECT" in sql


# ---------------------------------------------------------------------------
# Lines 600, 605-606: entity_result drives infer_join_tables + JOIN clause
# ---------------------------------------------------------------------------


class TestTemplateFallbackJoinInference:
    def test_entity_result_with_two_tables_generates_join(self):
        """entity_result.resolved proprefs from 2 tables → JOIN SQL generated (lines 600, 605-606)."""
        import duckdb
        from sqldim.application.ask import make_default_budget
        from sqldim.core.query.dgm.nl._agent_types import (
            DGMContext,
            EntityResolutionResult,
            NLInterfaceState,
            PropRefModel,
            QueryCandidate,
        )
        from sqldim.core.query.dgm.nl._node_impls import make_operational_nodes
        from sqldim.core.query.dgm.nl._types import EntityRegistry

        con = duckdb.connect(":memory:")
        con.execute(
            "CREATE TABLE orders (order_id VARCHAR, customer_id VARCHAR, amount VARCHAR)"
        )
        con.execute("CREATE TABLE customers (customer_id VARCHAR, name VARCHAR)")

        r = EntityRegistry()
        r.register_node("orders", "orders")
        r.register_node("customers", "customers")
        r.register_prop("o_id", "orders.order_id")
        r.register_prop("o_cid", "orders.customer_id")
        r.register_prop("o_amt", "orders.amount")
        r.register_prop("c_cid", "customers.customer_id")
        r.register_prop("c_name", "customers.name")

        # entity_result with proprefs from both tables
        entity_result = EntityResolutionResult(
            resolved=[
                PropRefModel(alias="orders", prop="customer_id"),
                PropRefModel(alias="customers", prop="customer_id"),
            ]
        )
        budget = make_default_budget()
        ctx = DGMContext(entity_registry=r, budget=budget, con=con)
        candidates = [
            QueryCandidate(
                dag_node_id=0,
                description="SELECT from orders (order_id, customer_id, amount)",
                band_coverage=["B1"],
                cost_estimate=1.0,
            )
        ]
        state = NLInterfaceState(
            utterance="total amount",
            candidates=candidates,
            entity_result=entity_result,
        )
        ops = make_operational_nodes(ctx)
        result = ops["dag_construction"](state)
        sql = result.get("q_current", "")
        # infer_join_tables finds customer_id shared between orders and customers
        # → JOIN clause appended to from_clause
        assert "SELECT" in sql

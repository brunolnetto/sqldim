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
    apply_schema_evolution,
    classify_compositional_intent,
    classify_temporal_intent,
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

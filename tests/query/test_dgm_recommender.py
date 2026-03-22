"""Phase 6 — Recommender architecture — RED tests.

Covers:
- Suggestion dataclass and SuggestionKind enum
- DGMRecommender three-layer architecture (§7.1)
- Two-stage trail exploration flow (§7.2)
- Annotation-driven suggestion rules (§7.4)
- TrailExpr-driven suggestion rules (§7.5)
- Graph algorithm signals (§7.6)
- SIGNATURE_ENTROPY routing signal
"""

import pytest

from sqldim.core.query._dgm_recommender import (
    SuggestionKind,
    Suggestion,
    Stage1Result,
    Stage2Result,
    DGMRecommender,
    ENTROPY_THRESHOLD,
)
from sqldim.core.query._dgm_annotations import (
    AnnotationSigma,
    Conformed,
    Grain,
    SCDType,
    Degenerate,
    RolePlaying,
    FactlessFact,
    DerivedFact,
    WeightConstraint,
    BridgeSemantics,
    Hierarchy,
    GrainKind,
    SCDKind,
    WeightConstraintKind,
    BridgeSemanticsKind,
)
from sqldim.core.query._dgm_graph import GraphStatistics


# ---------------------------------------------------------------------------
# SuggestionKind
# ---------------------------------------------------------------------------


class TestSuggestionKind:
    def test_b1_kinds_exist(self):
        assert SuggestionKind.SCALAR_PRED
        assert SuggestionKind.PATH_PRED
        assert SuggestionKind.TRIM_JOIN

    def test_b2_kinds_exist(self):
        assert SuggestionKind.GROUP_BY
        assert SuggestionKind.AGG
        assert SuggestionKind.HAVING

    def test_b3_kinds_exist(self):
        assert SuggestionKind.COMMUNITY_PARTITION
        assert SuggestionKind.K_SHORTEST

    def test_temporal_kinds_exist(self):
        assert SuggestionKind.TEMPORAL_PIVOT
        assert SuggestionKind.Q_DELTA

    def test_trail_kinds_exist(self):
        assert SuggestionKind.TRAIL_PIVOT
        assert SuggestionKind.SIGNATURE_RESTRICT


# ---------------------------------------------------------------------------
# Suggestion
# ---------------------------------------------------------------------------


class TestSuggestion:
    def test_basic(self):
        s = Suggestion(
            kind=SuggestionKind.PATH_PRED,
            band="B1",
            text="Add PathPred for dominant signature",
        )
        assert s.kind is SuggestionKind.PATH_PRED
        assert s.band == "B1"
        assert "PathPred" in s.text

    def test_default_priority(self):
        s = Suggestion(
            kind=SuggestionKind.SCALAR_PRED,
            band="B1",
            text="x",
        )
        assert isinstance(s.priority, int)

    def test_custom_priority(self):
        s = Suggestion(
            kind=SuggestionKind.TRIM_JOIN,
            band="B1",
            text="y",
            priority=90,
        )
        assert s.priority == 90

    def test_repr_contains_kind(self):
        s = Suggestion(kind=SuggestionKind.AGG, band="B2", text="z")
        assert "AGG" in repr(s) or "AGG" in str(s)


# ---------------------------------------------------------------------------
# Stage1Result / Stage2Result
# ---------------------------------------------------------------------------


class TestStage1Result:
    def test_basic(self):
        r = Stage1Result(
            anchor="sale",
            outgoing_sig_count=5,
            dominant_signature=["verb_a", "verb_b"],
            diversity=0.7,
        )
        assert r.anchor == "sale"
        assert r.outgoing_sig_count == 5
        assert r.dominant_signature == ["verb_a", "verb_b"]
        assert r.diversity == pytest.approx(0.7)

    def test_suggest_stage2_low_entropy(self):
        """Low diversity → recommend direct Bound→Bound deep-dive (Stage 2)."""
        r = Stage1Result(anchor="a", outgoing_sig_count=1, dominant_signature=["v"], diversity=0.1)
        assert r.recommend_stage2 is True

    def test_suggest_more_exploration_high_entropy(self):
        """High diversity → recommend more Stage 1 characterisation."""
        r = Stage1Result(anchor="a", outgoing_sig_count=5, dominant_signature=["v"], diversity=0.9)
        assert r.recommend_stage2 is False


class TestStage2Result:
    def test_basic(self):
        r = Stage2Result(
            source="sale",
            target="customer",
            distinct_sigs=3,
            dominant_signature=["purchase"],
            density=0.25,
        )
        assert r.source == "sale"
        assert r.target == "customer"
        assert r.distinct_sigs == 3
        assert r.density == pytest.approx(0.25)

    def test_default_density_none(self):
        r = Stage2Result(source="a", target="b", distinct_sigs=1, dominant_signature=[])
        assert r.density is None


# ---------------------------------------------------------------------------
# DGMRecommender — construction and routing
# ---------------------------------------------------------------------------


class TestDGMRecommenderInit:
    def test_minimal(self):
        rec = DGMRecommender(sigma=AnnotationSigma([]))
        assert rec.sigma is not None

    def test_with_statistics(self):
        stats = GraphStatistics(node_count=10, edge_count=20)
        rec = DGMRecommender(sigma=AnnotationSigma([]), statistics=stats)
        assert rec.statistics is stats

    def test_entropy_threshold_constant(self):
        assert 0 < ENTROPY_THRESHOLD < 1


class TestEntropyRouting:
    def test_high_entropy_routes_stage1(self):
        rec = DGMRecommender(sigma=AnnotationSigma([]))
        assert rec.route(entropy=0.9) == "stage1"

    def test_low_entropy_routes_stage2(self):
        rec = DGMRecommender(sigma=AnnotationSigma([]))
        assert rec.route(entropy=0.1) == "stage2"

    def test_entropy_at_threshold_routes_stage2(self):
        rec = DGMRecommender(sigma=AnnotationSigma([]))
        assert rec.route(entropy=ENTROPY_THRESHOLD) == "stage2"


# ---------------------------------------------------------------------------
# Annotation-driven rules (§7.4)
# ---------------------------------------------------------------------------


class TestAnnotationDrivenRules:
    def test_degenerate_suppresses_group_by(self):
        """Degenerate(d) → suppress GroupBy suggestion for d."""
        sigma = AnnotationSigma([Degenerate(dim="order_num")])
        rec = DGMRecommender(sigma=sigma)
        suggestions = rec.run_annotation_rules()
        suppressed = [s for s in suggestions if s.band == "suppress" and "order_num" in s.text]
        assert len(suppressed) >= 1

    def test_degenerate_adds_scalar_pred(self):
        sigma = AnnotationSigma([Degenerate(dim="order_num")])
        rec = DGMRecommender(sigma=sigma)
        suggestions = rec.run_annotation_rules()
        added = [s for s in suggestions if s.kind is SuggestionKind.SCALAR_PRED]
        assert len(added) >= 1

    def test_conformed_adds_constellation_path(self):
        sigma = AnnotationSigma([
            Conformed(dim="customer", fact_types={"Sale", "Return"})
        ])
        rec = DGMRecommender(sigma=sigma)
        suggestions = rec.run_annotation_rules()
        paths = [s for s in suggestions if s.kind is SuggestionKind.PATH_PRED]
        assert len(paths) >= 1

    def test_grain_period_suppresses_sum(self):
        sigma = AnnotationSigma([Grain(fact="balance", grain=GrainKind.PERIOD)])
        rec = DGMRecommender(sigma=sigma)
        suggestions = rec.run_annotation_rules()
        suppressed = [s for s in suggestions if s.band == "suppress"]
        assert len(suppressed) >= 1

    def test_grain_period_adds_q_delta(self):
        sigma = AnnotationSigma([Grain(fact="balance", grain=GrainKind.PERIOD)])
        rec = DGMRecommender(sigma=sigma)
        suggestions = rec.run_annotation_rules()
        q_delta = [s for s in suggestions if s.kind is SuggestionKind.Q_DELTA]
        assert len(q_delta) >= 1

    def test_factless_adds_count(self):
        sigma = AnnotationSigma([FactlessFact(fact="attendance")])
        rec = DGMRecommender(sigma=sigma)
        suggestions = rec.run_annotation_rules()
        added = [s for s in suggestions if s.kind is SuggestionKind.AGG]
        assert len(added) >= 1

    def test_hierarchy_adds_drill_down(self):
        sigma = AnnotationSigma([Hierarchy(root="geography", depth=3)])
        rec = DGMRecommender(sigma=sigma)
        suggestions = rec.run_annotation_rules()
        drill = [s for s in suggestions if "drill" in s.text.lower() or "roll" in s.text.lower()]
        assert len(drill) >= 1

    def test_scd3_adds_previous_value(self):
        sigma = AnnotationSigma([SCDType(dim="customer", scd=SCDKind.SCD3)])
        rec = DGMRecommender(sigma=sigma)
        suggestions = rec.run_annotation_rules()
        prev = [s for s in suggestions if "previous_value" in s.text or "SCD3" in s.text]
        assert len(prev) >= 1

    def test_weight_allocative_adds_weighted_form(self):
        sigma = AnnotationSigma([
            WeightConstraint(bridge="bridge1", constraint=WeightConstraintKind.ALLOCATIVE)
        ])
        rec = DGMRecommender(sigma=sigma)
        suggestions = rec.run_annotation_rules()
        weighted = [s for s in suggestions if "weight" in s.text.lower()]
        assert len(weighted) >= 1

    def test_bridge_causal_adds_betweenness(self):
        sigma = AnnotationSigma([
            BridgeSemantics(bridge="chain", sem=BridgeSemanticsKind.CAUSAL)
        ])
        rec = DGMRecommender(sigma=sigma)
        suggestions = rec.run_annotation_rules()
        between = [s for s in suggestions if "BETWEENNESS" in s.text or "TARJAN" in s.text]
        assert len(between) >= 1

    def test_role_playing_adds_cross_role(self):
        sigma = AnnotationSigma([RolePlaying(dim="date", roles=["order_date", "ship_date"])])
        rec = DGMRecommender(sigma=sigma)
        suggestions = rec.run_annotation_rules()
        cross = [s for s in suggestions if "cross" in s.text.lower() or "role" in s.text.lower()]
        assert len(cross) >= 1


# ---------------------------------------------------------------------------
# TrailExpr-driven rules (§7.5)
# ---------------------------------------------------------------------------


class TestTrailExprDrivenRules:
    def test_high_outgoing_adds_signature_pred(self):
        """OUTGOING_SIGNATURES high → suggest SignaturePred to isolate dominant."""
        rec = DGMRecommender(sigma=AnnotationSigma([]))
        suggestions = rec.run_trail_rules(
            anchor="sale",
            outgoing_sig_count=8,
            diversity=0.8,
            entropy=0.85,
        )
        sig = [s for s in suggestions if s.kind is SuggestionKind.SIGNATURE_RESTRICT]
        assert len(sig) >= 1

    def test_high_outgoing_adds_trim_join_from(self):
        rec = DGMRecommender(sigma=AnnotationSigma([]))
        suggestions = rec.run_trail_rules(
            anchor="sale",
            outgoing_sig_count=8,
            diversity=0.8,
            entropy=0.85,
        )
        trim = [s for s in suggestions if s.kind is SuggestionKind.TRIM_JOIN]
        assert len(trim) >= 1

    def test_low_diversity_recommends_bound_bound(self):
        rec = DGMRecommender(sigma=AnnotationSigma([]))
        suggestions = rec.run_trail_rules(
            anchor="sale",
            outgoing_sig_count=2,
            diversity=0.1,
            entropy=0.1,
        )
        bb = [s for s in suggestions if "Bound→Bound" in s.text]
        assert len(bb) >= 1

    def test_high_entropy_adds_global_dominant(self):
        rec = DGMRecommender(sigma=AnnotationSigma([]))
        suggestions = rec.run_trail_rules(
            anchor="sale",
            outgoing_sig_count=5,
            diversity=0.9,
            entropy=0.9,
        )
        global_dom = [
            s for s in suggestions if "GLOBAL_DOMINANT" in s.text
        ]
        assert len(global_dom) >= 1

    def test_low_entropy_adds_signature_restrict(self):
        rec = DGMRecommender(sigma=AnnotationSigma([]))
        suggestions = rec.run_trail_rules(
            anchor="sale",
            outgoing_sig_count=2,
            diversity=0.1,
            entropy=0.1,
        )
        restrict = [
            s for s in suggestions
            if s.kind in (SuggestionKind.SIGNATURE_RESTRICT, SuggestionKind.PATH_PRED)
        ]
        assert len(restrict) >= 1


# ---------------------------------------------------------------------------
# Stage1 characterisation
# ---------------------------------------------------------------------------


class TestStage1Characterise:
    def test_returns_stage1_result(self):
        rec = DGMRecommender(sigma=AnnotationSigma([]))
        result = rec.stage1_characterise(
            anchor="sale",
            outgoing_sig_count=5,
            dominant_signature=["purchase"],
            diversity=0.6,
        )
        assert isinstance(result, Stage1Result)
        assert result.anchor == "sale"

    def test_high_diversity_does_not_recommend_stage2(self):
        rec = DGMRecommender(sigma=AnnotationSigma([]))
        result = rec.stage1_characterise(
            anchor="a",
            outgoing_sig_count=10,
            dominant_signature=["x"],
            diversity=0.95,
        )
        assert result.recommend_stage2 is False


# ---------------------------------------------------------------------------
# Stage2 deep-dive
# ---------------------------------------------------------------------------


class TestStage2DeepDive:
    def test_returns_stage2_result(self):
        rec = DGMRecommender(sigma=AnnotationSigma([]))
        result = rec.stage2_deep_dive(
            source="sale",
            target="customer",
            distinct_sigs=3,
            dominant_signature=["purchase"],
            density=0.3,
        )
        assert isinstance(result, Stage2Result)
        assert result.source == "sale"
        assert result.target == "customer"

    def test_no_density_is_allowed(self):
        rec = DGMRecommender(sigma=AnnotationSigma([]))
        result = rec.stage2_deep_dive(
            source="a",
            target="b",
            distinct_sigs=1,
            dominant_signature=[],
        )
        assert result.density is None


# ---------------------------------------------------------------------------
# BDD feasibility filter (Layer 2)
# ---------------------------------------------------------------------------


class TestBDDFeasibilityFilter:
    def test_tautology_passes(self):
        from sqldim.core.query._dgm_bdd import BDDManager, DGMPredicateBDD
        rec = DGMRecommender(sigma=AnnotationSigma([]))
        mgr = BDDManager()
        bdd = DGMPredicateBDD(mgr)
        uid = bdd.compile_true()
        # A tautology candidate should pass the filter
        assert rec.bdd_feasible(bdd, uid) is True

    def test_contradition_filtered(self):
        from sqldim.core.query._dgm_bdd import BDDManager, DGMPredicateBDD
        rec = DGMRecommender(sigma=AnnotationSigma([]))
        mgr = BDDManager()
        bdd = DGMPredicateBDD(mgr)
        uid = bdd.compile_false()
        # A FALSE predicate is infeasible — should be filtered out
        assert rec.bdd_feasible(bdd, uid) is False

    def test_satisfiable_atom_passes(self):
        from sqldim.core.query._dgm_bdd import BDDManager, DGMPredicateBDD
        from sqldim.core.query._dgm_preds import ScalarPred
        from sqldim.core.query._dgm_refs import PropRef
        rec = DGMRecommender(sigma=AnnotationSigma([]))
        mgr = BDDManager()
        bdd = DGMPredicateBDD(mgr)
        pred = ScalarPred(PropRef("sale", "amount"), ">", 100)
        uid = bdd.compile(pred)
        assert rec.bdd_feasible(bdd, uid) is True

"""Phase 4 — Schema annotation layer (Σ) — RED tests.

Covers:
- All 11 SchemaAnnotation dataclass types and their fields
- Enum types: GrainKind, SCDKind, WeightConstraintKind, BridgeSemanticsKind
- RAGGED sentinel for Hierarchy depth
- Union-type dispatch helper: annotation_kind()
- GraphSchema.annotations attribute (Σ integration)
- Annotation consequence validation: grain-aware agg rejection at construction,
  FactlessFact measure-agg rejection, SCD policy access
- Annotation-driven planner helpers: scd_resolution(), grain_policy(),
  bridge_dag_mode(), weight_policy()
"""

import pytest

from sqldim.core.query._dgm_annotations import (
    Conformed,
    Grain,
    SCDType,
    Degenerate,
    RolePlaying,
    ProjectsFrom,
    FactlessFact,
    DerivedFact,
    WeightConstraint,
    BridgeSemantics,
    Hierarchy,
    GrainKind,
    SCDKind,
    WeightConstraintKind,
    BridgeSemanticsKind,
    RAGGED,
    SchemaAnnotation,
    annotation_kind,
    AnnotationSigma,
)
from sqldim.core.graph.schema_graph import GraphSchema


# ---------------------------------------------------------------------------
# Enum types
# ---------------------------------------------------------------------------


class TestGrainKind:
    def test_event(self):
        assert GrainKind.EVENT.value == "EVENT"

    def test_period(self):
        assert GrainKind.PERIOD.value == "PERIOD"

    def test_accumulating(self):
        assert GrainKind.ACCUMULATING.value == "ACCUMULATING"

    def test_all_members(self):
        assert set(GrainKind) == {GrainKind.EVENT, GrainKind.PERIOD, GrainKind.ACCUMULATING}


class TestSCDKind:
    def test_scd1(self):
        assert SCDKind.SCD1.value == "SCD1"

    def test_scd2(self):
        assert SCDKind.SCD2.value == "SCD2"

    def test_scd3(self):
        assert SCDKind.SCD3.value == "SCD3"

    def test_scd6(self):
        assert SCDKind.SCD6.value == "SCD6"

    def test_all_members(self):
        assert set(SCDKind) == {SCDKind.SCD1, SCDKind.SCD2, SCDKind.SCD3, SCDKind.SCD6}


class TestWeightConstraintKind:
    def test_allocative(self):
        assert WeightConstraintKind.ALLOCATIVE.value == "ALLOCATIVE"

    def test_unconstrained(self):
        assert WeightConstraintKind.UNCONSTRAINED.value == "UNCONSTRAINED"


class TestBridgeSemanticsKind:
    def test_causal(self):
        assert BridgeSemanticsKind.CAUSAL.value == "CAUSAL"

    def test_temporal(self):
        assert BridgeSemanticsKind.TEMPORAL.value == "TEMPORAL"

    def test_supersession(self):
        assert BridgeSemanticsKind.SUPERSESSION.value == "SUPERSESSION"

    def test_structural(self):
        assert BridgeSemanticsKind.STRUCTURAL.value == "STRUCTURAL"


class TestRAGGED:
    def test_ragged_is_singleton(self):
        assert RAGGED is RAGGED

    def test_ragged_repr(self):
        assert "RAGGED" in repr(RAGGED)


# ---------------------------------------------------------------------------
# Conformed
# ---------------------------------------------------------------------------


class TestConformed:
    def test_basic(self):
        a = Conformed(dim="customer", fact_types={"Sale", "Return"})
        assert a.dim == "customer"
        assert a.fact_types == {"Sale", "Return"}

    def test_isinstance_schema_annotation(self):
        a = Conformed(dim="d", fact_types={"F"})
        assert isinstance(a, SchemaAnnotation)

    def test_equality(self):
        a = Conformed(dim="d", fact_types={"F"})
        b = Conformed(dim="d", fact_types={"F"})
        assert a == b

    def test_different_dims_not_equal(self):
        a = Conformed(dim="d1", fact_types={"F"})
        b = Conformed(dim="d2", fact_types={"F"})
        assert a != b


# ---------------------------------------------------------------------------
# Grain
# ---------------------------------------------------------------------------


class TestGrain:
    def test_event_grain(self):
        a = Grain(fact="sale", grain=GrainKind.EVENT)
        assert a.fact == "sale"
        assert a.grain is GrainKind.EVENT

    def test_period_grain(self):
        a = Grain(fact="balance", grain=GrainKind.PERIOD)
        assert a.grain is GrainKind.PERIOD

    def test_accumulating_grain(self):
        a = Grain(fact="order_ls", grain=GrainKind.ACCUMULATING)
        assert a.grain is GrainKind.ACCUMULATING

    def test_isinstance_schema_annotation(self):
        a = Grain(fact="f", grain=GrainKind.EVENT)
        assert isinstance(a, SchemaAnnotation)


# ---------------------------------------------------------------------------
# SCDType
# ---------------------------------------------------------------------------


class TestSCDType:
    def test_scd2_minimal(self):
        a = SCDType(dim="customer", scd=SCDKind.SCD2)
        assert a.dim == "customer"
        assert a.scd is SCDKind.SCD2
        assert a.versioned_attrs is None
        assert a.overwrite_attrs is None
        assert a.prev_attrs is None

    def test_scd3_with_attrs(self):
        a = SCDType(
            dim="product",
            scd=SCDKind.SCD3,
            versioned_attrs={"price"},
            prev_attrs={"prev_price"},
        )
        assert a.versioned_attrs == {"price"}
        assert a.prev_attrs == {"prev_price"}

    def test_scd6_full_attrs(self):
        a = SCDType(
            dim="cust",
            scd=SCDKind.SCD6,
            versioned_attrs={"name"},
            overwrite_attrs={"email"},
            prev_attrs={"prev_name"},
        )
        assert a.overwrite_attrs == {"email"}

    def test_isinstance_schema_annotation(self):
        a = SCDType(dim="d", scd=SCDKind.SCD1)
        assert isinstance(a, SchemaAnnotation)


# ---------------------------------------------------------------------------
# Degenerate
# ---------------------------------------------------------------------------


class TestDegenerate:
    def test_basic(self):
        a = Degenerate(dim="order_number")
        assert a.dim == "order_number"

    def test_isinstance_schema_annotation(self):
        assert isinstance(Degenerate(dim="x"), SchemaAnnotation)


# ---------------------------------------------------------------------------
# RolePlaying
# ---------------------------------------------------------------------------


class TestRolePlaying:
    def test_basic(self):
        a = RolePlaying(dim="date", roles=["order_date", "ship_date", "due_date"])
        assert a.dim == "date"
        assert a.roles == ["order_date", "ship_date", "due_date"]

    def test_isinstance_schema_annotation(self):
        assert isinstance(RolePlaying(dim="d", roles=[]), SchemaAnnotation)


# ---------------------------------------------------------------------------
# ProjectsFrom
# ---------------------------------------------------------------------------


class TestProjectsFrom:
    def test_basic(self):
        a = ProjectsFrom(dim_mini="product_mini", dim_full="product")
        assert a.dim_mini == "product_mini"
        assert a.dim_full == "product"

    def test_isinstance_schema_annotation(self):
        assert isinstance(ProjectsFrom(dim_mini="m", dim_full="f"), SchemaAnnotation)


# ---------------------------------------------------------------------------
# FactlessFact
# ---------------------------------------------------------------------------


class TestFactlessFact:
    def test_basic(self):
        a = FactlessFact(fact="attendance")
        assert a.fact == "attendance"

    def test_isinstance_schema_annotation(self):
        assert isinstance(FactlessFact(fact="f"), SchemaAnnotation)


# ---------------------------------------------------------------------------
# DerivedFact
# ---------------------------------------------------------------------------


class TestDerivedFact:
    def test_basic(self):
        a = DerivedFact(fact="margin", sources=["revenue", "cost"], expr="revenue - cost")
        assert a.fact == "margin"
        assert a.sources == ["revenue", "cost"]
        assert a.expr == "revenue - cost"

    def test_isinstance_schema_annotation(self):
        assert isinstance(DerivedFact(fact="f", sources=[], expr=""), SchemaAnnotation)


# ---------------------------------------------------------------------------
# WeightConstraint
# ---------------------------------------------------------------------------


class TestWeightConstraint:
    def test_allocative(self):
        a = WeightConstraint(bridge="customer_bridge", constraint=WeightConstraintKind.ALLOCATIVE)
        assert a.bridge == "customer_bridge"
        assert a.constraint is WeightConstraintKind.ALLOCATIVE

    def test_unconstrained(self):
        a = WeightConstraint(bridge="b", constraint=WeightConstraintKind.UNCONSTRAINED)
        assert a.constraint is WeightConstraintKind.UNCONSTRAINED

    def test_isinstance_schema_annotation(self):
        assert isinstance(
            WeightConstraint(bridge="b", constraint=WeightConstraintKind.ALLOCATIVE),
            SchemaAnnotation,
        )


# ---------------------------------------------------------------------------
# BridgeSemantics
# ---------------------------------------------------------------------------


class TestBridgeSemantics:
    def test_causal(self):
        a = BridgeSemantics(bridge="event_chain", sem=BridgeSemanticsKind.CAUSAL)
        assert a.bridge == "event_chain"
        assert a.sem is BridgeSemanticsKind.CAUSAL

    def test_temporal(self):
        a = BridgeSemantics(bridge="b", sem=BridgeSemanticsKind.TEMPORAL)
        assert a.sem is BridgeSemanticsKind.TEMPORAL

    def test_supersession(self):
        a = BridgeSemantics(bridge="b", sem=BridgeSemanticsKind.SUPERSESSION)
        assert a.sem is BridgeSemanticsKind.SUPERSESSION

    def test_structural(self):
        a = BridgeSemantics(bridge="b", sem=BridgeSemanticsKind.STRUCTURAL)
        assert a.sem is BridgeSemanticsKind.STRUCTURAL

    def test_isinstance_schema_annotation(self):
        assert isinstance(
            BridgeSemantics(bridge="b", sem=BridgeSemanticsKind.CAUSAL),
            SchemaAnnotation,
        )


# ---------------------------------------------------------------------------
# Hierarchy
# ---------------------------------------------------------------------------


class TestHierarchy:
    def test_fixed_depth(self):
        a = Hierarchy(root="geography", depth=4)
        assert a.root == "geography"
        assert a.depth == 4

    def test_ragged_depth(self):
        a = Hierarchy(root="org", depth=RAGGED)
        assert a.depth is RAGGED

    def test_isinstance_schema_annotation(self):
        assert isinstance(Hierarchy(root="r", depth=1), SchemaAnnotation)


# ---------------------------------------------------------------------------
# annotation_kind() dispatch helper
# ---------------------------------------------------------------------------


class TestAnnotationKind:
    def test_conformed(self):
        assert annotation_kind(Conformed(dim="d", fact_types=set())) == "Conformed"

    def test_grain(self):
        assert annotation_kind(Grain(fact="f", grain=GrainKind.EVENT)) == "Grain"

    def test_scd_type(self):
        assert annotation_kind(SCDType(dim="d", scd=SCDKind.SCD1)) == "SCDType"

    def test_degenerate(self):
        assert annotation_kind(Degenerate(dim="d")) == "Degenerate"

    def test_role_playing(self):
        assert annotation_kind(RolePlaying(dim="d", roles=[])) == "RolePlaying"

    def test_projects_from(self):
        assert annotation_kind(ProjectsFrom(dim_mini="m", dim_full="f")) == "ProjectsFrom"

    def test_factless_fact(self):
        assert annotation_kind(FactlessFact(fact="f")) == "FactlessFact"

    def test_derived_fact(self):
        assert annotation_kind(DerivedFact(fact="f", sources=[], expr="")) == "DerivedFact"

    def test_weight_constraint(self):
        assert annotation_kind(
            WeightConstraint(bridge="b", constraint=WeightConstraintKind.ALLOCATIVE)
        ) == "WeightConstraint"

    def test_bridge_semantics(self):
        assert annotation_kind(
            BridgeSemantics(bridge="b", sem=BridgeSemanticsKind.CAUSAL)
        ) == "BridgeSemantics"

    def test_hierarchy(self):
        assert annotation_kind(Hierarchy(root="r", depth=1)) == "Hierarchy"

    def test_unknown_type_returns_class_name(self):
        """annotation_kind on an unknown subclass falls back to __name__."""
        class CustomAnnotation(SchemaAnnotation):
            pass
        obj = CustomAnnotation()
        result = annotation_kind(obj)
        assert result == "CustomAnnotation"


# ---------------------------------------------------------------------------
# AnnotationSigma — collection / lookup helper
# ---------------------------------------------------------------------------


class TestAnnotationSigma:
    def setup_method(self):
        self.sigma = AnnotationSigma([
            Conformed(dim="customer", fact_types={"Sale", "Return"}),
            Grain(fact="sale", grain=GrainKind.EVENT),
            Grain(fact="balance", grain=GrainKind.PERIOD),
            SCDType(dim="customer", scd=SCDKind.SCD2),
            FactlessFact(fact="attendance"),
            BridgeSemantics(bridge="event_chain", sem=BridgeSemanticsKind.CAUSAL),
            Hierarchy(root="geography", depth=3),
        ])

    def test_lookup_grain_event(self):
        result = self.sigma.grain_of("sale")
        assert result is GrainKind.EVENT

    def test_lookup_grain_period(self):
        result = self.sigma.grain_of("balance")
        assert result is GrainKind.PERIOD

    def test_lookup_grain_missing_returns_none(self):
        assert self.sigma.grain_of("nonexistent") is None

    def test_lookup_scd(self):
        result = self.sigma.scd_of("customer")
        assert result is SCDKind.SCD2

    def test_lookup_scd_missing_returns_none(self):
        assert self.sigma.scd_of("other") is None

    def test_is_factless(self):
        assert self.sigma.is_factless("attendance") is True

    def test_is_factless_false(self):
        assert self.sigma.is_factless("sale") is False

    def test_bridge_sem(self):
        assert self.sigma.bridge_sem_of("event_chain") is BridgeSemanticsKind.CAUSAL

    def test_bridge_sem_missing(self):
        assert self.sigma.bridge_sem_of("other") is None

    def test_is_conformed(self):
        assert self.sigma.is_conformed("customer", "Sale") is True
        assert self.sigma.is_conformed("customer", "Invoice") is False

    def test_is_conformed_unknown_dim_returns_false(self):
        """is_conformed returns False (line 374 fallback) when dim is not Conformed."""
        assert self.sigma.is_conformed("unknown_dim", "Sale") is False

    def test_hierarchy_depth(self):
        assert self.sigma.hierarchy_depth_of("geography") == 3

    def test_hierarchy_depth_missing(self):
        assert self.sigma.hierarchy_depth_of("other") is None

    def test_filter_by_type(self):
        grains = self.sigma.filter(Grain)
        assert len(grains) == 2
        assert all(isinstance(g, Grain) for g in grains)

    def test_iter(self):
        items = list(self.sigma)
        assert len(items) == 7

    def test_len(self):
        assert len(self.sigma) == 7

    def test_add(self):
        a2 = AnnotationSigma([])
        a2.add(Grain(fact="x", grain=GrainKind.ACCUMULATING))
        assert len(a2) == 1

    def test_add_duplicate_raises(self):
        """Adding the exact same annotation twice raises ValueError."""
        sigma = AnnotationSigma([])
        ann = Grain(fact="x", grain=GrainKind.EVENT)
        sigma.add(ann)
        with pytest.raises(ValueError, match="duplicate"):
            sigma.add(ann)


# ---------------------------------------------------------------------------
# GraphSchema integration — annotations attribute
# ---------------------------------------------------------------------------


class TestGraphSchemaAnnotations:
    def test_default_empty_annotations(self):
        gs = GraphSchema(vertices=[], edges=[])
        assert gs.annotations == []

    def test_pass_annotations(self):
        ann = [Grain(fact="sale", grain=GrainKind.EVENT)]
        gs = GraphSchema(vertices=[], edges=[], annotations=ann)
        assert gs.annotations == ann

    def test_to_dict_includes_annotations(self):
        gs = GraphSchema(
            vertices=[],
            edges=[],
            annotations=[Degenerate(dim="order_num")],
        )
        d = gs.to_dict()
        assert "annotations" in d
        assert len(d["annotations"]) == 1

    def test_to_dict_annotation_kind_key(self):
        gs = GraphSchema(
            vertices=[],
            edges=[],
            annotations=[Degenerate(dim="order_num")],
        )
        d = gs.to_dict()
        assert d["annotations"][0]["kind"] == "Degenerate"


# ---------------------------------------------------------------------------
# Annotation consequence validation helpers
# ---------------------------------------------------------------------------


class TestAnnotationConsequences:
    """Annotation objects enforce consequences described in spec §2.4 / Rule 1b."""

    def test_grain_period_sum_invalid(self):
        """Grain(PERIOD) declares SUM invalid — check via Grain.sum_invalid property."""
        g = Grain(fact="balance", grain=GrainKind.PERIOD)
        assert g.sum_invalid is True

    def test_grain_event_sum_valid(self):
        g = Grain(fact="sale", grain=GrainKind.EVENT)
        assert g.sum_invalid is False

    def test_grain_accumulating_sum_invalid(self):
        g = Grain(fact="order_ls", grain=GrainKind.ACCUMULATING)
        assert g.sum_invalid is False  # warn, not reject per spec

    def test_factless_measures_invalid(self):
        """FactlessFact declares all aggregation-measures invalid."""
        f = FactlessFact(fact="attendance")
        assert f.measures_invalid is True

    def test_bridge_causal_dag_mode(self):
        """BridgeSemantics(CAUSAL) declares DAG mode (drop cycle guard)."""
        b = BridgeSemantics(bridge="chain", sem=BridgeSemanticsKind.CAUSAL)
        assert b.is_dag is True

    def test_bridge_structural_not_dag(self):
        b = BridgeSemantics(bridge="link", sem=BridgeSemanticsKind.STRUCTURAL)
        assert b.is_dag is False

    def test_scd1_strip_temporal(self):
        """SCDType(SCD1) declares strip_temporal = True."""
        s = SCDType(dim="d", scd=SCDKind.SCD1)
        assert s.strip_temporal is True

    def test_scd2_no_strip_temporal(self):
        s = SCDType(dim="d", scd=SCDKind.SCD2)
        assert s.strip_temporal is False

    def test_weight_constraint_allocative(self):
        """WeightConstraint.is_allocative is True for ALLOCATIVE."""
        w = WeightConstraint(bridge="b", constraint=WeightConstraintKind.ALLOCATIVE)
        assert w.is_allocative is True

    def test_weight_constraint_unconstrained(self):
        w = WeightConstraint(bridge="b", constraint=WeightConstraintKind.UNCONSTRAINED)
        assert w.is_allocative is False

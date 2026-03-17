"""Tests for the Silver-layer build order encoding.

Covers: ModelKind enum, SilverBuildOrder classification, dependency checks,
and sorted build ordering for mixed model sets.
"""
from __future__ import annotations

import pytest

from sqldim.medallion.build_order import ModelKind, SilverBuildOrder
from sqldim.core.models import DimensionModel, FactModel, BridgeModel
from sqldim.graph.schema_graph import SchemaGraph


# ---------------------------------------------------------------------------
# Concrete model fixtures (minimal table=False to skip SQLite metaclass)
# ---------------------------------------------------------------------------

class Dim(DimensionModel, table=False):
    pass


class Fact(FactModel, table=False):
    pass


class Bridge(BridgeModel, table=False):
    pass


# SchemaGraph is NOT a SQLModel subclass — it is its own class; we use it
# as a sentinel "graph" type.


# ---------------------------------------------------------------------------
# TestModelKind
# ---------------------------------------------------------------------------

class TestModelKind:
    def test_values(self):
        assert ModelKind.DIMENSION.value == "dimension"
        assert ModelKind.FACT.value == "fact"
        assert ModelKind.BRIDGE.value == "bridge"
        assert ModelKind.GRAPH.value == "graph"

    def test_ordering_is_strict(self):
        """Earlier kinds must sort before later kinds."""
        kinds = list(ModelKind)
        assert kinds.index(ModelKind.DIMENSION) < kinds.index(ModelKind.FACT)
        assert kinds.index(ModelKind.FACT) < kinds.index(ModelKind.BRIDGE)
        assert kinds.index(ModelKind.BRIDGE) < kinds.index(ModelKind.GRAPH)

    def test_population_order_attribute(self):
        assert SilverBuildOrder.POPULATION_ORDER == [
            ModelKind.DIMENSION,
            ModelKind.FACT,
            ModelKind.BRIDGE,
            ModelKind.GRAPH,
        ]


# ---------------------------------------------------------------------------
# TestClassify
# ---------------------------------------------------------------------------

class TestClassify:
    def test_dimension_model(self):
        assert SilverBuildOrder().classify(Dim) == ModelKind.DIMENSION

    def test_fact_model(self):
        assert SilverBuildOrder().classify(Fact) == ModelKind.FACT

    def test_bridge_model(self):
        assert SilverBuildOrder().classify(Bridge) == ModelKind.BRIDGE

    def test_schema_graph(self):
        assert SilverBuildOrder().classify(SchemaGraph) == ModelKind.GRAPH

    def test_unknown_raises(self):
        class Unknown:
            pass
        with pytest.raises(TypeError, match="Cannot classify"):
            SilverBuildOrder().classify(Unknown)

    def test_non_class_raises(self):
        """issubclass raises TypeError for non-class; must still raise TypeError."""
        with pytest.raises(TypeError, match="Cannot classify"):
            SilverBuildOrder().classify(42)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# TestCanBuildAfter
# ---------------------------------------------------------------------------

class TestCanBuildAfter:
    def setup_method(self):
        self.sbo = SilverBuildOrder()

    def test_dimension_needs_nothing(self):
        assert self.sbo.can_build_after(ModelKind.DIMENSION, set()) is True

    def test_fact_needs_dimension(self):
        assert self.sbo.can_build_after(ModelKind.FACT, set()) is False
        assert self.sbo.can_build_after(ModelKind.FACT, {ModelKind.DIMENSION}) is True

    def test_bridge_needs_fact(self):
        assert self.sbo.can_build_after(ModelKind.BRIDGE, {ModelKind.DIMENSION}) is False
        assert self.sbo.can_build_after(
            ModelKind.BRIDGE, {ModelKind.DIMENSION, ModelKind.FACT}
        ) is True

    def test_graph_needs_bridge(self):
        assert self.sbo.can_build_after(
            ModelKind.GRAPH, {ModelKind.DIMENSION, ModelKind.FACT}
        ) is False
        assert self.sbo.can_build_after(
            ModelKind.GRAPH, {ModelKind.DIMENSION, ModelKind.FACT, ModelKind.BRIDGE}
        ) is True

    def test_extra_preceding_kinds_accepted(self):
        """Providing more than strictly required is still OK."""
        all_kinds = {ModelKind.DIMENSION, ModelKind.FACT, ModelKind.BRIDGE}
        assert self.sbo.can_build_after(ModelKind.GRAPH, all_kinds) is True


# ---------------------------------------------------------------------------
# TestBuildOrder
# ---------------------------------------------------------------------------

class TestBuildOrder:
    def setup_method(self):
        self.sbo = SilverBuildOrder()

    def test_empty_list(self):
        assert self.sbo.build_order([]) == []

    def test_single_dimension(self):
        assert self.sbo.build_order([Dim]) == [Dim]

    def test_dim_fact_sorted(self):
        result = self.sbo.build_order([Fact, Dim])
        assert result.index(Dim) < result.index(Fact)

    def test_full_pipeline_order(self):
        result = self.sbo.build_order([SchemaGraph, Bridge, Fact, Dim])
        assert result.index(Dim) < result.index(Fact)
        assert result.index(Fact) < result.index(Bridge)
        assert result.index(Bridge) < result.index(SchemaGraph)

    def test_stable_within_same_kind(self):
        """Multiple models of the same kind keep insertion order."""
        class Dim2(DimensionModel, table=False):
            pass

        class Fact2(FactModel, table=False):
            pass

        result = self.sbo.build_order([Fact, Dim, Fact2, Dim2])
        # All dims before all facts
        dim_idxs = [result.index(Dim), result.index(Dim2)]
        fact_idxs = [result.index(Fact), result.index(Fact2)]
        assert max(dim_idxs) < min(fact_idxs)
        # Insertion order preserved within same kind
        assert result.index(Dim) < result.index(Dim2)
        assert result.index(Fact) < result.index(Fact2)

    def test_unknown_model_raises(self):
        class Unknown:
            pass
        with pytest.raises(TypeError, match="Cannot classify"):
            self.sbo.build_order([Dim, Unknown])


# ---------------------------------------------------------------------------
# TestPublicExports
# ---------------------------------------------------------------------------

class TestPublicExports:
    def test_importable_from_medallion(self):
        from sqldim.medallion import ModelKind, SilverBuildOrder  # noqa: F401

    def test_importable_from_sqldim(self):
        from sqldim import SilverBuildOrder, ModelKind  # noqa: F401

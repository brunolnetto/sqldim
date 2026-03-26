"""
TDD tests for DGM edge type classification in GraphSchema.

Covers the τ_E : E → {verb, bridge} function from DGM §2.
"""

from __future__ import annotations


from sqldim import DimensionModel, FactModel, BridgeModel, Field
from sqldim.core.graph.schema_graph import SchemaGraph


# ---------------------------------------------------------------------------
# Minimal test models
# ---------------------------------------------------------------------------


class CustomerDim(DimensionModel, table=True):
    __tablename__ = "dgm_test_customer"
    __natural_key__ = ["email"]
    id: int = Field(primary_key=True, surrogate_key=True)
    email: str
    segment: str


class ProductDim(DimensionModel, table=True):
    __tablename__ = "dgm_test_product"
    __natural_key__ = ["sku"]
    id: int = Field(primary_key=True, surrogate_key=True)
    sku: str
    category: str


class SaleFact(FactModel, table=True):
    __tablename__ = "dgm_test_sale"
    __grain__ = "one row per sale"
    id: int = Field(primary_key=True)
    customer_id: int = Field(foreign_key="dgm_test_customer.id", dimension=CustomerDim)
    product_id: int = Field(foreign_key="dgm_test_product.id", dimension=ProductDim)
    revenue: float


class ProductSegmentBridge(BridgeModel, table=True):
    __tablename__ = "dgm_test_prod_seg_bridge"
    __bridge_keys__ = ["product_id", "segment_id"]
    id: int = Field(default=None, primary_key=True)
    product_id: int
    segment_id: int
    weight: float = Field(default=1.0)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestGraphSchemaEdgeKind:
    """§ DGM §4: edge type function τ_E maps each edge to verb or bridge."""

    def test_fact_model_classified_as_verb(self):
        sg = SchemaGraph([CustomerDim, ProductDim, SaleFact])
        schema = sg.graph_schema()
        verb_edges = [e for e in schema.edges if e["edge_kind"] == "verb"]
        names = [e["name"] for e in verb_edges]
        assert "SaleFact" in names

    def test_bridge_model_classified_as_bridge(self):
        sg = SchemaGraph(
            [CustomerDim, ProductDim],
            bridge_models=[ProductSegmentBridge],
        )
        schema = sg.graph_schema()
        bridge_edges = [e for e in schema.edges if e["edge_kind"] == "bridge"]
        names = [e["name"] for e in bridge_edges]
        assert "ProductSegmentBridge" in names

    def test_edge_kind_disjoint(self):
        """Every edge has exactly one kind: verb or bridge."""
        sg = SchemaGraph(
            [CustomerDim, ProductDim, SaleFact],
            bridge_models=[ProductSegmentBridge],
        )
        schema = sg.graph_schema()
        for edge in schema.edges:
            assert edge["edge_kind"] in ("verb", "bridge"), (
                f"Unknown edge_kind: {edge['edge_kind']!r}"
            )

    def test_no_bridge_models_default(self):
        """SchemaGraph without bridge_models has all verb edges."""
        sg = SchemaGraph([CustomerDim, ProductDim, SaleFact])
        schema = sg.graph_schema()
        kinds = {e["edge_kind"] for e in schema.edges}
        assert kinds == {"verb"}

    def test_bridge_edge_has_weight_flag(self):
        """Bridge edges expose that the model has a weight column."""
        sg = SchemaGraph([], bridge_models=[ProductSegmentBridge])
        schema = sg.graph_schema()
        bridge = next(e for e in schema.edges if e["edge_kind"] == "bridge")
        assert bridge.get("has_weight") is True

    def test_graph_schema_to_dict_includes_edge_kind(self):
        sg = SchemaGraph([CustomerDim, ProductDim, SaleFact])
        d = sg.graph_schema().to_dict()
        for edge in d["edges"]:
            assert "edge_kind" in edge

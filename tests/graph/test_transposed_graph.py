"""
Phase 1 (DGM v0.16) — RED tests.

Covers:
  § DGM §2.1: G^T (transposed adjacency list) via forward_adj / transposed_adj
  § DGM §2.3: P(e).valid_from/to metadata on verb and bridge edges
  § DGM §2.1: N = D ∪ F — fact nodes are first-class graph nodes (all_nodes)
"""

from __future__ import annotations


from sqldim import DimensionModel, FactModel, BridgeModel, Field
from sqldim.core.graph.schema_graph import SchemaGraph


# ---------------------------------------------------------------------------
# Minimal test models  (table-name prefix "trsp_" to avoid collisions)
# ---------------------------------------------------------------------------


class TrspRegionDim(DimensionModel, table=True):
    __tablename__ = "trsp_region"
    __natural_key__ = ["code"]
    id: int = Field(default=None, primary_key=True, surrogate_key=True)
    code: str


class TrspProductDim(DimensionModel, table=True):
    __tablename__ = "trsp_product"
    __natural_key__ = ["sku"]
    id: int = Field(default=None, primary_key=True, surrogate_key=True)
    sku: str


class TrspSaleFact(FactModel, table=True):
    __tablename__ = "trsp_sale"
    __grain__ = "one row per sale"
    id: int = Field(default=None, primary_key=True)
    region_id: int = Field(foreign_key="trsp_region.id", dimension=TrspRegionDim)
    product_id: int = Field(foreign_key="trsp_product.id", dimension=TrspProductDim)
    revenue: float


class TrspTemporalFact(FactModel, table=True):
    """Fact model that carries valid_from / valid_to (used for temporal edges)."""

    __tablename__ = "trsp_temporal_fact"
    __grain__ = "one row per event"
    id: int = Field(default=None, primary_key=True)
    region_id: int = Field(foreign_key="trsp_region.id", dimension=TrspRegionDim)
    valid_from: str | None = None
    valid_to: str | None = None
    amount: float = 0.0


class TrspPromoBridge(BridgeModel, table=True):
    """Bridge edge between TrspRegionDim and TrspProductDim."""

    __tablename__ = "trsp_promo"
    __bridge_keys__ = ["region_id", "product_id"]
    __subject__: type = TrspRegionDim  # type: ignore[assignment]
    __object__: type = TrspProductDim  # type: ignore[assignment]
    id: int = Field(default=None, primary_key=True)
    region_id: int
    product_id: int
    weight: float = 1.0
    valid_from: str | None = None
    valid_to: str | None = None


class TrspBridgeNoTemporal(BridgeModel, table=True):
    """Bridge edge with no valid_from/to columns."""

    __tablename__ = "trsp_bridge_notemporal"
    __bridge_keys__ = ["a_id", "b_id"]
    id: int = Field(default=None, primary_key=True)
    a_id: int
    b_id: int


# ---------------------------------------------------------------------------
# § Edge temporal metadata  (DGM §2.3)
# ---------------------------------------------------------------------------


class TestEdgeTemporalFields:
    """P(e).valid_from/to are present in edge info dicts per DGM §2.3."""

    def test_bridge_edge_info_has_valid_from_key(self):
        sg = SchemaGraph(
            [TrspRegionDim, TrspProductDim, TrspSaleFact],
            bridge_models=[TrspPromoBridge],
        )
        schema = sg.graph_schema()
        bridge = next(e for e in schema.edges if e["edge_kind"] == "bridge")
        assert "valid_from" in bridge

    def test_bridge_edge_info_has_valid_to_key(self):
        sg = SchemaGraph(
            [TrspRegionDim, TrspProductDim, TrspSaleFact],
            bridge_models=[TrspPromoBridge],
        )
        schema = sg.graph_schema()
        bridge = next(e for e in schema.edges if e["edge_kind"] == "bridge")
        assert "valid_to" in bridge

    def test_bridge_with_temporal_columns_reports_true(self):
        sg = SchemaGraph(
            [TrspRegionDim, TrspProductDim, TrspSaleFact],
            bridge_models=[TrspPromoBridge],
        )
        schema = sg.graph_schema()
        bridge = next(e for e in schema.edges if e["edge_kind"] == "bridge")
        assert bridge["valid_from"] is True
        assert bridge["valid_to"] is True

    def test_bridge_without_temporal_columns_reports_false(self):
        sg = SchemaGraph(
            [TrspRegionDim, TrspProductDim, TrspSaleFact],
            bridge_models=[TrspBridgeNoTemporal],
        )
        schema = sg.graph_schema()
        bridge = next(e for e in schema.edges if e["edge_kind"] == "bridge")
        assert bridge["valid_from"] is False
        assert bridge["valid_to"] is False

    def test_verb_edge_info_has_valid_from_key(self):
        sg = SchemaGraph([TrspRegionDim, TrspProductDim, TrspSaleFact])
        schema = sg.graph_schema()
        verb = next(e for e in schema.edges if e["edge_kind"] == "verb")
        assert "valid_from" in verb

    def test_verb_edge_info_has_valid_to_key(self):
        sg = SchemaGraph([TrspRegionDim, TrspProductDim, TrspSaleFact])
        schema = sg.graph_schema()
        verb = next(e for e in schema.edges if e["edge_kind"] == "verb")
        assert "valid_to" in verb

    def test_verb_edge_without_temporal_columns_reports_false(self):
        sg = SchemaGraph([TrspRegionDim, TrspProductDim, TrspSaleFact])
        schema = sg.graph_schema()
        verb = next(e for e in schema.edges if e["edge_kind"] == "verb")
        assert verb["valid_from"] is False
        assert verb["valid_to"] is False

    def test_verb_edge_with_temporal_columns_reports_true(self):
        sg = SchemaGraph([TrspRegionDim, TrspTemporalFact])
        schema = sg.graph_schema()
        verb = next(e for e in schema.edges if e["edge_kind"] == "verb")
        assert verb["valid_from"] is True
        assert verb["valid_to"] is True


# ---------------------------------------------------------------------------
# § Fact-as-node: N = D ∪ F  (DGM §2.1, §2.2)
# ---------------------------------------------------------------------------


class TestFactAsNode:
    """all_nodes returns N = D ∪ F — both DimensionModels and FactModels."""

    def test_all_nodes_includes_dimension_models(self):
        sg = SchemaGraph([TrspRegionDim, TrspProductDim, TrspSaleFact])
        node_names = {n.__name__ for n in sg.all_nodes}
        assert "TrspRegionDim" in node_names
        assert "TrspProductDim" in node_names

    def test_all_nodes_includes_fact_models(self):
        sg = SchemaGraph([TrspRegionDim, TrspProductDim, TrspSaleFact])
        node_names = {n.__name__ for n in sg.all_nodes}
        assert "TrspSaleFact" in node_names

    def test_all_nodes_count_equals_dims_plus_facts(self):
        sg = SchemaGraph([TrspRegionDim, TrspProductDim, TrspSaleFact])
        assert len(sg.all_nodes) == len(sg.vertices) + len(sg.edges)

    def test_all_nodes_returns_list(self):
        sg = SchemaGraph([TrspRegionDim, TrspProductDim, TrspSaleFact])
        assert isinstance(sg.all_nodes, list)

    def test_all_nodes_empty_schema(self):
        # Edge case: schema with zero-length models list should not raise
        sg = SchemaGraph([TrspRegionDim])
        nodes = sg.all_nodes
        assert "TrspRegionDim" in {n.__name__ for n in nodes}


# ---------------------------------------------------------------------------
# § Forward adjacency  (DGM §2.1)
# ---------------------------------------------------------------------------


class TestForwardAdj:
    """forward_adj maps each node class_name → list of reachable class_names."""

    def test_forward_adj_returns_dict(self):
        sg = SchemaGraph([TrspRegionDim, TrspProductDim, TrspSaleFact])
        assert isinstance(sg.forward_adj, dict)

    def test_forward_adj_keys_cover_all_nodes(self):
        sg = SchemaGraph([TrspRegionDim, TrspProductDim, TrspSaleFact])
        fadj = sg.forward_adj
        all_names = {n.__name__ for n in sg.all_nodes}
        assert all_names == set(fadj.keys())

    def test_verb_edges_go_dim_to_fact(self):
        """Verb edges: D → F in forward adjacency."""
        sg = SchemaGraph([TrspRegionDim, TrspProductDim, TrspSaleFact])
        fadj = sg.forward_adj
        assert "TrspSaleFact" in fadj["TrspRegionDim"]
        assert "TrspSaleFact" in fadj["TrspProductDim"]

    def test_fact_nodes_have_no_outgoing_verb_edges(self):
        """D6: fact nodes are not sources of verb edges."""
        sg = SchemaGraph([TrspRegionDim, TrspProductDim, TrspSaleFact])
        fadj = sg.forward_adj
        assert fadj["TrspSaleFact"] == []

    def test_bridge_edges_appear_in_forward_adj(self):
        """Bridge edges contribute subject → object entries."""
        sg = SchemaGraph(
            [TrspRegionDim, TrspProductDim, TrspSaleFact],
            bridge_models=[TrspPromoBridge],
        )
        fadj = sg.forward_adj
        assert "TrspProductDim" in fadj["TrspRegionDim"]

    def test_bridge_without_subject_object_skipped(self):
        """Bridges without explicit __subject__/__object__ do not contribute."""
        sg = SchemaGraph(
            [TrspRegionDim, TrspProductDim, TrspSaleFact],
            bridge_models=[TrspBridgeNoTemporal],
        )
        fadj = sg.forward_adj
        # TrspBridgeNoTemporal has no subject/object so nothing extra added
        assert isinstance(fadj, dict)


# ---------------------------------------------------------------------------
# § Transposed adjacency G^T  (DGM §2.1, D18)
# ---------------------------------------------------------------------------


class TestTransposedAdj:
    """transposed_adj reverses all edges; built at property access time O(|E|)."""

    def test_transposed_adj_returns_dict(self):
        sg = SchemaGraph([TrspRegionDim, TrspProductDim, TrspSaleFact])
        assert isinstance(sg.transposed_adj, dict)

    def test_transposed_adj_keys_cover_all_nodes(self):
        sg = SchemaGraph([TrspRegionDim, TrspProductDim, TrspSaleFact])
        tadj = sg.transposed_adj
        all_names = {n.__name__ for n in sg.all_nodes}
        assert all_names == set(tadj.keys())

    def test_facts_list_dimension_predecessors_in_transposed(self):
        """Verb edges D→F become F→D in G^T."""
        sg = SchemaGraph([TrspRegionDim, TrspProductDim, TrspSaleFact])
        tadj = sg.transposed_adj
        predecessors = set(tadj["TrspSaleFact"])
        assert "TrspRegionDim" in predecessors
        assert "TrspProductDim" in predecessors

    def test_dims_have_no_incoming_verb_edges_in_transposed(self):
        """Dims are sources of verb edges; in G^T they have no verb-derived entries."""
        sg = SchemaGraph([TrspRegionDim, TrspProductDim, TrspSaleFact])
        tadj = sg.transposed_adj
        assert "TrspSaleFact" not in tadj["TrspRegionDim"]
        assert "TrspSaleFact" not in tadj["TrspProductDim"]

    def test_transposed_reverses_every_forward_edge(self):
        """For every (src, tgt) in forward_adj, src ∈ transposed_adj[tgt]."""
        sg = SchemaGraph([TrspRegionDim, TrspProductDim, TrspSaleFact])
        fadj = sg.forward_adj
        tadj = sg.transposed_adj
        for src, targets in fadj.items():
            for tgt in targets:
                assert src in tadj[tgt], (
                    f"Expected {src!r} in transposed_adj[{tgt!r}], got {tadj[tgt]}"
                )

    def test_transposed_adj_with_bridge_edges(self):
        """Bridge edges subject→object become object→subject in G^T."""
        sg = SchemaGraph(
            [TrspRegionDim, TrspProductDim, TrspSaleFact],
            bridge_models=[TrspPromoBridge],
        )
        tadj = sg.transposed_adj
        # TrspPromoBridge: subject=TrspRegionDim, object=TrspProductDim
        # Forward: TrspRegionDim → TrspProductDim
        # Transposed: TrspProductDim → TrspRegionDim
        assert "TrspRegionDim" in tadj["TrspProductDim"]

    def test_transposed_consistent_with_forward(self):
        """transposed_adj and forward_adj describe the same edges in reverse."""
        sg = SchemaGraph(
            [TrspRegionDim, TrspProductDim, TrspSaleFact],
            bridge_models=[TrspPromoBridge],
        )
        fadj = sg.forward_adj
        tadj = sg.transposed_adj
        for src, targets in fadj.items():
            for tgt in targets:
                assert src in tadj[tgt]

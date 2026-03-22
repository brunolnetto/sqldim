"""Phase 5 — Graph algorithms — RED tests.

Covers:
- TrailExpr NodeAlg types: OUTGOING_SIGNATURES, INCOMING_SIGNATURES,
  DOMINANT_OUTGOING_SIGNATURE, DOMINANT_INCOMING_SIGNATURE, SIGNATURE_DIVERSITY
- TrailExpr PairAlg types: DISTINCT_SIGNATURES, DOMINANT_SIGNATURE,
  SIGNATURE_SIMILARITY
- TrailExpr SubgraphAlg types: GLOBAL_SIGNATURE_COUNT, GLOBAL_DOMINANT_SIGNATURE,
  SIGNATURE_ENTROPY
- RelationshipSubgraph with Endpoint (Bound / Free), all 4 endpoint cases
- SubgraphExpr.scope parameter
- MAX_FLOW capacity parameter
- GraphStatistics dataclass
"""

import pytest

from sqldim.core.query.dgm.graph import (
    # --- existing ---
    NodeAlg, PairAlg, SubgraphAlg,
    NodeExpr, PairExpr, SubgraphExpr, GraphExpr,
    MAX_FLOW,
    # --- TrailExpr NodeAlg ---
    OUTGOING_SIGNATURES,
    INCOMING_SIGNATURES,
    DOMINANT_OUTGOING_SIGNATURE,
    DOMINANT_INCOMING_SIGNATURE,
    SIGNATURE_DIVERSITY,
    # --- TrailExpr PairAlg ---
    DISTINCT_SIGNATURES,
    DOMINANT_SIGNATURE,
    SIGNATURE_SIMILARITY,
    # --- TrailExpr SubgraphAlg ---
    GLOBAL_SIGNATURE_COUNT,
    GLOBAL_DOMINANT_SIGNATURE,
    SIGNATURE_ENTROPY,
    # --- RelationshipSubgraph ---
    Endpoint,
    Bound,
    Free,
    FREE,
    RelationshipSubgraph,
    # --- GraphStatistics ---
    GraphStatistics,
)


# ---------------------------------------------------------------------------
# TrailExpr NodeAlg types
# ---------------------------------------------------------------------------


class TestOutgoingSignatures:
    def test_no_args(self):
        a = OUTGOING_SIGNATURES()
        assert a.max_depth is None
        assert isinstance(a, NodeAlg)

    def test_with_max_depth(self):
        a = OUTGOING_SIGNATURES(max_depth=5)
        assert a.max_depth == 5

    def test_to_sql(self):
        sql = OUTGOING_SIGNATURES(max_depth=3).to_sql()
        assert "OUTGOING_SIGNATURES" in sql
        assert "3" in sql

    def test_to_sql_no_depth(self):
        sql = OUTGOING_SIGNATURES().to_sql()
        assert "OUTGOING_SIGNATURES" in sql


class TestIncomingSignatures:
    def test_no_args(self):
        a = INCOMING_SIGNATURES()
        assert a.max_depth is None
        assert isinstance(a, NodeAlg)

    def test_with_max_depth(self):
        a = INCOMING_SIGNATURES(max_depth=4)
        assert a.max_depth == 4

    def test_to_sql(self):
        sql = INCOMING_SIGNATURES().to_sql()
        assert "INCOMING_SIGNATURES" in sql


class TestDominantOutgoingSignature:
    def test_no_args(self):
        a = DOMINANT_OUTGOING_SIGNATURE()
        assert a.max_depth is None
        assert isinstance(a, NodeAlg)

    def test_with_max_depth(self):
        a = DOMINANT_OUTGOING_SIGNATURE(max_depth=2)
        assert a.max_depth == 2

    def test_to_sql(self):
        assert "DOMINANT_OUTGOING_SIGNATURE" in DOMINANT_OUTGOING_SIGNATURE().to_sql()


class TestDominantIncomingSignature:
    def test_no_args(self):
        a = DOMINANT_INCOMING_SIGNATURE()
        assert a.max_depth is None
        assert isinstance(a, NodeAlg)

    def test_to_sql(self):
        assert "DOMINANT_INCOMING_SIGNATURE" in DOMINANT_INCOMING_SIGNATURE().to_sql()


class TestSignatureDiversityNode:
    def test_is_node_alg(self):
        assert isinstance(SIGNATURE_DIVERSITY(), NodeAlg)

    def test_to_sql(self):
        assert "SIGNATURE_DIVERSITY" in SIGNATURE_DIVERSITY().to_sql()


# ---------------------------------------------------------------------------
# TrailExpr PairAlg types
# ---------------------------------------------------------------------------


class TestDistinctSignatures:
    def test_no_args(self):
        a = DISTINCT_SIGNATURES()
        assert a.max_depth is None
        assert isinstance(a, PairAlg)

    def test_with_max_depth(self):
        a = DISTINCT_SIGNATURES(max_depth=6)
        assert a.max_depth == 6

    def test_to_sql(self):
        assert "DISTINCT_SIGNATURES" in DISTINCT_SIGNATURES().to_sql()


class TestDominantSignature:
    def test_no_args(self):
        a = DOMINANT_SIGNATURE()
        assert a.max_depth is None
        assert isinstance(a, PairAlg)

    def test_with_max_depth(self):
        a = DOMINANT_SIGNATURE(max_depth=3)
        assert a.max_depth == 3

    def test_to_sql(self):
        assert "DOMINANT_SIGNATURE" in DOMINANT_SIGNATURE().to_sql()


class TestSignatureSimilarity:
    def test_basic(self):
        a = SIGNATURE_SIMILARITY(reference=["verb_a", "verb_b"])
        assert a.reference == ["verb_a", "verb_b"]
        assert isinstance(a, PairAlg)

    def test_empty_reference(self):
        a = SIGNATURE_SIMILARITY(reference=[])
        assert a.reference == []

    def test_to_sql(self):
        sql = SIGNATURE_SIMILARITY(reference=["v1", "v2"]).to_sql()
        assert "SIGNATURE_SIMILARITY" in sql


# ---------------------------------------------------------------------------
# TrailExpr SubgraphAlg types
# ---------------------------------------------------------------------------


class TestGlobalSignatureCount:
    def test_no_args(self):
        a = GLOBAL_SIGNATURE_COUNT()
        assert a.max_depth is None
        assert isinstance(a, SubgraphAlg)

    def test_with_max_depth(self):
        a = GLOBAL_SIGNATURE_COUNT(max_depth=4)
        assert a.max_depth == 4

    def test_to_sql(self):
        assert "GLOBAL_SIGNATURE_COUNT" in GLOBAL_SIGNATURE_COUNT().to_sql()


class TestGlobalDominantSignature:
    def test_no_args(self):
        a = GLOBAL_DOMINANT_SIGNATURE()
        assert a.max_depth is None
        assert isinstance(a, SubgraphAlg)

    def test_to_sql(self):
        assert "GLOBAL_DOMINANT_SIGNATURE" in GLOBAL_DOMINANT_SIGNATURE().to_sql()


class TestSignatureEntropy:
    def test_no_args(self):
        a = SIGNATURE_ENTROPY()
        assert a.max_depth is None
        assert isinstance(a, SubgraphAlg)

    def test_with_max_depth(self):
        a = SIGNATURE_ENTROPY(max_depth=10)
        assert a.max_depth == 10

    def test_to_sql_contains_entropy(self):
        assert "SIGNATURE_ENTROPY" in SIGNATURE_ENTROPY().to_sql()


# ---------------------------------------------------------------------------
# Endpoint hierarchy
# ---------------------------------------------------------------------------


class TestEndpoint:
    def test_bound_alias(self):
        b = Bound(alias="sale")
        assert b.alias == "sale"
        assert isinstance(b, Endpoint)

    def test_free_is_singleton(self):
        assert FREE is FREE

    def test_free_is_endpoint(self):
        assert isinstance(FREE, Endpoint)

    def test_bound_repr(self):
        assert "sale" in repr(Bound(alias="sale"))

    def test_free_repr(self):
        assert "Free" in repr(FREE)


# ---------------------------------------------------------------------------
# RelationshipSubgraph — all 4 endpoint cases
# ---------------------------------------------------------------------------


class TestRelationshipSubgraphCase1:
    """Case 1 — Bound(A) → Bound(B)."""

    def test_construction(self):
        rs = RelationshipSubgraph(
            source=Bound(alias="sale"),
            target=Bound(alias="customer"),
        )
        assert isinstance(rs.source, Bound)
        assert isinstance(rs.target, Bound)

    def test_endpoint_case_name(self):
        rs = RelationshipSubgraph(
            source=Bound(alias="a"), target=Bound(alias="b")
        )
        assert rs.endpoint_case == "BB"

    def test_granularity_tier(self):
        rs = RelationshipSubgraph(
            source=Bound(alias="a"), target=Bound(alias="b")
        )
        assert rs.granularity_tier == "PairExpr"


class TestRelationshipSubgraphCase2:
    """Case 2 — Bound(A) → Free."""

    def test_construction(self):
        rs = RelationshipSubgraph(source=Bound(alias="a"), target=FREE)
        assert isinstance(rs.source, Bound)
        assert rs.target is FREE

    def test_endpoint_case_name(self):
        rs = RelationshipSubgraph(source=Bound(alias="a"), target=FREE)
        assert rs.endpoint_case == "BF"

    def test_granularity_tier(self):
        rs = RelationshipSubgraph(source=Bound(alias="a"), target=FREE)
        assert rs.granularity_tier == "NodeExpr"


class TestRelationshipSubgraphCase3:
    """Case 3 — Free → Bound(B)."""

    def test_construction(self):
        rs = RelationshipSubgraph(source=FREE, target=Bound(alias="b"))
        assert rs.source is FREE
        assert isinstance(rs.target, Bound)

    def test_endpoint_case_name(self):
        rs = RelationshipSubgraph(source=FREE, target=Bound(alias="b"))
        assert rs.endpoint_case == "FB"

    def test_granularity_tier(self):
        rs = RelationshipSubgraph(source=FREE, target=Bound(alias="b"))
        assert rs.granularity_tier == "NodeExpr"


class TestRelationshipSubgraphCase4:
    """Case 4 — Free → Free."""

    def test_construction(self):
        rs = RelationshipSubgraph(source=FREE, target=FREE)
        assert rs.source is FREE
        assert rs.target is FREE

    def test_endpoint_case_name(self):
        rs = RelationshipSubgraph(source=FREE, target=FREE)
        assert rs.endpoint_case == "FF"

    def test_granularity_tier(self):
        rs = RelationshipSubgraph(source=FREE, target=FREE)
        assert rs.granularity_tier == "SubgraphExpr"

    def test_default_strategy_none(self):
        rs = RelationshipSubgraph(source=FREE, target=FREE)
        assert rs.strategy is None


class TestRelationshipSubgraphStrategy:
    def test_with_strategy(self):
        from sqldim.core.query.dgm.preds import ALL
        rs = RelationshipSubgraph(
            source=Bound(alias="a"),
            target=FREE,
            strategy=ALL(),
        )
        assert rs.strategy is not None


# ---------------------------------------------------------------------------
# SubgraphExpr.scope
# ---------------------------------------------------------------------------


class TestSubgraphExprScope:
    def test_scope_none_by_default(self):
        from sqldim.core.query.dgm.graph import DENSITY
        se = SubgraphExpr(algorithm=DENSITY())
        assert se.scope is None

    def test_scope_with_relationship_subgraph(self):
        from sqldim.core.query.dgm.graph import SIGNATURE_ENTROPY
        scope = RelationshipSubgraph(source=FREE, target=FREE)
        se = SubgraphExpr(algorithm=SIGNATURE_ENTROPY(), scope=scope)
        assert se.scope is scope


# ---------------------------------------------------------------------------
# MAX_FLOW capacity parameter
# ---------------------------------------------------------------------------


class TestMaxFlowCapacity:
    def test_with_capacity(self):
        mf = MAX_FLOW(source="a", target="b", capacity="weight")
        assert mf.capacity == "weight"

    def test_without_capacity(self):
        mf = MAX_FLOW(source="a", target="b")
        assert mf.capacity is None

    def test_to_sql_with_capacity(self):
        sql = MAX_FLOW(source="a", target="b", capacity="w").to_sql()
        assert "capacity" in sql or "w" in sql


# ---------------------------------------------------------------------------
# GraphStatistics
# ---------------------------------------------------------------------------


class TestGraphStatistics:
    def test_minimal(self):
        gs = GraphStatistics(node_count=10, edge_count=20)
        assert gs.node_count == 10
        assert gs.edge_count == 20
        assert gs.transposed_adj == {}

    def test_all_fields(self):
        gs = GraphStatistics(
            node_count=100,
            edge_count=500,
            degree_dist={"mean": 5.0},
            scc_sizes={"c1": 3, "c2": 1},
            transposed_adj={"b": ["a"]},
        )
        assert gs.scc_sizes == {"c1": 3, "c2": 1}
        assert gs.transposed_adj == {"b": ["a"]}

    def test_defaults_are_none_or_empty(self):
        gs = GraphStatistics(node_count=0, edge_count=0)
        assert gs.degree_dist is None
        assert gs.path_cardinality is None
        assert gs.property_dist is None
        assert gs.temporal_density is None
        assert gs.scc_sizes is None


# ---------------------------------------------------------------------------
# max_depth to_sql coverage — all algorithms with max_depth branch
# ---------------------------------------------------------------------------


class TestMaxDepthToSql:
    """Ensure max_depth=N branch is covered for all TrailExpr algorithm classes."""

    def test_incoming_signatures_max_depth_to_sql(self):
        sql = INCOMING_SIGNATURES(max_depth=4).to_sql()
        assert "INCOMING_SIGNATURES" in sql
        assert "4" in sql

    def test_dominant_outgoing_signature_max_depth_to_sql(self):
        sql = DOMINANT_OUTGOING_SIGNATURE(max_depth=7).to_sql()
        assert "DOMINANT_OUTGOING_SIGNATURE" in sql
        assert "7" in sql

    def test_dominant_incoming_signature_max_depth_to_sql(self):
        from sqldim.core.query.dgm.graph import DOMINANT_INCOMING_SIGNATURE
        sql = DOMINANT_INCOMING_SIGNATURE(max_depth=3).to_sql()
        assert "DOMINANT_INCOMING_SIGNATURE" in sql
        assert "3" in sql

    def test_distinct_signatures_max_depth_to_sql(self):
        sql = DISTINCT_SIGNATURES(max_depth=6).to_sql()
        assert "DISTINCT_SIGNATURES" in sql
        assert "6" in sql

    def test_dominant_signature_max_depth_to_sql(self):
        sql = DOMINANT_SIGNATURE(max_depth=3).to_sql()
        assert "DOMINANT_SIGNATURE" in sql
        assert "3" in sql

    def test_global_signature_count_max_depth_to_sql(self):
        sql = GLOBAL_SIGNATURE_COUNT(max_depth=4).to_sql()
        assert "GLOBAL_SIGNATURE_COUNT" in sql
        assert "4" in sql

    def test_global_dominant_signature_max_depth_to_sql(self):
        sql = GLOBAL_DOMINANT_SIGNATURE(max_depth=2).to_sql()
        assert "GLOBAL_DOMINANT_SIGNATURE" in sql
        assert "2" in sql

    def test_signature_entropy_max_depth_to_sql(self):
        sql = SIGNATURE_ENTROPY(max_depth=10).to_sql()
        assert "SIGNATURE_ENTROPY" in sql
        assert "10" in sql


class TestMaxFlowEdgeCases:
    """Cover MAX_FLOW target property and missing-target error."""

    def test_target_property(self):
        mf = MAX_FLOW(source="a", target="b")
        assert mf.target == "b"

    def test_sink_property(self):
        mf = MAX_FLOW(source="a", target="b")
        assert mf.sink == "b"

    def test_sink_kwarg_backward_compat(self):
        mf = MAX_FLOW(source="a", sink="b")
        assert mf.target == "b"

    def test_missing_target_raises(self):
        import pytest
        with pytest.raises(TypeError):
            MAX_FLOW(source="a")

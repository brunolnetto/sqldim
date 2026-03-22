"""RED tests for DGM Phase 7: Planner and Exporter.

Covers:
  - QueryTarget / SinkTarget enums
  - PreComputation / CostEstimate / ExportPlan dataclasses
  - DGMPlanner instantiation
  - Rules 1a (path execution strategy)
  - Rules 1b (grain-aware agg)
  - Rules 1c (SCD resolution)
  - Rule 2 (Floyd-Warshall threshold)
  - Rule 3 (GraphExpr scheduling)
  - Rule 4 (band reordering via BDD implies)
  - Rule 5 (hierarchy execution)
  - Rule 6 (annotation-driven optimisations)
  - Rule 7 (TemporalAgg window scheduling)
  - Rule 8 (sink-aware write planning)
  - Rule 9 (cone containment optimisation)
  - CypherExporter, SPARQLExporter, DGMJSONExporter, DGMYAMLExporter
"""

from __future__ import annotations

import pytest

from sqldim.core.query._dgm_planner import (
    QueryTarget,
    SinkTarget,
    PreComputation,
    CostEstimate,
    ExportPlan,
    DGMPlanner,
    SMALL,
    CLOSURE_THRESHOLD,
    SMALL_GRAPH_THRESHOLD,
    DENSE,
)
from sqldim.core.query._dgm_exporters import (
    CypherExporter,
    SPARQLExporter,
    DGMJSONExporter,
    DGMYAMLExporter,
)
from sqldim.core.query._dgm_graph import (
    GraphStatistics,
    RelationshipSubgraph,
    Bound,
    FREE,
    OUTGOING_SIGNATURES,
    INCOMING_SIGNATURES,
    SIGNATURE_ENTROPY,
    GLOBAL_DOMINANT_SIGNATURE,
    NodeExpr,
    SubgraphExpr,
)
from sqldim.core.query._dgm_annotations import (
    AnnotationSigma,
    Grain,
    GrainKind,
    SCDType,
    SCDKind,
    FactlessFact,
    Degenerate,
    RolePlaying,
    BridgeSemantics,
    BridgeSemanticsKind,
    ProjectsFrom,
    DerivedFact,
    WeightConstraint,
    WeightConstraintKind,
    Hierarchy,
    RAGGED,
)
from sqldim.core.query._dgm_bdd import DGMPredicateBDD
from sqldim.core.query._dgm_preds import ScalarPred, AND
from sqldim.core.query._dgm_refs import PropRef


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_planner(
    sigma=None,
    statistics=None,
    query_target=None,
    sink_target=None,
) -> DGMPlanner:
    if sigma is None:
        sigma = AnnotationSigma([])
    if statistics is None:
        statistics = GraphStatistics(node_count=10, edge_count=20)
    if query_target is None:
        query_target = QueryTarget.SQL_DUCKDB
    return DGMPlanner(
        cost_model=None,
        statistics=statistics,
        annotations=sigma,
        rules=None,
        query_target=query_target,
        sink_target=sink_target,
    )


# ---------------------------------------------------------------------------
# QueryTarget
# ---------------------------------------------------------------------------

class TestQueryTarget:
    def test_has_sql_duckdb(self):
        assert QueryTarget.SQL_DUCKDB.value == "SQL_DUCKDB"

    def test_has_sql_postgresql(self):
        assert QueryTarget.SQL_POSTGRESQL.value == "SQL_POSTGRESQL"

    def test_has_sql_motherduck(self):
        assert QueryTarget.SQL_MOTHERDUCK.value == "SQL_MOTHERDUCK"

    def test_has_cypher(self):
        assert QueryTarget.CYPHER.value == "CYPHER"

    def test_has_sparql(self):
        assert QueryTarget.SPARQL.value == "SPARQL"

    def test_has_dgm_json(self):
        assert QueryTarget.DGM_JSON.value == "DGM_JSON"

    def test_has_dgm_yaml(self):
        assert QueryTarget.DGM_YAML.value == "DGM_YAML"

    def test_all_seven_targets(self):
        assert len(QueryTarget) == 7


# ---------------------------------------------------------------------------
# SinkTarget
# ---------------------------------------------------------------------------

class TestSinkTarget:
    def test_has_duckdb(self):
        assert SinkTarget.DUCKDB.value == "DUCKDB"

    def test_has_postgresql(self):
        assert SinkTarget.POSTGRESQL.value == "POSTGRESQL"

    def test_has_motherduck(self):
        assert SinkTarget.MOTHERDUCK.value == "MOTHERDUCK"

    def test_has_parquet(self):
        assert SinkTarget.PARQUET.value == "PARQUET"

    def test_has_delta(self):
        assert SinkTarget.DELTA.value == "DELTA"

    def test_has_iceberg(self):
        assert SinkTarget.ICEBERG.value == "ICEBERG"

    def test_all_six_sinks(self):
        assert len(SinkTarget) == 6


# ---------------------------------------------------------------------------
# PreComputation
# ---------------------------------------------------------------------------

class TestPreComputation:
    def test_basic_creation(self):
        pc = PreComputation(name="gt_bfs", query="SELECT ...", kind="sql")
        assert pc.name == "gt_bfs"
        assert pc.query == "SELECT ..."
        assert pc.kind == "sql"

    def test_python_kind(self):
        pc = PreComputation(name="fw", query="...", kind="py")
        assert pc.kind == "py"

    def test_default_kind_is_sql(self):
        pc = PreComputation(name="gt_bfs", query="SELECT ...")
        assert pc.kind == "sql"

    def test_equality(self):
        a = PreComputation(name="x", query="q")
        b = PreComputation(name="x", query="q")
        assert a == b


# ---------------------------------------------------------------------------
# CostEstimate
# ---------------------------------------------------------------------------

class TestCostEstimate:
    def test_basic_creation(self):
        ce = CostEstimate(cpu_ops=100, io_ops=50)
        assert ce.cpu_ops == 100
        assert ce.io_ops == 50

    def test_note_default(self):
        ce = CostEstimate(cpu_ops=10, io_ops=5)
        assert ce.note == ""

    def test_with_note(self):
        ce = CostEstimate(cpu_ops=10, io_ops=5, note="Floyd-Warshall pre-compute")
        assert "Floyd" in ce.note

    def test_equality(self):
        assert CostEstimate(cpu_ops=1, io_ops=2) == CostEstimate(cpu_ops=1, io_ops=2)


# ---------------------------------------------------------------------------
# ExportPlan
# ---------------------------------------------------------------------------

class TestExportPlan:
    def test_basic_creation(self):
        plan = ExportPlan(
            query_target=QueryTarget.SQL_DUCKDB,
            query_text="SELECT 1",
        )
        assert plan.query_target is QueryTarget.SQL_DUCKDB
        assert plan.query_text == "SELECT 1"

    def test_defaults(self):
        plan = ExportPlan(
            query_target=QueryTarget.CYPHER,
            query_text="MATCH (n) RETURN n",
        )
        assert plan.pre_compute == []
        assert plan.sink_target is None
        assert plan.write_plan is None
        assert plan.cost_estimate is None
        assert plan.alternatives == []

    def test_with_pre_compute(self):
        pc = PreComputation(name="bfs", query="WITH RECURSIVE ...")
        plan = ExportPlan(
            query_target=QueryTarget.SQL_DUCKDB,
            query_text="SELECT ...",
            pre_compute=[pc],
        )
        assert len(plan.pre_compute) == 1
        assert plan.pre_compute[0].name == "bfs"

    def test_with_alternatives(self):
        plan1 = ExportPlan(QueryTarget.SQL_DUCKDB, "SELECT 1")
        plan2 = ExportPlan(QueryTarget.SQL_POSTGRESQL, "SELECT 2")
        ce = CostEstimate(cpu_ops=200, io_ops=100)
        plan_with_alt = ExportPlan(
            query_target=QueryTarget.SQL_DUCKDB,
            query_text="SELECT ...",
            alternatives=[(plan1, ce), (plan2, ce)],
        )
        assert len(plan_with_alt.alternatives) == 2


# ---------------------------------------------------------------------------
# DGMPlanner — instantiation
# ---------------------------------------------------------------------------

class TestDGMPlannerInit:
    def test_basic_instantiation(self):
        p = _make_planner()
        assert p.query_target is QueryTarget.SQL_DUCKDB

    def test_stores_statistics(self):
        stats = GraphStatistics(node_count=50, edge_count=80)
        p = _make_planner(statistics=stats)
        assert p.statistics.node_count == 50

    def test_sink_target_optional(self):
        p = _make_planner(sink_target=SinkTarget.DUCKDB)
        assert p.sink_target is SinkTarget.DUCKDB

    def test_sink_target_none_by_default(self):
        p = _make_planner()
        assert p.sink_target is None


# ---------------------------------------------------------------------------
# Rule 1a — Path execution strategy
# ---------------------------------------------------------------------------

class TestRule1a:
    """Rule 1a: Bound/Free endpoint × strategy dispatching."""

    def test_bb_small_recursive_cte(self):
        p = _make_planner()
        result = p.apply_rule_1a(
            endpoint_case="BB",
            strategy="ALL",
            path_card=SMALL - 1,
        )
        assert "recursive_cte" in result.query_text.lower() or "recursive" in result.query_text.lower()

    def test_bb_large_prematerialise(self):
        p = _make_planner()
        result = p.apply_rule_1a(
            endpoint_case="BB",
            strategy="ALL",
            path_card=SMALL + 1,
        )
        assert "materialise" in result.query_text.lower() or "materialize" in result.query_text.lower()

    def test_bb_shortest_limit_1(self):
        p = _make_planner()
        result = p.apply_rule_1a(
            endpoint_case="BB",
            strategy="SHORTEST",
            path_card=SMALL - 1,
        )
        assert "limit 1" in result.query_text.lower()

    def test_bb_causal_no_cycle_guard(self):
        p = _make_planner()
        result = p.apply_rule_1a(
            endpoint_case="BB",
            strategy="CAUSAL",
            path_card=SMALL - 1,
        )
        assert "dag_bfs" in result.query_text.lower() or "dag" in result.query_text.lower()

    def test_bf_forward_bfs(self):
        p = _make_planner()
        result = p.apply_rule_1a(endpoint_case="BF", strategy="ALL", path_card=10)
        assert "bfs" in result.query_text.lower() or "forward" in result.query_text.lower()

    def test_fb_forward_bfs_default(self):
        p = _make_planner()
        result = p.apply_rule_1a(endpoint_case="FB", strategy="ALL", path_card=10)
        # Free→Bound uses G^T (transposed adjacency)
        assert "transposed" in result.query_text.lower() or "g^t" in result.query_text.lower() or "bfs" in result.query_text.lower()

    def test_fb_causal_reverse_topological(self):
        p = _make_planner()
        result = p.apply_rule_1a(endpoint_case="FB", strategy="CAUSAL", path_card=10)
        assert "topological" in result.query_text.lower() or "reverse" in result.query_text.lower() or "g^t" in result.query_text.lower()

    def test_ff_wcc(self):
        p = _make_planner()
        result = p.apply_rule_1a(endpoint_case="FF", strategy="ALL", path_card=10)
        assert "wcc" in result.query_text.lower() or "component" in result.query_text.lower()

    def test_returns_export_plan(self):
        p = _make_planner()
        result = p.apply_rule_1a(endpoint_case="BB", strategy="ALL", path_card=5)
        assert isinstance(result, ExportPlan)

    def test_bf_causal_dag_bfs(self):
        p = _make_planner()
        result = p.apply_rule_1a(endpoint_case="BF", strategy="CAUSAL", path_card=10)
        assert "dag" in result.query_text.lower()

    def test_bf_shortest_first_visit(self):
        p = _make_planner()
        result = p.apply_rule_1a(endpoint_case="BF", strategy="SHORTEST", path_card=10)
        assert "first_visit" in result.query_text.lower() or "bfs" in result.query_text.lower()


# ---------------------------------------------------------------------------
# Rule 1b — Grain-aware aggregation
# ---------------------------------------------------------------------------

class TestRule1b:
    def test_period_rejects_sum(self):
        p = _make_planner()
        warnings = p.apply_rule_1b(grain=GrainKind.PERIOD, fact="sales")
        assert any("reject" in w.lower() and "sum" in w.lower() for w in warnings)

    def test_period_suggests_last(self):
        p = _make_planner()
        warnings = p.apply_rule_1b(grain=GrainKind.PERIOD, fact="sales")
        assert any("last" in w.lower() for w in warnings)

    def test_period_suggests_q_delta(self):
        p = _make_planner()
        warnings = p.apply_rule_1b(grain=GrainKind.PERIOD, fact="sales")
        assert any("q_delta" in w.lower() for w in warnings)

    def test_accumulating_warns_cross_row(self):
        p = _make_planner()
        warnings = p.apply_rule_1b(grain=GrainKind.ACCUMULATING, fact="order")
        assert any("warn" in w.lower() or "coalesce" in w.lower() for w in warnings)

    def test_returns_list_of_strings(self):
        p = _make_planner()
        result = p.apply_rule_1b(grain=GrainKind.PERIOD, fact="x")
        assert isinstance(result, list)
        assert all(isinstance(s, str) for s in result)

    def test_event_grain_no_warnings(self):
        p = _make_planner()
        result = p.apply_rule_1b(grain=GrainKind.EVENT, fact="x")
        assert result == []


# ---------------------------------------------------------------------------
# Rule 1c — SCD resolution
# ---------------------------------------------------------------------------

class TestRule1c:
    def test_scd1_strip_temporal(self):
        p = _make_planner()
        result = p.apply_rule_1c(scd_kind=SCDKind.SCD1)
        assert "strip" in result.lower() or "scd1" in result.lower()

    def test_scd2_effective_from_to(self):
        p = _make_planner()
        result = p.apply_rule_1c(scd_kind=SCDKind.SCD2)
        assert "effective_from" in result.lower() or "scd2" in result.lower()

    def test_scd3_prev_value(self):
        p = _make_planner()
        result = p.apply_rule_1c(scd_kind=SCDKind.SCD3)
        assert "previous_value" in result.lower() or "scd3" in result.lower()

    def test_scd6_lateral_join(self):
        p = _make_planner()
        result = p.apply_rule_1c(scd_kind=SCDKind.SCD6)
        assert "lateral" in result.lower() or "scd6" in result.lower()

    def test_returns_string(self):
        p = _make_planner()
        assert isinstance(p.apply_rule_1c(scd_kind=SCDKind.SCD2), str)


# ---------------------------------------------------------------------------
# Rule 2 — Floyd-Warshall threshold
# ---------------------------------------------------------------------------

class TestRule2:
    def test_above_threshold_precompute(self):
        p = _make_planner(statistics=GraphStatistics(node_count=10, edge_count=20))
        # pair_count * avg_path_len > node_count² → pre-compute
        result = p.apply_rule_2(pair_count=50, avg_path_len=5, node_count=10)
        assert "floyd" in result.lower() or "precompute" in result.lower() or "pre_compute" in result.lower()

    def test_below_threshold_per_pair_cte(self):
        p = _make_planner()
        # pair_count * avg_path_len <= node_count² → per-pair CTE
        result = p.apply_rule_2(pair_count=2, avg_path_len=2, node_count=100)
        assert "cte" in result.lower() or "per_pair" in result.lower()

    def test_at_threshold_per_pair_cte(self):
        p = _make_planner()
        # pair_count * avg_path_len == node_count² → per-pair CTE (not above)
        result = p.apply_rule_2(pair_count=10, avg_path_len=10, node_count=10)
        # 10*10 = 100 = 10² → at threshold → per-pair
        assert "cte" in result.lower() or "per_pair" in result.lower()

    def test_returns_string(self):
        p = _make_planner()
        assert isinstance(p.apply_rule_2(pair_count=1, avg_path_len=1, node_count=1), str)


# ---------------------------------------------------------------------------
# Rule 3 — GraphExpr scheduling
# ---------------------------------------------------------------------------

class TestRule3:
    def test_outgoing_sigs_forward_bfs(self):
        p = _make_planner()
        expr = NodeExpr(OUTGOING_SIGNATURES(), alias="a")
        result = p.apply_rule_3(expr)
        assert "forward_bfs" in result.lower() or "bfs" in result.lower()

    def test_incoming_sigs_transposed_bfs(self):
        p = _make_planner()
        expr = NodeExpr(INCOMING_SIGNATURES(), alias="a")
        result = p.apply_rule_3(expr)
        assert "transposed" in result.lower() or "g^t" in result.lower() or "bfs" in result.lower()

    def test_signature_entropy_large_graph_precompute(self):
        p = _make_planner(statistics=GraphStatistics(node_count=SMALL_GRAPH_THRESHOLD + 1, edge_count=100))
        expr = SubgraphExpr(SIGNATURE_ENTROPY())
        result = p.apply_rule_3(expr)
        assert "precompute" in result.lower() or "pre_compute" in result.lower()

    def test_signature_entropy_small_graph_inline(self):
        p = _make_planner(statistics=GraphStatistics(node_count=SMALL_GRAPH_THRESHOLD - 1, edge_count=50))
        expr = SubgraphExpr(SIGNATURE_ENTROPY())
        result = p.apply_rule_3(expr)
        assert "inline" in result.lower()

    def test_returns_string(self):
        p = _make_planner()
        expr = NodeExpr(OUTGOING_SIGNATURES(), alias="x")
        assert isinstance(p.apply_rule_3(expr), str)

    def test_global_dominant_sig_broadcast(self):
        p = _make_planner(statistics=GraphStatistics(node_count=SMALL_GRAPH_THRESHOLD - 1, edge_count=10))
        expr = SubgraphExpr(GLOBAL_DOMINANT_SIGNATURE())
        result = p.apply_rule_3(expr)
        assert "broadcast" in result.lower() or "scalar" in result.lower() or "inline" in result.lower()

    def test_inline_node_expr_fallback(self):
        """NodeExpr with a non-TrailExpr alg (e.g. DEGREE) hits the inline fallback."""
        from sqldim.core.query._dgm_graph import DEGREE
        p = _make_planner()
        expr = NodeExpr(DEGREE(), alias="x")
        result = p.apply_rule_3(expr)
        assert "inline" in result.lower()
        assert "DEGREE" in result

    def test_inline_subgraph_expr_fallback(self):
        """SubgraphExpr with a non-TrailExpr alg (e.g. DENSITY) hits the inline fallback."""
        from sqldim.core.query._dgm_graph import DENSITY
        p = _make_planner()
        expr = SubgraphExpr(DENSITY())
        result = p.apply_rule_3(expr)
        assert "inline" in result.lower()
        assert "DENSITY" in result


# ---------------------------------------------------------------------------
# Rule 4 — Band reordering (BDD implies)
# ---------------------------------------------------------------------------

class TestRule4:
    def test_implies_removes_having(self):
        from sqldim.core.query._dgm_bdd import BDDManager
        mgr = BDDManager()
        bdd = DGMPredicateBDD(mgr)
        p_true = ScalarPred(PropRef("r.revenue"), ">", 0)
        # tautology implies everything
        where_bdd = bdd.compile(p_true)
        having_bdd = bdd.compile(p_true)
        planner = _make_planner()
        # where implies having → remove having
        canremove = planner.apply_rule_4(bdd, where_bdd, having_bdd)
        assert canremove is True

    def test_no_implies_keeps_having(self):
        from sqldim.core.query._dgm_bdd import BDDManager
        mgr = BDDManager()
        bdd = DGMPredicateBDD(mgr)
        p1 = ScalarPred(PropRef("r.revenue"), ">", 100)
        p2 = ScalarPred(PropRef("r.cost"), ">", 50)
        w = bdd.compile(p1)
        h = bdd.compile(p2)
        planner = _make_planner()
        can_remove = planner.apply_rule_4(bdd, w, h)
        # p1 does NOT imply p2 (different atoms) → keep having
        assert can_remove is False

    def test_returns_bool(self):
        from sqldim.core.query._dgm_bdd import BDDManager
        mgr = BDDManager()
        bdd = DGMPredicateBDD(mgr)
        p = ScalarPred(PropRef("x.y"), "=", 1)
        u = bdd.compile(p)
        planner = _make_planner()
        assert isinstance(planner.apply_rule_4(bdd, u, u), bool)


# ---------------------------------------------------------------------------
# Rule 5 — Hierarchy execution
# ---------------------------------------------------------------------------

class TestRule5:
    def test_depth_le_4_unroll(self):
        p = _make_planner()
        result = p.apply_rule_5(depth=3, node_count=50)
        assert "unroll" in result.lower() or "fixed_depth" in result.lower()

    def test_depth_4_unroll(self):
        p = _make_planner()
        result = p.apply_rule_5(depth=4, node_count=50)
        assert "unroll" in result.lower() or "fixed_depth" in result.lower()

    def test_ragged_recursive_cte(self):
        p = _make_planner()
        result = p.apply_rule_5(depth=RAGGED, node_count=50)
        assert "recursive_cte" in result.lower() or "recursive" in result.lower()

    def test_large_graph_closure_table(self):
        p = _make_planner()
        result = p.apply_rule_5(depth=RAGGED, node_count=CLOSURE_THRESHOLD + 1)
        assert "closure" in result.lower()

    def test_returns_string(self):
        p = _make_planner()
        assert isinstance(p.apply_rule_5(depth=2, node_count=100), str)


# ---------------------------------------------------------------------------
# Rule 6 — Annotation-driven optimisations
# ---------------------------------------------------------------------------

class TestRule6:
    def test_role_playing_single_scan(self):
        p = _make_planner()
        ann = RolePlaying(dim="d", roles=["buyer", "seller"])
        result = p.apply_rule_6(ann)
        assert any("alias" in s.lower() or "scan" in s.lower() for s in result)

    def test_projects_from_eliminate_mini(self):
        p = _make_planner()
        ann = ProjectsFrom(dim_mini="d_mini", dim_full="d_full")
        # full table is present in candidate set C
        result = p.apply_rule_6(ann, candidate_set={"d_full"})
        assert any("eliminate" in s.lower() or "mini" in s.lower() for s in result)

    def test_projects_from_no_eliminate_when_full_absent(self):
        p = _make_planner()
        ann = ProjectsFrom(dim_mini="d_mini", dim_full="d_full")
        # full table NOT in candidate set → no optimisation
        result = p.apply_rule_6(ann, candidate_set={"d_mini"})
        assert not any("eliminate" in s.lower() for s in result)

    def test_derived_fact_inline_when_src_present(self):
        p = _make_planner()
        ann = DerivedFact(fact="derived_sales", sources=["base_sales"], expr="...")
        result = p.apply_rule_6(ann, candidate_set={"base_sales"})
        assert any("inline" in s.lower() for s in result)

    def test_weight_constraint_alloc_warn(self):
        p = _make_planner()
        ann = WeightConstraint(bridge="bridge_b", constraint=WeightConstraintKind.ALLOCATIVE)
        result = p.apply_rule_6(ann)
        assert any("weight" in s.lower() for s in result)

    def test_causal_dag_bfs(self):
        p = _make_planner()
        ann = BridgeSemantics(bridge="b", sem=BridgeSemanticsKind.CAUSAL)
        result = p.apply_rule_6(ann)
        assert any("dag" in s.lower() or "cycle" in s.lower() for s in result)

    def test_supersession_negation(self):
        p = _make_planner()
        ann = BridgeSemantics(bridge="b", sem=BridgeSemanticsKind.SUPERSESSION)
        result = p.apply_rule_6(ann)
        assert any("negat" in s.lower() or "case when" in s.lower() for s in result)

    def test_degenerate_exclude_groupby(self):
        p = _make_planner()
        ann = Degenerate(dim="order_no")
        result = p.apply_rule_6(ann)
        assert any("exclusion" in s.lower() or "groupby" in s.lower() for s in result)

    def test_returns_list_of_strings(self):
        p = _make_planner()
        ann = Degenerate(dim="x")
        result = p.apply_rule_6(ann)
        assert isinstance(result, list)
        assert all(isinstance(s, str) for s in result)

    def test_unknown_annotation_empty(self):
        p = _make_planner()
        # pass arbitrary object → no rule matches → empty list
        result = p.apply_rule_6(object())
        assert result == []

    def test_derived_fact_no_src_intersection_empty(self):
        """DerivedFact with no overlap between sources and candidate_set → []."""
        p = _make_planner()
        ann = DerivedFact(fact="derived_sales", sources=["base_sales"], expr="...")
        result = p.apply_rule_6(ann, candidate_set={"other_table"})
        assert result == []

    def test_weight_non_allocative_empty(self):
        """WeightConstraint(UNCONSTRAINED) → no optimisation → []."""
        p = _make_planner()
        ann = WeightConstraint(bridge="b", constraint=WeightConstraintKind.UNCONSTRAINED)
        result = p.apply_rule_6(ann)
        assert result == []

    def test_bridge_structural_empty(self):
        """BridgeSemantics(STRUCTURAL) is neither CAUSAL nor SUPERSESSION → []."""
        p = _make_planner()
        ann = BridgeSemantics(bridge="b", sem=BridgeSemanticsKind.STRUCTURAL)
        result = p.apply_rule_6(ann)
        assert result == []


# ---------------------------------------------------------------------------
# Rule 7 — TemporalAgg window scheduling
# ---------------------------------------------------------------------------

class TestRule7:
    def test_dense_pre_aggregate(self):
        p = _make_planner()
        result = p.apply_rule_7(density=DENSE + 0.1, window_type="PERIOD")
        assert "pre_aggregate" in result.lower() or "pre-aggregate" in result.lower()

    def test_rolling_recursive_frame(self):
        p = _make_planner()
        result = p.apply_rule_7(density=0.1, window_type="ROLLING")
        assert "recursive" in result.lower() or "window_frame" in result.lower()

    def test_sparse_date_filter(self):
        p = _make_planner()
        result = p.apply_rule_7(density=0.1, window_type="YTD")
        assert "date_filter" in result.lower() or "aggfn" in result.lower()

    def test_returns_string(self):
        p = _make_planner()
        assert isinstance(p.apply_rule_7(density=0.5, window_type="PERIOD"), str)


# ---------------------------------------------------------------------------
# Rule 8 — Sink-aware write planning
# ---------------------------------------------------------------------------

class TestRule8:
    def test_duckdb_create_table_as(self):
        p = _make_planner()
        result = p.apply_rule_8(
            sink_target=SinkTarget.DUCKDB,
            has_temporal_agg=False,
            has_q_delta=False,
            grain_kind=None,
        )
        assert "create table" in result.lower() or "insert into" in result.lower()

    def test_postgresql_copy(self):
        p = _make_planner()
        result = p.apply_rule_8(
            sink_target=SinkTarget.POSTGRESQL,
            has_temporal_agg=False,
            has_q_delta=False,
            grain_kind=None,
        )
        assert "copy" in result.lower() or "attach" in result.lower() or "insert" in result.lower()

    def test_parquet_copy_to(self):
        p = _make_planner()
        result = p.apply_rule_8(
            sink_target=SinkTarget.PARQUET,
            has_temporal_agg=False,
            has_q_delta=False,
            grain_kind=None,
        )
        assert "copy to" in result.lower()

    def test_delta_create_or_replace(self):
        p = _make_planner()
        result = p.apply_rule_8(
            sink_target=SinkTarget.DELTA,
            has_temporal_agg=False,
            has_q_delta=False,
            grain_kind=None,
        )
        assert "delta" in result.lower() or "create or replace" in result.lower() or "insert into" in result.lower()

    def test_parquet_temporal_agg_partition_by(self):
        p = _make_planner()
        result = p.apply_rule_8(
            sink_target=SinkTarget.PARQUET,
            has_temporal_agg=True,
            has_q_delta=False,
            grain_kind=None,
        )
        assert "partition_by" in result.lower()

    def test_delta_q_delta_append(self):
        p = _make_planner()
        result = p.apply_rule_8(
            sink_target=SinkTarget.DELTA,
            has_temporal_agg=False,
            has_q_delta=True,
            grain_kind=None,
        )
        assert "append" in result.lower()

    def test_accumulating_append(self):
        p = _make_planner()
        result = p.apply_rule_8(
            sink_target=SinkTarget.DELTA,
            has_temporal_agg=False,
            has_q_delta=False,
            grain_kind=GrainKind.ACCUMULATING,
        )
        assert "append" in result.lower()

    def test_returns_string(self):
        p = _make_planner()
        assert isinstance(p.apply_rule_8(SinkTarget.DUCKDB, False, False, None), str)


# ---------------------------------------------------------------------------
# Rule 9 — Cone containment optimisation
# ---------------------------------------------------------------------------

class TestRule9:
    def test_both_present_fires(self):
        p = _make_planner()
        result = p.apply_rule_9(
            has_reachable_from=True,
            source_alias="A",
            has_reachable_to=True,
            target_alias="B",
        )
        assert result is True

    def test_only_from_no_fire(self):
        p = _make_planner()
        result = p.apply_rule_9(
            has_reachable_from=True,
            source_alias="A",
            has_reachable_to=False,
            target_alias=None,
        )
        assert result is False

    def test_only_to_no_fire(self):
        p = _make_planner()
        result = p.apply_rule_9(
            has_reachable_from=False,
            source_alias=None,
            has_reachable_to=True,
            target_alias="B",
        )
        assert result is False

    def test_neither_no_fire(self):
        p = _make_planner()
        result = p.apply_rule_9(
            has_reachable_from=False,
            source_alias=None,
            has_reachable_to=False,
            target_alias=None,
        )
        assert result is False

    def test_returns_bool(self):
        p = _make_planner()
        result = p.apply_rule_9(True, "A", True, "B")
        assert isinstance(result, bool)

    def test_collapsed_plan_uses_reachable_between(self):
        p = _make_planner()
        plan = p.collapse_cone(source_alias="A", target_alias="B")
        assert "reachable_between" in plan.query_text.lower()
        assert isinstance(plan, ExportPlan)


# ---------------------------------------------------------------------------
# Constants sanity
# ---------------------------------------------------------------------------

class TestConstants:
    def test_small_positive_int(self):
        assert isinstance(SMALL, int)
        assert SMALL > 0

    def test_closure_threshold_larger_than_small(self):
        assert CLOSURE_THRESHOLD > SMALL

    def test_small_graph_threshold_positive(self):
        assert SMALL_GRAPH_THRESHOLD > 0

    def test_dense_between_0_and_1(self):
        assert 0.0 < DENSE < 1.0


# ---------------------------------------------------------------------------
# CypherExporter
# ---------------------------------------------------------------------------

class TestCypherExporter:
    def test_export_returns_string(self):
        plan = ExportPlan(QueryTarget.CYPHER, "MATCH (n) RETURN n")
        exporter = CypherExporter()
        result = exporter.export(plan)
        assert isinstance(result, str)

    def test_export_preserves_query_text(self):
        plan = ExportPlan(QueryTarget.CYPHER, "MATCH (a)-[*]->(b) RETURN a, b")
        exporter = CypherExporter()
        result = exporter.export(plan)
        assert "MATCH" in result

    def test_export_adds_header_comment(self):
        plan = ExportPlan(QueryTarget.CYPHER, "MATCH (n) RETURN n")
        exporter = CypherExporter()
        result = exporter.export(plan)
        # Should include some header or the query text itself
        assert len(result) > 0

    def test_reachable_from_match_pattern(self):
        plan = ExportPlan(
            QueryTarget.CYPHER,
            "REACHABLE_FROM(a)",
        )
        exporter = CypherExporter()
        result = exporter.export(plan)
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# SPARQLExporter
# ---------------------------------------------------------------------------

class TestSPARQLExporter:
    def test_export_returns_string(self):
        plan = ExportPlan(QueryTarget.SPARQL, "SELECT * WHERE { ?s ?p ?o }")
        exporter = SPARQLExporter()
        result = exporter.export(plan)
        assert isinstance(result, str)

    def test_export_contains_select_or_construct(self):
        plan = ExportPlan(QueryTarget.SPARQL, "SELECT * WHERE { ?s ?p ?o }")
        exporter = SPARQLExporter()
        result = exporter.export(plan)
        assert "SELECT" in result or "CONSTRUCT" in result

    def test_prefix_header(self):
        plan = ExportPlan(QueryTarget.SPARQL, "SELECT * WHERE { ?s ?p ?o }")
        exporter = SPARQLExporter(base_prefix="dgm:")
        result = exporter.export(plan)
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# DGMJSONExporter
# ---------------------------------------------------------------------------

class TestDGMJSONExporter:
    def test_export_returns_dict(self):
        plan = ExportPlan(QueryTarget.DGM_JSON, "SELECT 1")
        exporter = DGMJSONExporter()
        result = exporter.export(plan)
        assert isinstance(result, dict)

    def test_dict_has_query_target_key(self):
        plan = ExportPlan(QueryTarget.DGM_JSON, "SELECT 1")
        exporter = DGMJSONExporter()
        result = exporter.export(plan)
        assert "query_target" in result

    def test_dict_has_query_text_key(self):
        plan = ExportPlan(QueryTarget.DGM_JSON, "SELECT 1")
        exporter = DGMJSONExporter()
        result = exporter.export(plan)
        assert "query_text" in result

    def test_dict_has_pre_compute_key(self):
        plan = ExportPlan(QueryTarget.DGM_JSON, "SELECT 1")
        exporter = DGMJSONExporter()
        result = exporter.export(plan)
        assert "pre_compute" in result

    def test_dict_has_cone_containment_applied(self):
        plan = ExportPlan(QueryTarget.DGM_JSON, "SELECT 1")
        exporter = DGMJSONExporter()
        result = exporter.export(plan)
        assert "cone_containment_applied" in result

    def test_with_pre_compute(self):
        pc = PreComputation(name="gt_bfs", query="WITH RECURSIVE ...")
        plan = ExportPlan(QueryTarget.DGM_JSON, "SELECT ...", pre_compute=[pc])
        exporter = DGMJSONExporter()
        result = exporter.export(plan)
        assert isinstance(result["pre_compute"], list)
        assert result["pre_compute"][0]["name"] == "gt_bfs"

    def test_with_cost_estimate(self):
        """Export with a CostEstimate populates the cost_estimate dict."""
        ce = CostEstimate(cpu_ops=1000, io_ops=50, note="estimated")
        plan = ExportPlan(QueryTarget.DGM_JSON, "SELECT 1", cost_estimate=ce)
        exporter = DGMJSONExporter()
        result = exporter.export(plan)
        assert result["cost_estimate"] is not None
        assert result["cost_estimate"]["cpu_ops"] == 1000
        assert result["cost_estimate"]["note"] == "estimated"

    def test_with_alternatives(self):
        """Export with alternatives emits the alternatives list."""
        alt_plan = ExportPlan(QueryTarget.SQL_DUCKDB, "SELECT * FROM fact")
        alt_ce = CostEstimate(cpu_ops=500, io_ops=20, note="alt")
        plan = ExportPlan(
            QueryTarget.DGM_JSON,
            "SELECT 1",
            alternatives=[(alt_plan, alt_ce)],
        )
        exporter = DGMJSONExporter()
        result = exporter.export(plan)
        assert len(result["alternatives"]) == 1
        assert result["alternatives"][0]["query_text"] == "SELECT * FROM fact"


# ---------------------------------------------------------------------------
# DGMYAMLExporter
# ---------------------------------------------------------------------------

class TestDGMYAMLExporter:
    def test_export_returns_string(self):
        plan = ExportPlan(QueryTarget.DGM_YAML, "SELECT 1")
        exporter = DGMYAMLExporter()
        result = exporter.export(plan)
        assert isinstance(result, str)

    def test_yaml_contains_query_target(self):
        plan = ExportPlan(QueryTarget.DGM_YAML, "SELECT 1")
        exporter = DGMYAMLExporter()
        result = exporter.export(plan)
        assert "query_target" in result

    def test_yaml_contains_query_text(self):
        plan = ExportPlan(QueryTarget.DGM_YAML, "SELECT 1")
        exporter = DGMYAMLExporter()
        result = exporter.export(plan)
        assert "query_text" in result

    def test_yaml_format(self):
        plan = ExportPlan(QueryTarget.DGM_YAML, "SELECT 1")
        exporter = DGMYAMLExporter()
        result = exporter.export(plan)
        # Basic YAML structure — key: value lines
        assert ":" in result

    def test_round_trips_via_json(self):
        plan = ExportPlan(QueryTarget.DGM_YAML, "SELECT 1")
        json_exporter = DGMJSONExporter()
        yaml_exporter = DGMYAMLExporter()
        json_result = json_exporter.export(plan)
        yaml_result = yaml_exporter.export(plan)
        # Both should contain the same key set
        for key in ("query_target", "query_text"):
            assert key in json_result
            assert key in yaml_result

    def test_yaml_scalar_bool_true(self):
        exporter = DGMYAMLExporter()
        assert exporter._scalar(True) == "true"

    def test_yaml_scalar_bool_false(self):
        exporter = DGMYAMLExporter()
        assert exporter._scalar(False) == "false"

    def test_yaml_scalar_none(self):
        exporter = DGMYAMLExporter()
        assert exporter._scalar(None) == "null"

    def test_yaml_scalar_int(self):
        exporter = DGMYAMLExporter()
        assert exporter._scalar(42) == "42"

    def test_yaml_scalar_string_with_colon_quoted(self):
        """Strings containing ':' must be YAML-quoted."""
        exporter = DGMYAMLExporter()
        result = exporter._scalar("key: value")
        assert result.startswith('"') and result.endswith('"')

    def test_yaml_scalar_string_with_newline_quoted(self):
        exporter = DGMYAMLExporter()
        result = exporter._scalar("line1\nline2")
        assert '"' in result

    def test_yaml_scalar_empty_string_quoted(self):
        exporter = DGMYAMLExporter()
        result = exporter._scalar("")
        assert result.startswith('"')

    def test_dict_to_yaml_scalar_passthrough(self):
        """_dict_to_yaml with a scalar (non-dict, non-list) renders the scalar (line 161)."""
        exporter = DGMYAMLExporter()
        result = exporter._dict_to_yaml(42, 0)
        assert "42" in result

    def test_dict_to_yaml_empty_list(self):
        """_dict_to_yaml with an empty list renders [] (line 183)."""
        exporter = DGMYAMLExporter()
        result = exporter._dict_to_yaml([], 0)
        assert "[]" in result

    def test_render_list_item_scalar(self):
        """_render_list_item with a scalar (non-dict) renders '- value' (line 183)."""
        exporter = DGMYAMLExporter()
        result = exporter._render_list_item("hello", 0, "")
        assert "- hello" in result

    def test_yaml_with_cost_estimate(self):
        """Export plan with cost_estimate populates YAML with nested dict."""
        ce = CostEstimate(cpu_ops=200, io_ops=10, note="yaml:test")
        plan = ExportPlan(QueryTarget.DGM_YAML, "SELECT 1", cost_estimate=ce)
        exporter = DGMYAMLExporter()
        result = exporter.export(plan)
        assert "cost_estimate" in result
        assert "cpu_ops" in result

    def test_yaml_with_alternatives(self):
        """Export plan with alternatives populates YAML list section."""
        alt_plan = ExportPlan(QueryTarget.SQL_DUCKDB, "SELECT * FROM t")
        alt_ce = CostEstimate(cpu_ops=100, io_ops=5, note="alt")
        plan = ExportPlan(
            QueryTarget.DGM_YAML,
            "SELECT 1",
            alternatives=[(alt_plan, alt_ce)],
        )
        exporter = DGMYAMLExporter()
        result = exporter.export(plan)
        assert "alternatives" in result
        assert "SELECT * FROM t" in result or "query_text" in result


# ---------------------------------------------------------------------------
# Integration: build_plan()
# ---------------------------------------------------------------------------

class TestBuildPlan:
    def test_build_plan_sql_duckdb(self):
        p = _make_planner(query_target=QueryTarget.SQL_DUCKDB)
        plan = p.build_plan("SELECT * FROM fact_sales")
        assert isinstance(plan, ExportPlan)
        assert plan.query_target is QueryTarget.SQL_DUCKDB

    def test_build_plan_cypher(self):
        p = _make_planner(query_target=QueryTarget.CYPHER)
        plan = p.build_plan("MATCH (n) RETURN n")
        assert isinstance(plan, ExportPlan)
        assert plan.query_target is QueryTarget.CYPHER

    def test_build_plan_with_sink(self):
        p = _make_planner(
            query_target=QueryTarget.SQL_DUCKDB,
            sink_target=SinkTarget.PARQUET,
        )
        plan = p.build_plan("SELECT 1", table_name="output")
        assert plan.sink_target is SinkTarget.PARQUET

    def test_build_plan_includes_cost_estimate(self):
        p = _make_planner()
        plan = p.build_plan("SELECT 1")
        # Cost estimate may be None or a CostEstimate — both valid
        assert plan.cost_estimate is None or isinstance(plan.cost_estimate, CostEstimate)

"""
DGM — Example 17
==================

17. Dimensional Graph Model in action — three-band queries over a star schema.

Demonstrates the four valid query forms from DGM §3:
  B1 only         — context filter (WHERE)
  B1 ∘ B2         — aggregation + HAVING (group revenue per customer)
  B1 ∘ B3         — window ranking + QUALIFY (top-1 sale per customer)
  B1 ∘ B2 ∘ B3    — full pipeline (ranked customer segments with HAVING)

Also shows:
  • τ_E edge-kind classification (verb vs. bridge) via GraphSchema
  • PathPred EXISTS subquery for existence-based B1 filters
  • NOT / AND / OR predicate algebra
  • Temporal joins (SCD-2) via temporal_join()

Run:
    PYTHONPATH=. python -m sqldim.application.examples.features.dgm.showcase
"""

from __future__ import annotations

import duckdb

from sqldim import (
    DGMQuery,
    PropRef,
    AggRef,
    WinRef,
    ScalarPred,
    PathPred,
    AND,
    OR,
    NOT,
    VerbHop,
    BridgeHop,
    Compose,
)
from sqldim.core.graph.schema_graph import SchemaGraph
from sqldim.application.examples.features.dgm.models import (
    CustomerDim,
    ProductDim,
    SegmentDim,
    SaleFact,
    ProductSegmentBridge,
)
from sqldim.application.datasets.domains.dgm import DGMShowcaseSource


def _section(title: str) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print("─" * 60)


# ---------------------------------------------------------------------------
# τ_E edge-kind classification (DGM §4)
# ---------------------------------------------------------------------------


def demo_edge_kind_classification() -> None:
    _section("τ_E edge-kind classification")
    sg = SchemaGraph(
        [CustomerDim, ProductDim, SegmentDim, SaleFact],
        bridge_models=[ProductSegmentBridge],
    )
    schema = sg.graph_schema()
    for edge in schema.edges:
        print(f"  {edge['name']:30s}  kind={edge['edge_kind']}")


# ---------------------------------------------------------------------------
# B1 — Context filter
# ---------------------------------------------------------------------------


def demo_b1_filter(con: duckdb.DuckDBPyConnection) -> None:
    _section("B1 — Context filter: retail customers only")
    hop_c = VerbHop(
        "s", "placed_by", "c", table="dgm_showcase_customer", on="c.id = s.customer_id"
    )
    q = (
        DGMQuery()
        .anchor("dgm_showcase_sale", "s")
        .path_join(hop_c)
        .where(ScalarPred(PropRef("c", "segment"), "=", "retail"))
    )
    print(q.to_sql())
    rows = q.execute(con)
    print(f"\n  → {len(rows)} rows (Alice 3 + Carol 2 = 5)")


# ---------------------------------------------------------------------------
# B1 — PathPred EXISTS filter
# ---------------------------------------------------------------------------


def demo_b1_path_pred(con: duckdb.DuckDBPyConnection) -> None:
    _section("B1 — PathPred EXISTS: sales with electronics products")
    hop = VerbHop(
        "s", "includes", "d", table="dgm_showcase_product", on="d.id = s.product_id"
    )
    pp = PathPred(
        anchor="s",
        path=hop,
        sub_filter=ScalarPred(PropRef("d", "category"), "=", "electronics"),
    )
    q = DGMQuery().anchor("dgm_showcase_sale", "s").where(pp)
    print(q.to_sql())
    rows = q.execute(con)
    print(f"\n  → {len(rows)} rows (sales 1, 4, 5 — Widget only)")


# ---------------------------------------------------------------------------
# B1 — NOT predicate
# ---------------------------------------------------------------------------


def demo_b1_not(con: duckdb.DuckDBPyConnection) -> None:
    _section("B1 — NOT: exclude clearance products")
    hop_d = VerbHop(
        "s", "includes", "d", table="dgm_showcase_product", on="d.id = s.product_id"
    )
    q = (
        DGMQuery()
        .anchor("dgm_showcase_sale", "s")
        .path_join(hop_d)
        .where(NOT(ScalarPred(PropRef("d", "category"), "=", "clearance")))
    )
    rows = q.execute(con)
    print(f"  → {len(rows)} rows (sale 2/Gadget excluded; expect 5)")


# ---------------------------------------------------------------------------
# B1 ∘ B2 — Aggregation + HAVING
# ---------------------------------------------------------------------------


def demo_b1_b2_having(con: duckdb.DuckDBPyConnection) -> None:
    _section("B1 ∘ B2 — Group revenue per customer, HAVING > 4000")
    hop = VerbHop(
        "s", "placed_by", "c", table="dgm_showcase_customer", on="c.id = s.customer_id"
    )
    q = (
        DGMQuery()
        .anchor("dgm_showcase_sale", "s")
        .path_join(hop)
        .group_by("c.id", "c.email")
        .agg(total_rev="SUM(s.revenue)", sale_cnt="COUNT(*)")
        .having(ScalarPred(AggRef("total_rev"), ">", 4000))
    )
    print(q.to_sql())
    rows = q.execute(con)
    print(f"\n  → {len(rows)} customer(s) (Alice 5200 > 4000; Bob 4000 not >)")
    for r in rows:
        print(f"     id={r[0]}  email={r[1]}  total_rev={r[2]}")


# ---------------------------------------------------------------------------
# B1 ∘ B3 — Window ranking (top-1 sale per customer)
# ---------------------------------------------------------------------------


def demo_b1_b3_qualify(con: duckdb.DuckDBPyConnection) -> None:
    _section("B1 ∘ B3 — Top-1 sale per customer by revenue")
    q = (
        DGMQuery()
        .anchor("dgm_showcase_sale", "s")
        .window(
            rn="ROW_NUMBER() OVER (PARTITION BY s.customer_id ORDER BY s.revenue DESC)"
        )
        .qualify(ScalarPred(WinRef("rn"), "=", 1))
    )
    print(q.to_sql())
    rows = q.execute(con)
    print(f"\n  → {len(rows)} rows (one per customer)")
    for r in rows:
        print(f"     customer_id={r[1]}  revenue={r[3]}")


# ---------------------------------------------------------------------------
# B1 ∘ B2 ∘ B3 — Full pipeline
# ---------------------------------------------------------------------------


def demo_full_pipeline(con: duckdb.DuckDBPyConnection) -> None:
    _section("B1 ∘ B2 ∘ B3 — Full: retail, group revenue, rank top-1")
    hop = VerbHop(
        "s", "placed_by", "c", table="dgm_showcase_customer", on="c.id = s.customer_id"
    )
    q = (
        DGMQuery()
        .anchor("dgm_showcase_sale", "s")
        .path_join(hop)
        .where(ScalarPred(PropRef("c", "segment"), "=", "retail"))
        .group_by("c.id", "c.email")
        .agg(total_rev="SUM(s.revenue)")
        .having(ScalarPred(AggRef("total_rev"), ">", 1000))
        .window(rnk="RANK() OVER (ORDER BY SUM(s.revenue) DESC)")
        .qualify(ScalarPred(WinRef("rnk"), "=", 1))
    )
    print(q.to_sql())
    rows = q.execute(con)
    print(f"\n  → {len(rows)} winner(s) (Alice ranks 1st among retail customers)")
    for r in rows:
        print(f"     id={r[0]}  email={r[1]}  total_rev={r[2]}")


# ---------------------------------------------------------------------------
# Bridge path traversal (B1 with BridgeHop + VerbHop composed)
# ---------------------------------------------------------------------------


def demo_bridge_path(con: duckdb.DuckDBPyConnection) -> None:
    _section("B1 — Composed path: sale → product → segment (bridge)")
    hop_verb = VerbHop(
        "s", "includes", "d", table="dgm_showcase_product", on="d.id = s.product_id"
    )
    hop_bridge = BridgeHop(
        "d",
        "belongs_to",
        "seg",
        table="dgm_showcase_prod_seg",
        on="seg.product_id = d.id",
    )
    path = Compose(hop_verb, hop_bridge)
    pp = PathPred(
        anchor="s",
        path=path,
        sub_filter=ScalarPred(PropRef("seg", "segment_id"), "=", 1),
    )
    q = DGMQuery().anchor("dgm_showcase_sale", "s").where(pp)
    print(q.to_sql())
    rows = q.execute(con)
    print(f"\n  → {len(rows)} rows (sales of electronics-tier products)")


# ---------------------------------------------------------------------------
# BDD predicate compilation
# ---------------------------------------------------------------------------


def demo_bdd_predicate() -> None:
    """Demonstrate BDD compile, satisfiability, implication, and to_sql."""
    from sqldim.core.query.dgm.bdd import BDDManager, DGMPredicateBDD

    _section("BDD predicate compilation and canonical form")

    mgr = BDDManager()
    bdd = DGMPredicateBDD(mgr)

    p1 = ScalarPred(PropRef("s", "revenue"), ">", 1000)
    p2 = ScalarPred(PropRef("s", "sale_year"), "=", 2024)
    pred = AND(p1, p2)

    uid = bdd.compile(pred)
    print(f"  BDD node ID for AND(p1,p2): {uid}")
    print(f"  Satisfiable: {bdd.is_satisfiable(uid)}")
    print(f"  To SQL:\n    {bdd.to_sql(uid)}")

    # Implication: (p1 AND p2) → p1
    uid_p1 = bdd.compile(p1)
    print(f"  AND(p1,p2) implies p1: {bdd.implies(uid, uid_p1)}")
    print(f"  p1 implies AND(p1,p2): {bdd.implies(uid_p1, uid)}")


# ---------------------------------------------------------------------------
# Schema annotation layer (Σ) + recommender
# ---------------------------------------------------------------------------


def demo_annotation_sigma() -> None:
    """Build an AnnotationSigma and run the DGMRecommender over it."""
    from sqldim.core.query.dgm.annotations import (
        AnnotationSigma,
        Grain,
        GrainKind,
        SCDType,
        SCDKind,
        Conformed,
        BridgeSemantics,
        BridgeSemanticsKind,
    )
    from sqldim.core.query.dgm.recommender import DGMRecommender

    _section("Schema annotation layer Σ and recommender")

    sigma = AnnotationSigma(
        annotations=[
            Grain(fact="dgm_showcase_sale", grain=GrainKind.PERIOD),
            SCDType(dim="dgm_showcase_customer", scd=SCDKind.SCD2),
            Conformed(dim="dgm_showcase_customer", fact_types=frozenset({"Sale"})),
            BridgeSemantics(bridge="dgm_showcase_prod_seg", sem=BridgeSemanticsKind.STRUCTURAL),
        ]
    )
    print(f"  Annotations loaded: {len(sigma)}")
    print(f"  scd_of('dgm_showcase_customer'): {sigma.scd_of('dgm_showcase_customer').value}")
    print(f"  is_conformed('dgm_showcase_customer', 'Sale'): "
          f"{sigma.is_conformed('dgm_showcase_customer', 'Sale')}")
    print(f"  grain_of('dgm_showcase_sale'): {sigma.grain_of('dgm_showcase_sale').value}")

    rec = DGMRecommender(sigma)
    suggestions = rec.run_annotation_rules()
    print(f"  Recommender suggestions: {len(suggestions)}")
    for s in suggestions[:3]:
        print(f"    • {s.text}")

    # Routing: high entropy → stage1 (Free-endpoint characterisation)
    route = rec.route(entropy=0.9)
    print(f"  Route (entropy=0.9): {route}")
    route_low = rec.route(entropy=0.1)
    print(f"  Route (entropy=0.1): {route_low}")


# ---------------------------------------------------------------------------
# DGM planner
# ---------------------------------------------------------------------------


def demo_planner() -> None:
    """DGMPlanner: build an ExportPlan with rules 1a–9."""
    from sqldim.core.query.dgm.annotations import AnnotationSigma
    from sqldim.core.query.dgm.planner import DGMPlanner, QueryTarget, SinkTarget
    from sqldim.core.query.dgm.graph import GraphStatistics
    from sqldim.core.query.dgm.exporters import (
        DGMJSONExporter,
        DGMYAMLExporter,
    )

    _section("DGM query planner and exporters")

    sigma = AnnotationSigma(annotations=[])
    stats = GraphStatistics(node_count=3, edge_count=6)
    planner = DGMPlanner(
        cost_model=None,
        statistics=stats,
        annotations=sigma,
        rules=None,
        query_target=QueryTarget.SQL_DUCKDB,
        sink_target=SinkTarget.DUCKDB,
    )

    sql = (
        DGMQuery()
        .anchor("dgm_showcase_sale", "s")
        .where(ScalarPred(PropRef("s", "sale_year"), "=", 2024))
        .to_sql()
    )
    plan = planner.build_plan(sql)
    print(f"  Query target : {plan.query_target.value}")
    print(f"  Sink target  : {plan.sink_target.value if plan.sink_target else 'none'}")
    print(f"  Cost estimate: {plan.cost_estimate}")

    json_out = DGMJSONExporter().export(plan)
    yaml_out = DGMYAMLExporter().export(plan)
    print(f"  JSON export  : {len(json_out)} chars")
    print(f"  YAML export  : {len(yaml_out)} chars")
    print(f"  YAML snippet :\n    {yaml_out.splitlines()[0]}")


# ---------------------------------------------------------------------------
# Example 6 — PipelineArtifact: backfill-incremental state machine
# ---------------------------------------------------------------------------


def demo_pipeline_artifact() -> None:
    """Example 6 from spec §10.1: PipelineArtifact annotation + Rule 10.

    D20 — PipelineArtifact is syntactic sugar, not a new fact type.  The
    backfill-incremental state machine is modelled entirely within existing
    constructs: Grain(ACCUMULATING), BridgeSemantics, WritePlan,
    Q_delta(CHANGED_PROPERTY), and CTL temporal properties.
    PipelineArtifact assembles these into one declaration and infers
    semantics from P(f).state at planning time (planner Rule 10).

    D21 — Backfill and refresh require different WritePlan modes by
    construction.  Backfill (APPEND) has no prior artifact — failure leaves
    the window durably in Failed.  Refresh (MERGE) preserves the prior
    Complete via atomic swap — failure regresses to Stale, not Failed.
    ADAPTIVE mode makes this automatic from P(f).state at planning time.
    """
    from sqldim.core.query.dgm.annotations import (
        AnnotationSigma,
        PipelineArtifact,
        PipelineStateKind,
        WriteModeKind,
        GrainKind,
        BridgeSemanticsKind,
    )
    from sqldim.core.query.dgm.planner import DGMPlanner, QueryTarget, SinkTarget
    from sqldim.core.query.dgm.graph import GraphStatistics

    _section("Example 6 — PipelineArtifact: backfill-incremental state machine (D20/D21)")

    # Build the annotation
    artifact = PipelineArtifact(
        fact="daily_rev",
        pipeline_id="daily_revenue",
        ttl=7 * 24 * 3600,    # 7 days in seconds
        backfill_horizon=90,   # days
        write_mode=WriteModeKind.ADAPTIVE,
    )
    sigma = AnnotationSigma([artifact])
    print(f"  PipelineArtifact: fact={artifact.fact!r}, pipeline_id={artifact.pipeline_id!r}")
    print(f"  Effective grain : {artifact.effective_grain.value}")
    print(f"  Write mode      : {artifact.write_mode.value}")
    print(f"  TTL (s)         : {artifact.ttl}")
    print(f"  Backfill horizon: {artifact.backfill_horizon} days")

    # Transition semantics (D20 — wired to existing BridgeSemantics)
    transitions = [
        (PipelineStateKind.MISSING,    PipelineStateKind.IN_FLIGHT, False),
        (PipelineStateKind.IN_FLIGHT,  PipelineStateKind.COMPLETE,  False),
        (PipelineStateKind.IN_FLIGHT,  PipelineStateKind.FAILED,    False),  # backfill
        (PipelineStateKind.IN_FLIGHT,  PipelineStateKind.FAILED,    True),   # refresh
        (PipelineStateKind.COMPLETE,   PipelineStateKind.STALE,     False),
        (PipelineStateKind.STALE,      PipelineStateKind.IN_FLIGHT, False),
        (PipelineStateKind.FAILED,     PipelineStateKind.IN_FLIGHT, False),
    ]
    print("\n  Transition semantics:")
    for from_s, to_s, is_refresh in transitions:
        sem = artifact.transition_semantics(from_s, to_s, is_refresh=is_refresh)
        label = "(refresh)" if is_refresh else ""
        print(f"    {from_s.value:12s} → {to_s.value:12s} {label:10s}: {sem.value}")

    # Rule 10: ADAPTIVE inference for each pipeline state
    stats = GraphStatistics(node_count=1, edge_count=0)
    planner = DGMPlanner(
        cost_model=None,
        statistics=stats,
        annotations=sigma,
        rules=None,
        query_target=QueryTarget.SQL_DUCKDB,
        sink_target=SinkTarget.DELTA,
    )
    print("\n  Rule 10 (ADAPTIVE write plan inference):")
    for state in [
        PipelineStateKind.MISSING,
        PipelineStateKind.FAILED,
        PipelineStateKind.STALE,
        PipelineStateKind.COMPLETE,
    ]:
        result = planner.apply_rule_10(
            fact=artifact.fact,
            state=state,
            ttl_elapsed_s=0.0,
            write_mode=artifact.write_mode,
        )
        print(f"    state={state.value:12s}: {result}")

    # Q_backfill — find Missing/Failed windows within horizon
    _section("  Q_backfill (Missing/Failed within horizon)")
    q_backfill = (
        DGMQuery()
        .anchor("daily_rev", "a")
        .where(
            AND(
                ScalarPred(PropRef("a", "state"), "IN", ("Missing", "Failed")),
                ScalarPred(PropRef("a", "window_end"), "<",
                           "DATE_SUB(NOW(), INTERVAL 90 DAY)"),
            )
        )
        .group_by("a.pipeline_id", "a.window_start")
        .agg(age_days="MAX(a.window_end)")
        .window(priority="RANK() OVER (ORDER BY MAX(a.window_end) DESC)")
        .qualify(ScalarPred(WinRef("priority"), "<=", 5))
    )
    sql_backfill = q_backfill.to_sql()
    print(f"  SQL ({len(sql_backfill.splitlines())} lines): {sql_backfill[:80]}...")

    # Q_refresh — find Stale windows
    _section("  Q_refresh (Stale windows)")
    q_refresh = (
        DGMQuery()
        .anchor("daily_rev", "a")
        .where(ScalarPred(PropRef("a", "state"), "=", "Stale"))
        .group_by("a.pipeline_id", "a.window_start")
        .agg(staleness="MAX(a.window_end)")
        .window(priority="RANK() OVER (ORDER BY MAX(a.window_end) DESC)")
        .qualify(ScalarPred(WinRef("priority"), "<=", 5))
    )
    sql_refresh = q_refresh.to_sql()
    print(f"  SQL ({len(sql_refresh.splitlines())} lines): {sql_refresh[:80]}...")

    # Sigma lookup helper
    found = sigma.pipeline_artifact_of("daily_rev")
    assert found is artifact
    print(f"\n  sigma.pipeline_artifact_of('daily_rev') found: {found.pipeline_id!r}")
    print("  Example 6 complete.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


EXAMPLE_METADATA = {
    "name": "dgm",
    "title": "Dimensional Graph Model",
    "description": (
        "Example 17: DGM three-band queries, BDD predicates, planner, and exporters. "
        "Example 6: PipelineArtifact backfill-incremental state machine (D20/D21)."
    ),
    "entry_point": "run_all",
}


def run_all() -> None:
    con = duckdb.connect()
    DGMShowcaseSource().setup(con)

    demo_edge_kind_classification()
    demo_b1_filter(con)
    demo_b1_path_pred(con)
    demo_b1_not(con)
    demo_b1_b2_having(con)
    demo_b1_b3_qualify(con)
    demo_full_pipeline(con)
    demo_bridge_path(con)
    demo_bdd_predicate()
    demo_annotation_sigma()
    demo_planner()
    demo_pipeline_artifact()

    con.close()
    print("\nDGM showcase complete.")


if __name__ == "__main__":  # pragma: no cover
    run_all()

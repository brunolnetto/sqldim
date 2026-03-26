"""DGM showcase — model demos (BDD, annotation sigma, planner, pipeline artifact)."""

from __future__ import annotations

from sqldim import (
    DGMQuery,
    PropRef,
    WinRef,
    ScalarPred,
    AND,
)
from sqldim.application.examples.features.dgm._demos_dql import _section


def demo_bdd_predicate() -> None:
    from sqldim import AND
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

    uid_p1 = bdd.compile(p1)
    print(f"  AND(p1,p2) implies p1: {bdd.implies(uid, uid_p1)}")
    print(f"  p1 implies AND(p1,p2): {bdd.implies(uid_p1, uid)}")


def demo_annotation_sigma() -> None:
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
            BridgeSemantics(
                bridge="dgm_showcase_prod_seg", sem=BridgeSemanticsKind.STRUCTURAL
            ),
        ]
    )
    print(f"  Annotations loaded: {len(sigma)}")
    print(
        f"  scd_of('dgm_showcase_customer'): {sigma.scd_of('dgm_showcase_customer').value}"  # type: ignore[union-attr]
    )
    print(
        f"  is_conformed('dgm_showcase_customer', 'Sale'): "
        f"{sigma.is_conformed('dgm_showcase_customer', 'Sale')}"
    )
    print(
        f"  grain_of('dgm_showcase_sale'): {sigma.grain_of('dgm_showcase_sale').value}"  # type: ignore[union-attr]
    )

    rec = DGMRecommender(sigma)
    suggestions = rec.run_annotation_rules()
    print(f"  Recommender suggestions: {len(suggestions)}")
    for s in suggestions[:3]:
        print(f"    • {s.text}")

    route = rec.route(entropy=0.9)
    print(f"  Route (entropy=0.9): {route}")
    print(f"  Route (entropy=0.1): {rec.route(entropy=0.1)}")


def demo_planner() -> None:
    from sqldim.core.query.dgm.annotations import AnnotationSigma
    from sqldim.core.query.dgm.planner import DGMPlanner, QueryTarget, SinkTarget
    from sqldim.core.query.dgm.graph import GraphStatistics
    from sqldim.core.query.dgm.exporters import DGMJSONExporter, DGMYAMLExporter

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


def demo_pipeline_artifact() -> None:
    """Example 6 — PipelineArtifact: backfill-incremental state machine (D20/D21)."""
    from sqldim.core.query.dgm.annotations import (
        AnnotationSigma,
        PipelineArtifact,
        PipelineStateKind,
        WriteModeKind,
    )
    from sqldim.core.query.dgm.planner import DGMPlanner, QueryTarget, SinkTarget
    from sqldim.core.query.dgm.graph import GraphStatistics

    _section(
        "Example 6 — PipelineArtifact: backfill-incremental state machine (D20/D21)"
    )

    artifact = PipelineArtifact(
        fact="daily_rev",
        pipeline_id="daily_revenue",
        ttl=7 * 24 * 3600,
        backfill_horizon=90,
        write_mode=WriteModeKind.ADAPTIVE,
    )
    sigma = AnnotationSigma([artifact])
    print(
        f"  PipelineArtifact: fact={artifact.fact!r}, pipeline_id={artifact.pipeline_id!r}"
    )
    print(f"  Effective grain : {artifact.effective_grain.value}")
    print(f"  Write mode      : {artifact.write_mode.value}")
    print(f"  TTL (s)         : {artifact.ttl}")
    print(f"  Backfill horizon: {artifact.backfill_horizon} days")

    transitions = [
        (PipelineStateKind.MISSING, PipelineStateKind.IN_FLIGHT, False),
        (PipelineStateKind.IN_FLIGHT, PipelineStateKind.COMPLETE, False),
        (PipelineStateKind.IN_FLIGHT, PipelineStateKind.FAILED, False),
        (PipelineStateKind.IN_FLIGHT, PipelineStateKind.FAILED, True),
        (PipelineStateKind.COMPLETE, PipelineStateKind.STALE, False),
        (PipelineStateKind.STALE, PipelineStateKind.IN_FLIGHT, False),
        (PipelineStateKind.FAILED, PipelineStateKind.IN_FLIGHT, False),
    ]
    print("\n  Transition semantics:")
    for from_s, to_s, is_refresh in transitions:
        sem = artifact.transition_semantics(from_s, to_s, is_refresh=is_refresh)
        label = "(refresh)" if is_refresh else ""
        print(f"    {from_s.value:12s} → {to_s.value:12s} {label:10s}: {sem.value}")

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

    _section("  Q_backfill (Missing/Failed within horizon)")
    q_backfill = (
        DGMQuery()
        .anchor("daily_rev", "a")
        .where(
            AND(
                ScalarPred(PropRef("a", "state"), "IN", ("Missing", "Failed")),
                ScalarPred(
                    PropRef("a", "window_end"), "<", "DATE_SUB(NOW(), INTERVAL 90 DAY)"
                ),
            )
        )
        .group_by("a.pipeline_id", "a.window_start")
        .agg(age_days="MAX(a.window_end)")
        .window(priority="RANK() OVER (ORDER BY MAX(a.window_end) DESC)")
        .qualify(ScalarPred(WinRef("priority"), "<=", 5))
    )
    sql_backfill = q_backfill.to_sql()
    print(f"  SQL ({len(sql_backfill.splitlines())} lines): {sql_backfill[:80]}...")

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

    found = sigma.pipeline_artifact_of("daily_rev")
    assert found is artifact
    print(f"\n  sigma.pipeline_artifact_of('daily_rev') found: {found.pipeline_id!r}")
    print("  Example 6 complete.")

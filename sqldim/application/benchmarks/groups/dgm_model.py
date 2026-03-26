"""DGM model benchmarks (groups Q–S): model building, layering, resolution."""

from __future__ import annotations

import time
import traceback as _traceback


from sqldim.application.benchmarks._dataset import (
    BenchmarkDatasetGenerator,
)
from sqldim.application.benchmarks.infra import (
    BenchmarkResult,
    _select_tiers,
)
from sqldim.application.benchmarks.memory_probe import MemoryProbe
from sqldim.core.query.dgm.annotations import (
    AnnotationSigma,
    Grain,
    GrainKind,
    SCDType,
    SCDKind,
    Conformed,
    BridgeSemantics,
    BridgeSemanticsKind,
    Hierarchy,
    FactlessFact,
    DerivedFact,
    WeightConstraint,
    WeightConstraintKind,
)
from sqldim.core.query.dgm.recommender import DGMRecommender
from sqldim.core.query.dgm.planner import DGMPlanner, QueryTarget, SinkTarget
from sqldim.core.query.dgm.graph import GraphStatistics

# ═══════════════════════════════════════════════════════════════════════════
# ── Group Q  DGMRecommender annotation + trail rules throughput ───────────
# ═══════════════════════════════════════════════════════════════════════════

_Q_SIGMA = AnnotationSigma(
    annotations=[
        Grain(fact="sale", grain=GrainKind.PERIOD),
        SCDType(dim="customer", scd=SCDKind.SCD2),
        Conformed(dim="customer", fact_types=frozenset({"Sale"})),
        BridgeSemantics(bridge="link", sem=BridgeSemanticsKind.CAUSAL),
        Hierarchy(root="category", depth=3),
        FactlessFact(fact="attendance"),
        DerivedFact(fact="margin", sources=["revenue", "cost"], expr="revenue - cost"),
        WeightConstraint(bridge="alloc", constraint=WeightConstraintKind.ALLOCATIVE),
    ]
)


def _q1_recommender_annotation(
    tier: str, n: int, temp_dir: str, rec: DGMRecommender
) -> BenchmarkResult:
    cid = f"Q-recommender-annotation-{tier}"
    result = BenchmarkResult(
        case_id=cid,
        group="Q",
        profile="recommender-annotation",
        tier=tier,
        processor="DGMRecommender",
        sink="none",
        source="synthetic",
        phase="compute",
        n_rows=n,
        n_changed=0,
    )
    try:
        MemoryProbe.check_safe_to_run(label=cid)
        probe = MemoryProbe(temp_dir=temp_dir, label=cid)
        with probe:
            t0 = time.perf_counter()
            for _ in range(n):
                rec.run_annotation_rules()
            result.wall_s = time.perf_counter() - t0
        m = probe.report
        result.peak_rss_gb = m.peak_rss_gb
        result.min_sys_avail_gb = m.min_sys_avail_gb
        result.rows_per_sec = n / max(result.wall_s, 0.001)
        result.inserted = n
    except RuntimeError as exc:
        result.ok = False
        result.error = f"SKIPPED: {exc}"
    except Exception as exc:
        result.ok = False
        result.error = f"{type(exc).__name__}: {exc}\n" + _traceback.format_exc()[-600:]
    return result


def _q2_recommender_trail(
    tier: str, n: int, temp_dir: str, rec: DGMRecommender
) -> BenchmarkResult:
    cid = f"Q-recommender-trail-{tier}"
    result = BenchmarkResult(
        case_id=cid,
        group="Q",
        profile="recommender-trail",
        tier=tier,
        processor="DGMRecommender",
        sink="none",
        source="synthetic",
        phase="compute",
        n_rows=n,
        n_changed=0,
    )
    try:
        MemoryProbe.check_safe_to_run(label=cid)
        probe = MemoryProbe(temp_dir=temp_dir, label=cid)
        with probe:
            t0 = time.perf_counter()
            for _ in range(n):
                rec.run_trail_rules("customer", 6, 0.8, 0.75)
                rec.run_trail_rules("product", 2, 0.3, 0.2)
            result.wall_s = time.perf_counter() - t0
        m = probe.report
        result.peak_rss_gb = m.peak_rss_gb
        result.min_sys_avail_gb = m.min_sys_avail_gb
        result.rows_per_sec = n * 2 / max(result.wall_s, 0.001)
        result.inserted = n * 2
    except RuntimeError as exc:
        result.ok = False
        result.error = f"SKIPPED: {exc}"
    except Exception as exc:
        result.ok = False
        result.error = f"{type(exc).__name__}: {exc}\n" + _traceback.format_exc()[-600:]
    return result


def group_q_recommender(
    gen: BenchmarkDatasetGenerator,
    temp_dir: str,
    max_tier: str = "m",
    **_,
) -> list[BenchmarkResult]:
    """Group Q — DGMRecommender annotation rule + trail rule throughput."""
    tier_map = {"xs": 200, "s": 2_000, "m": 10_000}
    tier_order = ["xs", "s", "m"]
    active_tiers = _select_tiers(tier_order, tier_map, max_tier)
    rec = DGMRecommender(_Q_SIGMA)
    results: list[BenchmarkResult] = []
    for tier in active_tiers:
        n = tier_map[tier]
        results.append(_q1_recommender_annotation(tier, n, temp_dir, rec))
        results.append(_q2_recommender_trail(tier, n, temp_dir, rec))
    return results


# ═══════════════════════════════════════════════════════════════════════════
# ── Group R  DGMPlanner rule cycle throughput ─────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════

_R_PLANNER = DGMPlanner(
    cost_model=None,
    statistics=GraphStatistics(node_count=100, edge_count=500),
    annotations=AnnotationSigma(annotations=[]),
    rules=None,
    query_target=QueryTarget.SQL_DUCKDB,
    sink_target=SinkTarget.PARQUET,
)


def _r1_rule1a(
    tier: str, n: int, temp_dir: str, planner: DGMPlanner
) -> BenchmarkResult:
    cid = f"R-planner-rule1a-{tier}"
    result = BenchmarkResult(
        case_id=cid,
        group="R",
        profile="planner-rule1a",
        tier=tier,
        processor="DGMPlanner",
        sink="none",
        source="synthetic",
        phase="plan",
        n_rows=n,
        n_changed=0,
    )
    try:
        MemoryProbe.check_safe_to_run(label=cid)
        probe = MemoryProbe(temp_dir=temp_dir, label=cid)
        with probe:
            t0 = time.perf_counter()
            for _ in range(n):
                planner.apply_rule_1a("Free-Free", "BFS", 100)
            result.wall_s = time.perf_counter() - t0
        m = probe.report
        result.peak_rss_gb = m.peak_rss_gb
        result.min_sys_avail_gb = m.min_sys_avail_gb
        result.rows_per_sec = n / max(result.wall_s, 0.001)
        result.inserted = n
    except RuntimeError as exc:
        result.ok = False
        result.error = f"SKIPPED: {exc}"
    except Exception as exc:
        result.ok = False
        result.error = f"{type(exc).__name__}: {exc}\n" + _traceback.format_exc()[-600:]
    return result


def _r2_rule9(tier: str, n: int, temp_dir: str, planner: DGMPlanner) -> BenchmarkResult:
    cid = f"R-planner-rule9-{tier}"
    result = BenchmarkResult(
        case_id=cid,
        group="R",
        profile="planner-rule9",
        tier=tier,
        processor="DGMPlanner",
        sink="none",
        source="synthetic",
        phase="plan",
        n_rows=n,
        n_changed=0,
    )
    try:
        MemoryProbe.check_safe_to_run(label=cid)
        probe = MemoryProbe(temp_dir=temp_dir, label=cid)
        with probe:
            t0 = time.perf_counter()
            for _ in range(n):
                planner.apply_rule_9(
                    has_reachable_from=True,
                    source_alias="src",
                    has_reachable_to=True,
                    target_alias="tgt",
                )
            result.wall_s = time.perf_counter() - t0
        m = probe.report
        result.peak_rss_gb = m.peak_rss_gb
        result.min_sys_avail_gb = m.min_sys_avail_gb
        result.rows_per_sec = n / max(result.wall_s, 0.001)
        result.inserted = n
    except RuntimeError as exc:
        result.ok = False
        result.error = f"SKIPPED: {exc}"
    except Exception as exc:
        result.ok = False
        result.error = f"{type(exc).__name__}: {exc}\n" + _traceback.format_exc()[-600:]
    return result


def _r3_build_plan(
    tier: str, n: int, temp_dir: str, planner: DGMPlanner
) -> BenchmarkResult:
    cid = f"R-planner-build-plan-{tier}"
    result = BenchmarkResult(
        case_id=cid,
        group="R",
        profile="planner-build-plan",
        tier=tier,
        processor="DGMPlanner",
        sink="none",
        source="synthetic",
        phase="plan",
        n_rows=n,
        n_changed=0,
    )
    try:
        MemoryProbe.check_safe_to_run(label=cid)
        probe = MemoryProbe(temp_dir=temp_dir, label=cid)
        with probe:
            t0 = time.perf_counter()
            for _ in range(n):
                planner.build_plan("SELECT * FROM fact WHERE year = 2024")
            result.wall_s = time.perf_counter() - t0
        m = probe.report
        result.peak_rss_gb = m.peak_rss_gb
        result.min_sys_avail_gb = m.min_sys_avail_gb
        result.rows_per_sec = n / max(result.wall_s, 0.001)
        result.inserted = n
    except RuntimeError as exc:
        result.ok = False
        result.error = f"SKIPPED: {exc}"
    except Exception as exc:
        result.ok = False
        result.error = f"{type(exc).__name__}: {exc}\n" + _traceback.format_exc()[-600:]
    return result


def group_r_planner(
    gen: BenchmarkDatasetGenerator,
    temp_dir: str,
    max_tier: str = "m",
    **_,
) -> list[BenchmarkResult]:
    """Group R — DGMPlanner rule cycles (1a, 9, build_plan)."""
    tier_map = {"xs": 500, "s": 5_000, "m": 20_000}
    tier_order = ["xs", "s", "m"]
    active_tiers = _select_tiers(tier_order, tier_map, max_tier)
    results: list[BenchmarkResult] = []
    for tier in active_tiers:
        n = tier_map[tier]
        results.append(_r1_rule1a(tier, n, temp_dir, _R_PLANNER))
        results.append(_r2_rule9(tier, n, temp_dir, _R_PLANNER))
        results.append(_r3_build_plan(tier, n, temp_dir, _R_PLANNER))
    return results


# ═══════════════════════════════════════════════════════════════════════════
# ── Group S  DGM exporter throughput ─────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════


def _s_exporter_case(
    tier: str,
    n: int,
    exporter,
    plan,
    fmt: str,
    complexity: str,
    temp_dir: str,
) -> "BenchmarkResult":
    cid = f"S-exporter-{fmt}-{complexity}-{tier}"
    result = BenchmarkResult(
        case_id=cid,
        group="S",
        profile=f"exporter-{fmt}-{complexity}",
        tier=tier,
        processor=type(exporter).__name__,
        sink="none",
        source="synthetic",
        phase="export",
        n_rows=n,
        n_changed=0,
    )
    try:
        MemoryProbe.check_safe_to_run(label=cid)
        probe = MemoryProbe(temp_dir=temp_dir, label=cid)
        with probe:
            t0 = time.perf_counter()
            for _ in range(n):
                exporter.export(plan)
            result.wall_s = time.perf_counter() - t0
        m = probe.report
        result.peak_rss_gb = m.peak_rss_gb
        result.min_sys_avail_gb = m.min_sys_avail_gb
        result.rows_per_sec = n / max(result.wall_s, 0.001)
        result.inserted = n
    except RuntimeError as exc:
        result.ok = False
        result.error = f"SKIPPED: {exc}"
    except Exception as exc:
        result.ok = False
        result.error = f"{type(exc).__name__}: {exc}\n" + _traceback.format_exc()[-600:]
    return result


def group_s_exporter(
    gen: BenchmarkDatasetGenerator,
    temp_dir: str,
    max_tier: str = "m",
    **_,
) -> list[BenchmarkResult]:
    """Group S — DGM multi-target exporter throughput (JSON / YAML x simple / complex)."""
    from sqldim.core.query.dgm.planner import QueryTarget, SinkTarget, ExportPlan
    from sqldim.core.query.dgm.exporters import DGMJSONExporter, DGMYAMLExporter

    tier_map = {"xs": 500, "s": 5_000, "m": 20_000}
    tier_order = ["xs", "s", "m"]
    active_tiers = _select_tiers(tier_order, tier_map, max_tier)

    simple_plan = ExportPlan(QueryTarget.SQL_DUCKDB, "SELECT * FROM fact")
    complex_plan = ExportPlan(
        QueryTarget.DGM_YAML,
        (
            "SELECT c.region, SUM(f.revenue) AS rev "
            "FROM fact f JOIN customer c ON c.id=f.customer_id "
            "GROUP BY 1"
        ),
        sink_target=SinkTarget.PARQUET,
        write_plan="COPY (SELECT 1) TO 'out.parquet'",
    )

    exporters = [
        ("json", "simple", DGMJSONExporter(), simple_plan),
        ("json", "complex", DGMJSONExporter(), complex_plan),
        ("yaml", "simple", DGMYAMLExporter(), simple_plan),
        ("yaml", "complex", DGMYAMLExporter(), complex_plan),
    ]

    results: list[BenchmarkResult] = []
    for tier in active_tiers:
        n = tier_map[tier]
        for fmt, complexity, exporter, plan in exporters:
            results.append(
                _s_exporter_case(tier, n, exporter, plan, fmt, complexity, temp_dir)
            )
    return results

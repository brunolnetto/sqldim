"""
benchmarks/groups/dgm.py
========================
DGM query-algebra benchmarks (groups O–S).

Covers: three-band DGM queries, BDD predicate compilation,
recommender routing, query planner, and exporters.
"""
from __future__ import annotations

import os
import time
import traceback as _traceback

import duckdb
from sqlalchemy.pool import StaticPool

from sqldim.application.benchmarks._dataset import BenchmarkDatasetGenerator, DatasetArtifact, SCALE_TIERS
from sqldim.application.benchmarks.infra import (
    BenchmarkResult,
    SOURCE_NAMES,
    _make_source,
    _remove_db,
    _configure,
    _run_scd2_batch,
    _run_metadata_batch,
)
from sqldim.application.benchmarks.memory_probe import MemoryProbe
from sqldim.application.benchmarks.scan_probe import DuckDBObjectTracker


def group_o_dgm_query(
    gen: BenchmarkDatasetGenerator,
    temp_dir: str,
    max_tier: str = "m",
    **_,
) -> list[BenchmarkResult]:
    """Group O — DGMQuery three-band throughput (B1, B1∘B2, B1∘B3, B1∘B2∘B3)."""
    from sqldim import (
        DGMQuery,
        PropRef, AggRef, WinRef,
        ScalarPred,
        AND,
        VerbHop,
    )

    tier_order   = ["xs", "s", "m"]
    _max = max_tier if max_tier in tier_order else tier_order[-1]
    active_tiers = [t for t in tier_order
                    if t in _DGM_TIERS
                    and tier_order.index(t) <= tier_order.index(_max)]
    results: list[BenchmarkResult] = []

    for tier in active_tiers:
        n     = _DGM_TIERS[tier]
        n_dim = max(n // 10, 10)

        def _setup_star(con: duckdb.DuckDBPyConnection) -> None:
            con.execute("""
                CREATE OR REPLACE TABLE o_fact (
                    id          INTEGER,
                    customer_id INTEGER,
                    product_id  INTEGER,
                    revenue     DOUBLE,
                    sale_year   INTEGER
                )
            """)
            con.execute("""
                CREATE OR REPLACE TABLE o_customer (
                    id      INTEGER,
                    segment VARCHAR,
                    region  VARCHAR
                )
            """)
            con.execute(f"""
                INSERT INTO o_fact
                SELECT i,
                       (i % {n_dim}) + 1,
                       (i % {max(n_dim // 5, 1)}) + 1,
                       (i % 100) * 10.0,
                       2020 + (i % 5)
                FROM range(1, {n + 1}) t(i)
            """)
            con.execute(f"""
                INSERT INTO o_customer
                SELECT i,
                       CASE (i % 2) WHEN 0 THEN 'retail' ELSE 'wholesale' END,
                       CASE (i % 3) WHEN 0 THEN 'US' WHEN 1 THEN 'EU' ELSE 'APAC' END
                FROM range(1, {n_dim + 1}) t(i)
            """)

        hop_c = VerbHop("f", "placed_by", "c",
                        table="o_customer", on="c.id = f.customer_id")

        # ── O-1: B1 filter ────────────────────────────────────────────────
        cid    = f"O-b1-filter-{tier}"
        result = BenchmarkResult(
            case_id=cid, group="O", profile="dgm-b1", tier=tier,
            processor="DGMQuery", sink="duckdb-memory",
            source="synthetic", phase="batch", n_rows=n, n_changed=0,
        )
        try:
            MemoryProbe.check_safe_to_run(label=cid)
            con = duckdb.connect()
            _setup_star(con)
            probe = MemoryProbe(temp_dir=temp_dir, label=cid)
            q = (DGMQuery()
                 .anchor("o_fact", "f")
                 .path_join(hop_c)
                 .where(ScalarPred(PropRef("c", "segment"), "=", "retail")))
            with probe:
                t0 = time.perf_counter()
                rows = q.execute(con)
                result.wall_s = time.perf_counter() - t0
            m = probe.report
            result.peak_rss_gb      = m.peak_rss_gb
            result.min_sys_avail_gb = m.min_sys_avail_gb
            result.total_spill_gb   = m.total_spill_gb
            result.safety_breach    = m.safety_breach
            result.rows_per_sec     = len(rows) / max(result.wall_s, 0.001)
            con.close()
        except RuntimeError as exc:
            result.ok = False; result.error = f"SKIPPED: {exc}"
        except Exception as exc:
            result.ok    = False
            result.error = (f"{type(exc).__name__}: {exc}\n"
                            + _traceback.format_exc()[-600:])
        results.append(result)

        # ── O-2: B1 ∘ B2 group+agg+having ────────────────────────────────
        cid    = f"O-b1b2-having-{tier}"
        result = BenchmarkResult(
            case_id=cid, group="O", profile="dgm-b1b2", tier=tier,
            processor="DGMQuery", sink="duckdb-memory",
            source="synthetic", phase="batch", n_rows=n, n_changed=0,
        )
        try:
            MemoryProbe.check_safe_to_run(label=cid)
            con = duckdb.connect()
            _setup_star(con)
            probe = MemoryProbe(temp_dir=temp_dir, label=cid)
            q = (DGMQuery()
                 .anchor("o_fact", "f")
                 .path_join(hop_c)
                 .group_by("c.id", "c.segment")
                 .agg(total_rev="SUM(f.revenue)", cnt="COUNT(*)")
                 .having(ScalarPred(AggRef("total_rev"), ">", 1000.0)))
            with probe:
                t0 = time.perf_counter()
                rows = q.execute(con)
                result.wall_s = time.perf_counter() - t0
            m = probe.report
            result.peak_rss_gb      = m.peak_rss_gb
            result.min_sys_avail_gb = m.min_sys_avail_gb
            result.total_spill_gb   = m.total_spill_gb
            result.safety_breach    = m.safety_breach
            result.rows_per_sec     = len(rows) / max(result.wall_s, 0.001)
            con.close()
        except RuntimeError as exc:
            result.ok = False; result.error = f"SKIPPED: {exc}"
        except Exception as exc:
            result.ok    = False
            result.error = (f"{type(exc).__name__}: {exc}\n"
                            + _traceback.format_exc()[-600:])
        results.append(result)

        # ── O-3: B1 ∘ B3 window+qualify ──────────────────────────────────
        cid    = f"O-b1b3-qualify-{tier}"
        result = BenchmarkResult(
            case_id=cid, group="O", profile="dgm-b1b3", tier=tier,
            processor="DGMQuery", sink="duckdb-memory",
            source="synthetic", phase="batch", n_rows=n, n_changed=0,
        )
        try:
            MemoryProbe.check_safe_to_run(label=cid)
            con = duckdb.connect()
            _setup_star(con)
            probe = MemoryProbe(temp_dir=temp_dir, label=cid)
            q = (DGMQuery()
                 .anchor("o_fact", "f")
                 .window(rn="ROW_NUMBER() OVER (PARTITION BY f.customer_id ORDER BY f.revenue DESC)")
                 .qualify(ScalarPred(WinRef("rn"), "=", 1)))
            with probe:
                t0 = time.perf_counter()
                rows = q.execute(con)
                result.wall_s = time.perf_counter() - t0
            m = probe.report
            result.peak_rss_gb      = m.peak_rss_gb
            result.min_sys_avail_gb = m.min_sys_avail_gb
            result.total_spill_gb   = m.total_spill_gb
            result.safety_breach    = m.safety_breach
            result.rows_per_sec     = len(rows) / max(result.wall_s, 0.001)
            con.close()
        except RuntimeError as exc:
            result.ok = False; result.error = f"SKIPPED: {exc}"
        except Exception as exc:
            result.ok    = False
            result.error = (f"{type(exc).__name__}: {exc}\n"
                            + _traceback.format_exc()[-600:])
        results.append(result)

        # ── O-4: B1 ∘ B2 ∘ B3 full pipeline ─────────────────────────────
        cid    = f"O-b1b2b3-full-{tier}"
        result = BenchmarkResult(
            case_id=cid, group="O", profile="dgm-full", tier=tier,
            processor="DGMQuery", sink="duckdb-memory",
            source="synthetic", phase="batch", n_rows=n, n_changed=0,
        )
        try:
            MemoryProbe.check_safe_to_run(label=cid)
            con = duckdb.connect()
            _setup_star(con)
            probe = MemoryProbe(temp_dir=temp_dir, label=cid)
            q = (DGMQuery()
                 .anchor("o_fact", "f")
                 .path_join(hop_c)
                 .where(AND(
                     ScalarPred(PropRef("c", "segment"), "=", "retail"),
                     ScalarPred(PropRef("f", "sale_year"), ">=", 2020),
                 ))
                 .group_by("c.id", "c.region")
                 .agg(total_rev="SUM(f.revenue)", cnt="COUNT(*)")
                 .having(ScalarPred(AggRef("total_rev"), ">", 500.0))
                 .window(rnk="RANK() OVER (ORDER BY SUM(f.revenue) DESC)")
                 .qualify(ScalarPred(WinRef("rnk"), "<=", 10)))
            with probe:
                t0 = time.perf_counter()
                rows = q.execute(con)
                result.wall_s = time.perf_counter() - t0
            m = probe.report
            result.peak_rss_gb      = m.peak_rss_gb
            result.min_sys_avail_gb = m.min_sys_avail_gb
            result.total_spill_gb   = m.total_spill_gb
            result.safety_breach    = m.safety_breach
            result.rows_per_sec     = n / max(result.wall_s, 0.001)
            result.inserted         = len(rows)
            con.close()
        except RuntimeError as exc:
            result.ok = False; result.error = f"SKIPPED: {exc}"
        except Exception as exc:
            result.ok    = False
            result.error = (f"{type(exc).__name__}: {exc}\n"
                            + _traceback.format_exc()[-600:])
        results.append(result)

    return results


# ═══════════════════════════════════════════════════════════════════════════
# ── Group P  BDD predicate compilation throughput ────────────────────────
# ═══════════════════════════════════════════════════════════════════════════

_BDD_TIERS: dict[str, int] = {"xs": 100, "s": 1_000, "m": 5_000}


def group_p_bdd_predicate(
    gen: BenchmarkDatasetGenerator,
    temp_dir: str,
    max_tier: str = "m",
    **_,
) -> list[BenchmarkResult]:
    """Group P — BDD predicate compilation throughput (compile, satisfiability, to_sql)."""
    from sqldim.core.query.dgm.bdd import BDDManager, DGMPredicateBDD
    from sqldim.core.query.dgm.preds import ScalarPred, AND, OR
    from sqldim.core.query.dgm.refs import PropRef

    tier_order   = ["xs", "s", "m"]
    _max = max_tier if max_tier in tier_order else tier_order[-1]
    active_tiers = [t for t in tier_order
                    if t in _BDD_TIERS
                    and tier_order.index(t) <= tier_order.index(_max)]
    results: list[BenchmarkResult] = []

    for tier in active_tiers:
        n = _BDD_TIERS[tier]

        # ── P-1: AND-chain compile ────────────────────────────────────────
        cid    = f"P-bdd-and-chain-{tier}"
        result = BenchmarkResult(
            case_id=cid, group="P", profile="bdd-compile", tier=tier,
            processor="BDDManager", sink="none",
            source="synthetic", phase="compile", n_rows=n, n_changed=0,
        )
        try:
            MemoryProbe.check_safe_to_run(label=cid)
            probe = MemoryProbe(temp_dir=temp_dir, label=cid)
            atoms = [ScalarPred(PropRef("t", f"c{i}"), "=", i) for i in range(10)]
            with probe:
                t0 = time.perf_counter()
                for _ in range(n):
                    mgr = BDDManager()
                    bdd = DGMPredicateBDD(mgr)
                    pred = atoms[0]
                    for a in atoms[1:]:
                        pred = AND(pred, a)
                    bdd.compile(pred)
                result.wall_s = time.perf_counter() - t0
            m = probe.report
            result.peak_rss_gb      = m.peak_rss_gb
            result.min_sys_avail_gb = m.min_sys_avail_gb
            result.rows_per_sec     = n / max(result.wall_s, 0.001)
            result.inserted         = n
        except RuntimeError as exc:
            result.ok = False; result.error = f"SKIPPED: {exc}"
        except Exception as exc:
            result.ok    = False
            result.error = (f"{type(exc).__name__}: {exc}\n"
                            + _traceback.format_exc()[-600:])
        results.append(result)

        # ── P-2: OR-chain satisfiability ─────────────────────────────────
        cid    = f"P-bdd-or-satisfiable-{tier}"
        result = BenchmarkResult(
            case_id=cid, group="P", profile="bdd-satisfiable", tier=tier,
            processor="BDDManager", sink="none",
            source="synthetic", phase="query", n_rows=n, n_changed=0,
        )
        try:
            MemoryProbe.check_safe_to_run(label=cid)
            mgr = BDDManager()
            bdd = DGMPredicateBDD(mgr)
            atoms = [ScalarPred(PropRef("t", f"d{i}"), ">", i * 10) for i in range(5)]
            pred = atoms[0]
            for a in atoms[1:]:
                pred = OR(pred, a)
            uid = bdd.compile(pred)
            probe = MemoryProbe(temp_dir=temp_dir, label=cid)
            with probe:
                t0 = time.perf_counter()
                for _ in range(n):
                    bdd.is_satisfiable(uid)
                result.wall_s = time.perf_counter() - t0
            m = probe.report
            result.peak_rss_gb      = m.peak_rss_gb
            result.min_sys_avail_gb = m.min_sys_avail_gb
            result.rows_per_sec     = n / max(result.wall_s, 0.001)
            result.inserted         = n
        except RuntimeError as exc:
            result.ok = False; result.error = f"SKIPPED: {exc}"
        except Exception as exc:
            result.ok    = False
            result.error = (f"{type(exc).__name__}: {exc}\n"
                            + _traceback.format_exc()[-600:])
        results.append(result)

        # ── P-3: to_sql round-trip ────────────────────────────────────────
        cid    = f"P-bdd-to-sql-{tier}"
        result = BenchmarkResult(
            case_id=cid, group="P", profile="bdd-to-sql", tier=tier,
            processor="DGMPredicateBDD", sink="none",
            source="synthetic", phase="export", n_rows=n, n_changed=0,
        )
        try:
            MemoryProbe.check_safe_to_run(label=cid)
            mgr = BDDManager()
            bdd = DGMPredicateBDD(mgr)
            atoms = [ScalarPred(PropRef("t", f"e{i}"), "=", i) for i in range(4)]
            pred = AND(atoms[0], OR(atoms[1], AND(atoms[2], atoms[3])))
            uid = bdd.compile(pred)
            probe = MemoryProbe(temp_dir=temp_dir, label=cid)
            with probe:
                t0 = time.perf_counter()
                for _ in range(n):
                    bdd.to_sql(uid)
                result.wall_s = time.perf_counter() - t0
            m = probe.report
            result.peak_rss_gb      = m.peak_rss_gb
            result.min_sys_avail_gb = m.min_sys_avail_gb
            result.rows_per_sec     = n / max(result.wall_s, 0.001)
            result.inserted         = n
        except RuntimeError as exc:
            result.ok = False; result.error = f"SKIPPED: {exc}"
        except Exception as exc:
            result.ok    = False
            result.error = (f"{type(exc).__name__}: {exc}\n"
                            + _traceback.format_exc()[-600:])
        results.append(result)

    return results


# ═══════════════════════════════════════════════════════════════════════════
# ── Group Q  DGMRecommender annotation + trail rules throughput ───────────
# ═══════════════════════════════════════════════════════════════════════════

def group_q_recommender(
    gen: BenchmarkDatasetGenerator,
    temp_dir: str,
    max_tier: str = "m",
    **_,
) -> list[BenchmarkResult]:
    """Group Q — DGMRecommender annotation rule + trail rule throughput."""
    from sqldim.core.query.dgm.annotations import (
        AnnotationSigma,
        Grain, GrainKind,
        SCDType, SCDKind,
        Conformed,
        BridgeSemantics, BridgeSemanticsKind,
        Hierarchy,
        FactlessFact,
        DerivedFact,
        WeightConstraint, WeightConstraintKind,
    )
    from sqldim.core.query.dgm.recommender import DGMRecommender

    tier_map   = {"xs": 200, "s": 2_000, "m": 10_000}
    tier_order = ["xs", "s", "m"]
    _max = max_tier if max_tier in tier_order else tier_order[-1]
    active_tiers = [t for t in tier_order
                    if t in tier_map
                    and tier_order.index(t) <= tier_order.index(_max)]
    results: list[BenchmarkResult] = []

    sigma = AnnotationSigma(annotations=[
        Grain(fact="sale", grain=GrainKind.PERIOD),
        SCDType(dim="customer", scd=SCDKind.SCD2),
        Conformed(dim="customer", fact_types=frozenset({"Sale"})),
        BridgeSemantics(bridge="link", sem=BridgeSemanticsKind.CAUSAL),
        Hierarchy(root="category", depth=3),
        FactlessFact(fact="attendance"),
        DerivedFact(fact="margin", sources=["revenue", "cost"], expr="revenue - cost"),
        WeightConstraint(bridge="alloc", constraint=WeightConstraintKind.ALLOCATIVE),
    ])
    rec = DGMRecommender(sigma)

    for tier in active_tiers:
        n = tier_map[tier]

        # ── Q-1: run_annotation_rules throughput ─────────────────────────
        cid    = f"Q-recommender-annotation-{tier}"
        result = BenchmarkResult(
            case_id=cid, group="Q", profile="recommender-annotation", tier=tier,
            processor="DGMRecommender", sink="none",
            source="synthetic", phase="compute", n_rows=n, n_changed=0,
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
            result.peak_rss_gb      = m.peak_rss_gb
            result.min_sys_avail_gb = m.min_sys_avail_gb
            result.rows_per_sec     = n / max(result.wall_s, 0.001)
            result.inserted         = n
        except RuntimeError as exc:
            result.ok = False; result.error = f"SKIPPED: {exc}"
        except Exception as exc:
            result.ok    = False
            result.error = (f"{type(exc).__name__}: {exc}\n"
                            + _traceback.format_exc()[-600:])
        results.append(result)

        # ── Q-2: run_trail_rules throughput ───────────────────────────────
        cid    = f"Q-recommender-trail-{tier}"
        result = BenchmarkResult(
            case_id=cid, group="Q", profile="recommender-trail", tier=tier,
            processor="DGMRecommender", sink="none",
            source="synthetic", phase="compute", n_rows=n, n_changed=0,
        )
        try:
            MemoryProbe.check_safe_to_run(label=cid)
            probe = MemoryProbe(temp_dir=temp_dir, label=cid)
            with probe:
                t0 = time.perf_counter()
                for _ in range(n):
                    rec.run_trail_rules("customer", 6, 0.8, 0.75)
                    rec.run_trail_rules("product",  2, 0.3, 0.2)
                result.wall_s = time.perf_counter() - t0
            m = probe.report
            result.peak_rss_gb      = m.peak_rss_gb
            result.min_sys_avail_gb = m.min_sys_avail_gb
            result.rows_per_sec     = n * 2 / max(result.wall_s, 0.001)
            result.inserted         = n * 2
        except RuntimeError as exc:
            result.ok = False; result.error = f"SKIPPED: {exc}"
        except Exception as exc:
            result.ok    = False
            result.error = (f"{type(exc).__name__}: {exc}\n"
                            + _traceback.format_exc()[-600:])
        results.append(result)

    return results


# ═══════════════════════════════════════════════════════════════════════════
# ── Group R  DGMPlanner rule cycle throughput ─────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════

def group_r_planner(
    gen: BenchmarkDatasetGenerator,
    temp_dir: str,
    max_tier: str = "m",
    **_,
) -> list[BenchmarkResult]:
    """Group R — DGMPlanner rule cycles (1a, 9, build_plan)."""
    from sqldim.core.query.dgm.annotations import AnnotationSigma
    from sqldim.core.query.dgm.planner import DGMPlanner, QueryTarget, SinkTarget
    from sqldim.core.query.dgm.graph import GraphStatistics

    tier_map   = {"xs": 500, "s": 5_000, "m": 20_000}
    tier_order = ["xs", "s", "m"]
    _max = max_tier if max_tier in tier_order else tier_order[-1]
    active_tiers = [t for t in tier_order
                    if t in tier_map
                    and tier_order.index(t) <= tier_order.index(_max)]
    results: list[BenchmarkResult] = []

    sigma   = AnnotationSigma(annotations=[])
    stats   = GraphStatistics(node_count=100, edge_count=500)
    planner = DGMPlanner(
        cost_model=None,
        statistics=stats,
        annotations=sigma,
        rules=None,
        query_target=QueryTarget.SQL_DUCKDB,
        sink_target=SinkTarget.PARQUET,
    )

    for tier in active_tiers:
        n = tier_map[tier]

        # ── R-1: apply_rule_1a throughput ─────────────────────────────────
        cid    = f"R-planner-rule1a-{tier}"
        result = BenchmarkResult(
            case_id=cid, group="R", profile="planner-rule1a", tier=tier,
            processor="DGMPlanner", sink="none",
            source="synthetic", phase="plan", n_rows=n, n_changed=0,
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
            result.peak_rss_gb      = m.peak_rss_gb
            result.min_sys_avail_gb = m.min_sys_avail_gb
            result.rows_per_sec     = n / max(result.wall_s, 0.001)
            result.inserted         = n
        except RuntimeError as exc:
            result.ok = False; result.error = f"SKIPPED: {exc}"
        except Exception as exc:
            result.ok    = False
            result.error = (f"{type(exc).__name__}: {exc}\n"
                            + _traceback.format_exc()[-600:])
        results.append(result)

        # ── R-2: apply_rule_9 (cone containment) throughput ───────────────
        cid    = f"R-planner-rule9-{tier}"
        result = BenchmarkResult(
            case_id=cid, group="R", profile="planner-rule9", tier=tier,
            processor="DGMPlanner", sink="none",
            source="synthetic", phase="plan", n_rows=n, n_changed=0,
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
            result.peak_rss_gb      = m.peak_rss_gb
            result.min_sys_avail_gb = m.min_sys_avail_gb
            result.rows_per_sec     = n / max(result.wall_s, 0.001)
            result.inserted         = n
        except RuntimeError as exc:
            result.ok = False; result.error = f"SKIPPED: {exc}"
        except Exception as exc:
            result.ok    = False
            result.error = (f"{type(exc).__name__}: {exc}\n"
                            + _traceback.format_exc()[-600:])
        results.append(result)

        # ── R-3: build_plan throughput ─────────────────────────────────────
        cid    = f"R-planner-build-plan-{tier}"
        result = BenchmarkResult(
            case_id=cid, group="R", profile="planner-build-plan", tier=tier,
            processor="DGMPlanner", sink="none",
            source="synthetic", phase="plan", n_rows=n, n_changed=0,
        )
        try:
            MemoryProbe.check_safe_to_run(label=cid)
            query_text = "SELECT * FROM fact WHERE year = 2024"
            probe = MemoryProbe(temp_dir=temp_dir, label=cid)
            with probe:
                t0 = time.perf_counter()
                for _ in range(n):
                    planner.build_plan(query_text)
                result.wall_s = time.perf_counter() - t0
            m = probe.report
            result.peak_rss_gb      = m.peak_rss_gb
            result.min_sys_avail_gb = m.min_sys_avail_gb
            result.rows_per_sec     = n / max(result.wall_s, 0.001)
            result.inserted         = n
        except RuntimeError as exc:
            result.ok = False; result.error = f"SKIPPED: {exc}"
        except Exception as exc:
            result.ok    = False
            result.error = (f"{type(exc).__name__}: {exc}\n"
                            + _traceback.format_exc()[-600:])
        results.append(result)

    return results


# ═══════════════════════════════════════════════════════════════════════════
# ── Group S  DGM exporter throughput ─────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════

def group_s_exporter(
    gen: BenchmarkDatasetGenerator,
    temp_dir: str,
    max_tier: str = "m",
    **_,
) -> list[BenchmarkResult]:
    """Group S — DGM multi-target exporter throughput (JSON / YAML x simple / complex)."""
    from sqldim.core.query.dgm.planner import QueryTarget, SinkTarget, ExportPlan
    from sqldim.core.query.dgm.exporters import DGMJSONExporter, DGMYAMLExporter

    tier_map   = {"xs": 500, "s": 5_000, "m": 20_000}
    tier_order = ["xs", "s", "m"]
    _max = max_tier if max_tier in tier_order else tier_order[-1]
    active_tiers = [t for t in tier_order
                    if t in tier_map
                    and tier_order.index(t) <= tier_order.index(_max)]
    results: list[BenchmarkResult] = []

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

    json_exporter = DGMJSONExporter()
    yaml_exporter = DGMYAMLExporter()

    exporters = [
        ("json", "simple",  json_exporter, simple_plan),
        ("json", "complex", json_exporter, complex_plan),
        ("yaml", "simple",  yaml_exporter, simple_plan),
        ("yaml", "complex", yaml_exporter, complex_plan),
    ]

    for tier in active_tiers:
        n = tier_map[tier]
        for fmt, complexity, exporter, plan in exporters:
            cid    = f"S-exporter-{fmt}-{complexity}-{tier}"
            result = BenchmarkResult(
                case_id=cid, group="S", profile=f"exporter-{fmt}-{complexity}", tier=tier,
                processor=type(exporter).__name__, sink="none",
                source="synthetic", phase="export", n_rows=n, n_changed=0,
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
                result.peak_rss_gb      = m.peak_rss_gb
                result.min_sys_avail_gb = m.min_sys_avail_gb
                result.rows_per_sec     = n / max(result.wall_s, 0.001)
                result.inserted         = n
            except RuntimeError as exc:
                result.ok = False; result.error = f"SKIPPED: {exc}"
            except Exception as exc:
                result.ok    = False
                result.error = (f"{type(exc).__name__}: {exc}\n"
                                + _traceback.format_exc()[-600:])
            results.append(result)

    return results

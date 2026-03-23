"""DGM query benchmarks (groups O–P): query patterns, aggregations."""
from __future__ import annotations

import os
import time
import traceback as _traceback

import duckdb
from sqlalchemy.pool import StaticPool

from sqldim.application.benchmarks._dataset import (
    BenchmarkDatasetGenerator,
    DatasetArtifact,
    SCALE_TIERS,
)
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


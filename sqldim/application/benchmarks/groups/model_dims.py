"""Model dimension benchmarks (groups J–K): date/time dims, geo dims."""
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
    _select_tiers,
)
from sqldim.application.benchmarks.memory_probe import MemoryProbe
from sqldim.application.benchmarks.scan_probe import DuckDBObjectTracker

# ═══════════════════════════════════════════════════════════════════════════
# ── Group J  Prebuilt dimension generation  (Date · Time) ────────────────
# ═══════════════════════════════════════════════════════════════════════════

_DATE_CASES: list[tuple[str, str, str]] = [
    ("J-date-1y",  "2024-01-01", "2024-12-31"),   #   366 rows
    ("J-date-5y",  "2020-01-01", "2024-12-31"),   # ~1 827 rows
    ("J-date-20y", "2005-01-01", "2024-12-31"),   # ~7 305 rows
    ("J-date-50y", "1975-01-01", "2024-12-31"),   # ~18 263 rows
]

def _j_date_case(cid: str, start: str, end: str, temp_dir: str) -> BenchmarkResult:
    from sqlalchemy.pool import StaticPool
    from sqlmodel import Session, create_engine, SQLModel
    from sqldim.core.kimball.dimensions.date import DateDimension
    result = BenchmarkResult(
        case_id=cid, group="J", profile="date-calendar", tier="n/a",
        processor="DateDimension.generate", sink="SQLite",
        source="synthetic", phase="generate", n_rows=0, n_changed=0,
    )
    try:
        MemoryProbe.check_safe_to_run(label=cid)
        engine = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        SQLModel.metadata.create_all(engine)
        with Session(engine) as session:
            probe = MemoryProbe(temp_dir=temp_dir, label=cid)
            with probe:
                t0   = time.perf_counter()
                rows = DateDimension.generate(start, end, session)
                session.commit()
                result.wall_s = time.perf_counter() - t0
            n = len(rows)
            m = probe.report
        engine.dispose()
        result.n_rows           = n
        result.peak_rss_gb      = m.peak_rss_gb
        result.min_sys_avail_gb = m.min_sys_avail_gb
        result.total_spill_gb   = m.total_spill_gb
        result.safety_breach    = m.safety_breach
        result.breach_detail    = m.breach_detail
        result.rows_per_sec     = n / max(result.wall_s, 0.001)
    except RuntimeError as exc:
        result.ok = False; result.error = f"SKIPPED: {exc}"
    except Exception as exc:
        result.ok    = False
        result.error = (f"{type(exc).__name__}: {exc}\n" + _traceback.format_exc()[-600:])
    return result


def _j_time_case(temp_dir: str) -> BenchmarkResult:
    from sqlalchemy.pool import StaticPool
    from sqlmodel import Session, create_engine, SQLModel
    from sqldim.core.kimball.dimensions.time import TimeDimension
    cid    = "J-time-1440"
    result = BenchmarkResult(
        case_id=cid, group="J", profile="time-minute-grain", tier="n/a",
        processor="TimeDimension.generate", sink="SQLite",
        source="synthetic", phase="generate", n_rows=1440, n_changed=0,
    )
    try:
        MemoryProbe.check_safe_to_run(label=cid)
        engine = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        SQLModel.metadata.create_all(engine)
        with Session(engine) as session:
            probe = MemoryProbe(temp_dir=temp_dir, label=cid)
            with probe:
                t0   = time.perf_counter()
                rows = TimeDimension.generate(session)
                session.commit()
                result.wall_s = time.perf_counter() - t0
            n = len(rows)
            m = probe.report
        engine.dispose()
        result.n_rows           = n
        result.peak_rss_gb      = m.peak_rss_gb
        result.min_sys_avail_gb = m.min_sys_avail_gb
        result.rows_per_sec     = n / max(result.wall_s, 0.001)
    except RuntimeError as exc:
        result.ok = False; result.error = f"SKIPPED: {exc}"
    except Exception as exc:
        result.ok    = False
        result.error = (f"{type(exc).__name__}: {exc}\n" + _traceback.format_exc()[-600:])
    return result


def group_j_dim_generation(
    gen: BenchmarkDatasetGenerator,
    temp_dir: str,
    max_tier: str = "m",
    **_,
) -> list[BenchmarkResult]:
    """Group J — DateDimension.generate() and TimeDimension.generate() throughput."""
    results: list[BenchmarkResult] = []
    for cid, start, end in _DATE_CASES:
        results.append(_j_date_case(cid, start, end, temp_dir))
    results.append(_j_time_case(temp_dir))
    return results


# ═══════════════════════════════════════════════════════════════════════════
# ── Group K  Graph traversal + query builder ─────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════

_GRAPH_TIERS: dict[str, int] = {"xs": 1_000, "s": 10_000, "m": 100_000}


def _k_graph_case(tier: str, n: int, temp_dir: str) -> BenchmarkResult:
    from sqldim.core.graph.traversal import DuckDBTraversalEngine
    cid    = f"K-graph-{tier}"
    result = BenchmarkResult(
        case_id=cid, group="K", profile="edge-graph", tier=tier,
        processor="DuckDBTraversalEngine", sink="DuckDB",
        source="synthetic", phase="batch", n_rows=n, n_changed=0,
    )
    try:
        MemoryProbe.check_safe_to_run(label=cid)
        con = duckdb.connect()
        con.execute("""
            CREATE TABLE bench_edge (
                id INTEGER, subject_id INTEGER,
                object_id INTEGER, weight DOUBLE
            )
        """)
        con.execute(f"""
            INSERT INTO bench_edge
            SELECT i, 1, i + 1, 1.0
            FROM range(1, {n + 1}) t(i)
        """)

        class _BenchEdge:
            __tablename__  = "bench_edge"
            __edge_type__  = "bench"
            __directed__   = True

        engine = DuckDBTraversalEngine(con)
        probe  = MemoryProbe(temp_dir=temp_dir, label=cid)
        with probe:
            t0 = time.perf_counter()
            neighbors = engine.neighbors(_BenchEdge, 1, direction="out")
            engine.degree(_BenchEdge, 1, direction="out")
            engine.aggregate(_BenchEdge, 1, "weight", "sum", direction="out")
            result.wall_s = time.perf_counter() - t0
        m = probe.report
        result.peak_rss_gb      = m.peak_rss_gb
        result.min_sys_avail_gb = m.min_sys_avail_gb
        result.total_spill_gb   = m.total_spill_gb
        result.safety_breach    = m.safety_breach
        result.breach_detail    = m.breach_detail
        result.rows_per_sec     = len(neighbors) / max(result.wall_s, 0.001)
        con.close()
    except RuntimeError as exc:
        result.ok = False; result.error = f"SKIPPED: {exc}"
    except Exception as exc:
        result.ok    = False
        result.error = (f"{type(exc).__name__}: {exc}\n" + _traceback.format_exc()[-600:])
    return result


def _k_query_case(tier: str, n: int, temp_dir: str) -> BenchmarkResult:
    cid   = f"K-query-{tier}"
    n_dim = max(n // 10, 10)
    result = BenchmarkResult(
        case_id=cid, group="K", profile="fact-dim-join", tier=tier,
        processor="DimensionalQuery", sink="DuckDB",
        source="synthetic", phase="batch", n_rows=n, n_changed=0,
    )
    try:
        MemoryProbe.check_safe_to_run(label=cid)
        con   = duckdb.connect()
        con.execute("""
            CREATE TABLE bench_fact (
                id INTEGER, product_id INTEGER, revenue DOUBLE
            )
        """)
        con.execute("""
            CREATE TABLE bench_dim (
                id INTEGER, category VARCHAR,
                is_current BOOLEAN,
                valid_from DATE, valid_to DATE
            )
        """)
        con.execute(f"""
            INSERT INTO bench_fact
            SELECT i,
                   (i % {n_dim}) + 1 AS product_id,
                   (i % 100) * 1.5   AS revenue
            FROM range(1, {n + 1}) t(i)
        """)
        con.execute(f"""
            INSERT INTO bench_dim
            SELECT i,
                   'cat_' || (i % 5)::VARCHAR AS category,
                   TRUE, DATE '2020-01-01', NULL
            FROM range(1, {n_dim + 1}) t(i)
        """)
        probe = MemoryProbe(temp_dir=temp_dir, label=cid)
        with probe:
            t0   = time.perf_counter()
            con.execute("""
                SELECT d.category, SUM(f.revenue) AS total_revenue
                FROM bench_fact f
                JOIN bench_dim d ON f.product_id = d.id
                WHERE d.is_current = TRUE
                GROUP BY d.category
                ORDER BY d.category
            """).fetchall()
            result.wall_s = time.perf_counter() - t0
        m = probe.report
        result.peak_rss_gb      = m.peak_rss_gb
        result.min_sys_avail_gb = m.min_sys_avail_gb
        result.total_spill_gb   = m.total_spill_gb
        result.safety_breach    = m.safety_breach
        result.breach_detail    = m.breach_detail
        result.rows_per_sec     = n / max(result.wall_s, 0.001)
        con.close()
    except RuntimeError as exc:
        result.ok = False; result.error = f"SKIPPED: {exc}"
    except Exception as exc:
        result.ok    = False
        result.error = (f"{type(exc).__name__}: {exc}\n" + _traceback.format_exc()[-600:])
    return result


def group_k_graph_query(
    gen: BenchmarkDatasetGenerator,
    temp_dir: str,
    max_tier: str = "m",
    **_,
) -> list[BenchmarkResult]:
    """Group K — DuckDBTraversalEngine and DuckDBDimensionalQuery throughput."""
    tier_order   = ["xs", "s", "m"]
    active_tiers = _select_tiers(tier_order, _GRAPH_TIERS, max_tier)
    results: list[BenchmarkResult] = []
    for tier in active_tiers:
        n = _GRAPH_TIERS[tier]
        results.append(_k_graph_case(tier, n, temp_dir))
        results.append(_k_query_case(tier, n, temp_dir))
    return results



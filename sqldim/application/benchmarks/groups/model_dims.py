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
)
from sqldim.application.benchmarks.memory_probe import MemoryProbe
from sqldim.application.benchmarks.scan_probe import DuckDBObjectTracker

def group_j_dim_generation(
    gen: BenchmarkDatasetGenerator,
    temp_dir: str,
    max_tier: str = "m",
    **_,
) -> list[BenchmarkResult]:
    """Group J — DateDimension.generate() and TimeDimension.generate() throughput."""
    from sqlalchemy.pool import StaticPool
    from sqlmodel import Session, create_engine, SQLModel
    from sqldim.core.kimball.dimensions.date import DateDimension
    from sqldim.core.kimball.dimensions.time import TimeDimension

    results: list[BenchmarkResult] = []

    # ── Date dimension ──────────────────────────────────────────────────────
    for cid, start, end in _DATE_CASES:
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
            result.error = (f"{type(exc).__name__}: {exc}\n"
                            + _traceback.format_exc()[-600:])
        results.append(result)

    # ── Time dimension (always 1440 minute-level rows) ─────────────────────
    cid = "J-time-1440"
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
        result.n_rows       = n
        result.peak_rss_gb  = m.peak_rss_gb
        result.min_sys_avail_gb = m.min_sys_avail_gb
        result.rows_per_sec = n / max(result.wall_s, 0.001)
    except RuntimeError as exc:
        result.ok = False; result.error = f"SKIPPED: {exc}"
    except Exception as exc:
        result.ok    = False
        result.error = (f"{type(exc).__name__}: {exc}\n"
                        + _traceback.format_exc()[-600:])
    results.append(result)

    return results


# ═══════════════════════════════════════════════════════════════════════════
# ── Group K  Graph traversal + query builder ─────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════

_GRAPH_TIERS: dict[str, int] = {"xs": 1_000, "s": 10_000, "m": 100_000}


def group_k_graph_query(
    gen: BenchmarkDatasetGenerator,
    temp_dir: str,
    max_tier: str = "m",
    **_,
) -> list[BenchmarkResult]:
    """Group K — DuckDBTraversalEngine and DuckDBDimensionalQuery throughput."""
    from sqldim.core.graph.traversal import DuckDBTraversalEngine
    from sqldim.core.query.builder import DuckDBDimensionalQuery

    tier_order   = ["xs", "s", "m"]
    _max = max_tier if max_tier in tier_order else tier_order[-1]
    active_tiers = [t for t in tier_order
                    if t in _GRAPH_TIERS
                    and tier_order.index(t) <= tier_order.index(_max)]
    results: list[BenchmarkResult] = []

    for tier in active_tiers:
        n = _GRAPH_TIERS[tier]

        # ── Graph: neighbors() + aggregate() at scale ─────────────────────
        cid = f"K-graph-{tier}"
        result = BenchmarkResult(
            case_id=cid, group="K", profile="edge-graph", tier=tier,
            processor="DuckDBTraversalEngine", sink="DuckDB",
            source="synthetic", phase="batch", n_rows=n, n_changed=0,
        )
        try:
            MemoryProbe.check_safe_to_run(label=cid)
            con = duckdb.connect()
            # Directed edge table: subject→object fan-out from node 1
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
                # neighbors() — returns all outgoing from node 1
                neighbors = engine.neighbors(_BenchEdge, 1, direction="out")
                # degree() — edge count for node 1
                engine.degree(_BenchEdge, 1, direction="out")
                # aggregate() — sum of weights
                engine.aggregate(_BenchEdge, 1, "weight", "sum",
                                         direction="out")
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
            result.error = (f"{type(exc).__name__}: {exc}\n"
                            + _traceback.format_exc()[-600:])
        results.append(result)

        # ── Query: join_dim + by + sum at scale ────────────────────────────
        cid = f"K-query-{tier}"
        result = BenchmarkResult(
            case_id=cid, group="K", profile="fact-dim-join", tier=tier,
            processor="DuckDBDimensionalQuery", sink="DuckDB",
            source="synthetic", phase="batch", n_rows=n, n_changed=0,
        )
        try:
            MemoryProbe.check_safe_to_run(label=cid)
            n_dim = max(n // 10, 10)   # 1/10th as many dimension rows
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
                (
                    DuckDBDimensionalQuery("bench_fact")
                    .join_dim("bench_dim", "product_id")
                    .by("d_bench_dim.category")
                    .sum("f.revenue")
                    .execute(con)
                )
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
            result.error = (f"{type(exc).__name__}: {exc}\n"
                            + _traceback.format_exc()[-600:])
        results.append(result)

    return results


# ═══════════════════════════════════════════════════════════════════════════
# ── Group L  Narwhals backfill ───────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════

_BACKFILL_TIERS: dict[str, dict[str, int]] = {
    "xs": {"n_players": 100,    "n_seasons": 10},   # 1 000 rows
    "s":  {"n_players": 1_000,  "n_seasons": 10},   # 10 000 rows
    "m":  {"n_players": 10_000, "n_seasons": 10},   # 100 000 rows
}



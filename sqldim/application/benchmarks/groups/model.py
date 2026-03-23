"""
benchmarks/groups/model.py
==========================
Dimensional model infrastructure benchmarks (groups J–N).

Covers: prebuilt dimension generation, graph traversal,
Narwhals backfill, loader + medallion throughput, drift observatory.
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


def group_l_narwhals_backfill(
    gen: BenchmarkDatasetGenerator,
    temp_dir: str,
    max_tier: str = "m",
    **_,
) -> list[BenchmarkResult]:
    """Group L — backfill_scd2_narwhals() throughput with Polars frames."""
    import polars as pl
    from sqldim.core.kimball.dimensions.scd.processors.backfill import (
        backfill_scd2_narwhals,
    )

    tier_order   = ["xs", "s", "m"]
    _max = max_tier if max_tier in tier_order else tier_order[-1]
    active_tiers = [t for t in tier_order
                    if t in _BACKFILL_TIERS
                    and tier_order.index(t) <= tier_order.index(_max)]
    results: list[BenchmarkResult] = []

    for tier in active_tiers:
        cfg      = _BACKFILL_TIERS[tier]
        n_player = cfg["n_players"]
        n_season = cfg["n_seasons"]
        n_rows   = n_player * n_season
        cid      = f"L-backfill-{tier}"
        result   = BenchmarkResult(
            case_id=cid, group="L", profile="scd2-snapshot", tier=tier,
            processor="backfill_scd2_narwhals", sink="Polars",
            source="synthetic", phase="batch", n_rows=n_rows, n_changed=0,
        )
        try:
            MemoryProbe.check_safe_to_run(label=cid)
            # Build a flat snapshot: player_id × season, scoring_class changes
            # every 3rd season so ~1/3 of rows generate new SCD2 versions.
            player_ids    = [f"p_{p}" for p in range(n_player)
                             for _ in range(n_season)]
            seasons       = [s for _ in range(n_player)
                             for s in range(2000, 2000 + n_season)]
            scoring_class = [
                "Elite" if (p * n_season + (s - 2000)) % 3 == 0 else "Star"
                for p in range(n_player)
                for s in range(2000, 2000 + n_season)
            ]
            frame = pl.DataFrame({
                "player_id":     player_ids,
                "season":        seasons,
                "scoring_class": scoring_class,
            })
            probe = MemoryProbe(temp_dir=temp_dir, label=cid)
            with probe:
                t0  = time.perf_counter()
                cnt = backfill_scd2_narwhals(
                    frame,
                    partition_by="player_id",
                    order_by="season",
                    track_columns=["scoring_class"],
                    dry_run=False,
                )
                result.wall_s = time.perf_counter() - t0
            m = probe.report
            result.n_changed        = int(cnt) if isinstance(cnt, int) else 0
            result.peak_rss_gb      = m.peak_rss_gb
            result.min_sys_avail_gb = m.min_sys_avail_gb
            result.total_spill_gb   = m.total_spill_gb
            result.safety_breach    = m.safety_breach
            result.breach_detail    = m.breach_detail
            result.rows_per_sec     = n_rows / max(result.wall_s, 0.001)
        except RuntimeError as exc:
            result.ok = False; result.error = f"SKIPPED: {exc}"
        except Exception as exc:
            result.ok    = False
            result.error = (f"{type(exc).__name__}: {exc}\n"
                            + _traceback.format_exc()[-600:])
        results.append(result)

    return results


# ═══════════════════════════════════════════════════════════════════════════
# ── Group M  ORM loaders · Medallion ────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════

_LOADER_TIERS: dict[str, int] = {"xs": 500, "s": 5_000}


def group_m_loaders_medallion(
    gen: BenchmarkDatasetGenerator,
    temp_dir: str,
    max_tier: str = "s",
    **_,
) -> list[BenchmarkResult]:
    """Group M — ORM loader throughput and Medallion registry compute."""
    import asyncio
    from sqlalchemy.pool import StaticPool
    from sqlmodel import Session, create_engine, SQLModel
    from sqldim import DimensionModel, FactModel, SCD2Mixin, Field as SqdimField
    from sqldim.medallion import MedallionRegistry, Layer
    from sqldim.medallion.build_order import SilverBuildOrder

    # ── Local SQLModel fixtures (defined once per process) ──────────────────
    class _MBenchDim(DimensionModel, SCD2Mixin, table=True):
        __natural_key__ = ["sku"]
        __tablename__   = "m_bench_dim"
        id:    int   = SqdimField(default=None, primary_key=True, surrogate_key=True)
        sku:   str   = SqdimField(default="")
        price: float = SqdimField(default=0.0)

    class _MBenchFact(FactModel, table=True):
        __tablename__ = "m_bench_fact"
        id:         int   = SqdimField(default=None, primary_key=True)
        product_id: int   = SqdimField(default=0,
                                       foreign_key="m_bench_dim.id",
                                       dimension=_MBenchDim)
        quantity:   int   = SqdimField(default=0)

    tier_order   = ["xs", "s"]
    effective_max = max_tier if max_tier in tier_order else tier_order[-1]
    active_tiers = [t for t in tier_order
                    if t in _LOADER_TIERS
                    and tier_order.index(t) <= tier_order.index(effective_max)]
    results: list[BenchmarkResult] = []

    # ── DimensionalLoader bulk-insert throughput ───────────────────────────
    for tier in active_tiers:
        n   = _LOADER_TIERS[tier]
        cid = f"M-dimloader-{tier}"
        result = BenchmarkResult(
            case_id=cid, group="M", profile="dimensional-loader", tier=tier,
            processor="DimensionalLoader", sink="SQLite",
            source="synthetic", phase="batch", n_rows=n, n_changed=0,
        )
        try:
            from sqldim.core.loaders.dimensional import DimensionalLoader
            MemoryProbe.check_safe_to_run(label=cid)
            engine = create_engine(
                "sqlite:///:memory:",
                connect_args={"check_same_thread": False},
                poolclass=StaticPool,
            )
            SQLModel.metadata.create_all(
                engine,
                tables=[_MBenchDim.__table__, _MBenchFact.__table__],
            )
            dims  = [{"sku": f"SKU-{i}", "price": float(i)} for i in range(n)]
            facts = [{"product_id": f"SKU-{i % n}", "quantity": i}
                     for i in range(n)]
            with Session(engine) as session:
                loader = DimensionalLoader(session, models=[_MBenchDim, _MBenchFact])
                loader.register(_MBenchDim, dims)
                loader.register(
                    _MBenchFact, facts,
                    key_map={"product_id": (_MBenchDim, "sku")},
                )
                probe = MemoryProbe(temp_dir=temp_dir, label=cid)
                with probe:
                    t0 = time.perf_counter()
                    asyncio.run(loader.run())
                    result.wall_s = time.perf_counter() - t0
                m = probe.report
            engine.dispose()
            result.peak_rss_gb      = m.peak_rss_gb
            result.min_sys_avail_gb = m.min_sys_avail_gb
            result.total_spill_gb   = m.total_spill_gb
            result.safety_breach    = m.safety_breach
            result.breach_detail    = m.breach_detail
            result.rows_per_sec     = (n * 2) / max(result.wall_s, 0.001)
        except RuntimeError as exc:
            result.ok = False; result.error = f"SKIPPED: {exc}"
        except Exception as exc:
            result.ok    = False
            result.error = (f"{type(exc).__name__}: {exc}\n"
                            + _traceback.format_exc()[-600:])
        results.append(result)

    # ── Medallion registry + SilverBuildOrder compute ─────────────────────
    cid    = "M-medallion-registry"
    result = BenchmarkResult(
        case_id=cid, group="M", profile="medallion-registry", tier="n/a",
        processor="MedallionRegistry", sink="memory",
        source="synthetic", phase="compute", n_rows=0, n_changed=0,
    )
    try:
        MemoryProbe.check_safe_to_run(label=cid)
        n_datasets = 500
        probe  = MemoryProbe(temp_dir=temp_dir, label=cid)
        with probe:
            t0       = time.perf_counter()
            registry = MedallionRegistry()
            layers   = [Layer.BRONZE, Layer.SILVER, Layer.GOLD]
            for i in range(n_datasets):
                registry.register(f"dataset_{i}", layers[i % len(layers)])
            _ = registry.all_datasets()
            _ = registry.datasets_in(Layer.SILVER)
            result.wall_s = time.perf_counter() - t0
        m = probe.report
        result.n_rows           = n_datasets
        result.peak_rss_gb      = m.peak_rss_gb
        result.min_sys_avail_gb = m.min_sys_avail_gb
        result.rows_per_sec     = n_datasets / max(result.wall_s, 0.001)
    except RuntimeError as exc:
        result.ok = False; result.error = f"SKIPPED: {exc}"
    except Exception as exc:
        result.ok    = False
        result.error = (f"{type(exc).__name__}: {exc}\n"
                        + _traceback.format_exc()[-600:])
    results.append(result)

    cid    = "M-build-order"
    result = BenchmarkResult(
        case_id=cid, group="M", profile="silver-build-order", tier="n/a",
        processor="SilverBuildOrder", sink="memory",
        source="synthetic", phase="compute", n_rows=0, n_changed=0,
    )
    try:
        MemoryProbe.check_safe_to_run(label=cid)
        sbo = SilverBuildOrder()
        # Classify the two local models and verify ordering
        probe = MemoryProbe(temp_dir=temp_dir, label=cid)
        with probe:
            t0     = time.perf_counter()
            sbo.build_order([_MBenchFact, _MBenchDim])
            # Do 10 000 classify calls to get a meaningful timing
            for _ in range(10_000):
                sbo.classify(_MBenchDim)
                sbo.classify(_MBenchFact)
            result.wall_s = time.perf_counter() - t0
        m = probe.report
        n_ops = 20_000
        result.n_rows           = n_ops
        result.peak_rss_gb      = m.peak_rss_gb
        result.min_sys_avail_gb = m.min_sys_avail_gb
        result.rows_per_sec     = n_ops / max(result.wall_s, 0.001)
    except RuntimeError as exc:
        result.ok = False; result.error = f"SKIPPED: {exc}"
    except Exception as exc:
        result.ok    = False
        result.error = (f"{type(exc).__name__}: {exc}\n"
                        + _traceback.format_exc()[-600:])
    results.append(result)

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Group N — Observability drift pipeline throughput
# ─────────────────────────────────────────────────────────────────────────────
#
# Measures the cost of treating schema/quality drift as first-class Kimball
# facts inside sqldim's own observability pipeline.  Three sub-cases:
#
#   N-drift-ingest   : Batch-insert k EvolutionReport events into the star schema
#   N-quality-ingest : Batch-insert k ContractReport violation events
#   N-drift-query    : Execute all five gold-layer analytical queries (×100 reps)

# DriftObservatory is an OLTP star-schema writer (~120–300 rows/sec ceiling).
# These tiers reflect realistic operational ingest volumes, not bulk-load scale.
_DRIFT_TIERS = {"xs": 100, "s": 500, "m": 2_000}


def group_n_drift_observatory(
    gen: BenchmarkDatasetGenerator,
    temp_dir: str,
    max_tier: str = "s",
    **_,
) -> list[BenchmarkResult]:
    """Group N — Schema/quality drift observability pipeline throughput."""
    import datetime as _dt
    from sqldim.contracts.engine import EvolutionChange, EvolutionReport
    from sqldim.contracts.report import ContractReport, ContractViolation
    from sqldim.observability.drift import DriftObservatory

    tier_order    = ["xs", "s", "m"]
    effective_max = max_tier if max_tier in tier_order else tier_order[-1]
    active_tiers  = [t for t in tier_order
                     if t in _DRIFT_TIERS
                     and tier_order.index(t) <= tier_order.index(effective_max)]
    results: list[BenchmarkResult] = []

    _datasets     = [f"dim_{i}" for i in range(20)]
    _change_types = ["added", "widened", "narrowed", "type_changed", "renamed", "removed"]
    _severities   = ["error", "warning", "info"]
    _rules        = ["not_null", "unique", "range_check", "freshness", "regex_match"]
    _base_ts      = _dt.datetime(2026, 1, 1, tzinfo=_dt.timezone.utc)

    # ── N-1: Evolution fact ingest throughput ────────────────────────────
    for tier in active_tiers:
        MemoryProbe.reset_hard_abort()  # fresh observatory per case; clear any prior signal
        n     = _DRIFT_TIERS[tier]
        cid   = f"N-drift-ingest-{tier}"
        result = BenchmarkResult(
            case_id=cid, group="N", profile="drift-ingest", tier=tier,
            processor="DriftObservatory", sink="duckdb-memory",
            source="synthetic", phase="bulk-insert", n_rows=n, n_changed=0,
        )
        try:
            MemoryProbe.check_safe_to_run(label=cid)
            obs   = DriftObservatory.in_memory()
            probe = MemoryProbe(temp_dir=temp_dir, label=cid)
            # Batch 100 changes per EvolutionReport call — one call per run_id
            # block (matching the existing run-NNNNN grouping).  This avoids
            # 5K individual Python→DuckDB round-trips in favour of n//100 calls
            # each flushing 100 facts via executemany.
            _BATCH = 100
            with probe:
                t0 = time.perf_counter()
                with obs.transaction():
                    for run_seq in range(0, n, _BATCH):
                        rep = EvolutionReport()
                        for j in range(run_seq, min(run_seq + _BATCH, n)):
                            ct = _change_types[j % len(_change_types)]
                            ch = EvolutionChange(ct, f"col_{j % 50}", f"detail {j}")
                            if ct == "added":
                                rep.safe_changes.append(ch)
                            elif ct == "widened":
                                rep.additive_changes.append(ch)
                            else:
                                rep.breaking_changes.append(ch)
                        obs.ingest_evolution(
                            rep,
                            dataset=_datasets[run_seq % len(_datasets)],
                            run_id=f"run-{run_seq // _BATCH:05d}",
                            layer="silver",
                            detected_at=_base_ts + _dt.timedelta(hours=run_seq),
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

    # ── N-2: Quality drift fact ingest throughput ────────────────────────
    for tier in active_tiers:
        MemoryProbe.reset_hard_abort()  # fresh observatory per case; clear any prior signal
        n     = _DRIFT_TIERS[tier]
        cid   = f"N-quality-ingest-{tier}"
        result = BenchmarkResult(
            case_id=cid, group="N", profile="quality-ingest", tier=tier,
            processor="DriftObservatory", sink="duckdb-memory",
            source="synthetic", phase="bulk-insert", n_rows=n, n_changed=0,
        )
        try:
            MemoryProbe.check_safe_to_run(label=cid)
            obs   = DriftObservatory.in_memory()
            probe = MemoryProbe(temp_dir=temp_dir, label=cid)
            _BATCH = 100
            with probe:
                t0 = time.perf_counter()
                with obs.transaction():
                    for run_seq in range(0, n, _BATCH):
                        viols = [
                            ContractViolation(
                                rule=_rules[j % len(_rules)],
                                severity=_severities[j % len(_severities)],
                                count=j % 100,
                                detail=f"detail {j}",
                            )
                            for j in range(run_seq, min(run_seq + _BATCH, n))
                        ]
                        rpt = ContractReport(
                            violations=viols,
                            view=_datasets[run_seq % len(_datasets)],
                            elapsed_s=0.01,
                        )
                        obs.ingest_quality(
                            rpt,
                            dataset=_datasets[run_seq % len(_datasets)],
                            run_id=f"run-{run_seq // _BATCH:05d}",
                            layer="silver",
                            checked_at=_base_ts + _dt.timedelta(hours=run_seq),
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

    # ── N-3: Gold-layer analytical query planning + execution ────────────
    cid   = "N-drift-gold-queries"
    result = BenchmarkResult(
        case_id=cid, group="N", profile="drift-gold", tier="n/a",
        processor="DriftObservatory", sink="duckdb-memory",
        source="synthetic", phase="query", n_rows=0, n_changed=0,
    )
    try:
        MemoryProbe.check_safe_to_run(label=cid)
        obs = DriftObservatory.in_memory()
        _SEED_BATCH = 100
        with obs.transaction():
            for run_seq in range(0, 1_000, _SEED_BATCH):
                rep = EvolutionReport()
                for j in range(run_seq, run_seq + _SEED_BATCH):
                    ct  = _change_types[j % len(_change_types)]
                    ch  = EvolutionChange(ct, f"col_{j % 50}", "")
                    if ct == "added":
                        rep.safe_changes.append(ch)
                    elif ct == "widened":
                        rep.additive_changes.append(ch)
                    else:
                        rep.breaking_changes.append(ch)
                obs.ingest_evolution(rep, dataset=_datasets[run_seq % len(_datasets)],
                                     run_id=f"run-{run_seq // _SEED_BATCH:04d}", layer="silver",
                                     detected_at=_base_ts + _dt.timedelta(hours=run_seq))
        with obs.transaction():
            for run_seq in range(0, 1_000, _SEED_BATCH):
                viols = [
                    ContractViolation(rule=_rules[j % len(_rules)],
                                      severity=_severities[j % len(_severities)],
                                      count=j % 200, detail="")
                    for j in range(run_seq, run_seq + _SEED_BATCH)
                ]
                rpt = ContractReport(violations=viols, view=_datasets[run_seq % len(_datasets)])
                obs.ingest_quality(rpt, dataset=_datasets[run_seq % len(_datasets)],
                                   run_id=f"run-{run_seq // _SEED_BATCH:04d}", layer="silver",
                                   checked_at=_base_ts + _dt.timedelta(hours=run_seq))

        repeats = 100
        n_queries = repeats * 5
        probe = MemoryProbe(temp_dir=temp_dir, label=cid)
        with probe:
            t0 = time.perf_counter()
            for _ in range(repeats):
                obs.breaking_change_rate().fetchall()
                obs.worst_quality_datasets(top_n=5).fetchall()
                obs.drift_velocity(bucket="week").fetchall()
                obs.migration_backlog().fetchall()
                obs.rule_failure_heatmap().fetchall()
            result.wall_s = time.perf_counter() - t0
        m = probe.report
        result.n_rows           = n_queries
        result.peak_rss_gb      = m.peak_rss_gb
        result.min_sys_avail_gb = m.min_sys_avail_gb
        result.rows_per_sec     = n_queries / max(result.wall_s, 0.001)
    except RuntimeError as exc:
        result.ok = False; result.error = f"SKIPPED: {exc}"
    except Exception as exc:
        result.ok    = False
        result.error = (f"{type(exc).__name__}: {exc}\n"
                        + _traceback.format_exc()[-600:])
    results.append(result)

    return results


# ═══════════════════════════════════════════════════════════════════════════
# ── Group O  DGM three-band query builder ────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════

_DGM_TIERS: dict[str, int] = {"xs": 1_000, "s": 10_000, "m": 100_000}



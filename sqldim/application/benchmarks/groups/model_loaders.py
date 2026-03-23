"""Model loader benchmarks (groups L–M): bulk loaders, incremental loaders."""
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



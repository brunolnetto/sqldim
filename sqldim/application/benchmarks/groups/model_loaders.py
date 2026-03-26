"""Model loader benchmarks (groups L–M): bulk loaders, incremental loaders."""

from __future__ import annotations

import time
import traceback as _traceback

from sqlalchemy.pool import StaticPool

from sqldim.application.benchmarks._dataset import (
    BenchmarkDatasetGenerator,
)
from sqldim.application.benchmarks.infra import (
    BenchmarkResult,
    _select_tiers,
)
from sqldim.application.benchmarks.memory_probe import MemoryProbe

import asyncio
from sqlmodel import Session, create_engine, SQLModel
from sqldim import DimensionModel, FactModel, SCD2Mixin, Field as SqdimField
from sqldim.medallion import MedallionRegistry, Layer
from sqldim.medallion.build_order import SilverBuildOrder

# ═══════════════════════════════════════════════════════════════════════════
# ── Group L  Narwhals backfill ───────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════

_BACKFILL_TIERS: dict[str, dict[str, int]] = {
    "xs": {"n_players": 100, "n_seasons": 10},  # 1 000 rows
    "s": {"n_players": 1_000, "n_seasons": 10},  # 10 000 rows
    "m": {"n_players": 10_000, "n_seasons": 10},  # 100 000 rows
}


def _l_scoring_class_list(n_player: int, n_season: int) -> list:
    return [
        "Elite" if (p * n_season + (s - 2000)) % 3 == 0 else "Star"
        for p in range(n_player)
        for s in range(2000, 2000 + n_season)
    ]


def _l_build_frame(n_player: int, n_season: int):
    """Build a flat player×season Polars DataFrame for the backfill benchmark."""
    import polars as pl

    player_ids = [f"p_{p}" for p in range(n_player) for _ in range(n_season)]
    seasons = [s for _ in range(n_player) for s in range(2000, 2000 + n_season)]
    scoring_class = _l_scoring_class_list(n_player, n_season)
    return pl.DataFrame(
        {
            "player_id": player_ids,
            "season": seasons,
            "scoring_class": scoring_class,
        }
    )


def _l_backfill_case(tier: str, temp_dir: str) -> BenchmarkResult:
    # backfill_scd2_narwhals is kept lazy (polars is an optional dependency)
    from sqldim.core.kimball.dimensions.scd.processors.backfill import (
        backfill_scd2_narwhals,
    )

    cfg = _BACKFILL_TIERS[tier]
    n_player = cfg["n_players"]
    n_season = cfg["n_seasons"]
    n_rows = n_player * n_season
    cid = f"L-backfill-{tier}"
    result = BenchmarkResult(
        case_id=cid,
        group="L",
        profile="scd2-snapshot",
        tier=tier,
        processor="backfill_scd2_narwhals",
        sink="Polars",
        source="synthetic",
        phase="batch",
        n_rows=n_rows,
        n_changed=0,
    )
    try:
        MemoryProbe.check_safe_to_run(label=cid)
        frame = _l_build_frame(n_player, n_season)
        probe = MemoryProbe(temp_dir=temp_dir, label=cid)
        with probe:
            t0 = time.perf_counter()
            cnt = backfill_scd2_narwhals(
                frame,
                partition_by="player_id",
                order_by="season",
                track_columns=["scoring_class"],
                dry_run=False,
            )
            result.wall_s = time.perf_counter() - t0
        m = probe.report
        result.n_changed = int(cnt) if isinstance(cnt, int) else 0
        result.peak_rss_gb = m.peak_rss_gb
        result.min_sys_avail_gb = m.min_sys_avail_gb
        result.total_spill_gb = m.total_spill_gb
        result.safety_breach = m.safety_breach
        result.breach_detail = m.breach_detail
        result.rows_per_sec = n_rows / max(result.wall_s, 0.001)
    except RuntimeError as exc:
        result.ok = False
        result.error = f"SKIPPED: {exc}"
    except Exception as exc:
        result.ok = False
        result.error = f"{type(exc).__name__}: {exc}\n" + _traceback.format_exc()[-600:]
    return result


def group_l_narwhals_backfill(
    gen: BenchmarkDatasetGenerator,
    temp_dir: str,
    max_tier: str = "m",
    **_,
) -> list[BenchmarkResult]:
    """Group L — backfill_scd2_narwhals() throughput with Polars frames."""
    tier_order = ["xs", "s", "m"]
    active_tiers = _select_tiers(tier_order, _BACKFILL_TIERS, max_tier)
    return [_l_backfill_case(tier, temp_dir) for tier in active_tiers]


# ═══════════════════════════════════════════════════════════════════════════
# ── Group M  ORM loaders · Medallion ────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════

_LOADER_TIERS: dict[str, int] = {"xs": 500, "s": 5_000}


def _m_dimloader_case(
    tier: str,
    n: int,
    temp_dir: str,
    MBenchDim,
    MBenchFact,
) -> BenchmarkResult:
    cid = f"M-dimloader-{tier}"
    result = BenchmarkResult(
        case_id=cid,
        group="M",
        profile="dimensional-loader",
        tier=tier,
        processor="DimensionalLoader",
        sink="SQLite",
        source="synthetic",
        phase="batch",
        n_rows=n,
        n_changed=0,
    )
    try:
        MemoryProbe.check_safe_to_run(label=cid)
        engine = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        SQLModel.metadata.create_all(
            engine,
            tables=[MBenchDim.__table__, MBenchFact.__table__],
        )
        dims = [{"sku": f"SKU-{i}", "price": float(i)} for i in range(n)]
        facts = [{"product_id": f"SKU-{i % n}", "quantity": i} for i in range(n)]
        with Session(engine) as session:
            from sqldim.core.loaders.dimension.dimensional import DimensionalLoader  # noqa: PLC0415

            loader = DimensionalLoader(session, models=[MBenchDim, MBenchFact])
            loader.register(MBenchDim, dims)
            loader.register(
                MBenchFact,
                facts,
                key_map={"product_id": (MBenchDim, "sku")},
            )
            probe = MemoryProbe(temp_dir=temp_dir, label=cid)
            with probe:
                t0 = time.perf_counter()
                asyncio.run(loader.run())
                result.wall_s = time.perf_counter() - t0
            m = probe.report
        engine.dispose()
        result.peak_rss_gb = m.peak_rss_gb
        result.min_sys_avail_gb = m.min_sys_avail_gb
        result.total_spill_gb = m.total_spill_gb
        result.safety_breach = m.safety_breach
        result.breach_detail = m.breach_detail
        result.rows_per_sec = (n * 2) / max(result.wall_s, 0.001)
    except RuntimeError as exc:
        result.ok = False
        result.error = f"SKIPPED: {exc}"
    except Exception as exc:
        result.ok = False
        result.error = f"{type(exc).__name__}: {exc}\n" + _traceback.format_exc()[-600:]
    return result


def _m_medallion_registry_case(temp_dir: str) -> BenchmarkResult:
    cid = "M-medallion-registry"
    result = BenchmarkResult(
        case_id=cid,
        group="M",
        profile="medallion-registry",
        tier="n/a",
        processor="MedallionRegistry",
        sink="memory",
        source="synthetic",
        phase="compute",
        n_rows=0,
        n_changed=0,
    )
    try:
        MemoryProbe.check_safe_to_run(label=cid)
        n_datasets = 500
        probe = MemoryProbe(temp_dir=temp_dir, label=cid)
        with probe:
            t0 = time.perf_counter()
            registry = MedallionRegistry()
            layers = [Layer.BRONZE, Layer.SILVER, Layer.GOLD]
            for i in range(n_datasets):
                registry.register(f"dataset_{i}", layers[i % len(layers)])
            _ = registry.all_datasets()
            _ = registry.datasets_in(Layer.SILVER)
            result.wall_s = time.perf_counter() - t0
        m = probe.report
        result.n_rows = n_datasets
        result.peak_rss_gb = m.peak_rss_gb
        result.min_sys_avail_gb = m.min_sys_avail_gb
        result.rows_per_sec = n_datasets / max(result.wall_s, 0.001)
    except RuntimeError as exc:
        result.ok = False
        result.error = f"SKIPPED: {exc}"
    except Exception as exc:
        result.ok = False
        result.error = f"{type(exc).__name__}: {exc}\n" + _traceback.format_exc()[-600:]
    return result


def _m_build_order_case(temp_dir: str, MBenchDim, MBenchFact) -> BenchmarkResult:
    cid = "M-build-order"
    result = BenchmarkResult(
        case_id=cid,
        group="M",
        profile="silver-build-order",
        tier="n/a",
        processor="SilverBuildOrder",
        sink="memory",
        source="synthetic",
        phase="compute",
        n_rows=0,
        n_changed=0,
    )
    try:
        MemoryProbe.check_safe_to_run(label=cid)
        sbo = SilverBuildOrder()
        probe = MemoryProbe(temp_dir=temp_dir, label=cid)
        with probe:
            t0 = time.perf_counter()
            sbo.build_order([MBenchFact, MBenchDim])
            for _ in range(10_000):
                sbo.classify(MBenchDim)
                sbo.classify(MBenchFact)
            result.wall_s = time.perf_counter() - t0
        m = probe.report
        n_ops = 20_000
        result.n_rows = n_ops
        result.peak_rss_gb = m.peak_rss_gb
        result.min_sys_avail_gb = m.min_sys_avail_gb
        result.rows_per_sec = n_ops / max(result.wall_s, 0.001)
    except RuntimeError as exc:
        result.ok = False
        result.error = f"SKIPPED: {exc}"
    except Exception as exc:
        result.ok = False
        result.error = f"{type(exc).__name__}: {exc}\n" + _traceback.format_exc()[-600:]
    return result


def group_m_loaders_medallion(
    gen: BenchmarkDatasetGenerator,
    temp_dir: str,
    max_tier: str = "s",
    **_,
) -> list[BenchmarkResult]:
    """Group M — ORM loader throughput and Medallion registry compute."""

    # ── Local SQLModel fixtures (table=True — must stay inside function) ──
    class _MBenchDim(DimensionModel, SCD2Mixin, table=True):  # type: ignore[call-arg]
        __natural_key__ = ["sku"]
        __tablename__ = "m_bench_dim"
        id: int = SqdimField(default=None, primary_key=True, surrogate_key=True)
        sku: str = SqdimField(default="")
        price: float = SqdimField(default=0.0)

    class _MBenchFact(FactModel, table=True):  # type: ignore[call-arg]
        __tablename__ = "m_bench_fact"
        id: int = SqdimField(default=None, primary_key=True)
        product_id: int = SqdimField(
            default=0, foreign_key="m_bench_dim.id", dimension=_MBenchDim
        )
        quantity: int = SqdimField(default=0)

    tier_order = ["xs", "s"]
    active_tiers = _select_tiers(tier_order, _LOADER_TIERS, max_tier)
    results: list[BenchmarkResult] = [
        _m_dimloader_case(tier, _LOADER_TIERS[tier], temp_dir, _MBenchDim, _MBenchFact)
        for tier in active_tiers
    ]
    results.append(_m_medallion_registry_case(temp_dir))
    results.append(_m_build_order_case(temp_dir, _MBenchDim, _MBenchFact))
    return results


# ─────────────────────────────────────────────────────────────────────────────

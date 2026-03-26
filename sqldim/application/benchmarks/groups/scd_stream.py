"""SCD streaming and processor benchmarks (groups D–H)."""

from __future__ import annotations

import os
import shutil
import time
import traceback as _traceback

import duckdb

from sqldim.application.benchmarks._dataset import (
    BenchmarkDatasetGenerator,
    SCALE_TIERS,
)
from sqldim.application.benchmarks.infra import (
    BenchmarkResult,
    SOURCE_NAMES,
    _remove_db,
    _configure,
    _run_scd2_batch,
    _run_metadata_batch,
)
from sqldim.application.benchmarks.memory_probe import MemoryProbe
from sqldim.application.benchmarks.scan_probe import DuckDBObjectTracker


def _d_stream_case(ds, temp_dir: str) -> BenchmarkResult:
    from sqldim.sinks.sql.duckdb import DuckDBSink
    from sqldim.core.kimball.dimensions.scd.processors.lazy.type2._lazy_type2 import (
        LazySCDProcessor,
    )
    from sqldim.sources.batch.sql import SQLSource

    db_path = os.path.join(temp_dir, "D_stream.duckdb")
    tier = "m"
    result = BenchmarkResult(
        case_id="D_products_m_stream_100k",
        group="D_stream_vs_batch",
        profile="products",
        tier=tier,
        processor="LazySCDProcessor",
        sink="DuckDBSink",
        phase="stream_100k",
        n_rows=ds.n_rows,
        n_changed=ds.n_changed,
    )
    try:
        MemoryProbe.check_safe_to_run("D_stream")
        s = duckdb.connect(db_path)
        _configure(s, temp_dir)
        s.execute(ds.ddl.format(table="dim_products_stream"))
        s.close()

        with DuckDBSink(db_path) as sink:
            _configure(sink._con, temp_dir)  # type: ignore[arg-type]
            proc = LazySCDProcessor(
                natural_key=ds.natural_key,
                track_columns=ds.track_columns,
                sink=sink,
                con=sink._con,
            )
            batch_size = 100_000
            offset = 0
            agg_ins = agg_ver = agg_unc = 0
            probe = MemoryProbe(temp_dir=temp_dir, label="D_stream")
            with probe:
                t0 = time.perf_counter()
                while offset < ds.n_rows:
                    frag = (
                        f"SELECT * FROM read_parquet('{ds.snapshot_path}') "
                        f"ORDER BY {ds.natural_key} "
                        f"LIMIT {batch_size} OFFSET {offset}"
                    )
                    r = proc.process(SQLSource(frag), "dim_products_stream")
                    agg_ins += r.inserted
                    agg_ver += r.versioned
                    agg_unc += r.unchanged
                    offset += batch_size
                result.wall_s = time.perf_counter() - t0

        m = probe.report
        result.peak_rss_gb = m.peak_rss_gb
        result.peak_duckdb_gb = m.peak_duckdb_gb
        result.min_sys_avail_gb = m.min_sys_avail_gb
        result.total_spill_gb = m.total_spill_gb
        result.safety_breach = m.safety_breach
        result.inserted = agg_ins
        result.versioned = agg_ver
        result.unchanged = agg_unc
        result.rows_per_sec = ds.n_rows / max(result.wall_s, 0.001)
    except RuntimeError as exc:
        result.ok = False
        result.error = f"SKIPPED: {exc}"
    except Exception as exc:
        result.ok = False
        result.error = f"{type(exc).__name__}: {exc}\n" + _traceback.format_exc()[-400:]
    finally:
        _remove_db(db_path)
    return result


def _d_csv_case(ds, temp_dir: str) -> BenchmarkResult:
    from sqldim.sinks.sql.duckdb import DuckDBSink
    from sqldim.core.kimball.dimensions.scd.processors.lazy.type2._lazy_type2 import (
        LazySCDProcessor,
    )
    from sqldim.sources.batch.sql import SQLSource
    from sqldim.sources.streaming.csv_stream import CSVStreamSource

    csv_path = os.path.join(temp_dir, "D_stream_products.csv")
    db_path_csv = os.path.join(temp_dir, "D_stream_csv.duckdb")
    tier = "m"
    result = BenchmarkResult(
        case_id="D_products_m_stream_csv",
        group="D_stream_vs_batch",
        profile="products",
        tier=tier,
        processor="LazySCDProcessor",
        sink="DuckDBSink",
        phase="stream_csv_100k",
        n_rows=ds.n_rows,
        n_changed=ds.n_changed,
    )
    try:
        MemoryProbe.check_safe_to_run("D_stream_csv")
        _tmp = duckdb.connect()
        _tmp.execute(
            f"COPY (SELECT * FROM read_parquet('{ds.snapshot_path}')) "
            f"TO '{csv_path}' (FORMAT CSV, HEADER TRUE)"
        )
        _tmp.close()

        s = duckdb.connect(db_path_csv)
        _configure(s, temp_dir)
        s.execute(ds.ddl.format(table="dim_products_csv"))
        s.close()

        with DuckDBSink(db_path_csv) as sink:
            _configure(sink._con, temp_dir)  # type: ignore[arg-type]
            proc = LazySCDProcessor(
                natural_key=ds.natural_key,
                track_columns=ds.track_columns,
                sink=sink,
                con=sink._con,
            )
            source = CSVStreamSource(csv_path)
            agg_ins = agg_ver = agg_unc = 0
            probe = MemoryProbe(temp_dir=temp_dir, label="D_stream_csv")
            with probe:
                t0 = time.perf_counter()
                for frag in source.stream(sink._con, batch_size=100_000):
                    r = proc.process(SQLSource(frag), "dim_products_csv")
                    agg_ins += r.inserted
                    agg_ver += r.versioned
                    agg_unc += r.unchanged
                result.wall_s = time.perf_counter() - t0

        m = probe.report
        result.peak_rss_gb = m.peak_rss_gb
        result.peak_duckdb_gb = m.peak_duckdb_gb
        result.min_sys_avail_gb = m.min_sys_avail_gb
        result.total_spill_gb = m.total_spill_gb
        result.safety_breach = m.safety_breach
        result.inserted = agg_ins
        result.versioned = agg_ver
        result.unchanged = agg_unc
        result.rows_per_sec = ds.n_rows / max(result.wall_s, 0.001)
    except RuntimeError as exc:
        result.ok = False
        result.error = f"SKIPPED: {exc}"
    except Exception as exc:
        result.ok = False
        result.error = f"{type(exc).__name__}: {exc}\n" + _traceback.format_exc()[-400:]
    finally:
        _remove_db(db_path_csv)
        if os.path.exists(csv_path):
            os.remove(csv_path)
    return result


def group_d_stream_vs_batch(
    gen: BenchmarkDatasetGenerator, temp_dir: str, **_
) -> list[BenchmarkResult]:
    tier = "m"
    ds = gen.generate("products", tier=tier)

    r_batch = _run_scd2_batch(
        ds, "D_products_m_batch", "D_stream_vs_batch", tier, temp_dir
    )
    r_batch.phase = "batch"

    r_stream = _d_stream_case(ds, temp_dir)
    r_csv = _d_csv_case(ds, temp_dir)

    ds.cleanup()
    return [r_batch, r_stream, r_csv]


# ── Group E — Change rate sensitivity ────────────────────────────────────


def group_e_change_rate_sensitivity(
    gen: BenchmarkDatasetGenerator, temp_dir: str, **_
) -> list[BenchmarkResult]:
    results = []
    for rate, label in [(0.01, "1pct"), (0.20, "20pct"), (0.99, "99pct")]:
        ds = gen.generate("products", tier="s", change_rate=rate)
        r = _run_scd2_batch(ds, f"E_products_s_{label}", "E_change_rate", "s", temp_dir)
        r.phase = f"initial_load_{label}"
        ds.cleanup()
        results.append(r)
    return results


# ── Group F — Processor comparison ───────────────────────────────────────


def group_f_processor_comparison(
    gen: BenchmarkDatasetGenerator, temp_dir: str, **_
) -> list[BenchmarkResult]:
    from sqldim.sinks.sql.duckdb import DuckDBSink
    from sqldim.core.kimball.dimensions.scd.processors.lazy.type6._lazy_type6 import (
        LazyType6Processor,
    )
    from sqldim.sources.batch.parquet import ParquetSource

    results = []
    tier = "s"

    ds = gen.generate("products", tier=tier)
    r = _run_scd2_batch(ds, "F_products_SCD2", "F_processor_comparison", tier, temp_dir)
    r.processor = "LazySCDProcessor"
    results.append(r)
    ds.cleanup()

    ds = gen.generate("cnpj_empresa", tier=tier)
    r = _run_metadata_batch(
        ds, "F_cnpj_Metadata", "F_processor_comparison", tier, temp_dir
    )
    r.processor = "LazySCDMetadataProcessor"
    results.append(r)
    ds.cleanup()

    ds = gen.generate("employees", tier=tier)
    db_path = os.path.join(temp_dir, "F_type6.duckdb")
    r6 = BenchmarkResult(
        case_id="F_employees_Type6",
        group="F_processor_comparison",
        profile="employees",
        tier=tier,
        processor="LazyType6Processor",
        sink="DuckDBSink",
        phase="batch",
        n_rows=ds.n_rows,
        n_changed=ds.n_changed,
    )
    try:
        MemoryProbe.check_safe_to_run("F_type6")
        s = duckdb.connect(db_path)
        _configure(s, temp_dir)
        s.execute(ds.ddl.format(table="dim_emp_t6"))
        s.close()

        with DuckDBSink(db_path) as sink:
            _configure(sink._con, temp_dir)  # type: ignore[arg-type]
            tracker = DuckDBObjectTracker(sink._con)
            tracker.wrap()
            proc = LazyType6Processor(
                natural_key="employee_id",
                type1_columns=["title"],
                type2_columns=["department"],
                sink=sink,
                con=sink._con,
            )
            probe = MemoryProbe(temp_dir=temp_dir, label="F_type6")
            with probe:
                t0 = time.perf_counter()
                scd = proc.process(ParquetSource(ds.snapshot_path), "dim_emp_t6")
                r6.wall_s = time.perf_counter() - t0
            tracker.snapshot()
            tracker.unwrap()

        m = probe.report
        obj = tracker.report()
        r6.peak_rss_gb = m.peak_rss_gb
        r6.peak_duckdb_gb = m.peak_duckdb_gb
        r6.min_sys_avail_gb = m.min_sys_avail_gb
        r6.total_spill_gb = m.total_spill_gb
        r6.safety_breach = m.safety_breach
        r6.scan_regression = obj["regression_detected"]
        r6.current_state_as_table = obj["current_state_as_table"]
        r6.inserted = scd.inserted
        r6.versioned = scd.versioned
        r6.rows_per_sec = ds.n_rows / max(r6.wall_s, 0.001)
    except RuntimeError as exc:
        r6.ok = False
        r6.error = f"SKIPPED: {exc}"
    except Exception as exc:
        r6.ok = False
        r6.error = f"{type(exc).__name__}: {exc}\n" + _traceback.format_exc()[-400:]
    finally:
        _remove_db(db_path)

    ds.cleanup()
    results.append(r6)
    return results


# ── Group G — Spill simulation ────────────────────────────────────────────


def group_g_beyond_memory(
    gen: BenchmarkDatasetGenerator, temp_dir: str, max_tier: str = "m", **_
) -> list[BenchmarkResult]:
    # Tight DuckDB memory cap: small enough to force spill on m/l/xl tier working
    # sets (typically 200 MB – 4 GB) without being so low that DuckDB can't start.
    SPILL_LIMIT_GB = 0.5
    results = []

    tier_order = list(SCALE_TIERS.keys())  # ["xs","s","m","l","xl","xxl"]
    max_idx = tier_order.index(max_tier)
    tiers_to_run = tier_order[: max_idx + 1]

    for tier in tiers_to_run:
        try:
            MemoryProbe.check_safe_to_run(f"G_spill_{tier}")
        except RuntimeError as e:
            results.append(
                BenchmarkResult(
                    case_id=f"G_cnpj_{tier}_spill",
                    group="G_beyond_memory",
                    profile="cnpj_empresa",
                    tier=tier,
                    processor="LazySCDMetadataProcessor",
                    sink="DuckDBSink",
                    phase="batch_tight_memory",
                    n_rows=SCALE_TIERS[tier],
                    n_changed=0,
                    ok=False,
                    error=str(e),
                )
            )
            continue

        spill_dir = os.path.join(temp_dir, f"spill_{tier}")
        os.makedirs(spill_dir, exist_ok=True)
        ds = gen.generate("cnpj_empresa", tier=tier)
        r = _run_metadata_batch(
            ds,
            f"G_cnpj_{tier}_spill",
            "G_beyond_memory",
            tier,
            temp_dir=temp_dir,
            mem_limit_override_gb=SPILL_LIMIT_GB,
            spill_dir=spill_dir,
        )
        r.phase = f"tight_{SPILL_LIMIT_GB:.1f}GB_limit"
        ds.cleanup()
        results.append(r)
        shutil.rmtree(spill_dir, ignore_errors=True)

    return results


# ── Group H — Source / Sink matrix ───────────────────────────────────────


def group_h_source_sink_matrix(
    gen: BenchmarkDatasetGenerator,
    temp_dir: str,
    **_,
) -> list[BenchmarkResult]:
    """Run the same workload for every registered source, DuckDB sink.

    Enables direct apples-to-apples comparison of:
      - ``parquet`` — zero-copy columnar scan, best-case baseline
      - ``csv``     — CSV parsing overhead, reflects ingestion pipelines

    Both share identical SCD logic so any throughput delta is purely the
    source adapter's parse cost.  Fixed at tier ``s`` (100K rows) to keep
    Group H fast.
    """
    results = []
    tier = "s"
    profile = "products"

    for src in SOURCE_NAMES:
        cid = f"H_{profile}_{tier}_{src}_duckdb"
        try:
            MemoryProbe.check_safe_to_run(cid)
        except RuntimeError as exc:
            results.append(
                BenchmarkResult(
                    case_id=cid,
                    group="H_source_sink_matrix",
                    profile=profile,
                    tier=tier,
                    processor="LazySCDProcessor",
                    sink="DuckDBSink",
                    source=src,
                    phase="h_source_matrix",
                    n_rows=SCALE_TIERS[tier],
                    n_changed=0,
                    ok=False,
                    error=str(exc),
                )
            )
            continue
        ds = gen.generate(profile, tier=tier)
        r = _run_scd2_batch(
            ds,
            cid,
            "H_source_sink_matrix",
            tier,
            temp_dir,
            source_name=src,
        )
        r.phase = "h_source_matrix"
        ds.cleanup()
        results.append(r)

    return results

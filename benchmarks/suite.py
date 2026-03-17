"""
benchmarks/suite.py
====================
Core benchmark cases for sqldim.

Each case is a self-contained function that:
  1. Checks memory safety (aborts if system is too low)
  2. Generates a dataset via BenchmarkDatasetGenerator
  3. Runs the pipeline under MemoryProbe + ScanProbe
  4. Returns a BenchmarkResult with timing, memory, scan count, throughput

Cases are grouped by the bottleneck category they probe:

  Group A — VIEW vs TABLE regression (scan count verification)
  Group B — Memory floor safety across all processors
  Group C — Throughput scaling (rows/sec at each tier)
  Group D — Streaming vs batch comparison
  Group E — Processor comparison (same data, different SCD type)
  Group F — Sink comparison (DuckDB vs mock-Postgres throughput)
  Group G — Transform overhead (with vs without SQLTransformPipeline)
  Group H — Beyond-memory datasets (tier > available RAM)
"""
from __future__ import annotations

import os
import shutil
import time
import traceback as _traceback
from dataclasses import dataclass, field

import duckdb

from benchmarks.dataset_gen import BenchmarkDatasetGenerator, DatasetArtifact, SCALE_TIERS
from benchmarks.memory_probe import MemoryProbe
from benchmarks.scan_probe import DuckDBObjectTracker


# ── Result ────────────────────────────────────────────────────────────────

@dataclass
class BenchmarkResult:
    case_id:          str
    group:            str
    profile:          str
    tier:             str
    processor:        str
    sink:             str
    phase:            str
    n_rows:           int
    n_changed:        int
    source:           str   = "parquet"
    ok:               bool  = True
    error:            str   = ""
    wall_s:           float = 0.0
    rows_per_sec:     float = 0.0
    peak_rss_gb:      float = 0.0
    peak_duckdb_gb:   float = 0.0
    min_sys_avail_gb: float = 0.0
    total_spill_gb:   float = 0.0
    safety_breach:    bool  = False
    breach_detail:    str   = ""
    scan_count:       int   = 0
    scan_regression:  bool  = False
    current_state_as_table: bool = False
    inserted:         int   = 0
    versioned:        int   = 0
    unchanged:        int   = 0

    def row(self) -> dict:
        return {
            "case_id":          self.case_id,
            "group":            self.group,
            "profile":          self.profile,
            "tier":             self.tier,
            "n_rows":           self.n_rows,
            "processor":        self.processor,
            "source":           self.source,
            "sink":             self.sink,
            "phase":            self.phase,
            "ok":               self.ok,
            "wall_s":           round(self.wall_s, 2),
            "rows_per_sec":     int(self.rows_per_sec),
            "peak_rss_gb":      round(self.peak_rss_gb, 3),
            "peak_duckdb_gb":   round(self.peak_duckdb_gb, 3),
            "min_sys_avail_gb": round(self.min_sys_avail_gb, 3),
            "total_spill_gb":   round(self.total_spill_gb, 4),
            "safety_breach":    self.safety_breach,
            "scan_count":       self.scan_count,
            "scan_regression":  self.scan_regression,
            "current_state_as_table": self.current_state_as_table,
            "inserted":         self.inserted,
            "versioned":        self.versioned,
            "unchanged":        self.unchanged,
            "error":            self.error[:300] if self.error else "",
        }


# ── Source / Sink registries ─────────────────────────────────────────────

SOURCE_NAMES = ["parquet", "csv"]
SINK_NAMES   = ["duckdb"]


def _make_source(source_name: str, artifact: DatasetArtifact, temp_dir: str, case_id: str):
    """Instantiate a SourceAdapter from the registry for *source_name*.

    CSV sources are produced by exporting the artifact's Parquet snapshot
    on-demand — the CSV file is cached in *temp_dir* for the lifetime of
    the benchmark run.
    """
    if source_name == "parquet":
        from sqldim.sources.parquet import ParquetSource
        return ParquetSource(artifact.snapshot_path)
    elif source_name == "csv":
        from sqldim.sources.csv import CSVSource
        csv_path = os.path.join(temp_dir, f"{case_id}_snap.csv")
        if not os.path.exists(csv_path):
            tmp = duckdb.connect()
            tmp.execute(
                f"COPY (SELECT * FROM read_parquet('{artifact.snapshot_path}'))"
                f" TO '{csv_path}' (FORMAT CSV, HEADER TRUE)"
            )
            tmp.close()
        return CSVSource(csv_path)
    else:
        raise ValueError(
            f"Unknown source: {source_name!r}. Available: {SOURCE_NAMES}"
        )


# ── Helpers ───────────────────────────────────────────────────────────────

def _remove_db(db_path: str) -> None:
    for p in [db_path, db_path + ".wal"]:
        try:
            if p and os.path.exists(p):
                os.unlink(p)
        except OSError:
            pass


def _configure(con: duckdb.DuckDBPyConnection, temp_dir: str,
               mem_limit_gb: float | None = None) -> None:
    """
    Apply memory/spill settings. Does NOT cap threads — DuckDB defaults to
    all available cores, which is correct for throughput benchmarks.
    The artificial SET threads=4 was suppressing 2x throughput on 8-core machines.
    """
    limit = mem_limit_gb or MemoryProbe.recommended_memory_limit_gb()
    con.execute(f"SET memory_limit = '{limit:.1f}GB'")
    con.execute(f"SET temp_directory = '{temp_dir}'"  )


# ── Core runner: LazySCDProcessor ────────────────────────────────────────

def _run_scd2_batch(
    artifact: DatasetArtifact,
    case_id: str,
    group: str,
    tier: str,
    temp_dir: str,
    table_name: str | None = None,
    source_name: str = "parquet",
) -> BenchmarkResult:
    from sqldim.sinks.duckdb import DuckDBSink
    from sqldim.core.kimball.dimensions.scd.processors._lazy_type2 import LazySCDProcessor

    tname   = table_name or f"dim_{artifact.profile}"
    db_path = os.path.join(temp_dir, f"{case_id}.duckdb")
    result  = BenchmarkResult(
        case_id=case_id, group=group, profile=artifact.profile, tier=tier,
        processor="LazySCDProcessor", sink="DuckDBSink", source=source_name,
        phase="batch", n_rows=artifact.n_rows, n_changed=artifact.n_changed,
    )
    try:
        MemoryProbe.check_safe_to_run(label=case_id)
        s = duckdb.connect(db_path); _configure(s, temp_dir)
        s.execute(artifact.ddl.format(table=tname)); s.close()

        with DuckDBSink(db_path) as sink:
            _configure(sink._con, temp_dir)
            tracker = DuckDBObjectTracker(sink._con); tracker.wrap()
            proc  = LazySCDProcessor(
                natural_key=artifact.natural_key,
                track_columns=artifact.track_columns,
                sink=sink, con=sink._con,
            )
            probe = MemoryProbe(temp_dir=temp_dir, label=case_id)
            with probe:
                t0  = time.perf_counter()
                scd = proc.process(
                    _make_source(source_name, artifact, temp_dir, case_id), tname
                )
                result.wall_s = time.perf_counter() - t0
            # Read DuckDB memory from MAIN THREAD after process() — never from bg thread
            from benchmarks.memory_probe import read_duckdb_memory_once
            post_duckdb_gb = read_duckdb_memory_once(sink._con)
            tracker.snapshot(); tracker.unwrap()

        m   = probe.report; obj = tracker.report()
        result.peak_rss_gb          = m.peak_rss_gb
        result.peak_duckdb_gb       = post_duckdb_gb
        result.min_sys_avail_gb     = m.min_sys_avail_gb
        result.total_spill_gb       = m.total_spill_gb
        result.safety_breach        = m.safety_breach
        result.breach_detail        = m.breach_detail
        result.scan_regression      = obj["regression_detected"]
        result.current_state_as_table = obj["current_state_as_table"]
        result.scan_count   = len([v for v in obj["views_created"] if "current" in v])
        result.inserted     = scd.inserted
        result.versioned    = scd.versioned
        result.unchanged    = scd.unchanged
        result.rows_per_sec = artifact.n_rows / max(result.wall_s, 0.001)
    except RuntimeError as exc:
        result.ok = False; result.error = f"SKIPPED: {exc}"
    except Exception as exc:
        result.ok = False
        result.error = f"{type(exc).__name__}: {exc}\n" + _traceback.format_exc()[-600:]
    finally:
        _remove_db(db_path)
    return result


# ── Core runner: LazySCDMetadataProcessor ────────────────────────────────

def _run_metadata_batch(
    artifact: DatasetArtifact,
    case_id: str,
    group: str,
    tier: str,
    temp_dir: str,
    table_name: str | None = None,
    mem_limit_override_gb: float | None = None,
    spill_dir: str | None = None,
    source_name: str = "parquet",
) -> BenchmarkResult:
    from sqldim.sinks.duckdb import DuckDBSink
    from sqldim.core.kimball.dimensions.scd.processors._lazy_metadata import LazySCDMetadataProcessor

    tname     = table_name or f"dim_{artifact.profile}"
    db_path   = os.path.join(temp_dir, f"{case_id}.duckdb")
    eff_spill = spill_dir or temp_dir
    result    = BenchmarkResult(
        case_id=case_id, group=group, profile=artifact.profile, tier=tier,
        processor="LazySCDMetadataProcessor", sink="DuckDBSink", source=source_name,
        phase="batch", n_rows=artifact.n_rows, n_changed=artifact.n_changed,
    )
    try:
        MemoryProbe.check_safe_to_run(label=case_id)
        s = duckdb.connect(db_path); _configure(s, eff_spill, mem_limit_override_gb)
        s.execute(artifact.ddl.format(table=tname)); s.close()

        with DuckDBSink(db_path) as sink:
            _configure(sink._con, eff_spill, mem_limit_override_gb)
            tracker = DuckDBObjectTracker(sink._con); tracker.wrap()
            proc  = LazySCDMetadataProcessor(
                natural_key=artifact.natural_key,
                metadata_columns=artifact.metadata_columns,
                sink=sink, con=sink._con,
            )
            probe = MemoryProbe(temp_dir=eff_spill, label=case_id)
            with probe:
                t0  = time.perf_counter()
                scd = proc.process(
                    _make_source(source_name, artifact, temp_dir, case_id), tname
                )
                result.wall_s = time.perf_counter() - t0
            from benchmarks.memory_probe import read_duckdb_memory_once
            post_duckdb_gb = read_duckdb_memory_once(sink._con)
            tracker.snapshot(); tracker.unwrap()

        m   = probe.report; obj = tracker.report()
        result.peak_rss_gb          = m.peak_rss_gb
        result.peak_duckdb_gb       = post_duckdb_gb
        result.min_sys_avail_gb     = m.min_sys_avail_gb
        result.total_spill_gb       = m.total_spill_gb
        result.safety_breach        = m.safety_breach
        result.breach_detail        = m.breach_detail
        result.scan_regression      = obj["regression_detected"]
        result.current_state_as_table = obj["current_state_as_table"]
        result.inserted     = scd.inserted
        result.versioned    = scd.versioned
        result.unchanged    = scd.unchanged
        result.rows_per_sec = artifact.n_rows / max(result.wall_s, 0.001)
    except RuntimeError as exc:
        result.ok = False; result.error = f"SKIPPED: {exc}"
    except Exception as exc:
        result.ok = False
        result.error = f"{type(exc).__name__}: {exc}\n" + _traceback.format_exc()[-600:]
    finally:
        _remove_db(db_path)
    return result


# ── Group A — VIEW vs TABLE regression ───────────────────────────────────

def group_a_scan_regression(gen: BenchmarkDatasetGenerator, temp_dir: str, **_) -> list[BenchmarkResult]:
    results = []
    for profile in ["products", "employees", "saas_users"]:
        ds = gen.generate(profile, tier="xs")
        results.append(_run_scd2_batch(ds, f"A_{profile}_scan_regression", "A_scan_regression", "xs", temp_dir))
        ds.cleanup()
    ds = gen.generate("cnpj_empresa", tier="xs")
    results.append(_run_metadata_batch(ds, "A_cnpj_metadata_scan_regression", "A_scan_regression", "xs", temp_dir))
    ds.cleanup()
    return results


# ── Group B — Memory safety ───────────────────────────────────────────────

def group_b_memory_safety(gen: BenchmarkDatasetGenerator, temp_dir: str, **_) -> list[BenchmarkResult]:
    results = []
    cases = [
        ("products",     "s",  "scd2"),
        ("products",     "m",  "scd2"),
        ("employees",    "s",  "scd2"),
        ("employees",    "m",  "scd2"),
        ("saas_users",   "s",  "scd2"),
        ("saas_users",   "m",  "scd2"),
        ("cnpj_empresa", "s",  "meta"),
        ("cnpj_empresa", "m",  "meta"),
    ]
    for profile, tier, proc_type in cases:
        ds = gen.generate(profile, tier=tier)
        cid = f"B_{profile}_{tier}_memory"
        r = (_run_metadata_batch(ds, cid, "B_memory_safety", tier, temp_dir)
             if proc_type == "meta"
             else _run_scd2_batch(ds, cid, "B_memory_safety", tier, temp_dir))
        ds.cleanup(); results.append(r)
    return results


# ── Group C — Throughput scaling ─────────────────────────────────────────

def group_c_throughput_scaling(gen: BenchmarkDatasetGenerator, temp_dir: str, max_tier: str = "m", **_) -> list[BenchmarkResult]:
    tier_order = list(SCALE_TIERS.keys())
    max_idx    = tier_order.index(max_tier)
    results    = []
    for tier in tier_order[: max_idx + 1]:
        try:
            MemoryProbe.check_safe_to_run(f"C_{tier}")
        except RuntimeError as e:
            results.append(BenchmarkResult(
                case_id=f"C_products_{tier}_throughput", group="C_throughput",
                profile="products", tier=tier, processor="LazySCDProcessor",
                sink="DuckDBSink", phase="batch",
                n_rows=SCALE_TIERS[tier], n_changed=0, ok=False, error=str(e),
            ))
            continue
        ds = gen.generate("products", tier=tier)
        results.append(_run_scd2_batch(ds, f"C_products_{tier}_throughput", "C_throughput", tier, temp_dir))
        ds.cleanup()
    return results


# ── Group D — Streaming vs batch ─────────────────────────────────────────

def group_d_stream_vs_batch(gen: BenchmarkDatasetGenerator, temp_dir: str, **_) -> list[BenchmarkResult]:
    from sqldim.sinks.duckdb import DuckDBSink
    from sqldim.core.kimball.dimensions.scd.processors._lazy_type2 import LazySCDProcessor
    from sqldim.sources.sql import SQLSource
    from sqldim.sources.csv_stream import CSVStreamSource

    results = []
    tier = "m"
    ds   = gen.generate("products", tier=tier)

    r_batch = _run_scd2_batch(ds, "D_products_m_batch", "D_stream_vs_batch", tier, temp_dir)
    r_batch.phase = "batch"; results.append(r_batch)

    db_path  = os.path.join(temp_dir, "D_stream.duckdb")
    r_stream = BenchmarkResult(
        case_id="D_products_m_stream_100k", group="D_stream_vs_batch",
        profile="products", tier=tier, processor="LazySCDProcessor",
        sink="DuckDBSink", phase="stream_100k",
        n_rows=ds.n_rows, n_changed=ds.n_changed,
    )
    try:
        MemoryProbe.check_safe_to_run("D_stream")
        s = duckdb.connect(db_path); _configure(s, temp_dir)
        s.execute(ds.ddl.format(table="dim_products_stream")); s.close()

        with DuckDBSink(db_path) as sink:
            _configure(sink._con, temp_dir)
            proc = LazySCDProcessor(natural_key=ds.natural_key,
                                    track_columns=ds.track_columns,
                                    sink=sink, con=sink._con)
            batch_size = 100_000; offset = 0
            agg_ins = agg_ver = agg_unc = 0
            probe = MemoryProbe(temp_dir=temp_dir, label="D_stream")
            with probe:
                t0 = time.perf_counter()
                while offset < ds.n_rows:
                    frag = (f"SELECT * FROM read_parquet('{ds.snapshot_path}') "
                            f"LIMIT {batch_size} OFFSET {offset}")
                    r = proc.process(SQLSource(frag), "dim_products_stream")
                    agg_ins += r.inserted; agg_ver += r.versioned; agg_unc += r.unchanged
                    offset  += batch_size
                r_stream.wall_s = time.perf_counter() - t0

        m = probe.report
        r_stream.peak_rss_gb      = m.peak_rss_gb
        r_stream.peak_duckdb_gb   = m.peak_duckdb_gb
        r_stream.min_sys_avail_gb = m.min_sys_avail_gb
        r_stream.total_spill_gb   = m.total_spill_gb
        r_stream.safety_breach    = m.safety_breach
        r_stream.inserted  = agg_ins; r_stream.versioned = agg_ver; r_stream.unchanged = agg_unc
        r_stream.rows_per_sec = ds.n_rows / max(r_stream.wall_s, 0.001)
    except RuntimeError as exc:
        r_stream.ok = False; r_stream.error = f"SKIPPED: {exc}"
    except Exception as exc:
        r_stream.ok = False
        r_stream.error = f"{type(exc).__name__}: {exc}\n" + _traceback.format_exc()[-400:]
    finally:
        _remove_db(db_path)

    results.append(r_stream)

    # ── D3: CSVStreamSource (row_number pagination, no OFFSET penalty) ──────
    csv_path    = os.path.join(temp_dir, "D_stream_products.csv")
    db_path_csv = os.path.join(temp_dir, "D_stream_csv.duckdb")
    r_csv = BenchmarkResult(
        case_id="D_products_m_stream_csv", group="D_stream_vs_batch",
        profile="products", tier=tier, processor="LazySCDProcessor",
        sink="DuckDBSink", phase="stream_csv_100k",
        n_rows=ds.n_rows, n_changed=ds.n_changed,
    )
    try:
        MemoryProbe.check_safe_to_run("D_stream_csv")
        # Export Parquet → CSV once so we can benchmark CSV pagination
        _tmp = duckdb.connect()
        _tmp.execute(
            f"COPY (SELECT * FROM read_parquet('{ds.snapshot_path}')) "
            f"TO '{csv_path}' (FORMAT CSV, HEADER TRUE)"
        )
        _tmp.close()

        s = duckdb.connect(db_path_csv); _configure(s, temp_dir)
        s.execute(ds.ddl.format(table="dim_products_csv")); s.close()

        with DuckDBSink(db_path_csv) as sink:
            _configure(sink._con, temp_dir)
            proc = LazySCDProcessor(natural_key=ds.natural_key,
                                    track_columns=ds.track_columns,
                                    sink=sink, con=sink._con)
            source = CSVStreamSource(csv_path)
            agg_ins = agg_ver = agg_unc = 0
            probe = MemoryProbe(temp_dir=temp_dir, label="D_stream_csv")
            with probe:
                t0 = time.perf_counter()
                for frag in source.stream(sink._con, batch_size=100_000):
                    r = proc.process(SQLSource(frag), "dim_products_csv")
                    agg_ins += r.inserted; agg_ver += r.versioned; agg_unc += r.unchanged
                r_csv.wall_s = time.perf_counter() - t0

        m = probe.report
        r_csv.peak_rss_gb      = m.peak_rss_gb
        r_csv.peak_duckdb_gb   = m.peak_duckdb_gb
        r_csv.min_sys_avail_gb = m.min_sys_avail_gb
        r_csv.total_spill_gb   = m.total_spill_gb
        r_csv.safety_breach    = m.safety_breach
        r_csv.inserted  = agg_ins; r_csv.versioned = agg_ver; r_csv.unchanged = agg_unc
        r_csv.rows_per_sec = ds.n_rows / max(r_csv.wall_s, 0.001)
    except RuntimeError as exc:
        r_csv.ok = False; r_csv.error = f"SKIPPED: {exc}"
    except Exception as exc:
        r_csv.ok = False
        r_csv.error = f"{type(exc).__name__}: {exc}\n" + _traceback.format_exc()[-400:]
    finally:
        _remove_db(db_path_csv)
        if os.path.exists(csv_path):
            os.remove(csv_path)

    results.append(r_csv)
    ds.cleanup(); return results


# ── Group E — Change rate sensitivity ────────────────────────────────────

def group_e_change_rate_sensitivity(gen: BenchmarkDatasetGenerator, temp_dir: str, **_) -> list[BenchmarkResult]:
    results = []
    for rate, label in [(0.01, "1pct"), (0.20, "20pct"), (0.99, "99pct")]:
        ds = gen.generate("products", tier="s", change_rate=rate)
        r  = _run_scd2_batch(ds, f"E_products_s_{label}", "E_change_rate", "s", temp_dir)
        r.phase = f"initial_load_{label}"; ds.cleanup(); results.append(r)
    return results


# ── Group F — Processor comparison ───────────────────────────────────────

def group_f_processor_comparison(gen: BenchmarkDatasetGenerator, temp_dir: str, **_) -> list[BenchmarkResult]:
    from sqldim.sinks.duckdb import DuckDBSink
    from sqldim.core.kimball.dimensions.scd.processors._lazy_type3_6 import LazyType6Processor
    from sqldim.sources.parquet import ParquetSource

    results = []
    tier = "s"

    ds = gen.generate("products", tier=tier)
    r  = _run_scd2_batch(ds, "F_products_SCD2", "F_processor_comparison", tier, temp_dir)
    r.processor = "LazySCDProcessor"; results.append(r); ds.cleanup()

    ds = gen.generate("cnpj_empresa", tier=tier)
    r  = _run_metadata_batch(ds, "F_cnpj_Metadata", "F_processor_comparison", tier, temp_dir)
    r.processor = "LazySCDMetadataProcessor"; results.append(r); ds.cleanup()

    ds      = gen.generate("employees", tier=tier)
    db_path = os.path.join(temp_dir, "F_type6.duckdb")
    r6      = BenchmarkResult(
        case_id="F_employees_Type6", group="F_processor_comparison",
        profile="employees", tier=tier, processor="LazyType6Processor",
        sink="DuckDBSink", phase="batch",
        n_rows=ds.n_rows, n_changed=ds.n_changed,
    )
    try:
        MemoryProbe.check_safe_to_run("F_type6")
        s = duckdb.connect(db_path); _configure(s, temp_dir)
        s.execute(ds.ddl.format(table="dim_emp_t6")); s.close()

        with DuckDBSink(db_path) as sink:
            _configure(sink._con, temp_dir)
            tracker = DuckDBObjectTracker(sink._con); tracker.wrap()
            proc  = LazyType6Processor(natural_key="employee_id",
                                       type1_columns=["title"],
                                       type2_columns=["department"],
                                       sink=sink, con=sink._con)
            probe = MemoryProbe(temp_dir=temp_dir, label="F_type6")
            with probe:
                t0  = time.perf_counter()
                scd = proc.process(ParquetSource(ds.snapshot_path), "dim_emp_t6")
                r6.wall_s = time.perf_counter() - t0
            tracker.snapshot(); tracker.unwrap()

        m = probe.report; obj = tracker.report()
        r6.peak_rss_gb = m.peak_rss_gb; r6.peak_duckdb_gb = m.peak_duckdb_gb
        r6.min_sys_avail_gb = m.min_sys_avail_gb; r6.total_spill_gb = m.total_spill_gb
        r6.safety_breach = m.safety_breach; r6.scan_regression = obj["regression_detected"]
        r6.current_state_as_table = obj["current_state_as_table"]
        r6.inserted = scd.inserted; r6.versioned = scd.versioned
        r6.rows_per_sec = ds.n_rows / max(r6.wall_s, 0.001)
    except RuntimeError as exc:
        r6.ok = False; r6.error = f"SKIPPED: {exc}"
    except Exception as exc:
        r6.ok = False
        r6.error = f"{type(exc).__name__}: {exc}\n" + _traceback.format_exc()[-400:]
    finally:
        _remove_db(db_path)

    ds.cleanup(); results.append(r6)
    return results


# ── Group G — Spill simulation ────────────────────────────────────────────

def group_g_beyond_memory(gen: BenchmarkDatasetGenerator, temp_dir: str, max_tier: str = "m", **_) -> list[BenchmarkResult]:
    # Tight DuckDB memory cap: small enough to force spill on m/l/xl tier working
    # sets (typically 200 MB – 4 GB) without being so low that DuckDB can't start.
    SPILL_LIMIT_GB = 0.5
    results = []

    tier_order   = list(SCALE_TIERS.keys())          # ["xs","s","m","l","xl","xxl"]
    max_idx      = tier_order.index(max_tier)
    tiers_to_run = tier_order[:max_idx + 1]

    for tier in tiers_to_run:
        try:
            MemoryProbe.check_safe_to_run(f"G_spill_{tier}")
        except RuntimeError as e:
            results.append(BenchmarkResult(
                case_id=f"G_cnpj_{tier}_spill", group="G_beyond_memory",
                profile="cnpj_empresa", tier=tier,
                processor="LazySCDMetadataProcessor", sink="DuckDBSink",
                phase="batch_tight_memory",
                n_rows=SCALE_TIERS[tier], n_changed=0, ok=False, error=str(e),
            ))
            continue

        spill_dir = os.path.join(temp_dir, f"spill_{tier}")
        os.makedirs(spill_dir, exist_ok=True)
        ds = gen.generate("cnpj_empresa", tier=tier)
        r  = _run_metadata_batch(
            ds, f"G_cnpj_{tier}_spill", "G_beyond_memory", tier,
            temp_dir=temp_dir,
            mem_limit_override_gb=SPILL_LIMIT_GB,
            spill_dir=spill_dir,
        )
        r.phase = f"tight_{SPILL_LIMIT_GB:.1f}GB_limit"
        ds.cleanup(); results.append(r)
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
    tier    = "s"
    profile = "products"

    for src in SOURCE_NAMES:
        cid = f"H_{profile}_{tier}_{src}_duckdb"
        try:
            MemoryProbe.check_safe_to_run(cid)
        except RuntimeError as exc:
            results.append(BenchmarkResult(
                case_id=cid, group="H_source_sink_matrix",
                profile=profile, tier=tier,
                processor="LazySCDProcessor", sink="DuckDBSink",
                source=src, phase="h_source_matrix",
                n_rows=SCALE_TIERS[tier], n_changed=0,
                ok=False, error=str(exc),
            ))
            continue
        ds = gen.generate(profile, tier=tier)
        r  = _run_scd2_batch(
            ds, cid, "H_source_sink_matrix", tier, temp_dir,
            source_name=src,
        )
        r.phase = "h_source_matrix"
        ds.cleanup()
        results.append(r)

    return results

# ═══════════════════════════════════════════════════════════════════════════
# ── Group I  SCD type variety  (Type3 · Type4) ───────────────────────────
# ═══════════════════════════════════════════════════════════════════════════

_VARIETY_TIERS: dict[str, int] = {"xs": 1_000, "s": 10_000, "m": 100_000}


class _BenchSink:
    """Minimal DuckDB-native sink used by Type3 / Type4 / Type5 benchmarks."""

    def current_state_sql(self, table_name: str) -> str:
        return f"SELECT * FROM {table_name}"

    def write(self, con, view_name: str, table_name: str,
              batch_size: int = 100_000) -> int:
        n = con.execute(f"SELECT count(*) FROM {view_name}").fetchone()[0]
        try:
            con.execute(
                f"INSERT INTO {table_name} BY NAME SELECT * FROM {view_name}"
            )
        except Exception:
            con.execute(
                f"CREATE TABLE {table_name} AS SELECT * FROM {view_name}"
            )
        return n

    def close_versions(self, con, table_name: str, nk_col: str,
                       nk_view: str, valid_to: str) -> int:
        con.execute(f"""
            UPDATE {table_name}
               SET is_current = FALSE, valid_to = '{valid_to}'
             WHERE {nk_col} IN (SELECT {nk_col} FROM {nk_view})
               AND is_current = TRUE
        """)
        return con.execute(f"SELECT count(*) FROM {nk_view}").fetchone()[0]

    def update_attributes(self, con, table_name: str, nk_col: str,
                          updates_view: str, update_cols: list[str]) -> int:
        for col in update_cols:
            con.execute(f"""
                UPDATE {table_name}
                   SET {col} = (
                       SELECT u.{col} FROM {updates_view} u
                        WHERE cast(u.{nk_col} as varchar)
                              = cast({table_name}.{nk_col} as varchar)
                   )
                 WHERE {nk_col} IN (SELECT {nk_col} FROM {updates_view})
                   AND is_current = TRUE
            """)
        return con.execute(f"SELECT count(*) FROM {updates_view}").fetchone()[0]

    def upsert(self, con, view_name: str, table_name: str,
               conflict_cols: list[str], returning_col: str,
               output_view: str) -> int:
        cols_str   = ", ".join(conflict_cols)
        inner_join = " AND ".join(f"src.{c} = t.{c}" for c in conflict_cols)
        view_join  = " AND ".join(f"t.{c} = v.{c}" for c in conflict_cols)
        con.execute(f"""
            INSERT INTO {table_name} ({returning_col}, {cols_str})
            SELECT
                (SELECT COALESCE(MAX({returning_col}), 0) FROM {table_name})
                    + row_number() OVER () AS {returning_col},
                {', '.join(f'src.{c}' for c in conflict_cols)}
            FROM (
                SELECT DISTINCT {', '.join(f'src.{c}' for c in conflict_cols)}
                FROM {view_name} src
                WHERE NOT EXISTS (
                    SELECT 1 FROM {table_name} t WHERE {inner_join}
                )
            ) src
        """)
        con.execute(f"""
            CREATE OR REPLACE VIEW {output_view} AS
            SELECT t.{returning_col},
                   {', '.join(f't.{c}' for c in conflict_cols)}
            FROM {table_name} t
            INNER JOIN (SELECT DISTINCT {cols_str} FROM {view_name}) v
                ON {view_join}
        """)
        return con.execute(f"SELECT count(*) FROM {output_view}").fetchone()[0]

    def rotate_attributes(self, con, table_name: str, nk_col: str,
                          rotations_view: str,
                          column_pairs: list[tuple[str, str]]) -> int:
        """Rotate current→previous and write incoming for each changed row."""
        for curr_col, prev_col in column_pairs:
            con.execute(f"""
                UPDATE {table_name}
                   SET {prev_col} = {curr_col},
                       {curr_col} = (
                           SELECT r.{curr_col}
                           FROM {rotations_view} r
                           WHERE cast(r.{nk_col} as varchar)
                                 = cast({table_name}.{nk_col} as varchar)
                       )
                 WHERE {nk_col} IN (SELECT {nk_col} FROM {rotations_view})
                   AND is_current = TRUE
            """)
        return con.execute(
            f"SELECT count(*) FROM {rotations_view}"
        ).fetchone()[0]


def group_i_scd_type_variety(
    gen: BenchmarkDatasetGenerator,
    temp_dir: str,
    max_tier: str = "m",
    **_,
) -> list[BenchmarkResult]:
    """Group I — SCD Type3 and Type4 processor throughput at scale."""
    from sqldim.core.kimball.dimensions.scd.processors._lazy_type3_6 import (
        LazyType3Processor,
    )
    from sqldim.core.kimball.dimensions.scd.processors.scd_engine import (
        LazyType4Processor,
    )

    tier_order  = ["xs", "s", "m"]
    active_tiers = [t for t in tier_order
                    if t in _VARIETY_TIERS
                    and tier_order.index(t) <= tier_order.index(max_tier)]
    sink    = _BenchSink()
    results: list[BenchmarkResult] = []

    for tier in active_tiers:
        n = _VARIETY_TIERS[tier]

        # ── Type3: emp_id + region (current / previous) ────────────────────
        cid = f"I-type3-{tier}"
        result = BenchmarkResult(
            case_id=cid, group="I", profile="scd3-employees", tier=tier,
            processor="LazyType3Processor", sink="InMemory",
            source="synthetic", phase="batch", n_rows=n, n_changed=0,
        )
        try:
            MemoryProbe.check_safe_to_run(label=cid)
            con = duckdb.connect()
            con.execute("""
                CREATE TABLE dim_scd3 (
                    emp_id      VARCHAR,
                    region      VARCHAR,
                    prev_region VARCHAR,
                    checksum    VARCHAR,
                    is_current  BOOLEAN,
                    valid_from  VARCHAR,
                    valid_to    VARCHAR
                )
            """)
            con.execute(f"""
                CREATE OR REPLACE VIEW src AS
                SELECT 'emp_' || i::VARCHAR AS emp_id,
                       CASE WHEN i % 3 = 0 THEN 'East'
                            WHEN i % 3 = 1 THEN 'West'
                            ELSE 'North' END AS region
                FROM range(1, {n + 1}) t(i)
            """)
            proc  = LazyType3Processor(
                natural_key="emp_id",
                column_pairs=[("region", "prev_region")],
                sink=sink, con=con,
            )
            probe = MemoryProbe(temp_dir=temp_dir, label=cid)
            with probe:
                t0  = time.perf_counter()
                scd = proc.process("src", "dim_scd3")
                result.wall_s = time.perf_counter() - t0
            m = probe.report
            result.peak_rss_gb      = m.peak_rss_gb
            result.min_sys_avail_gb = m.min_sys_avail_gb
            result.total_spill_gb   = m.total_spill_gb
            result.safety_breach    = m.safety_breach
            result.breach_detail    = m.breach_detail
            result.inserted         = scd.inserted
            result.versioned        = scd.versioned
            result.unchanged        = scd.unchanged
            result.rows_per_sec     = n / max(result.wall_s, 0.001)
            con.close()
        except RuntimeError as exc:
            result.ok = False; result.error = f"SKIPPED: {exc}"
        except Exception as exc:
            result.ok    = False
            result.error = (f"{type(exc).__name__}: {exc}\n"
                            + _traceback.format_exc()[-600:])
        results.append(result)

        # ── Type4: customer + mini-dimension (age/income) ──────────────────
        cid = f"I-type4-{tier}"
        result = BenchmarkResult(
            case_id=cid, group="I", profile="scd4-customers", tier=tier,
            processor="LazyType4Processor", sink="InMemory",
            source="synthetic", phase="batch", n_rows=n, n_changed=0,
        )
        try:
            MemoryProbe.check_safe_to_run(label=cid)
            con = duckdb.connect()
            con.execute("""
                CREATE TABLE dim_mini (
                    id          INTEGER,
                    age_band    VARCHAR,
                    income_band VARCHAR
                )
            """)
            con.execute("""
                CREATE TABLE dim_base (
                    customer_id VARCHAR,
                    name        VARCHAR,
                    profile_sk  INTEGER,
                    checksum    VARCHAR,
                    valid_from  VARCHAR,
                    valid_to    VARCHAR,
                    is_current  BOOLEAN
                )
            """)
            con.execute(f"""
                CREATE OR REPLACE TABLE src AS
                SELECT 'cust_' || i::VARCHAR AS customer_id,
                       'Customer_' || i::VARCHAR AS name,
                       CASE WHEN i % 3 = 0 THEN 'Young'
                            WHEN i % 3 = 1 THEN 'Middle'
                            ELSE 'Senior' END AS age_band,
                       CASE WHEN i % 2 = 0 THEN 'Low'
                            ELSE 'High' END AS income_band
                FROM range(1, {n + 1}) t(i)
            """)
            proc  = LazyType4Processor(
                natural_key="customer_id",
                base_columns=["name"],
                mini_dim_columns=["age_band", "income_band"],
                base_dim_table="dim_base",
                mini_dim_table="dim_mini",
                mini_dim_fk_col="profile_sk",
                mini_dim_id_col="id",
                sink=sink, con=con,
            )
            probe = MemoryProbe(temp_dir=temp_dir, label=cid)
            with probe:
                t0  = time.perf_counter()
                res = proc.process("src")
                result.wall_s = time.perf_counter() - t0
            m = probe.report
            result.peak_rss_gb      = m.peak_rss_gb
            result.min_sys_avail_gb = m.min_sys_avail_gb
            result.total_spill_gb   = m.total_spill_gb
            result.safety_breach    = m.safety_breach
            result.breach_detail    = m.breach_detail
            result.inserted         = res.get("inserted", 0)
            result.versioned        = res.get("versioned", 0)
            result.unchanged        = res.get("unchanged", 0)
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
# ── Group J  Prebuilt dimension generation  (Date · Time) ────────────────
# ═══════════════════════════════════════════════════════════════════════════

_DATE_CASES: list[tuple[str, str, str]] = [
    ("J-date-1y",  "2024-01-01", "2024-12-31"),   #   366 rows
    ("J-date-5y",  "2020-01-01", "2024-12-31"),   # ~1 827 rows
    ("J-date-20y", "2005-01-01", "2024-12-31"),   # ~7 305 rows
    ("J-date-50y", "1975-01-01", "2024-12-31"),   # ~18 263 rows
]


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
    active_tiers = [t for t in tier_order
                    if t in _GRAPH_TIERS
                    and tier_order.index(t) <= tier_order.index(max_tier)]
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
                deg = engine.degree(_BenchEdge, 1, direction="out")
                # aggregate() — sum of weights
                total = engine.aggregate(_BenchEdge, 1, "weight", "sum",
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
                rows = (
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
    active_tiers = [t for t in tier_order
                    if t in _BACKFILL_TIERS
                    and tier_order.index(t) <= tier_order.index(max_tier)]
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
    from sqldim.medallion.build_order import SilverBuildOrder, ModelKind

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
    active_tiers = [t for t in tier_order
                    if t in _LOADER_TIERS
                    and tier_order.index(t) <= tier_order.index(max_tier)]
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
            order  = sbo.build_order([_MBenchFact, _MBenchDim])
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

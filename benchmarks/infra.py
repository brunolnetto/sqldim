"""
benchmarks/infra.py
====================
Shared infrastructure for all benchmark groups.

Provides ``BenchmarkResult`` (the canonical result dataclass),
source/sink registries, memory/spill helpers, and the two core
SCD batch runners used by most groups.
"""
from __future__ import annotations

import os
import shutil
import time
import traceback as _traceback
from dataclasses import dataclass

import duckdb

from benchmarks.dataset_gen import BenchmarkDatasetGenerator, DatasetArtifact, SCALE_TIERS
from benchmarks.memory_probe import MemoryProbe
from benchmarks.scan_probe import DuckDBObjectTracker


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


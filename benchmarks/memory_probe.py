"""
benchmarks/memory_probe.py
===========================
Memory monitoring for sqldim benchmarks.

ROOT CAUSE OF SEGFAULT (Group B):
    The original implementation called con.execute() from a background thread
    while the main thread was also executing queries on the same connection.
    DuckDB connections are NOT thread-safe. Concurrent access = SIGSEGV.

FIX:
    The background thread NEVER touches any DuckDB connection.
    Memory tracking uses only psutil (process RSS) and the OS filesystem
    (spill file sizes). Both are safe to call from any thread.

    DuckDB internal allocator stats (pragma_database_size) are read
    ONCE from the main thread after process() returns, not during execution.

Tracks three signals (all thread-safe):
  - RSS          : OS-level resident set size via psutil
  - sys_available: System-wide free + reclaimable RAM via psutil
  - spill_bytes  : Size of temp files written to temp_directory (filesystem stat)

DuckDB internal memory is read once post-execution via read_duckdb_memory_once().
"""
from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

import psutil


# ── Constants ─────────────────────────────────────────────────────────────

SAFE_PCT          = 0.65   # never allow sqldim to use more than 65% of total RAM
ABORT_FLOOR_GB    = 1.5    # refuse to start a benchmark if less than this is free
SAMPLE_INTERVAL_S = 0.5    # memory polling interval (increased from 0.25 to reduce overhead)


# ── Data containers ───────────────────────────────────────────────────────

@dataclass
class MemorySample:
    ts:          float   # epoch seconds
    rss_gb:      float   # process RSS in GB (thread-safe via psutil)
    sys_avail_gb: float  # system available RAM in GB (thread-safe via psutil)
    spill_bytes: int     # bytes written to temp_directory since probe start


@dataclass
class MemoryReport:
    samples:          list[MemorySample] = field(default_factory=list)
    peak_rss_gb:      float = 0.0
    peak_duckdb_gb:   float = 0.0   # populated by read_duckdb_memory_once() post-execution
    min_sys_avail_gb: float = float("inf")
    total_spill_gb:   float = 0.0
    safety_breach:    bool  = False
    breach_detail:    str   = ""

    @property
    def duration_s(self) -> float:
        if len(self.samples) < 2:
            return 0.0
        return self.samples[-1].ts - self.samples[0].ts

    def summary(self) -> dict:
        return {
            "peak_rss_gb":      round(self.peak_rss_gb, 3),
            "peak_duckdb_gb":   round(self.peak_duckdb_gb, 3),
            "min_sys_avail_gb": round(self.min_sys_avail_gb, 3),
            "total_spill_gb":   round(self.total_spill_gb, 4),
            "duration_s":       round(self.duration_s, 2),
            "safety_breach":    self.safety_breach,
            "breach_detail":    self.breach_detail,
            "sample_count":     len(self.samples),
        }


# ── Spill tracker (filesystem only — thread-safe) ─────────────────────────

def _spill_bytes(temp_dir: str | None) -> int:
    """Return total bytes of DuckDB spill files. Safe to call from any thread."""
    if not temp_dir:
        return 0
    p = Path(temp_dir)
    if not p.exists():
        return 0
    try:
        return sum(f.stat().st_size for f in p.rglob("*.tmp") if f.is_file())
    except (OSError, PermissionError):
        return 0


# ── DuckDB memory reader — MAIN THREAD ONLY ──────────────────────────────

def read_duckdb_memory_once(con) -> float:
    """
    Read DuckDB's internal memory_usage from pragma_database_size.

    MUST be called from the main thread, NEVER from a background thread.
    Call this after process() returns, not during execution.

    Returns GB as float, 0.0 on any error.
    """
    if con is None:
        return 0.0
    try:
        rows = con.execute("SELECT memory_usage FROM pragma_database_size()").fetchall()
        for row in rows:
            raw = str(row[0]).strip()
            if raw in ("0 bytes", ""):
                return 0.0
            parts = raw.split()
            if len(parts) != 2:
                continue
            val  = float(parts[0])
            unit = parts[1].lower()
            if "gib" in unit or "gb" in unit:
                return val
            if "mib" in unit or "mb" in unit:
                return val / 1024
            if "kib" in unit or "kb" in unit:
                return val / (1024 ** 2)
            return val / (1024 ** 3)
    except Exception:
        return 0.0
    return 0.0


# ── MemoryProbe ───────────────────────────────────────────────────────────

class MemoryProbe:
    """
    Background thread that polls process RSS and system RAM every SAMPLE_INTERVAL_S.

    THREAD SAFETY:
        The background thread ONLY uses psutil and filesystem stat calls.
        It NEVER calls any DuckDB method. This eliminates the SIGSEGV
        caused by concurrent DuckDB connection access in Group B.

    DuckDB internal memory:
        Read once from the main thread after process() returns via
        report.peak_duckdb_gb = read_duckdb_memory_once(con).

    Usage::

        probe = MemoryProbe(temp_dir="/tmp/duckdb")
        with probe:
            proc.process(source, "dim_product")
        # Read DuckDB memory from main thread AFTER process() completes:
        probe.report.peak_duckdb_gb = read_duckdb_memory_once(sink._con)
        print(probe.report.summary())
    """

    def __init__(
        self,
        con=None,            # accepted for API compatibility, NOT used in background thread
        temp_dir: str | None = None,
        safe_pct: float = SAFE_PCT,
        label: str = "",
    ):
        # con is stored but NEVER accessed from the background thread
        self._con_unused    = con
        self._temp_dir      = temp_dir
        self._safe_pct      = safe_pct
        self._label         = label
        self._process       = psutil.Process(os.getpid())
        self._running       = False
        self._thread: threading.Thread | None = None
        self.report         = MemoryReport()
        self._spill_baseline = _spill_bytes(temp_dir)
        self._total_ram_gb  = psutil.virtual_memory().total / (1024 ** 3)
        self._safe_ceiling  = self._total_ram_gb * safe_pct

    # ── Context manager ───────────────────────────────────────────────────

    def __enter__(self) -> "MemoryProbe":
        self.report = MemoryReport()
        self._spill_baseline = _spill_bytes(self._temp_dir)
        self._running = True
        self._thread = threading.Thread(target=self._poll, daemon=True, name=f"MemProbe-{self._label}")
        self._thread.start()
        return self

    def __exit__(self, *_) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=3.0)
        self._finalize()

    # ── Polling (background thread — NO DuckDB calls) ─────────────────────

    def _poll(self) -> None:
        while self._running:
            self._take_sample()
            time.sleep(SAMPLE_INTERVAL_S)

    def _take_sample(self) -> None:
        """
        Called from background thread — ONLY psutil and filesystem.
        No DuckDB, no shared Python objects that could cause races.
        """
        try:
            rss_gb    = self._process.memory_info().rss / (1024 ** 3)
            sys_avail = psutil.virtual_memory().available / (1024 ** 3)
            spill     = max(0, _spill_bytes(self._temp_dir) - self._spill_baseline)

            sample = MemorySample(
                ts=time.time(),
                rss_gb=rss_gb,
                sys_avail_gb=sys_avail,
                spill_bytes=spill,
            )
            self.report.samples.append(sample)

            if rss_gb > self._safe_ceiling:
                self.report.safety_breach = True
                self.report.breach_detail = (
                    f"RSS {rss_gb:.2f}GB exceeded safe ceiling "
                    f"{self._safe_ceiling:.2f}GB "
                    f"({self._safe_pct*100:.0f}% of {self._total_ram_gb:.1f}GB total)"
                )
        except Exception:
            pass

    def _finalize(self) -> None:
        r = self.report
        if not r.samples:
            return
        r.peak_rss_gb      = max(s.rss_gb for s in r.samples)
        r.min_sys_avail_gb = min(s.sys_avail_gb for s in r.samples)
        r.total_spill_gb   = max(s.spill_bytes for s in r.samples) / (1024 ** 3)

    # ── Static helpers ────────────────────────────────────────────────────

    @staticmethod
    def check_safe_to_run(label: str = "") -> None:
        """Raise RuntimeError if available system RAM is below ABORT_FLOOR_GB."""
        avail = psutil.virtual_memory().available / (1024 ** 3)
        if avail < ABORT_FLOOR_GB:
            raise RuntimeError(
                f"[sqldim-bench] ABORTED{' — ' + label if label else ''}. "
                f"Only {avail:.2f}GB RAM available; "
                f"minimum required is {ABORT_FLOOR_GB:.1f}GB."
            )

    @staticmethod
    def recommended_memory_limit_gb(safe_pct: float = SAFE_PCT) -> float:
        """Return the recommended DuckDB memory_limit for this machine."""
        total = psutil.virtual_memory().total / (1024 ** 3)
        avail = psutil.virtual_memory().available / (1024 ** 3)
        return round(min(total * safe_pct, max(avail - 1.0, 0.5)), 1)
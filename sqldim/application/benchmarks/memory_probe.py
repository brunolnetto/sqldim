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

import psutil  # type: ignore[import-untyped]


# ── Constants ─────────────────────────────────────────────────────────────

SAFE_PCT = 0.65  # flag breach when process RSS exceeds this fraction of total RAM
ABORT_FLOOR_GB = 2.5  # refuse to start a case if available RAM is below this
HARD_CEILING_PCT = (
    0.90  # set hard-abort event when RSS exceeds this fraction of total RAM
)
SAMPLE_INTERVAL_S = 0.5  # memory polling interval

# Minimum available RAM to safely execute each tier.
# Used by auto_max_tier() to select the highest tier that won't OOM.
_TIER_MIN_AVAIL_GB: dict[str, float] = {
    "xs": 1.5,
    "s": 3.0,
    "m": 6.0,
    "l": 14.0,
    "xl": 30.0,
    "xxl": 64.0,
}

# Module-level hard-abort event.  Set by MemoryProbe background thread when
# RSS exceeds HARD_CEILING_PCT.  Checked by check_safe_to_run() so the *next*
# case in the same group is aborted before it starts.  Cleared by
# reset_hard_abort() at the beginning of each group in the runner.
_HARD_ABORT_EVENT: threading.Event = threading.Event()


def auto_max_tier(available_gb: float) -> str:
    """Return the highest scale tier that is safe to run given *available_gb* of
    free RAM.  Iterates from largest to smallest and returns the first tier
    whose minimum requirement is met.
    """
    for tier in ["xxl", "xl", "l", "m", "s", "xs"]:
        if available_gb >= _TIER_MIN_AVAIL_GB[tier]:
            return tier
    return "xs"


# ── Data containers ───────────────────────────────────────────────────────


@dataclass
class MemorySample:
    ts: float  # epoch seconds
    rss_gb: float  # process RSS in GB (thread-safe via psutil)
    sys_avail_gb: float  # system available RAM in GB (thread-safe via psutil)
    spill_bytes: int  # bytes written to temp_directory since probe start


@dataclass
class MemoryReport:
    samples: list[MemorySample] = field(default_factory=list)
    peak_rss_gb: float = 0.0
    peak_duckdb_gb: float = 0.0  # populated by read_duckdb_memory_once() post-execution
    min_sys_avail_gb: float = float("inf")
    total_spill_gb: float = 0.0
    safety_breach: bool = False
    breach_detail: str = ""

    @property
    def duration_s(self) -> float:
        if len(self.samples) < 2:
            return 0.0
        return self.samples[-1].ts - self.samples[0].ts

    def summary(self) -> dict:
        return {
            "peak_rss_gb": round(self.peak_rss_gb, 3),
            "peak_duckdb_gb": round(self.peak_duckdb_gb, 3),
            "min_sys_avail_gb": round(self.min_sys_avail_gb, 3),
            "total_spill_gb": round(self.total_spill_gb, 4),
            "duration_s": round(self.duration_s, 2),
            "safety_breach": self.safety_breach,
            "breach_detail": self.breach_detail,
            "sample_count": len(self.samples),
        }


# ── Spill tracker (filesystem only — thread-safe) ─────────────────────────


def _iter_spill_files(p: "Path"):
    for f in p.rglob("*.tmp"):
        if f.is_file():
            yield f


def _spill_bytes(temp_dir: str | None) -> int:
    """Return total bytes of DuckDB spill files. Safe to call from any thread."""
    if not temp_dir:
        return 0
    p = Path(temp_dir)
    if not p.exists():
        return 0
    try:
        return sum(f.stat().st_size for f in _iter_spill_files(p))
    except (OSError, PermissionError):
        return 0


# ── DuckDB memory reader — MAIN THREAD ONLY ──────────────────────────────

_MEMORY_UNIT_BASES: list[tuple[str, float]] = [
    ("gib", 1.0),
    ("gb", 1.0),
    ("mib", 1 / 1024),
    ("mb", 1 / 1024),
    ("kib", 1 / 1024**2),
    ("kb", 1 / 1024**2),
]


def _parse_memory_str(raw: str) -> float | None:
    """Parse a DuckDB memory string like '1.23 GiB' → GB float, or None if unparseable."""
    if raw in ("0 bytes", ""):
        return 0.0
    parts = raw.split()
    if len(parts) != 2:
        return None
    val = float(parts[0])
    unit = parts[1].lower()
    for pfx, scale in _MEMORY_UNIT_BASES:
        if pfx in unit:
            return val * scale
    return val / (1024**3)


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
            result = _parse_memory_str(str(row[0]).strip())
            if result is not None:
                return result
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
        con=None,  # accepted for API compatibility, NOT used in background thread
        temp_dir: str | None = None,
        safe_pct: float = SAFE_PCT,
        label: str = "",
    ):
        # con is stored but NEVER accessed from the background thread
        self._con_unused = con
        self._temp_dir = temp_dir
        self._safe_pct = safe_pct
        self._label = label
        self._process = psutil.Process(os.getpid())
        self._running = False
        self._thread: threading.Thread | None = None
        self.report = MemoryReport()
        self._spill_baseline = _spill_bytes(temp_dir)
        self._total_ram_gb = psutil.virtual_memory().total / (1024**3)
        self._safe_ceiling = self._total_ram_gb * safe_pct
        self._hard_ceiling = self._total_ram_gb * HARD_CEILING_PCT

    # ── Context manager ───────────────────────────────────────────────────

    def __enter__(self) -> "MemoryProbe":
        self.report = MemoryReport()
        self._spill_baseline = _spill_bytes(self._temp_dir)
        self._running = True
        self._thread = threading.Thread(
            target=self._poll, daemon=True, name=f"MemProbe-{self._label}"
        )
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

    def _check_breach(self, rss_gb: float) -> None:
        if rss_gb > self._hard_ceiling and not _HARD_ABORT_EVENT.is_set():
            _HARD_ABORT_EVENT.set()
            self.report.safety_breach = True
            self.report.breach_detail = (
                f"HARD ABORT: RSS {rss_gb:.2f}GB exceeded hard ceiling "
                f"{self._hard_ceiling:.2f}GB "
                f"({HARD_CEILING_PCT * 100:.0f}% of {self._total_ram_gb:.1f}GB). "
                f"Remaining cases in this group will be skipped."
            )
        elif rss_gb > self._safe_ceiling and not self.report.safety_breach:
            self.report.safety_breach = True
            self.report.breach_detail = (
                f"RSS {rss_gb:.2f}GB exceeded safe ceiling "
                f"{self._safe_ceiling:.2f}GB "
                f"({self._safe_pct * 100:.0f}% of {self._total_ram_gb:.1f}GB total)"
            )

    def _take_sample(self) -> None:
        """
        Called from background thread — ONLY psutil and filesystem.
        No DuckDB, no shared Python objects that could cause races.
        """
        try:
            rss_gb = self._process.memory_info().rss / (1024**3)
            sys_avail = psutil.virtual_memory().available / (1024**3)
            spill = max(0, _spill_bytes(self._temp_dir) - self._spill_baseline)

            sample = MemorySample(
                ts=time.time(),
                rss_gb=rss_gb,
                sys_avail_gb=sys_avail,
                spill_bytes=spill,
            )
            self.report.samples.append(sample)
            self._check_breach(rss_gb)
        except Exception:
            pass

    def _finalize(self) -> None:
        r = self.report
        if not r.samples:
            return
        r.peak_rss_gb = max(s.rss_gb for s in r.samples)
        r.min_sys_avail_gb = min(s.sys_avail_gb for s in r.samples)
        r.total_spill_gb = max(s.spill_bytes for s in r.samples) / (1024**3)

    # ── Static helpers ────────────────────────────────────────────────────

    @staticmethod
    def check_safe_to_run(label: str = "") -> None:
        """Raise RuntimeError if the hard-abort event is set or if available
        system RAM is below ABORT_FLOOR_GB."""
        if _HARD_ABORT_EVENT.is_set():
            raise RuntimeError(
                f"[sqldim-bench] ABORTED{' — ' + label if label else ''}. "
                f"Hard memory ceiling ({HARD_CEILING_PCT * 100:.0f}% of total RAM) "
                f"was breached in a previous case — skipping to protect the system."
            )
        avail = psutil.virtual_memory().available / (1024**3)
        if avail < ABORT_FLOOR_GB:
            raise RuntimeError(
                f"[sqldim-bench] ABORTED{' — ' + label if label else ''}. "
                f"Only {avail:.2f}GB RAM available; "
                f"minimum required is {ABORT_FLOOR_GB:.1f}GB."
            )

    @staticmethod
    def reset_hard_abort() -> None:
        """Clear the hard-abort event.  Call once before each group so that a
        breach in group N does not permanently block groups N+1 … M."""
        _HARD_ABORT_EVENT.clear()

    @staticmethod
    def recommended_memory_limit_gb(safe_pct: float = SAFE_PCT) -> float:
        """Return the recommended DuckDB memory_limit for this machine."""
        total = psutil.virtual_memory().total / (1024**3)
        avail = psutil.virtual_memory().available / (1024**3)
        return round(min(total * safe_pct, max(avail - 1.0, 0.5)), 1)

"""
benchmarks/scan_probe.py
=========================
Intercepts sink.current_state_sql() to count how many times the remote
source is actually scanned during a processor run.

The VIEW→TABLE bug manifests as scan_count > 1 per process() call.
The correct value is always 1 — the slim TABLE is built once and
all downstream steps join against the local copy.

Usage::

    with ScanProbe(sink) as probe:
        proc.process(source, "dim_product")
    assert probe.scan_count == 1, f"Re-scan detected: {probe.scan_count} scans"

    # Or in a benchmark:
    report = probe.report
    print(report)  # ScanReport(scan_count=3, expected=1, regression=True)
"""
from __future__ import annotations

from dataclasses import dataclass


# ── Data ──────────────────────────────────────────────────────────────────

@dataclass
class ScanReport:
    scan_count:  int   = 0
    expected:    int   = 1
    table_name:  str   = ""
    calls:       list  = None

    def __post_init__(self):
        if self.calls is None:
            self.calls = []

    @property
    def regression(self) -> bool:
        return self.scan_count > self.expected

    @property
    def extra_scans(self) -> int:
        return max(0, self.scan_count - self.expected)

    def __str__(self) -> str:
        status = "🔴 REGRESSION" if self.regression else "✅ OK"
        return (
            f"ScanReport [{self.table_name}] {status} — "
            f"scans={self.scan_count} expected={self.expected} "
            f"extra={self.extra_scans}"
        )


# ── DuckDB catalog tracker ────────────────────────────────────────────────

# Object names that indicate current-state materialization
_CURRENT_STATE_NAMES = frozenset({
    "current_checksums",
    "current_hashes",
    "current_state",
})


def _is_state_object(name: str) -> bool:
    return any(n in name.lower() for n in _CURRENT_STATE_NAMES)

def _append_new(existing: list, new_set: set) -> None:
    for n in new_set:
        if n not in existing:
            existing.append(n)

class DuckDBObjectTracker:
    """
    Tracks DuckDB VIEW and TABLE creation during a processor run.

    Called from the MAIN THREAD at two checkpoints:
        1. wrap()     — baseline snapshot before process() starts
        2. snapshot() — delta snapshot after process() completes
        3. unwrap()   — calls snapshot() and stops tracking

    Thread safety: all methods must be called from the main thread.
    DuckDB catalog queries (duckdb_views, duckdb_tables) are safe on
    the main thread; they must NEVER be called from a background thread.

    Usage::

        tracker = DuckDBObjectTracker(con)
        tracker.wrap()                          # baseline before
        scd = proc.process(source, table)       # main thread execution
        tracker.snapshot()                      # delta after
        tracker.unwrap()
        print(tracker.report())

    The regression is present when:
        "current_checksums" appears in views_created
        (because LazySCDProcessor creates it as a VIEW)

    The fix is confirmed when:
        "current_checksums" or "current_hashes" appears in tables_created
        (because LazySCDMetadataProcessor and the fixed processors use TABLE)
    """

    def __init__(self, con):
        self._con = con
        self.views_created:  list[str] = []
        self.tables_created: list[str] = []
        self._tracking = False
        self._baseline_views:  set[str] = set()
        self._baseline_tables: set[str] = set()

    def _query_catalog(self, query: str) -> set[str]:
        """Execute a catalog query safely. Returns empty set on any error."""
        try:
            rows = self._con.execute(query).fetchall()
            return {r[0].lower() for r in rows}
        except Exception:
            return set()

    def _current_views(self) -> set[str]:
        return self._query_catalog(
            "SELECT view_name FROM duckdb_views() WHERE NOT internal"
        )

    def _current_tables(self) -> set[str]:
        return self._query_catalog(
            "SELECT table_name FROM duckdb_tables() WHERE NOT internal"
        )

    def wrap(self) -> "DuckDBObjectTracker":
        """Take baseline snapshot. Call before process() starts."""
        self._tracking = True
        self._baseline_views  = self._current_views()
        self._baseline_tables = self._current_tables()
        return self

    def snapshot(self) -> None:
        """
        Compute delta since baseline and add new objects to tracking lists.
        Call from the main thread after process() completes (or at any checkpoint).
        """
        if not self._tracking:
            return
        current_v = self._current_views()
        current_t = self._current_tables()
        new_views  = current_v  - self._baseline_views
        new_tables = current_t - self._baseline_tables
        _append_new(self.views_created, new_views)
        _append_new(self.tables_created, new_tables)

    def unwrap(self) -> None:
        """Final snapshot and stop tracking."""
        self.snapshot()
        self._tracking = False

    def __enter__(self) -> "DuckDBObjectTracker":
        return self.wrap()

    def __exit__(self, *_) -> None:
        self.unwrap()

    # ── Analysis ─────────────────────────────────────────────────────────

    @property
    def current_state_as_table(self) -> bool:
        """True when a current-state object was correctly created as TABLE."""
        return any(_is_state_object(n) for n in self.tables_created)

    @property
    def current_state_as_view(self) -> bool:
        """True when a current-state object was incorrectly created as VIEW (the bug)."""
        return any(_is_state_object(n) for n in self.views_created)

    def report(self) -> dict:
        return {
            "views_created":          self.views_created,
            "tables_created":         self.tables_created,
            "current_state_as_table": self.current_state_as_table,
            "current_state_as_view":  self.current_state_as_view,
            "regression_detected":    self.current_state_as_view,
        }


# ── ScanProbe — wraps a SinkAdapter to count current_state_sql() calls ───

class ScanProbe:
    """
    Context manager that intercepts sink.current_state_sql() calls.

    Each call represents one potential remote scan when the result is used
    as a VIEW. With the TABLE fix, there should be exactly 1 call per
    process() invocation (to build current_checksums TABLE), not 3+.

    Works with any SinkAdapter subclass. Replaces the method on the
    instance (not the class) so it doesn't affect other sink instances.

    Note: This probe is complementary to DuckDBObjectTracker.
    - DuckDBObjectTracker detects VIEW vs TABLE by inspecting the catalog.
    - ScanProbe counts how many times the remote source is queried.
    Both regressions are related: a VIEW causes multiple calls; a TABLE
    means one call during materialization, zero calls thereafter.
    """

    def __init__(self, sink, table_name: str = "", expected_scans: int = 1):
        self._sink       = sink
        self._table_name = table_name
        self._expected   = expected_scans
        self.report      = ScanReport(table_name=table_name, expected=expected_scans)
        self._original   = None

    def __enter__(self) -> "ScanProbe":
        self.report    = ScanReport(table_name=self._table_name, expected=self._expected)
        self._original = self._sink.__class__.current_state_sql

        probe = self

        def _intercepted(self_sink, table_name: str) -> str:
            probe.report.scan_count += 1
            probe.report.calls.append(table_name)
            return probe._original(self_sink, table_name)

        # Bind to instance, not class, so other sinks aren't affected
        import types
        self._sink.current_state_sql = types.MethodType(_intercepted, self._sink)
        return self

    def __exit__(self, *_) -> None:
        if self._original is not None:
            # Restore original by deleting instance-level override
            try:
                del self._sink.current_state_sql
            except AttributeError:
                pass

    @property
    def scan_count(self) -> int:
        return self.report.scan_count
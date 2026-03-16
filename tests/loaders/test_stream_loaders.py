"""
Phase 2 tests — load_stream() on LazyTransactionLoader and LazySnapshotLoader.

All tests use StubStreamSource (finite, no real Kafka/Kinesis) and
InMemorySink (shared DuckDB in-memory connection).

Coverage targets
----------------
sqldim/loaders/snapshot.py  → load_stream() on LazyTransactionLoader &
                               LazySnapshotLoader
"""
from __future__ import annotations

import duckdb
import pytest

from sqldim.loaders.snapshot import LazyTransactionLoader, LazySnapshotLoader
from sqldim.sources.stream import StreamResult


# ---------------------------------------------------------------------------
# StubStreamSource
# ---------------------------------------------------------------------------

class StubStreamSource:
    """Yields ``SELECT * FROM {view}`` for each pre-registered view name."""

    def __init__(self, views: list[str]):
        self._views = views
        self._idx = 0
        self.committed: list = []

    def stream(self, con, batch_size=10_000):
        for view in self._views:
            self._idx += 1
            yield f"SELECT * FROM {view}"

    def commit(self, offset) -> None:
        self.committed.append(offset)

    def checkpoint(self):
        return self._idx


# ---------------------------------------------------------------------------
# InMemorySink
# ---------------------------------------------------------------------------

class InMemorySink:
    def current_state_sql(self, table_name: str) -> str:
        return f"SELECT * FROM {table_name}"

    def write(self, con, view_name, table_name, batch_size=100_000):
        n = con.execute(f"SELECT count(*) FROM {view_name}").fetchone()[0]
        try:
            con.execute(f"INSERT INTO {table_name} BY NAME SELECT * FROM {view_name}")
        except Exception:
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {view_name}")
        return n


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _register_batch(con, view_name, rows: list[dict]):
    select_rows = " UNION ALL ".join(
        "SELECT " + ", ".join(
            f"'{v}' AS {k}" if isinstance(v, str) else f"{v} AS {k}"
            for k, v in row.items()
        )
        for row in rows
    )
    con.execute(f"CREATE OR REPLACE VIEW {view_name} AS {select_rows}")


# ---------------------------------------------------------------------------
# LazyTransactionLoader.load_stream()
# ---------------------------------------------------------------------------

class TestLazyTransactionLoaderStream:

    def _make(self, con):
        sink = InMemorySink()
        return LazyTransactionLoader(sink, con=con), sink

    def test_single_batch_appends_rows(self):
        con = duckdb.connect()
        _register_batch(con, "b1", [
            {"event_id": "1", "event_type": "click"},
            {"event_id": "2", "event_type": "view"},
        ])
        loader, _ = self._make(con)
        result = loader.load_stream(StubStreamSource(["b1"]), "fact_events")
        assert result.inserted == 2
        assert result.batches_processed == 1
        assert result.batches_failed == 0

    def test_two_batches_accumulate_inserts(self):
        con = duckdb.connect()
        _register_batch(con, "b1", [{"event_id": "1", "event_type": "click"}])
        _register_batch(con, "b2", [{"event_id": "2", "event_type": "view"},
                                    {"event_id": "3", "event_type": "purchase"}])
        loader, _ = self._make(con)
        result = loader.load_stream(StubStreamSource(["b1", "b2"]), "fact_events")
        assert result.inserted == 3
        assert result.batches_processed == 2

    def test_rows_written_to_target_table(self):
        con = duckdb.connect()
        _register_batch(con, "b1", [{"val": "10"}, {"val": "20"}])
        loader, _ = self._make(con)
        loader.load_stream(StubStreamSource(["b1"]), "fact_vals")
        count = con.execute("SELECT count(*) FROM fact_vals").fetchone()[0]
        assert count == 2

    def test_max_batches_limits_loading(self):
        con = duckdb.connect()
        _register_batch(con, "b1", [{"v": "1"}])
        _register_batch(con, "b2", [{"v": "2"}])
        _register_batch(con, "b3", [{"v": "3"}])
        loader, _ = self._make(con)
        result = loader.load_stream(
            StubStreamSource(["b1", "b2", "b3"]), "fact_t", max_batches=2
        )
        assert result.batches_processed == 2
        count = con.execute("SELECT count(*) FROM fact_t").fetchone()[0]
        assert count == 2

    def test_max_batches_zero_processes_nothing(self):
        con = duckdb.connect()
        _register_batch(con, "b1", [{"v": "1"}])
        loader, _ = self._make(con)
        result = loader.load_stream(
            StubStreamSource(["b1"]), "fact_t", max_batches=0
        )
        assert result.batches_processed == 0
        assert result.inserted == 0

    def test_on_batch_callback_invoked(self):
        con = duckdb.connect()
        _register_batch(con, "b1", [{"v": "1"}])
        _register_batch(con, "b2", [{"v": "2"}, {"v": "3"}])
        calls = []
        loader, _ = self._make(con)
        loader.load_stream(
            StubStreamSource(["b1", "b2"]), "fact_t",
            on_batch=lambda i, n: calls.append((i, n)),
        )
        assert len(calls) == 2
        assert calls[0] == (0, 1)
        assert calls[1] == (1, 2)

    def test_commit_called_after_each_batch(self):
        con = duckdb.connect()
        _register_batch(con, "b1", [{"v": "1"}])
        _register_batch(con, "b2", [{"v": "2"}])
        source = StubStreamSource(["b1", "b2"])
        loader, _ = self._make(con)
        loader.load_stream(source, "fact_t")
        assert len(source.committed) == 2

    def test_commit_not_called_on_failed_batch(self):
        con = duckdb.connect()
        source = StubStreamSource(["no_such_view"])
        loader, _ = self._make(con)
        result = loader.load_stream(source, "fact_t")
        assert result.batches_failed == 1
        assert len(source.committed) == 0

    def test_failed_batch_does_not_halt_remaining(self):
        con = duckdb.connect()
        _register_batch(con, "b1", [{"v": "1"}])
        _register_batch(con, "b3", [{"v": "3"}])
        # b2 is missing
        source = StubStreamSource(["b1", "missing", "b3"])
        loader, _ = self._make(con)
        result = loader.load_stream(source, "fact_t")
        assert result.batches_failed == 1
        assert result.batches_processed == 2

    def test_returns_stream_result_type(self):
        con = duckdb.connect()
        _register_batch(con, "b1", [{"v": "1"}])
        loader, _ = self._make(con)
        result = loader.load_stream(StubStreamSource(["b1"]), "fact_t")
        assert isinstance(result, StreamResult)

    def test_drop_view_exception_in_finally_is_swallowed(self):
        """DROP VIEW exception raised in finally block must not propagate."""
        con = duckdb.connect()
        _register_batch(con, "b1", [{"v": "1"}])
        loader, _ = self._make(con)

        class _DroppingFails:
            """Wraps real connection but raises on DROP VIEW."""
            def __init__(self, real):
                self._real = real
            def __getattr__(self, name):
                return getattr(self._real, name)
            def execute(self, sql, *args, **kwargs):
                if "DROP VIEW" in sql:
                    raise Exception("DROP failed")
                return self._real.execute(sql, *args, **kwargs)

        loader._con = _DroppingFails(con)
        result = loader.load_stream(StubStreamSource(["b1"]), "fact_t")
        # Batch should still complete despite DROP raising in finally
        assert result.batches_processed == 1


# ---------------------------------------------------------------------------
# LazySnapshotLoader.load_stream()
# ---------------------------------------------------------------------------

class TestLazySnapshotLoaderStream:

    def _make(self, con, snapshot_date="2024-03-31", date_field="snapshot_date"):
        sink = InMemorySink()
        return LazySnapshotLoader(
            sink, snapshot_date=snapshot_date, date_field=date_field, con=con
        ), sink

    def test_single_batch_injects_snapshot_date(self):
        con = duckdb.connect()
        _register_batch(con, "b1", [
            {"account_id": "ACC1", "balance": "100.0"},
        ])
        loader, _ = self._make(con)
        loader.load_stream(StubStreamSource(["b1"]), "fact_balance")
        row = con.execute("SELECT snapshot_date FROM fact_balance").fetchone()
        assert str(row[0]) == "2024-03-31"

    def test_two_batches_written_with_same_snapshot_date(self):
        con = duckdb.connect()
        _register_batch(con, "b1", [{"account_id": "ACC1", "balance": "100"}])
        _register_batch(con, "b2", [{"account_id": "ACC2", "balance": "200"}])
        loader, _ = self._make(con, snapshot_date="2024-06-30")
        result = loader.load_stream(StubStreamSource(["b1", "b2"]), "fact_balance")
        assert result.inserted == 2
        assert result.batches_processed == 2
        dates = con.execute(
            "SELECT DISTINCT snapshot_date FROM fact_balance"
        ).fetchall()
        assert len(dates) == 1
        assert str(dates[0][0]) == "2024-06-30"

    def test_custom_date_field_name(self):
        con = duckdb.connect()
        _register_batch(con, "b1", [{"id": "X", "val": "5"}])
        loader, _ = self._make(con, snapshot_date="2024-01-15",
                               date_field="period_date")
        loader.load_stream(StubStreamSource(["b1"]), "fact_snap")
        row = con.execute("SELECT period_date FROM fact_snap").fetchone()
        assert str(row[0]) == "2024-01-15"

    def test_max_batches_limits_snapshot_loading(self):
        con = duckdb.connect()
        _register_batch(con, "b1", [{"id": "1", "v": "a"}])
        _register_batch(con, "b2", [{"id": "2", "v": "b"}])
        loader, _ = self._make(con)
        result = loader.load_stream(
            StubStreamSource(["b1", "b2"]), "fact_snap", max_batches=1
        )
        assert result.batches_processed == 1

    def test_on_batch_callback_invoked(self):
        con = duckdb.connect()
        _register_batch(con, "b1", [{"id": "1", "v": "a"}])
        calls = []
        loader, _ = self._make(con)
        loader.load_stream(
            StubStreamSource(["b1"]), "fact_snap",
            on_batch=lambda i, n: calls.append((i, n)),
        )
        assert calls == [(0, 1)]

    def test_commit_called_after_successful_batch(self):
        con = duckdb.connect()
        _register_batch(con, "b1", [{"id": "1", "v": "a"}])
        source = StubStreamSource(["b1"])
        loader, _ = self._make(con)
        loader.load_stream(source, "fact_snap")
        assert len(source.committed) == 1

    def test_failed_batch_increments_counter(self):
        con = duckdb.connect()
        source = StubStreamSource(["no_such_view"])
        loader, _ = self._make(con)
        result = loader.load_stream(source, "fact_snap")
        assert result.batches_failed == 1
        assert result.batches_processed == 0

    def test_returns_stream_result_type(self):
        con = duckdb.connect()
        _register_batch(con, "b1", [{"id": "1", "v": "x"}])
        loader, _ = self._make(con)
        result = loader.load_stream(StubStreamSource(["b1"]), "fact_snap")
        assert isinstance(result, StreamResult)

    def test_drop_view_exception_in_finally_is_swallowed(self):
        """DROP VIEW exception raised in finally block must not propagate."""
        con = duckdb.connect()
        _register_batch(con, "b1", [{"id": "1", "v": "x"}])
        loader, _ = self._make(con)

        class _DroppingFails:
            def __init__(self, real):
                self._real = real
            def __getattr__(self, name):
                return getattr(self._real, name)
            def execute(self, sql, *args, **kwargs):
                if "DROP VIEW" in sql:
                    raise Exception("DROP failed")
                return self._real.execute(sql, *args, **kwargs)

        loader._con = _DroppingFails(con)
        result = loader.load_stream(StubStreamSource(["b1"]), "fact_snap")
        # Batch should still complete despite DROP raising in finally
        assert result.batches_processed == 1

"""
Pure unit tests for sqldim/sinks/postgresql.py — no live PostgreSQL required.

Covers the lines that cannot be reached by the integration test suite when
pytest-postgresql is unavailable (or postgres is not installed):
  - current_state_sql() branch where self._con is not None  (line 33)
  - write()       returns 0 for empty view                  (line 60)
  - write_named() returns 0 for empty view                  (line 109)
  - write_named() complete happy-path (lines 108–136) via mock
"""
from __future__ import annotations

from unittest.mock import MagicMock, call

from sqldim.sinks.postgresql import PostgreSQLSink


class TestCurrentStateSqlWithConSet:
    def test_uses_alias_path_when_con_is_set(self):
        """When _con is not None, return alias.schema.table (not postgres_scan)."""
        sink = PostgreSQLSink("host=127.0.0.1 port=5432 user=pg dbname=db")
        sink._con = object()   # any non-None value triggers the alias branch
        sql = sink.current_state_sql("dim_emp")
        assert "sqldim_pg" in sql
        assert "public" in sql
        assert "dim_emp" in sql
        assert "postgres_scan" not in sql

    def test_alias_path_includes_custom_schema(self):
        sink = PostgreSQLSink("dsn=ignored", schema="analytics")
        sink._con = object()
        sql = sink.current_state_sql("dim_emp")
        assert "analytics" in sql
        assert "postgres_scan" not in sql


class TestWriteEmptyView:
    def test_returns_zero_without_executing_insert(self):
        """write() must return 0 immediately if the source view is empty."""
        sink = PostgreSQLSink("dsn=ignored")
        con = MagicMock()
        con.execute.return_value.fetchone.return_value = (0,)
        result = sink.write(con, "empty_view", "dim_emp")
        assert result == 0
        # Only one execute call (the COUNT), no INSERT
        assert con.execute.call_count == 1

    def test_called_with_count_query(self):
        sink = PostgreSQLSink("dsn=ignored")
        con = MagicMock()
        con.execute.return_value.fetchone.return_value = (0,)
        sink.write(con, "empty_view", "dim_emp")
        called_sql = con.execute.call_args[0][0]
        assert "SELECT count(*)" in called_sql
        assert "empty_view" in called_sql


class TestWriteNamedEmptyView:
    def test_returns_zero_without_executing_insert(self):
        """write_named() must return 0 immediately if source view is empty."""
        sink = PostgreSQLSink("dsn=ignored")
        con = MagicMock()
        con.execute.return_value.fetchone.return_value = (0,)
        result = sink.write_named(con, "empty_view", "dim_emp", ["emp_id", "dept"])
        assert result == 0
        assert con.execute.call_count == 1

    def test_called_with_count_query(self):
        sink = PostgreSQLSink("dsn=ignored")
        con = MagicMock()
        con.execute.return_value.fetchone.return_value = (0,)
        sink.write_named(con, "empty_view", "dim_emp", ["emp_id"])
        called_sql = con.execute.call_args[0][0]
        assert "SELECT count(*)" in called_sql
        assert "empty_view" in called_sql


class TestWriteNamedHappyPath:
    def _con_with_count(self, n: int) -> MagicMock:
        """Return a mock DuckDB connection that reports *n* rows in the view."""
        con = MagicMock()
        con.execute.return_value.fetchone.return_value = (n,)
        return con

    def test_returns_total_rows_inserted(self):
        sink = PostgreSQLSink("dsn=ignored")
        con = self._con_with_count(3)
        result = sink.write_named(con, "new_rows", "dim_emp", ["emp_id", "dept"])
        assert result == 3

    def test_executes_insert_with_column_list(self):
        sink = PostgreSQLSink("dsn=ignored")
        con = self._con_with_count(2)
        sink.write_named(con, "new_rows", "dim_emp", ["emp_id", "dept", "valid_from"])
        # Collect all execute call args
        all_calls = [c[0][0] for c in con.execute.call_args_list]
        insert_calls = [s for s in all_calls if "INSERT INTO" in s]
        assert len(insert_calls) == 1
        assert "emp_id, dept, valid_from" in insert_calls[0]
        assert "new_rows" in insert_calls[0]

    def test_insert_goes_to_alias_schema_table(self):
        sink = PostgreSQLSink("dsn=ignored", schema="analytics")
        con = self._con_with_count(1)
        sink.write_named(con, "new_rows", "dim_emp", ["emp_id"])
        all_calls = [c[0][0] for c in con.execute.call_args_list]
        insert_calls = [s for s in all_calls if "INSERT INTO" in s]
        assert "sqldim_pg.analytics.dim_emp" in insert_calls[0]

    def test_single_chunk_no_debug_log(self):
        """With 1 chunk, n_chunks == 1, so no debug _log.debug() call should branch."""
        import logging
        sink = PostgreSQLSink("dsn=ignored")
        con = self._con_with_count(5)
        # Should not raise; n_chunks=1 means the debug branch is skipped
        result = sink.write_named(con, "new_rows", "dim_emp", ["emp_id"], batch_size=100)
        assert result == 5


class TestCloseVersions:
    """Covers postgresql.py line 172 (single-column else branch) and the
    composite-key isinstance branch."""

    def _mock_con(self, count=2):
        from unittest.mock import MagicMock
        con = MagicMock()
        con.execute.return_value.fetchone.return_value = (count,)
        return con

    def test_single_column_nk_string_uses_simple_join(self):
        """String nk_col → else branch: join_cond = 't.col = n.col' (line 172)."""
        sink = PostgreSQLSink("dsn=ignored")
        con = self._mock_con(2)
        result = sink.close_versions(con, "dim_emp", "emp_id", "changed_nks", "2099-01-01")
        assert result == 2
        all_sql = [c[0][0] for c in con.execute.call_args_list]
        update_sql = next(s for s in all_sql if "UPDATE" in s)
        assert "t.emp_id = n.emp_id" in update_sql

    def test_composite_nk_list_uses_and_join(self):
        """List nk_col → isinstance branch: conditions joined with AND."""
        sink = PostgreSQLSink("dsn=ignored")
        con = self._mock_con(1)
        result = sink.close_versions(
            con, "dim_ol", ["order_id", "line_no"], "chg", "2099-01-01"
        )
        assert result == 1
        all_sql = [c[0][0] for c in con.execute.call_args_list]
        update_sql = next(s for s in all_sql if "UPDATE" in s)
        assert "t.order_id = n.order_id" in update_sql
        assert "t.line_no = n.line_no" in update_sql

"""
Tests for sqldim/sinks/delta.py — DeltaLakeSink.

Strategy
--------
* current_state_sql()  — pure SQL-string tests + one real delta_scan read test
* close_versions()     — no DB needed (always returns 0)
* write / update_attributes / rotate_attributes / update_milestones
                       — DuckDB's delta extension in 1.5.x doesn't support
                         MERGE INTO; we mock the DuckDB connection so the SQL
                         generation paths are covered without blocking on that
                         extension limitation.
* __enter__ / __exit__ — patch duckdb.connect to avoid extension install in CI.
"""
from __future__ import annotations
from unittest.mock import MagicMock, patch, call
import duckdb
import pytest

from sqldim.sinks.delta import DeltaLakeSink


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_con(row_count: int = 5) -> MagicMock:
    """Return a MagicMock DuckDB connection whose fetchone() yields (row_count,)."""
    con = MagicMock(spec=duckdb.DuckDBPyConnection)
    con.execute.return_value.fetchone.return_value = (row_count,)
    return con


# ---------------------------------------------------------------------------
# current_state_sql — pure string tests
# ---------------------------------------------------------------------------


class TestDeltaLakeSinkCurrentStateSql:
    def test_contains_delta_scan(self):
        sink = DeltaLakeSink("/data/lake")
        assert "delta_scan" in sink.current_state_sql("dim_player")

    def test_contains_path_and_table(self):
        sink = DeltaLakeSink("/data/lake")
        sql = sink.current_state_sql("dim_player")
        assert "/data/lake/dim_player" in sql

    def test_real_delta_read(self, tmp_path):
        """current_state_sql points DuckDB at a real Delta table."""
        pytest.importorskip("deltalake")
        import pyarrow as pa
        from deltalake import write_deltalake

        table_path = str(tmp_path / "dim_player")
        write_deltalake(
            table_path,
            pa.table({
                "player_id": pa.array([1, 2], type=pa.int64()),
                "name":      pa.array(["Alice", "Bob"], type=pa.string()),
            }),
        )

        sink = DeltaLakeSink(str(tmp_path))
        sql  = sink.current_state_sql("dim_player")

        con = duckdb.connect()
        try:
            con.execute("INSTALL delta; LOAD delta;")
            rows = con.execute(f"SELECT * FROM {sql}").fetchall()
        finally:
            con.close()

        assert len(rows) == 2


# ---------------------------------------------------------------------------
# close_versions — always no-op (handled by write's MERGE INTO)
# ---------------------------------------------------------------------------


class TestDeltaLakeSinkCloseVersions:
    def test_returns_zero(self):
        sink = DeltaLakeSink("/data/lake")
        con  = _mock_con()
        result = sink.close_versions(con, "dim_player", "player_id", "v_nk", "2024-12-31")
        assert result == 0

    def test_does_not_execute_sql(self):
        sink = DeltaLakeSink("/data/lake")
        con  = _mock_con()
        sink.close_versions(con, "dim_player", "player_id", "v_nk", "2024-12-31")
        con.execute.assert_not_called()


# ---------------------------------------------------------------------------
# write
# ---------------------------------------------------------------------------


class TestDeltaLakeSinkWrite:
    def test_execute_called_twice(self):
        """Expect at least a MERGE INTO call and a SELECT count(*) call."""
        sink = DeltaLakeSink("/data/lake", natural_key="player_id")
        con  = _mock_con(3)
        sink.write(con, "v_source", "dim_player")
        assert con.execute.call_count == 2

    def test_merge_sql_contains_natural_key(self):
        sink = DeltaLakeSink("/data/lake", natural_key="pid")
        con  = _mock_con()
        sink.write(con, "v_source", "dim_player")
        merge_sql = con.execute.call_args_list[0][0][0]
        assert "pid" in merge_sql

    def test_merge_sql_contains_path(self):
        sink = DeltaLakeSink("/data/lake", natural_key="player_id")
        con  = _mock_con()
        sink.write(con, "v_source", "dim_player")
        merge_sql = con.execute.call_args_list[0][0][0]
        assert "/data/lake/dim_player" in merge_sql

    def test_returns_source_count(self):
        sink = DeltaLakeSink("/data/lake")
        con  = _mock_con(7)
        result = sink.write(con, "v_source", "dim_player")
        assert result == 7

    def test_merge_sql_contains_is_current(self):
        sink = DeltaLakeSink("/data/lake")
        con  = _mock_con()
        sink.write(con, "v_source", "dim_player")
        merge_sql = con.execute.call_args_list[0][0][0]
        assert "is_current" in merge_sql


# ---------------------------------------------------------------------------
# update_attributes
# ---------------------------------------------------------------------------


class TestDeltaLakeSinkUpdateAttributes:
    def test_set_clause_contains_columns(self):
        sink = DeltaLakeSink("/data/lake")
        con  = _mock_con(2)
        sink.update_attributes(con, "dim_player", "pid", "v_updates", ["rating", "team"])
        merge_sql = con.execute.call_args_list[0][0][0]
        assert "rating" in merge_sql
        assert "team"   in merge_sql

    def test_returns_source_count(self):
        sink = DeltaLakeSink("/data/lake")
        con  = _mock_con(4)
        result = sink.update_attributes(con, "dim_player", "pid", "v_updates", ["rating"])
        assert result == 4

    def test_on_clause_uses_nk_col(self):
        sink = DeltaLakeSink("/data/lake")
        con  = _mock_con()
        sink.update_attributes(con, "dim_player", "my_key", "v_updates", ["col1"])
        merge_sql = con.execute.call_args_list[0][0][0]
        assert "my_key" in merge_sql


# ---------------------------------------------------------------------------
# rotate_attributes
# ---------------------------------------------------------------------------


class TestDeltaLakeSinkRotateAttributes:
    def test_set_clause_contains_both_columns(self):
        sink = DeltaLakeSink("/data/lake")
        con  = _mock_con()
        sink.rotate_attributes(
            con, "dim_player", "pid", "v_rot",
            column_pairs=[("rating", "prev_rating")],
        )
        merge_sql = con.execute.call_args_list[0][0][0]
        assert "rating"      in merge_sql
        assert "prev_rating" in merge_sql

    def test_returns_source_count(self):
        sink = DeltaLakeSink("/data/lake")
        con  = _mock_con(6)
        result = sink.rotate_attributes(
            con, "dim_player", "pid", "v_rot",
            column_pairs=[("rating", "prev_rating")],
        )
        assert result == 6


# ---------------------------------------------------------------------------
# update_milestones
# ---------------------------------------------------------------------------


class TestDeltaLakeSinkUpdateMilestones:
    def test_coalesce_clause_contains_columns(self):
        sink = DeltaLakeSink("/data/lake")
        con  = _mock_con()
        sink.update_milestones(con, "dim_player", "pid", "v_ms", ["first_game", "last_game"])
        merge_sql = con.execute.call_args_list[0][0][0]
        assert "first_game" in merge_sql
        assert "last_game"  in merge_sql
        assert "COALESCE"   in merge_sql

    def test_returns_source_count(self):
        sink = DeltaLakeSink("/data/lake")
        con  = _mock_con(9)
        result = sink.update_milestones(
            con, "dim_player", "pid", "v_ms", ["first_game"]
        )
        assert result == 9


# ---------------------------------------------------------------------------
# __init__ defaults
# ---------------------------------------------------------------------------


class TestDeltaLakeSinkInit:
    def test_default_natural_key(self):
        sink = DeltaLakeSink("/data/lake")
        assert sink._natural_key == "id"
        assert sink._con         is None

    def test_custom_path_and_key(self):
        sink = DeltaLakeSink("/custom/path", natural_key="uuid")
        assert sink._path        == "/custom/path"
        assert sink._natural_key == "uuid"


# ---------------------------------------------------------------------------
# __enter__ / __exit__ — context manager
# ---------------------------------------------------------------------------


class TestDeltaLakeSinkContextManager:
    def test_enter_sets_con_and_loads_extension(self):
        sink = DeltaLakeSink("/data/lake")
        mock_con = MagicMock()

        with patch("sqldim.sinks.delta.duckdb.connect", return_value=mock_con):
            with sink:
                assert sink._con is mock_con
                mock_con.execute.assert_any_call("INSTALL delta; LOAD delta;")

    def test_exit_closes_connection(self):
        sink = DeltaLakeSink("/data/lake")
        mock_con = MagicMock()

        with patch("sqldim.sinks.delta.duckdb.connect", return_value=mock_con):
            with sink:
                pass

        mock_con.close.assert_called_once()

    def test_exit_with_no_con_is_safe(self):
        sink = DeltaLakeSink("/data/lake")
        # __exit__ with _con = None should not raise
        sink.__exit__(None, None, None)

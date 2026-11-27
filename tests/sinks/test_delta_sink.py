"""
Tests for sqldim/sinks/delta.py — DeltaLakeSink.

Strategy
--------
* current_state_sql()  — pure SQL-string tests + one real delta_scan read test
* write()              — ATTACH … (TYPE delta) + INSERT INTO, checked on a mock
                         connection; the real append is exercised by the
                         conformance backend ``sink-delta``
* close_versions / update_attributes / rotate_attributes / update_milestones
                       — refused with SinkUnsupported, nothing sent (#178)
* __enter__ / __exit__ — patch duckdb.connect to avoid extension install in CI.
"""

from __future__ import annotations
from unittest.mock import MagicMock, patch
import duckdb
import pytest

from sqldim.sinks import SinkUnsupported
from sqldim.sinks.file.delta import DeltaLakeSink


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
            pa.table(
                {
                    "player_id": pa.array([1, 2], type=pa.int64()),
                    "name": pa.array(["Alice", "Bob"], type=pa.string()),
                }
            ),
        )

        sink = DeltaLakeSink(str(tmp_path))
        sql = sink.current_state_sql("dim_player")

        con = duckdb.connect()
        try:
            con.execute("INSTALL delta; LOAD delta;")
            rows = con.execute(f"SELECT * FROM {sql}").fetchall()
        finally:
            con.close()

        assert len(rows) == 2


# ---------------------------------------------------------------------------
# write — APPEND through an attached Delta table (§6.2)
# ---------------------------------------------------------------------------


def _write_con(columns=("id", "name"), row_count: int = 3) -> MagicMock:
    con = MagicMock(spec=duckdb.DuckDBPyConnection)
    con.execute.return_value.fetchall.return_value = [(c,) for c in columns]
    con.execute.return_value.fetchone.return_value = (row_count,)
    return con


def _sent(con: MagicMock) -> list[str]:
    return [c[0][0] for c in con.execute.call_args_list]


class TestDeltaLakeSinkWrite:
    def test_attaches_inserts_and_detaches(self):
        sink = DeltaLakeSink("/data/lake")
        con = _write_con()
        assert sink.write(con, "v_source", "dim_player") == 3
        sent = _sent(con)
        assert sent[0].startswith("ATTACH '/data/lake/dim_player' AS sqldim_delta_")
        assert sent[0].endswith("(TYPE delta)")
        alias = sent[0].split(" AS ")[1].split()[0]
        assert f'INSERT INTO {alias} SELECT "id", "name" FROM v_source' in sent
        assert sent[-1] == f"DETACH {alias}"

    def test_never_targets_delta_scan_or_spark_sql(self):
        con = _write_con()
        DeltaLakeSink("/data/lake").write(con, "v", "t")
        for sql in _sent(con):
            assert "delta_scan" not in sql
            assert "delta.`" not in sql
            assert "MERGE" not in sql

    def test_every_statement_parses_as_duckdb(self):
        con = _write_con()
        DeltaLakeSink("/data/lake").write(con, "v", "t")
        parser = duckdb.connect()
        for sql in _sent(con):
            assert parser.extract_statements(sql)

    def test_missing_table_is_refused_and_detached(self):
        con = _write_con(columns=())
        with pytest.raises(SinkUnsupported, match="DuckDB 2.0"):
            DeltaLakeSink("/data/lake").write(con, "v", "t")
        assert _sent(con)[-1].startswith("DETACH ")
        assert not any(s.startswith("INSERT") for s in _sent(con))


# ---------------------------------------------------------------------------
# In-place changes — refused (§6.2: the delta extension appends only)
# ---------------------------------------------------------------------------


class TestDeltaLakeSinkRefusals:
    @pytest.mark.parametrize(
        ("method", "args"),
        [
            ("close_versions", ("dim", "id", "v_nk", "2024-12-31")),
            ("update_attributes", ("dim", "id", "v_up", ["rating"])),
            ("rotate_attributes", ("dim", "id", "v_rot", [("a", "prev_a")])),
            ("update_milestones", ("dim", "id", "v_ms", ["shipped_at"])),
        ],
    )
    def test_refused_without_sending_anything(self, method, args):
        con = _mock_con()
        with pytest.raises(SinkUnsupported, match="appends only") as info:
            getattr(DeltaLakeSink("/data/lake"), method)(con, *args)
        assert info.value.code == "UNSUPPORTED"
        con.execute.assert_not_called()


# ---------------------------------------------------------------------------
# __init__ defaults
# ---------------------------------------------------------------------------


class TestDeltaLakeSinkInit:
    def test_default_natural_key(self):
        sink = DeltaLakeSink("/data/lake")
        assert sink._natural_key == "id"
        assert sink._con is None

    def test_custom_path_and_key(self):
        sink = DeltaLakeSink("/custom/path", natural_key="uuid")
        assert sink._path == "/custom/path"
        assert sink._natural_key == "uuid"


# ---------------------------------------------------------------------------
# __enter__ / __exit__ — context manager
# ---------------------------------------------------------------------------


class TestDeltaLakeSinkContextManager:
    def test_enter_sets_con_and_loads_extension(self):
        sink = DeltaLakeSink("/data/lake")
        mock_con = MagicMock()

        with patch("sqldim.sinks.file.delta.duckdb.connect", return_value=mock_con):
            with sink:
                assert sink._con is mock_con
                mock_con.execute.assert_any_call("INSTALL delta; LOAD delta;")

    def test_exit_closes_connection(self):
        sink = DeltaLakeSink("/data/lake")
        mock_con = MagicMock()

        with patch("sqldim.sinks.file.delta.duckdb.connect", return_value=mock_con):
            with sink:
                pass

        mock_con.close.assert_called_once()

    def test_exit_with_no_con_is_safe(self):
        sink = DeltaLakeSink("/data/lake")
        # __exit__ with _con = None should not raise
        sink.__exit__(None, None, None)

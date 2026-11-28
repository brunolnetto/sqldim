"""
Tests for sqldim/sinks/delta.py — DeltaLakeSink.

Strategy
--------
* current_state_sql()  — pure SQL-string tests + one real delta_scan read test
* write()              — ATTACH … (TYPE delta) + INSERT INTO … BY NAME (or
                         CREATE TABLE <alias> AS on the first write), checked
                         on a mock connection and on real Delta storage read
                         back after DETACH (DuckDB 2.0.1, #193)
* close_versions / update_attributes / rotate_attributes / update_milestones
                       — refused with SinkUnsupported, nothing sent (#178)
* __enter__ / __exit__ — patch duckdb.connect to avoid extension install in CI.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from sqldim.exceptions import SchemaError
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


def _write_con(exists: bool = True, row_count: int = 3) -> MagicMock:
    """A mock connection: the table exists (or not), the view has *row_count* rows."""
    con = MagicMock(spec=duckdb.DuckDBPyConnection)
    con.execute.return_value.fetchall.return_value = [("id",), ("name",)]
    con.execute.return_value.fetchone.side_effect = [(int(exists),), (row_count,)]
    return con


def _sent(con: MagicMock) -> list[str]:
    return [c[0][0] for c in con.execute.call_args_list]


class TestDeltaLakeSinkWrite:
    def test_attaches_inserts_by_name_and_detaches(self):
        sink = DeltaLakeSink("/data/lake")
        con = _write_con()
        assert sink.write(con, "v_source", "dim_player") == 3
        sent = _sent(con)
        assert sent[0].startswith("ATTACH '/data/lake/dim_player' AS sqldim_delta_")
        assert sent[0].endswith("(TYPE delta)")
        alias = sent[0].split(" AS ")[1].split()[0]
        assert f"INSERT INTO {alias} BY NAME SELECT * FROM v_source" in sent
        assert not any("duckdb_columns" in s for s in sent)
        assert sent[-1] == f"DETACH {alias}"

    def test_missing_table_is_created_by_the_first_write(self):
        con = _write_con(exists=False)
        assert DeltaLakeSink("/data/lake").write(con, "v", "t") == 3
        sent = _sent(con)
        alias = sent[0].split(" AS ")[1].split()[0]
        assert f"CREATE TABLE {alias}.{alias} AS SELECT * FROM v" in sent
        assert not any(s.startswith("INSERT") for s in sent)
        assert sent[-1] == f"DETACH {alias}"

    def test_never_targets_delta_scan_or_spark_sql(self):
        con = _write_con()
        DeltaLakeSink("/data/lake").write(con, "v", "t")
        for sql in _sent(con):
            assert "delta_scan" not in sql
            assert "delta.`" not in sql
            assert "MERGE" not in sql

    def test_every_statement_parses_as_duckdb(self):
        for exists in (True, False):
            con = _write_con(exists=exists)
            DeltaLakeSink("/data/lake").write(con, "v", "t")
            parser = duckdb.connect()
            for sql in _sent(con):
                assert parser.extract_statements(sql)


# ---------------------------------------------------------------------------
# Real Delta storage (DuckDB 2.0.1 delta extension, #193)
# ---------------------------------------------------------------------------


@pytest.fixture
def delta_con():
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta")
    con.execute(
        "CREATE TABLE src AS SELECT * FROM (VALUES "
        "('k1', 'a', TIMESTAMP '2024-01-01 10:00'), ('k2', 'b', NULL)) "
        "t(id, name, valid_to)"
    )
    yield con
    con.close()


def _scan(path: str) -> list[tuple]:
    """Read the table back on a new connection: what was committed to disk."""
    con = duckdb.connect()
    try:
        con.execute("LOAD delta")
        return con.execute(f"SELECT * FROM delta_scan('{path}') ORDER BY id").fetchall()
    finally:
        con.close()


class TestDeltaLakeSinkStorage:
    def test_write_persists_after_detach(self, tmp_path, delta_con):
        """write, DETACH, read back by delta_scan and by a new ATTACH."""
        sink = DeltaLakeSink(str(tmp_path))
        assert sink.write(delta_con, "src", "dim") == 2  # creates the table
        assert sink.write(delta_con, "src", "dim") == 2  # appends by name
        assert sorted(os.listdir(tmp_path / "dim" / "_delta_log")) == [
            f"{0:020d}.json",
            f"{1:020d}.json",
        ]
        rows = _scan(str(tmp_path / "dim"))
        assert [r[:2] for r in rows] == [("k1", "a")] * 2 + [("k2", "b")] * 2
        detached = delta_con.execute(
            "SELECT count(*) FROM duckdb_databases() "
            "WHERE database_name LIKE 'sqldim_delta_%'"
        ).fetchone()
        assert detached == (0,)
        delta_con.execute(f"ATTACH '{tmp_path}/dim' AS again (TYPE delta)")
        assert delta_con.execute("SELECT count(*) FROM again").fetchone() == (4,)
        delta_con.execute("DETACH again")

    def test_insert_by_name_ignores_column_order(self, tmp_path, delta_con):
        sink = DeltaLakeSink(str(tmp_path))
        sink.write(delta_con, "src", "dim")
        delta_con.execute(
            "CREATE VIEW shuffled AS SELECT NULL::TIMESTAMP AS valid_to, 'c' AS name, 'k3' AS id"
        )
        sink.write(delta_con, "shuffled", "dim")
        assert _scan(str(tmp_path / "dim"))[-1] == ("k3", "c", None)

    def test_unknown_column_is_refused_before_a_commit(self, tmp_path, delta_con):
        sink = DeltaLakeSink(str(tmp_path))
        sink.write(delta_con, "src", "dim")
        delta_con.execute("CREATE VIEW extra AS SELECT *, 1 AS surplus FROM src")
        with pytest.raises(SchemaError, match="surplus"):
            sink.write(delta_con, "extra", "dim")
        assert len(os.listdir(tmp_path / "dim" / "_delta_log")) == 1
        assert len(_scan(str(tmp_path / "dim"))) == 2

    @pytest.mark.parametrize(
        "sql",
        [
            "UPDATE t SET name = 'z' WHERE id = 'k1'",
            "DELETE FROM t WHERE id = 'k1'",
            "MERGE INTO t USING src s ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET name = s.name",
        ],
    )
    def test_extension_still_rejects_in_place_changes(self, tmp_path, delta_con, sql):
        """Why the in-place operations stay SinkUnsupported on DuckDB 2.0.1.

        Re-evaluate the refusals of TestDeltaLakeSinkRefusals when this fails.
        """
        DeltaLakeSink(str(tmp_path)).write(delta_con, "src", "t")
        delta_con.execute(f"ATTACH '{tmp_path}/t' AS t (TYPE delta)")
        try:
            with pytest.raises(duckdb.BinderException, match="base table"):
                delta_con.execute(sql)
        finally:
            delta_con.execute("DETACH t")
        assert len(os.listdir(tmp_path / "t" / "_delta_log")) == 1


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

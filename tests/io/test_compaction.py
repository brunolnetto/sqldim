"""Compactor and the legacy Parquet migration against ParquetSink's layout (#189)."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from sqldim.exceptions import SqldimError
from sqldim.io import Compactor, ParquetLayoutError, migrate_legacy_parquet
from sqldim.sinks import ParquetSink, SinkUnsupported


def _files(root: Path) -> set[str]:
    return {str(p.relative_to(root)) for p in root.rglob("*.parquet")}


def _inodes(root: Path) -> set[int]:
    return {p.stat().st_ino for p in root.rglob("*.parquet")}


def _sink_table(
    tmp_path: Path, con: duckdb.DuckDBPyConnection, batches: int = 3
) -> tuple[ParquetSink, Path]:
    """A table appended *batches* times by ParquetSink: many small files."""
    sink = ParquetSink(str(tmp_path))
    for b in range(batches):
        con.execute(
            "CREATE OR REPLACE VIEW batch AS SELECT "
            f"{b} * 10 + i AS id, 'v' || i AS label, i % 2 = 0 AS is_current, "
            "DATE '2024-01-01' + i::INT AS valid_from, NULL::VARCHAR AS valid_to "
            "FROM range(10) t(i)"
        )
        sink.write(con, "batch", "dim")
    return sink, tmp_path / "dim"


def _legacy_table(tmp_path: Path, con: duckdb.DuckDBPyConnection) -> Path:
    """A table in the layout of the sink before #178: is_current only in dirs."""
    root = tmp_path / "dim"
    con.execute(
        "COPY (SELECT i AS id, 'v' || i AS label, i % 2 = 0 AS is_current "
        f"FROM range(10) t(i)) TO '{root}' (FORMAT parquet, PARTITION_BY (is_current))"
    )
    return root


def _rows(con: duckdb.DuckDBPyConnection, source: str) -> list[tuple]:
    return sorted(con.execute(f"SELECT * FROM {source}").fetchall())


def _one(con: duckdb.DuckDBPyConnection, sql: str) -> object:
    row = con.execute(sql).fetchone()
    assert row is not None
    return row[0]


def test_layout_error_is_typed() -> None:
    assert issubclass(ParquetLayoutError, SqldimError)
    assert ParquetLayoutError.code == "INVALID_ARGUMENTS"


class TestCompactor:
    def test_round_trip_with_parquet_sink(self, tmp_path: Path) -> None:
        con = duckdb.connect()
        sink, root = _sink_table(tmp_path, con)
        src = sink.current_state_sql("dim")
        before, types = (
            _rows(con, src),
            con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall(),
        )
        assert len(_files(root)) == 6

        n = Compactor.compact(
            con, str(root), partition_by=["is_current"], zorder_by=["id"]
        )

        assert n == 30
        assert len(_files(root)) == 2
        assert _rows(con, src) == before
        assert con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall() == types
        assert [r[1] for r in types if r[0] == "is_current"] == ["BOOLEAN"]
        for f in root.rglob("*.parquet"):
            cols = {r[0] for r in con.execute(f"DESCRIBE '{f}'").fetchall()}
            assert "is_current" in cols

    def test_sink_keeps_working_after_compaction(self, tmp_path: Path) -> None:
        con = duckdb.connect()
        sink, root = _sink_table(tmp_path, con, batches=2)
        Compactor.compact(con, str(root), partition_by=["is_current"])
        con.execute("CREATE OR REPLACE VIEW keys AS SELECT 0 AS id")
        assert sink.close_versions(con, "dim", "id", "keys", "2024-02-01") == 1
        sink.write(con, "batch", "dim")
        src = sink.current_state_sql("dim")
        assert _one(con, f"SELECT count(*) FROM {src}") == 30

    def test_never_overwrites_a_written_file(self, tmp_path: Path) -> None:
        con = duckdb.connect()
        _, root = _sink_table(tmp_path, con, batches=2)
        old = _inodes(root)
        Compactor.compact(con, str(root), partition_by=["is_current"])
        assert not old & _inodes(root)
        assert not list(tmp_path.glob("dim.sqldim-*"))

    def test_unpartitioned_with_file_size(self, tmp_path: Path) -> None:
        con = duckdb.connect()
        sink, root = _sink_table(tmp_path, con)
        before = _rows(con, sink.current_state_sql("dim"))
        assert Compactor.compact(con, str(root), target_file_size_mb=64) == 30
        assert _rows(con, sink.current_state_sql("dim")) == before
        assert all("=" not in f for f in _files(root))

    def test_unpartitioned_single_file(self, tmp_path: Path) -> None:
        con = duckdb.connect()
        sink, root = _sink_table(tmp_path, con)
        assert Compactor.compact(con, str(root)) == 30
        assert _files(root) == {"data_0.parquet"}

    def test_file_size_with_partitions_is_unsupported(self, tmp_path: Path) -> None:
        con = duckdb.connect()
        _, root = _sink_table(tmp_path, con, batches=1)
        before = _files(root)
        with pytest.raises(SinkUnsupported, match="FILE_SIZE_BYTES"):
            Compactor.compact(
                con, str(root), partition_by=["is_current"], target_file_size_mb=64
            )
        assert _files(root) == before

    def test_legacy_layout_is_refused(self, tmp_path: Path) -> None:
        con = duckdb.connect()
        root = _legacy_table(tmp_path, con)
        before = _inodes(root)
        with pytest.raises(ParquetLayoutError, match="migrate_legacy_parquet"):
            Compactor.compact(con, str(root), partition_by=["is_current"])
        assert _inodes(root) == before

    def test_no_file_is_refused(self, tmp_path: Path) -> None:
        with pytest.raises(ParquetLayoutError, match="no Parquet file"):
            Compactor.compact(duckdb.connect(), str(tmp_path / "missing"))

    def test_remote_uri_is_unsupported(self) -> None:
        with pytest.raises(SinkUnsupported, match="local filesystem"):
            Compactor.compact(duckdb.connect(), "s3://bucket/dim")

    def test_bad_file_size(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="target_file_size_mb"):
            Compactor.compact(duckdb.connect(), str(tmp_path), target_file_size_mb=0)


class TestMigrateLegacyParquet:
    def test_sink_reads_a_migrated_table(self, tmp_path: Path) -> None:
        con = duckdb.connect()
        root = _legacy_table(tmp_path, con)
        src = ParquetSink(str(tmp_path)).current_state_sql("dim")
        with pytest.raises(duckdb.Error):
            con.execute(f"SELECT is_current FROM {src}")

        assert migrate_legacy_parquet(con, str(root)) == 10

        types = {
            r[0]: r[1] for r in con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()
        }
        assert types["is_current"] == "BOOLEAN"
        assert _one(con, f"SELECT count(*) FROM {src} WHERE is_current") == 5
        ids = sorted(r[0] for r in con.execute(f"SELECT id FROM {src}").fetchall())
        assert ids == list(range(10))

    def test_mixed_table_with_files_appended_after_178(self, tmp_path: Path) -> None:
        con = duckdb.connect()
        root = _legacy_table(tmp_path, con)
        sink = ParquetSink(str(tmp_path))
        con.execute(
            "CREATE VIEW more AS SELECT 99 AS id, 'new' AS label, TRUE AS is_current"
        )
        sink.write(con, "more", "dim")
        assert migrate_legacy_parquet(con, str(root)) == 11
        src = sink.current_state_sql("dim")
        assert _one(con, f"SELECT count(*) FROM {src} WHERE is_current") == 6
        assert Compactor.compact(con, str(root), partition_by=["is_current"]) == 11

    def test_current_layout_is_left_alone(self, tmp_path: Path) -> None:
        con = duckdb.connect()
        _, root = _sink_table(tmp_path, con, batches=1)
        before = _inodes(root)
        assert migrate_legacy_parquet(con, str(root)) == 0
        assert _inodes(root) == before

    def test_value_that_does_not_cast_is_refused(self, tmp_path: Path) -> None:
        con = duckdb.connect()
        root = tmp_path / "dim"
        con.execute(
            f"COPY (SELECT 1 AS id, 'maybe' AS is_current) TO '{root}' "
            "(FORMAT parquet, PARTITION_BY (is_current))"
        )
        before = _inodes(root)
        with pytest.raises(ParquetLayoutError, match="does not cast"):
            migrate_legacy_parquet(con, str(root))
        assert _inodes(root) == before
        assert not list(tmp_path.glob("dim.sqldim-*"))

    def test_file_without_directory_value_is_refused(self, tmp_path: Path) -> None:
        con = duckdb.connect()
        root = tmp_path / "dim"
        root.mkdir()
        con.execute(f"COPY (SELECT 1 AS id) TO '{root}/flat.parquet'")
        with pytest.raises(ParquetLayoutError, match="cannot be migrated"):
            migrate_legacy_parquet(con, str(root))

    def test_partial_partition_columns_are_refused(self, tmp_path: Path) -> None:
        con = duckdb.connect()
        root = tmp_path / "dim"
        root.mkdir()
        con.execute(f"COPY (SELECT 1 AS id, TRUE AS is_current) TO '{root}/f.parquet'")
        with pytest.raises(ParquetLayoutError, match="neither layout"):
            migrate_legacy_parquet(
                con,
                str(root),
                partitions={"is_current": "BOOLEAN", "region": "VARCHAR"},
            )

    def test_needs_partitions(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="partition columns"):
            migrate_legacy_parquet(duckdb.connect(), str(tmp_path), partitions={})

    def test_remote_uri_is_unsupported(self) -> None:
        with pytest.raises(SinkUnsupported):
            migrate_legacy_parquet(duckdb.connect(), "s3://bucket/dim")

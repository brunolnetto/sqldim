"""
Tests for CSVStreamSource and ParquetStreamSource.

Coverage target: sqldim/sources/csv_stream.py and parquet_stream.py — ~90%
"""

from __future__ import annotations
import csv

import duckdb

from sqldim.sources.streaming.csv_stream import CSVStreamSource
from sqldim.sources.streaming.parquet_stream import ParquetStreamSource


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_csv(path: str, rows: list[dict]) -> None:
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _write_parquet(con: duckdb.DuckDBPyConnection, path: str, rows: list[dict]) -> None:
    """Write rows to a Parquet file via DuckDB."""
    rows_sql = " UNION ALL ".join(
        "SELECT "
        + ", ".join(
            f"'{v}' AS {k}" if isinstance(v, str) else f"{v} AS {k}"
            for k, v in row.items()
        )
        for row in rows
    )
    con.execute(f"COPY ({rows_sql}) TO '{path}' (FORMAT PARQUET)")


# ---------------------------------------------------------------------------
# CSVStreamSource tests
# ---------------------------------------------------------------------------


class TestCSVStreamSourceConstructor:
    def test_defaults(self, tmp_path):
        src = CSVStreamSource(str(tmp_path / "data.csv"))
        assert src._delimiter == ","
        assert src._encoding == "utf-8"
        assert src._header is True
        assert src._offset == 0
        assert src._total is None

    def test_custom_delimiter(self, tmp_path):
        src = CSVStreamSource(str(tmp_path / "data.csv"), delimiter=";")
        assert src._delimiter == ";"

    def test_custom_encoding(self, tmp_path):
        src = CSVStreamSource(str(tmp_path / "data.csv"), encoding="latin-1")
        assert src._encoding == "latin-1"

    def test_header_false(self, tmp_path):
        src = CSVStreamSource(str(tmp_path / "data.csv"), header=False)
        assert src._header is False


class TestCSVStreamSourceStream:
    def test_yields_correct_number_of_batches(self, tmp_path):
        path = str(tmp_path / "data.csv")
        rows = [{"id": str(i), "val": str(i * 10)} for i in range(5)]
        _write_csv(path, rows)

        con = duckdb.connect()
        src = CSVStreamSource(path)
        fragments = list(src.stream(con, batch_size=2))
        # 5 rows with batch_size=2 → 3 batches (2+2+1)
        assert len(fragments) == 3

    def test_all_rows_covered_across_batches(self, tmp_path):
        path = str(tmp_path / "data.csv")
        rows = [{"id": str(i), "val": str(i)} for i in range(7)]
        _write_csv(path, rows)

        con = duckdb.connect()
        src = CSVStreamSource(path)
        total = 0
        for fragment in src.stream(con, batch_size=3):
            total += con.execute(f"SELECT count(*) FROM ({fragment})").fetchone()[0]
        assert total == 7

    def test_fragment_excludes_rowid_column(self, tmp_path):
        path = str(tmp_path / "data.csv")
        rows = [{"id": "1", "val": "a"}]
        _write_csv(path, rows)

        con = duckdb.connect()
        src = CSVStreamSource(path)
        fragment = next(src.stream(con, batch_size=100))
        cols = [r[0] for r in con.execute(f"DESCRIBE ({fragment})").fetchall()]
        assert "_sqldim_rowid" not in cols
        assert "id" in cols

    def test_empty_file_yields_no_fragments(self, tmp_path):
        path = str(tmp_path / "empty.csv")
        _write_csv(path, [{"id": "placeholder"}])
        # Overwrite with just-header file
        with open(path, "w") as f:
            f.write("id,val\n")

        con = duckdb.connect()
        src = CSVStreamSource(path)
        fragments = list(src.stream(con, batch_size=100))
        assert fragments == []

    def test_total_cached_after_first_stream(self, tmp_path):
        path = str(tmp_path / "data.csv")
        rows = [{"id": str(i)} for i in range(4)]
        _write_csv(path, rows)

        con = duckdb.connect()
        src = CSVStreamSource(path)
        list(src.stream(con, batch_size=10))
        assert src._total == 4

    def test_offset_advances_after_streaming(self, tmp_path):
        path = str(tmp_path / "data.csv")
        rows = [{"id": str(i)} for i in range(4)]
        _write_csv(path, rows)

        con = duckdb.connect()
        src = CSVStreamSource(path)
        list(src.stream(con, batch_size=2))
        # 4 rows, batch_size=2 → offsets: 0→2→4; loop exits when offset >= total=4
        assert src._offset >= 4


class TestCSVStreamSourceCheckpointCommitReset:
    def test_checkpoint_returns_current_offset(self, tmp_path):
        src = CSVStreamSource(str(tmp_path / "d.csv"))
        src._offset = 42
        assert src.checkpoint() == 42

    def test_commit_sets_offset(self, tmp_path):
        src = CSVStreamSource(str(tmp_path / "d.csv"))
        src.commit(100)
        assert src._offset == 100

    def test_reset_clears_offset_and_total(self, tmp_path):
        src = CSVStreamSource(str(tmp_path / "d.csv"))
        src._offset = 50
        src._total = 200
        src.reset()
        assert src._offset == 0
        assert src._total is None

    def test_stream_resumes_from_offset(self, tmp_path):
        path = str(tmp_path / "data.csv")
        rows = [{"id": str(i)} for i in range(6)]
        _write_csv(path, rows)

        con = duckdb.connect()
        src = CSVStreamSource(path)
        # Consume first 2 rows and commit
        gen = src.stream(con, batch_size=2)
        next(gen)
        src.commit(src.checkpoint())
        # Force total to be known
        list(src.stream(con, batch_size=2))
        # Only rows from offset 2 onward were processed in second loop
        assert src._offset == 6


# ---------------------------------------------------------------------------
# ParquetStreamSource tests
# ---------------------------------------------------------------------------


class TestParquetStreamSourceConstructor:
    def test_defaults(self, tmp_path):
        src = ParquetStreamSource(str(tmp_path / "data.parquet"))
        assert src._hive is False
        assert src._order is None
        assert src._offset == 0
        assert src._total is None

    def test_hive_partitioning(self, tmp_path):
        src = ParquetStreamSource(str(tmp_path / "*.parquet"), hive_partitioning=True)
        assert src._hive is True

    def test_order_by(self, tmp_path):
        src = ParquetStreamSource(str(tmp_path / "data.parquet"), order_by="id")
        assert src._order == "id"


class TestParquetStreamSourceStream:
    def test_yields_correct_number_of_batches(self, tmp_path):
        path = str(tmp_path / "data.parquet")
        rows = [{"id": i, "val": i * 2} for i in range(5)]
        con = duckdb.connect()
        _write_parquet(con, path, rows)

        src = ParquetStreamSource(path)
        fragments = list(src.stream(con, batch_size=2))
        assert len(fragments) == 3

    def test_all_rows_covered_across_batches(self, tmp_path):
        path = str(tmp_path / "data.parquet")
        rows = [{"id": i, "v": i} for i in range(9)]
        con = duckdb.connect()
        _write_parquet(con, path, rows)

        src = ParquetStreamSource(path)
        total = 0
        for fragment in src.stream(con, batch_size=4):
            total += con.execute(f"SELECT count(*) FROM ({fragment})").fetchone()[0]
        assert total == 9

    def test_fragment_excludes_rowid_column(self, tmp_path):
        path = str(tmp_path / "data.parquet")
        rows = [{"id": 1, "name": "a"}]
        con = duckdb.connect()
        _write_parquet(con, path, rows)

        src = ParquetStreamSource(path)
        fragment = next(src.stream(con, batch_size=100))
        cols = [r[0] for r in con.execute(f"DESCRIBE ({fragment})").fetchall()]
        assert "_sqldim_rowid" not in cols
        assert "id" in cols

    def test_empty_dataset_yields_no_fragments(self, tmp_path):
        path = str(tmp_path / "empty.parquet")
        con = duckdb.connect()
        con.execute(f"COPY (SELECT 1 AS id WHERE 1=0) TO '{path}' (FORMAT PARQUET)")

        src = ParquetStreamSource(path)
        fragments = list(src.stream(con, batch_size=100))
        assert fragments == []

    def test_total_cached_after_first_stream(self, tmp_path):
        path = str(tmp_path / "data.parquet")
        rows = [{"id": i} for i in range(3)]
        con = duckdb.connect()
        _write_parquet(con, path, rows)

        src = ParquetStreamSource(path)
        list(src.stream(con, batch_size=10))
        assert src._total == 3

    def test_order_by_streams_in_order(self, tmp_path):
        path = str(tmp_path / "data.parquet")
        rows = [{"id": 3}, {"id": 1}, {"id": 2}]
        con = duckdb.connect()
        _write_parquet(con, path, rows)

        src = ParquetStreamSource(path, order_by="id")
        results = []
        for fragment in src.stream(con, batch_size=10):
            ids = [
                r[0]
                for r in con.execute(
                    f"SELECT id FROM ({fragment}) ORDER BY id"
                ).fetchall()
            ]
            results.extend(ids)
        assert results == sorted(results)

    def test_offset_advances_after_streaming(self, tmp_path):
        path = str(tmp_path / "data.parquet")
        rows = [{"id": i} for i in range(5)]
        con = duckdb.connect()
        _write_parquet(con, path, rows)

        src = ParquetStreamSource(path)
        list(src.stream(con, batch_size=2))
        # 5 rows, batch_size=2 → offsets: 0→2→4→6; loop exits when offset >= 5
        assert src._offset >= 5


class TestParquetStreamSourceCheckpointCommitReset:
    def test_checkpoint_returns_offset(self, tmp_path):
        src = ParquetStreamSource(str(tmp_path / "d.parquet"))
        src._offset = 77
        assert src.checkpoint() == 77

    def test_commit_sets_offset(self, tmp_path):
        src = ParquetStreamSource(str(tmp_path / "d.parquet"))
        src.commit(200)
        assert src._offset == 200

    def test_reset_clears_state(self, tmp_path):
        src = ParquetStreamSource(str(tmp_path / "d.parquet"))
        src._offset = 100
        src._total = 500
        src.reset()
        assert src._offset == 0
        assert src._total is None

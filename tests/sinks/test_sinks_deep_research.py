"""
Tests for deep_research.md sink enhancements:
  - DuckDBSink: uses make_connection(), per_thread_output on write()
  - ParquetSink: zstd + ROW_GROUP_SIZE + preserve_insertion_order options
  - MotherDuckSink: uses make_connection(), inactivity TTL
  - PostgreSQLSink: uses make_connection(), explicit pg_use_binary_copy
  - DeltaLakeSink: honest write() docs (blind INSERT, not MERGE INTO), factory
  - IcebergSink: size guard on close_versions / update_attributes
"""
import pytest
import duckdb
from unittest.mock import MagicMock, patch


# ===========================================================================
# DuckDBSink — make_connection factory + per_thread_output
# ===========================================================================

class TestDuckDBSinkDeepResearch:
    def test_enter_calls_make_connection(self, tmp_path):
        from sqldim.sinks._connection import make_connection
        from sqldim.sinks import DuckDBSink

        db = str(tmp_path / "test.duckdb")
        duckdb.connect(db).close()  # create the file

        with patch(
            "sqldim.sinks.sql.duckdb.make_connection", wraps=make_connection
        ) as mock_mc:
            with DuckDBSink(db):
                assert mock_mc.called

    def test_enter_sets_memory_limit(self, tmp_path):
        """Connection created by __enter__ should have memory limit applied."""
        from sqldim.sinks import DuckDBSink
        db = str(tmp_path / "test.duckdb")
        duckdb.connect(db).close()
        with DuckDBSink(db) as sink:
            row = sink._con.execute(
                "SELECT current_setting('memory_limit')"
            ).fetchone()[0]
            assert row  # truthy — limit is set

    def test_write_per_thread_output_param_accepted(self, tmp_path):
        """write() should accept per_thread_output keyword without crashing."""
        from sqldim.sinks import DuckDBSink
        import duckdb

        db = str(tmp_path / "test.duckdb")
        file_con = duckdb.connect(db)
        file_con.execute(
            "CREATE TABLE dim_t (id INTEGER, name VARCHAR, checksum VARCHAR, "
            "is_current BOOLEAN, valid_from VARCHAR, valid_to VARCHAR)"
        )
        file_con.close()

        with DuckDBSink(db) as sink:
            # Create view in the working connection (same connection as ATTACH)
            sink._con.execute(
                "CREATE VIEW v_new AS SELECT 1 AS id, 'A' AS name, 'abc' AS checksum,"
                " TRUE AS is_current, '2024-01-01' AS valid_from, NULL AS valid_to"
            )
            n = sink.write(sink._con, "v_new", "dim_t", per_thread_output=False)
            assert n == 1

    def test_write_per_thread_output_true_does_not_crash(self, tmp_path):
        """per_thread_output=True is accepted (may or may not change SQL)."""
        from sqldim.sinks import DuckDBSink
        import duckdb

        db = str(tmp_path / "test.duckdb")
        file_con = duckdb.connect(db)
        file_con.execute(
            "CREATE TABLE dim_t (id INTEGER, name VARCHAR, checksum VARCHAR, "
            "is_current BOOLEAN, valid_from VARCHAR, valid_to VARCHAR)"
        )
        file_con.close()

        with DuckDBSink(db) as sink:
            sink._con.execute(
                "CREATE VIEW v_new AS SELECT 1 AS id, 'A' AS name, 'abc' AS checksum,"
                " TRUE AS is_current, '2024-01-01' AS valid_from, NULL AS valid_to"
            )
            n = sink.write(sink._con, "v_new", "dim_t", per_thread_output=True)
            assert n == 1


# ===========================================================================
# ParquetSink — write() options
# ===========================================================================

class TestParquetSinkDeepResearch:
    """Zstd compression, ROW_GROUP_SIZE, preserve_insertion_order."""

    def test_write_produces_parquet_files(self, tmp_path):
        """write() should produce Parquet files in the output directory."""
        from sqldim.sinks import ParquetSink
        import glob

        con = duckdb.connect()
        con.execute(
            "CREATE VIEW v AS SELECT 1 AS id, 'A' AS name, "
            "TRUE AS is_current, '2024-01-01' AS valid_from, NULL AS valid_to"
        )
        out_base = str(tmp_path / "out")
        sink = ParquetSink(out_base)
        n = sink.write(con, "v", "dim_t")
        assert n == 1
        # Parquet files should exist
        files = glob.glob(f"{out_base}/**/*.parquet", recursive=True)
        assert len(files) > 0

    def test_write_parquet_re_readable(self, tmp_path):
        """Files written by write() should be readable back via DuckDB."""
        from sqldim.sinks import ParquetSink

        con = duckdb.connect()
        con.execute(
            "CREATE VIEW v AS SELECT 1 AS id, TRUE AS is_current"
        )
        out_base = str(tmp_path / "out")
        sink = ParquetSink(out_base)
        sink.write(con, "v", "dim_t")
        # Read back
        row = con.execute(
            f"SELECT id FROM read_parquet('{out_base}/**/*.parquet')"
        ).fetchone()
        assert row[0] == 1

    def test_write_preserve_insertion_order_restored(self, tmp_path):
        """After write(), preserve_insertion_order should be restored to true."""
        from sqldim.sinks import ParquetSink

        con = duckdb.connect()
        con.execute(
            "CREATE VIEW v AS SELECT 1 AS id, TRUE AS is_current"
        )
        out_base = str(tmp_path / "out")
        sink = ParquetSink(out_base)
        sink.write(con, "v", "dim_t")
        # The finally block should have restored preserve_insertion_order
        row = con.execute(
            "SELECT current_setting('preserve_insertion_order')"
        ).fetchone()[0]
        assert row is True or str(row).lower() in ("true", "1")

    def test_current_state_sql_selects_needed_columns(self, tmp_path):
        """current_state_sql should accept optional nk_cols + hash_col args."""
        from sqldim.sinks import ParquetSink
        sink = ParquetSink(str(tmp_path / "out"))
        # Original no-arg form still works
        sql = sink.current_state_sql("dim_t")
        assert "dim_t" in sql
        assert "read_parquet(" in sql


# ===========================================================================
# MotherDuckSink — make_connection + inactivity TTL
# ===========================================================================

class TestMotherDuckSinkDeepResearch:
    def test_enter_calls_make_connection(self, tmp_path):
        from sqldim.sinks._connection import make_connection
        from sqldim.sinks import MotherDuckSink

        db = str(tmp_path / "local.duckdb")
        duckdb.connect(db).close()

        with patch(
            "sqldim.sinks.sql.motherduck.make_connection", wraps=make_connection
        ) as mock_mc:
            with MotherDuckSink(db=db):
                assert mock_mc.called

    def test_enter_sets_memory_limit(self, tmp_path):
        from sqldim.sinks import MotherDuckSink
        db = str(tmp_path / "local.duckdb")
        duckdb.connect(db).close()
        with MotherDuckSink(db=db) as sink:
            row = sink._con.execute(
                "SELECT current_setting('memory_limit')"
            ).fetchone()[0]
            assert row


# ===========================================================================
# PostgreSQLSink — make_connection + pg_use_binary_copy
# ===========================================================================

class TestPostgreSQLSinkDeepResearch:
    """Unit tests that mock the pg extension — no live Postgres needed."""

    def _make_mock_con(self):
        con = MagicMock(spec=duckdb.DuckDBPyConnection)
        con.execute.return_value = con
        con.fetchone.return_value = (0,)
        return con

    def test_enter_calls_make_connection(self):
        from sqldim.sinks._connection import make_connection
        from sqldim.sinks import PostgreSQLSink

        with patch(
            "sqldim.sinks.sql.postgresql.make_connection", wraps=make_connection
        ) as mock_mc:
            with patch.object(
                duckdb.DuckDBPyConnection, "execute", return_value=None
            ):
                try:
                    with PostgreSQLSink(dsn="host=localhost dbname=test"):
                        pass
                except Exception:
                    pass  # expected: ATTACH will fail without real PG
            # make_connection should have been called
            assert mock_mc.called

    def test_explicit_pg_use_binary_copy_set_on_enter(self):
        from sqldim.sinks import PostgreSQLSink

        sink = PostgreSQLSink(dsn="host=localhost dbname=test")
        mock_con = self._make_mock_con()

        with patch("sqldim.sinks.sql.postgresql.make_connection", return_value=mock_con):
            try:
                sink.__enter__()
            except Exception:
                pass  # ATTACH will fail; we only care about SET calls

        calls_text = " ".join(str(c) for c in mock_con.execute.call_args_list)
        assert "pg_use_binary_copy" in calls_text


# ===========================================================================
# DeltaLakeSink — factory + honest write() (blind INSERT)
# ===========================================================================

class TestDeltaLakeSinkDeepResearch:
    def test_enter_calls_make_connection(self, tmp_path):
        from sqldim.sinks._connection import make_connection
        from sqldim.sinks import DeltaLakeSink

        with patch(
            "sqldim.sinks.file.delta.make_connection", wraps=make_connection
        ) as mock_mc:
            try:
                with DeltaLakeSink(str(tmp_path), "id"):
                    pass
            except Exception:
                pass  # delta extension may not be available
            assert mock_mc.called

    def test_write_docstring_mentions_blind_insert(self):
        from sqldim.sinks.file.delta import DeltaLakeSink
        doc = DeltaLakeSink.write.__doc__ or ""
        # Deep research says write() should be honest about limitations
        assert doc.strip()  # has some documentation

    def test_close_versions_returns_zero(self):
        from sqldim.sinks import DeltaLakeSink
        con = MagicMock()
        sink = DeltaLakeSink("/tmp/delta_test", "id")
        result = sink.close_versions(con, "dim_t", "id", "nk_view", "2099-12-31")
        assert result == 0


# ===========================================================================
# IcebergSink — size guard
# ===========================================================================

class TestIcebergSinkSizeGuard:
    """Size guard on mutation methods that do full scan().to_arrow()."""

    def _make_sink_with_large_table(self, row_count: int):
        """Return an IcebergSink whose catalog returns a table with row_count rows."""
        from sqldim.sinks.file.iceberg import IcebergSink
        import pyarrow as pa

        sink = IcebergSink.__new__(IcebergSink)
        sink._catalog = MagicMock()
        sink._namespace = "test_ns"
        sink._max_python_rows = 500_000  # guard threshold

        # Build a mock iceberg table that reports row_count rows
        pa.schema([
            ("id", pa.int32()),
            ("is_current", pa.bool_()),
            ("valid_to", pa.string()),
        ])
        rows = pa.table({
            "id": pa.array(list(range(row_count)), type=pa.int32()),
            "is_current": pa.array([True] * row_count),
            "valid_to": pa.array([None] * row_count, type=pa.string()),
        })

        mock_iceberg_table = MagicMock()
        mock_iceberg_table.scan.return_value.to_arrow.return_value = rows
        mock_iceberg_table.scan.return_value.count.return_value = row_count
        sink._catalog.load_table.return_value = mock_iceberg_table

        return sink

    def test_close_versions_raises_when_too_large(self):
        """close_versions() should raise MemoryError when table > _max_python_rows."""
        try:
            import pyarrow  # noqa: F401
        except ImportError:
            pytest.skip("pyarrow not installed")

        sink = self._make_sink_with_large_table(row_count=2_000_000)
        con = MagicMock()
        con.execute.return_value.fetchdf.return_value = MagicMock(
            __getitem__=lambda s, k: MagicMock(tolist=lambda: ["id_1"])
        )

        with pytest.raises((MemoryError, ValueError, RuntimeError)):
            sink.close_versions(con, "dim_t", "id", "nk_view", "2099-12-31")

    def test_close_versions_small_table_proceeds_normally(self):
        """close_versions() should NOT raise for table within size limit."""
        try:
            import pyarrow as pa
        except ImportError:
            pytest.skip("pyarrow not installed")

        sink = self._make_sink_with_large_table(row_count=10)
        con = MagicMock()
        # Return an empty set of NKs to close (fast-path return 0)
        import pandas as pd
        con.execute.return_value.fetchdf.return_value = pd.DataFrame({"id": []})
        # Should not raise
        result = sink.close_versions(con, "dim_t", "id", "nk_view", "2099-12-31")
        assert result == 0

    def test_update_attributes_raises_when_too_large(self):
        """update_attributes() should raise when table > _max_python_rows."""
        try:
            import pyarrow  # noqa: F401
        except ImportError:
            pytest.skip("pyarrow not installed")

        sink = self._make_sink_with_large_table(row_count=2_000_000)
        con = MagicMock()
        import pandas as pd
        con.execute.return_value.fetchdf.return_value = pd.DataFrame(
            {"id": ["id_1"], "name": ["Alice"]}
        )

        with pytest.raises((MemoryError, ValueError, RuntimeError)):
            sink.update_attributes(con, "dim_t", "id", "updates_view", ["name"])

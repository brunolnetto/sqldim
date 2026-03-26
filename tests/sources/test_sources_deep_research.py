"""
Tests for deep_research.md source enhancements:
  - ParquetSource: union_by_name, filename, remote_threads
  - CSVSource: columns, nullstr, ignore_errors
  - DeltaSource: use_attach, alias, _attached flag
  - PostgreSQLSource: filter_pushdown, pages_per_task
  - IcebergSource: new class
"""

from unittest.mock import MagicMock


# ===========================================================================
# ParquetSource
# ===========================================================================


class TestParquetSourceDeepResearch:
    """New params from deep_research.md §Sources/parquet.py"""

    def test_union_by_name_false_by_default(self):
        from sqldim.sources import ParquetSource

        src = ParquetSource("data/*.parquet")
        assert "union_by_name" not in src.as_sql(None)

    def test_union_by_name_true_adds_option(self):
        from sqldim.sources import ParquetSource

        src = ParquetSource("data/*.parquet", union_by_name=True)
        assert "union_by_name=true" in src.as_sql(None)

    def test_filename_false_by_default(self):
        from sqldim.sources import ParquetSource

        src = ParquetSource("data/*.parquet")
        assert "filename" not in src.as_sql(None)

    def test_filename_true_adds_option(self):
        from sqldim.sources import ParquetSource

        src = ParquetSource("data/*.parquet", filename=True)
        assert "filename=true" in src.as_sql(None)

    def test_remote_threads_none_by_default(self):
        from sqldim.sources import ParquetSource

        src = ParquetSource("s3://bucket/data.parquet")
        assert "threads" not in src.as_sql(None)

    def test_remote_threads_sets_option(self):
        from sqldim.sources import ParquetSource

        src = ParquetSource("s3://bucket/data.parquet", remote_threads=16)
        sql = src.as_sql(None)
        assert "threads=16" in sql

    def test_all_new_params_combine_with_hive(self):
        from sqldim.sources import ParquetSource

        src = ParquetSource(
            "s3://bucket/*.parquet",
            hive_partitioning=True,
            union_by_name=True,
            filename=True,
            remote_threads=8,
        )
        sql = src.as_sql(None)
        assert "hive_partitioning=true" in sql
        assert "union_by_name=true" in sql
        assert "filename=true" in sql
        assert "threads=8" in sql

    def test_backward_compat_single_path_no_new_params(self):
        from sqldim.sources import ParquetSource

        src = ParquetSource("data/foo.parquet")
        # Original behaviour unchanged
        assert src.as_sql(None) == "read_parquet('data/foo.parquet')"

    def test_remote_threads_zero_not_added(self):
        # remote_threads=0 should be treated as None (not set)
        from sqldim.sources import ParquetSource

        src = ParquetSource("s3://bucket/d.parquet", remote_threads=0)
        assert "threads" not in src.as_sql(None)


# ===========================================================================
# CSVSource
# ===========================================================================


class TestCSVSourceDeepResearch:
    """New params from deep_research.md §Sources/csv.py"""

    def test_columns_none_by_default(self):
        from sqldim.sources import CSVSource

        src = CSVSource("data.csv")
        assert "columns=" not in src.as_sql(None)

    def test_columns_dict_appears_in_sql(self):
        from sqldim.sources import CSVSource

        src = CSVSource("data.csv", columns={"id": "INTEGER", "name": "VARCHAR"})
        sql = src.as_sql(None)
        assert "columns=" in sql
        assert "id" in sql
        assert "INTEGER" in sql

    def test_nullstr_empty_by_default(self):
        from sqldim.sources import CSVSource

        src = CSVSource("data.csv")
        # Default '' should still be emitted so DuckDB interprets empty as NULL
        sql = src.as_sql(None)
        assert "nullstr" in sql

    def test_nullstr_custom_value(self):
        from sqldim.sources import CSVSource

        src = CSVSource("data.csv", nullstr="NA")
        assert "nullstr='NA'" in src.as_sql(None)

    def test_ignore_errors_false_by_default(self):
        from sqldim.sources import CSVSource

        src = CSVSource("data.csv")
        sql = src.as_sql(None)
        # When false, either omitted or explicitly false — just verify no crash
        assert "read_csv(" in sql

    def test_ignore_errors_true_adds_option(self):
        from sqldim.sources import CSVSource

        src = CSVSource("data.csv", ignore_errors=True)
        assert "ignore_errors=true" in src.as_sql(None).lower()

    def test_ignore_errors_false_emits_false(self):
        from sqldim.sources import CSVSource

        src = CSVSource("data.csv", ignore_errors=False)
        sql = src.as_sql(None).lower()
        assert "ignore_errors=false" in sql

    def test_backward_compat_no_new_params(self):
        from sqldim.sources import CSVSource

        src = CSVSource("data/foo.csv")
        sql = src.as_sql(None)
        # All original options still present
        assert "delim=','" in sql
        assert "header=TRUE" in sql
        assert "encoding='utf-8'" in sql

    def test_columns_disables_auto_detect(self):
        from sqldim.sources import CSVSource

        src = CSVSource("data.csv", columns={"id": "INTEGER"})
        sql = src.as_sql(None)
        # When columns is a dict, auto_detect should be false
        assert "auto_detect=false" in sql.lower()


# ===========================================================================
# DeltaSource
# ===========================================================================


class TestDeltaSourceDeepResearch:
    """use_attach mode from deep_research.md §Sources/delta.py"""

    def test_use_attach_true_by_default(self):
        from sqldim.sources import DeltaSource

        src = DeltaSource("/data/delta/empresa")
        assert src._use_attach is True

    def test_use_attach_false_falls_back_to_delta_scan(self):
        from sqldim.sources import DeltaSource

        con = MagicMock()
        src = DeltaSource("/data/delta/empresa", use_attach=False)
        sql = src.as_sql(con)
        assert "delta_scan(" in sql

    def test_use_attach_true_returns_select_from_alias(self):
        from sqldim.sources import DeltaSource

        con = MagicMock()
        src = DeltaSource("/data/delta/empresa", use_attach=True)
        sql = src.as_sql(con)
        # Should ATTACH and return SELECT * FROM <alias>.<table>
        assert "SELECT" in sql or "sqldim_delta" in sql

    def test_alias_default_is_sqldim_delta_src(self):
        from sqldim.sources import DeltaSource

        src = DeltaSource("/data/delta/empresa")
        assert "sqldim_delta" in src._alias

    def test_alias_custom(self):
        from sqldim.sources import DeltaSource

        src = DeltaSource("/data/delta/empresa", alias="my_delta")
        assert src._alias == "my_delta"

    def test_use_attach_false_backward_compat(self):
        from sqldim.sources import DeltaSource

        con = MagicMock()
        src = DeltaSource("/data/delta/empresa", use_attach=False)
        sql = src.as_sql(con)
        assert "/data/delta/empresa" in sql

    def test_attached_flag_starts_false(self):
        from sqldim.sources import DeltaSource

        src = DeltaSource("/data/delta/x")
        assert src._attached is False

    def test_install_load_only_called_once_on_repeated_calls(self):
        from sqldim.sources import DeltaSource

        con = MagicMock()
        src = DeltaSource("/data/delta/empresa", use_attach=True)
        src.as_sql(con)
        src.as_sql(con)
        # INSTALL delta should only be called once (first call sets _attached=True)
        install_calls = [c for c in con.execute.call_args_list if "INSTALL" in str(c)]
        assert len(install_calls) <= 1


# ===========================================================================
# PostgreSQLSource
# ===========================================================================


class TestPostgreSQLSourceDeepResearch:
    """filter_pushdown + pages_per_task from deep_research.md"""

    def test_filter_pushdown_false_by_default(self):
        from sqldim.sources import PostgreSQLSource

        con = MagicMock()
        src = PostgreSQLSource(dsn="host=localhost dbname=test", table="emp")
        sql = src.as_sql(con)
        # Default: no filter pushdown SET
        assert "filter_pushdown" not in sql or "false" in sql.lower()

    def test_filter_pushdown_true_sets_experimental_flag(self):
        from sqldim.sources import PostgreSQLSource

        con = MagicMock()
        src = PostgreSQLSource(
            dsn="host=localhost dbname=test",
            table="emp",
            filter_pushdown=True,
        )
        src.as_sql(con)
        # Should call SET pg_experimental_filter_pushdown = true
        calls_text = " ".join(str(c) for c in con.execute.call_args_list)
        assert "pg_experimental_filter_pushdown" in calls_text

    def test_pages_per_task_default_1000(self):
        from sqldim.sources import PostgreSQLSource

        src = PostgreSQLSource(dsn="host=localhost dbname=test", table="emp")
        assert src._pages_per_task == 1000

    def test_pages_per_task_custom(self):
        from sqldim.sources import PostgreSQLSource

        con = MagicMock()
        src = PostgreSQLSource(
            dsn="host=localhost dbname=test",
            table="emp",
            pages_per_task=500,
        )
        src.as_sql(con)
        calls_text = " ".join(str(c) for c in con.execute.call_args_list)
        assert "pg_pages_per_task" in calls_text
        assert "500" in calls_text

    def test_backward_compat_sql_unchanged(self):
        from sqldim.sources import PostgreSQLSource

        con = MagicMock()
        src = PostgreSQLSource(dsn="host=localhost dbname=test", table="emp")
        sql = src.as_sql(con)
        assert "postgres_scan(" in sql
        assert "emp" in sql


# ===========================================================================
# IcebergSource (new file)
# ===========================================================================


class TestIcebergSource:
    """IcebergSource from deep_research.md §Sources/iceberg.py"""

    def test_class_importable(self):
        from sqldim.sources.batch.iceberg import IcebergSource

        assert IcebergSource is not None

    def test_exported_from_sources_init(self):
        from sqldim.sources import IcebergSource

        assert IcebergSource is not None

    def test_as_sql_returns_iceberg_scan(self):
        from sqldim.sources.batch.iceberg import IcebergSource

        con = MagicMock()
        src = IcebergSource("/data/iceberg/my_table")
        sql = src.as_sql(con)
        assert "iceberg_scan(" in sql

    def test_path_in_sql(self):
        from sqldim.sources.batch.iceberg import IcebergSource

        con = MagicMock()
        src = IcebergSource("/data/iceberg/my_table")
        sql = src.as_sql(con)
        assert "/data/iceberg/my_table" in sql

    def test_install_and_load_called(self):
        from sqldim.sources.batch.iceberg import IcebergSource

        con = MagicMock()
        src = IcebergSource("/data/iceberg/my_table")
        src.as_sql(con)
        calls_text = " ".join(str(c) for c in con.execute.call_args_list)
        assert "iceberg" in calls_text.lower()

    def test_is_source_adapter(self):
        from sqldim.sources.batch.iceberg import IcebergSource
        from sqldim.sources.base import SourceAdapter

        src = IcebergSource("/data/iceberg/my_table")
        assert isinstance(src, SourceAdapter)

    def test_allow_moved_tables_false_by_default(self):
        from sqldim.sources.batch.iceberg import IcebergSource

        con = MagicMock()
        src = IcebergSource("/data/iceberg/my_table")
        sql = src.as_sql(con)
        # allow_moved_tables should be False by default (for safety)
        assert (
            "allow_moved_tables=false" in sql.lower()
            or "allow_moved_tables" not in sql.lower()
        )

    def test_allow_moved_tables_true(self):
        from sqldim.sources.batch.iceberg import IcebergSource

        con = MagicMock()
        src = IcebergSource(
            "s3://bucket/iceberg/my_table/metadata/v1.metadata.json",
            allow_moved_tables=True,
        )
        sql = src.as_sql(con)
        assert "allow_moved_tables=true" in sql.lower()

    def test_s3_metadata_file_path(self):
        from sqldim.sources.batch.iceberg import IcebergSource

        con = MagicMock()
        src = IcebergSource("s3://bucket/table/metadata/v1.metadata.json")
        sql = src.as_sql(con)
        assert "s3://bucket/table/metadata/v1.metadata.json" in sql

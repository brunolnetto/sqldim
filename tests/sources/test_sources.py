"""
Tests for sqldim.sources — all 7 implementations + coerce_source().
Target: bring sources/ package from 0 % to ~95 % coverage.
"""

import pytest
from unittest.mock import MagicMock

from sqldim.sources import (
    SourceAdapter,
    ParquetSource,
    CSVSource,
    DuckDBSource,
    PostgreSQLSource,
    DeltaSource,
    SQLSource,
    coerce_source,
)


# ---------------------------------------------------------------------------
# SourceAdapter runtime protocol
# ---------------------------------------------------------------------------


class TestSourceAdapterProtocol:
    def test_parquet_is_source_adapter(self):
        assert isinstance(ParquetSource("x.parquet"), SourceAdapter)

    def test_csv_is_source_adapter(self):
        assert isinstance(CSVSource("x.csv"), SourceAdapter)

    def test_duckdb_is_source_adapter(self):
        assert isinstance(DuckDBSource("my_view"), SourceAdapter)

    def test_sql_is_source_adapter(self):
        assert isinstance(SQLSource("SELECT 1"), SourceAdapter)

    def test_plain_object_is_not_source_adapter(self):
        assert not isinstance(object(), SourceAdapter)

    def test_string_is_not_source_adapter(self):
        assert not isinstance("some_path.parquet", SourceAdapter)


# ---------------------------------------------------------------------------
# ParquetSource
# ---------------------------------------------------------------------------


class TestParquetSource:
    def test_single_path(self):
        src = ParquetSource("data/foo.parquet")
        assert src.as_sql(None) == "read_parquet('data/foo.parquet')"

    def test_list_of_paths_contains_both(self):
        src = ParquetSource(["a.parquet", "b.parquet"])
        sql = src.as_sql(None)
        assert "a.parquet" in sql
        assert "b.parquet" in sql

    def test_list_of_paths_uses_array_syntax(self):
        src = ParquetSource(["a.parquet", "b.parquet"])
        sql = src.as_sql(None)
        assert sql.startswith("read_parquet([")

    def test_no_hive_partitioning_by_default(self):
        src = ParquetSource("data/*.parquet")
        assert "hive_partitioning" not in src.as_sql(None)

    def test_hive_partitioning_true(self):
        src = ParquetSource("data/*.parquet", hive_partitioning=True)
        assert "hive_partitioning=true" in src.as_sql(None)

    def test_glob_path(self):
        src = ParquetSource("s3://bucket/prefix/*.parquet")
        sql = src.as_sql(None)
        assert sql.startswith("read_parquet(")
        assert "s3://bucket" in sql

    def test_as_sql_ignores_con(self):
        # con is not used for parquet — passing None is safe
        src = ParquetSource("foo.parquet")
        assert src.as_sql(None) == src.as_sql(MagicMock())


# ---------------------------------------------------------------------------
# CSVSource
# ---------------------------------------------------------------------------


class TestCSVSource:
    def test_single_path_defaults(self):
        src = CSVSource("data/foo.csv")
        sql = src.as_sql(None)
        assert "read_csv(" in sql
        assert "delim=','" in sql
        assert "header=TRUE" in sql
        assert "encoding='utf-8'" in sql

    def test_custom_delimiter(self):
        src = CSVSource("data/foo.csv", delimiter=";")
        assert "delim=';'" in src.as_sql(None)

    def test_custom_encoding(self):
        src = CSVSource("data/foo.csv", encoding="latin-1")
        assert "encoding='latin-1'" in src.as_sql(None)

    def test_header_false(self):
        src = CSVSource("data/foo.csv", header=False)
        assert "header=FALSE" in src.as_sql(None)

    def test_list_of_paths_contains_both(self):
        src = CSVSource(["a.csv", "b.csv"])
        sql = src.as_sql(None)
        assert "a.csv" in sql
        assert "b.csv" in sql

    def test_list_of_paths_uses_array_syntax(self):
        src = CSVSource(["a.csv", "b.csv"])
        sql = src.as_sql(None)
        assert sql.startswith("read_csv([")

    def test_tsv_uses_tab_delimiter(self):
        src = CSVSource("data.csv", delimiter="\t")
        assert "\\t" in src.as_sql(None) or "\t" in src.as_sql(None)


# ---------------------------------------------------------------------------
# DuckDBSource
# ---------------------------------------------------------------------------


class TestDuckDBSource:
    def test_plain_table_name(self):
        src = DuckDBSource("my_view")
        assert src.as_sql(None) == "SELECT * FROM my_view"

    def test_schema_and_table(self):
        src = DuckDBSource("my_table", schema="staging")
        assert src.as_sql(None) == "SELECT * FROM staging.my_table"

    def test_already_qualified_name(self):
        src = DuckDBSource("staging.my_table")
        assert src.as_sql(None) == "SELECT * FROM staging.my_table"

    def test_schema_plus_already_qualified_raises_value_error(self):
        with pytest.raises(ValueError, match="already schema-qualified"):
            DuckDBSource("staging.my_table", schema="main")

    def test_error_message_contains_offending_name(self):
        with pytest.raises(ValueError, match="staging.foo"):
            DuckDBSource("staging.foo", schema="public")

    def test_error_message_contains_schema(self):
        with pytest.raises(ValueError, match="'public'"):
            DuckDBSource("staging.foo", schema="public")

    def test_as_sql_ignores_con(self):
        src = DuckDBSource("my_view")
        assert src.as_sql(None) == src.as_sql(MagicMock())

    def test_both_forms_produce_same_qualified_reference(self):
        a = DuckDBSource("staging.foo")
        b = DuckDBSource("foo", schema="staging")
        assert a.as_sql(None) == b.as_sql(None)
        assert "staging.foo" in a.as_sql(None)


# ---------------------------------------------------------------------------
# PostgreSQLSource
# ---------------------------------------------------------------------------


class TestPostgreSQLSource:
    def _mock_con(self):
        return MagicMock()

    def test_returns_postgres_scan(self):
        con = self._mock_con()
        src = PostgreSQLSource(dsn="host=localhost dbname=test", table="employees")
        sql = src.as_sql(con)
        assert "postgres_scan(" in sql

    def test_table_name_in_sql(self):
        con = self._mock_con()
        src = PostgreSQLSource(dsn="host=localhost", table="orders")
        sql = src.as_sql(con)
        assert "orders" in sql

    def test_default_schema_is_public(self):
        con = self._mock_con()
        src = PostgreSQLSource(dsn="host=localhost", table="tbl")
        assert "public" in src.as_sql(con)

    def test_custom_schema_in_sql(self):
        con = self._mock_con()
        src = PostgreSQLSource(dsn="host=localhost", table="tbl", schema="hr")
        assert "hr" in src.as_sql(con)

    def test_dsn_in_sql(self):
        dsn = "host=pg.example.com dbname=mydb"
        con = self._mock_con()
        src = PostgreSQLSource(dsn=dsn, table="tbl")
        assert dsn in src.as_sql(con)

    def test_loads_extension(self):
        con = self._mock_con()
        src = PostgreSQLSource(dsn="host=localhost", table="tbl")
        src.as_sql(con)
        con.execute.assert_called()


# ---------------------------------------------------------------------------
# DeltaSource
# ---------------------------------------------------------------------------


class TestDeltaSource:
    def _mock_con(self):
        return MagicMock()

    def test_returns_delta_scan_when_use_attach_false(self):
        con = self._mock_con()
        src = DeltaSource("/data/lakehouse/my_table", use_attach=False)
        assert "delta_scan(" in src.as_sql(con)

    def test_local_path_in_sql_when_use_attach_false(self):
        con = self._mock_con()
        src = DeltaSource("/data/lakehouse/my_table", use_attach=False)
        assert "/data/lakehouse/my_table" in src.as_sql(con)

    def test_s3_path_in_sql_when_use_attach_false(self):
        con = self._mock_con()
        src = DeltaSource("s3://bucket/delta/tbl", use_attach=False)
        assert "s3://bucket/delta/tbl" in src.as_sql(con)

    def test_loads_extension(self):
        con = self._mock_con()
        src = DeltaSource("/some/path")
        src.as_sql(con)
        con.execute.assert_called()

    def test_default_use_attach_true(self):
        src = DeltaSource("/data/lakehouse/my_table")
        assert src._use_attach is True

    def test_attach_mode_returns_select(self):
        con = self._mock_con()
        src = DeltaSource("/data/lakehouse/my_table", use_attach=True)
        sql = src.as_sql(con)
        # ATTACH mode: returns SELECT * FROM <alias>
        assert "SELECT" in sql or "sqldim_delta" in sql

    def test_attach_failure_falls_back_to_delta_scan(self):
        """When ATTACH raises, as_sql falls back to delta_scan()."""
        con = self._mock_con()

        # Make ATTACH raise but INSTALL/LOAD succeed
        def _execute_side_effect(sql, *args, **kwargs):
            if sql.startswith("ATTACH"):
                raise RuntimeError("delta not supported")

        con.execute.side_effect = _execute_side_effect

        src = DeltaSource("/some/delta/path", use_attach=True)
        sql = src.as_sql(con)
        assert "delta_scan" in sql
        assert "/some/delta/path" in sql


# ---------------------------------------------------------------------------
# SQLSource
# ---------------------------------------------------------------------------


class TestSQLSource:
    def test_returns_verbatim_sql(self):
        raw = "SELECT a, b FROM raw WHERE c = 1"
        src = SQLSource(raw)
        assert src.as_sql(None) == raw

    def test_strips_trailing_semicolon(self):
        src = SQLSource("SELECT 1;")
        assert not src.as_sql(None).endswith(";")

    def test_strips_leading_whitespace(self):
        src = SQLSource("   SELECT 1")
        assert not src.as_sql(None).startswith(" ")

    def test_strips_trailing_whitespace_and_semicolon(self):
        src = SQLSource("  SELECT 1  ;  ")
        result = src.as_sql(None)
        assert not result.endswith(";")
        assert not result.startswith(" ")

    def test_complex_join_query(self):
        sql = "SELECT a.*, b.name FROM a JOIN b ON a.id = b.id"
        src = SQLSource(sql)
        assert src.as_sql(None) == sql

    def test_as_sql_ignores_con(self):
        src = SQLSource("SELECT 1")
        assert src.as_sql(None) == src.as_sql(MagicMock())

    def test_no_mutation_between_calls(self):
        src = SQLSource("SELECT x FROM t;")
        assert src.as_sql(None) == src.as_sql(None)


# ---------------------------------------------------------------------------
# coerce_source
# ---------------------------------------------------------------------------


class TestCoerceSource:
    def test_parquet_extension_returns_parquet_source(self):
        assert isinstance(coerce_source("data.parquet"), ParquetSource)

    def test_parq_extension_returns_parquet_source(self):
        assert isinstance(coerce_source("data.parq"), ParquetSource)

    def test_parquet_in_path_returns_parquet_source(self):
        assert isinstance(coerce_source("s3://bucket/parquet/my_table"), ParquetSource)

    def test_csv_extension_returns_csv_source(self):
        assert isinstance(coerce_source("data.csv"), CSVSource)

    def test_tsv_extension_returns_csv_source(self):
        assert isinstance(coerce_source("data.tsv"), CSVSource)

    def test_unknown_string_becomes_duckdb_source(self):
        assert isinstance(coerce_source("my_view_name"), DuckDBSource)

    def test_unknown_string_preserves_name(self):
        src = coerce_source("staging.my_table")
        assert "staging.my_table" in src.as_sql(None)

    def test_passthrough_source_adapter_unchanged(self):
        original = SQLSource("SELECT 1")
        assert coerce_source(original) is original

    def test_passthrough_parquet_source_unchanged(self):
        original = ParquetSource("x.parquet")
        assert coerce_source(original) is original

    def test_invalid_int_raises_type_error(self):
        with pytest.raises(TypeError, match="SourceAdapter or str"):
            coerce_source(42)

    def test_invalid_list_raises_type_error(self):
        with pytest.raises(TypeError):
            coerce_source(["a.parquet"])

    def test_invalid_none_raises_type_error(self):
        with pytest.raises(TypeError):
            coerce_source(None)

    def test_uppercase_extension_handled(self):
        # Path is lowercased for heuristic, ensure it works
        result = coerce_source("DATA.PARQUET")
        assert isinstance(result, ParquetSource)

    def test_parquet_result_generates_correct_sql(self):
        src = coerce_source("my_data.parquet")
        sql = src.as_sql(None)
        assert "read_parquet" in sql
        assert "my_data.parquet" in sql

    def test_csv_result_generates_correct_sql(self):
        src = coerce_source("my_data.csv")
        sql = src.as_sql(None)
        assert "read_csv" in sql

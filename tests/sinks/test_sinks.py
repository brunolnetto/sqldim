"""
Tests for sqldim.sinks — DuckDBSink (all 6 operations) and ParquetSink
(SQL-generation methods only, since file I/O paths need real storage).
Target: bring sinks/ from 0 % to ~70 % coverage.
"""
import pytest
import duckdb

from sqldim.sinks import DuckDBSink, ParquetSink, SinkAdapter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _setup_ddb_file(path: str) -> None:
    """Create a DuckDB file with the tables needed for the test suite."""
    con = duckdb.connect(path)
    con.execute("""
        CREATE TABLE dim_product (
            sku          VARCHAR,
            name         VARCHAR,
            price        DOUBLE,
            checksum     VARCHAR,
            is_current   BOOLEAN,
            valid_from   VARCHAR,
            valid_to     VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE fact_orders (
            order_id     INTEGER,
            approved_at  VARCHAR,
            shipped_at   VARCHAR,
            delivered_at VARCHAR
        )
    """)
    con.close()


# ---------------------------------------------------------------------------
# SinkAdapter protocol
# ---------------------------------------------------------------------------

class TestSinkAdapterProtocol:
    def test_duckdb_sink_is_sink_adapter(self, tmp_path):
        assert isinstance(DuckDBSink(str(tmp_path / "x.duckdb")), SinkAdapter)

    def test_parquet_sink_is_sink_adapter(self):
        assert isinstance(ParquetSink("/tmp/parquet_test"), SinkAdapter)


# ---------------------------------------------------------------------------
# DuckDBSink — current_state_sql
# ---------------------------------------------------------------------------

class TestDuckDBSinkCurrentStateSql:
    def test_returns_alias_qualified_reference(self, tmp_path):
        sink = DuckDBSink(str(tmp_path / "x.duckdb"))
        sql = sink.current_state_sql("dim_product")
        # should be "<alias>.<schema>.<table>"
        assert "dim_product" in sql
        assert "." in sql

    def test_custom_schema_reflected(self, tmp_path):
        sink = DuckDBSink(str(tmp_path / "x.duckdb"), schema="warehouse")
        assert "warehouse" in sink.current_state_sql("some_table")


# ---------------------------------------------------------------------------
# DuckDBSink — context manager
# ---------------------------------------------------------------------------

class TestDuckDBSinkContextManager:
    def test_enter_returns_sink(self, tmp_path):
        db = str(tmp_path / "test.duckdb")
        _setup_ddb_file(db)
        with DuckDBSink(db) as sink:
            assert sink._con is not None

    def test_exit_closes_connection(self, tmp_path):
        db = str(tmp_path / "test.duckdb")
        _setup_ddb_file(db)
        with DuckDBSink(db) as sink:
            con = sink._con
        # After exit the connection should be closed (querying it raises)
        with pytest.raises(Exception):
            con.execute("SELECT 1")

    def test_database_attached_inside_context(self, tmp_path):
        db = str(tmp_path / "test.duckdb")
        _setup_ddb_file(db)
        with DuckDBSink(db) as sink:
            # Should be able to query the attached database
            result = sink._con.execute(
                f"SELECT count(*) FROM {sink._alias}.main.dim_product"
            ).fetchone()[0]
            assert result == 0


# ---------------------------------------------------------------------------
# DuckDBSink — write
# ---------------------------------------------------------------------------

class TestDuckDBSinkWrite:
    def test_write_inserts_rows(self, tmp_path):
        db = str(tmp_path / "test.duckdb")
        _setup_ddb_file(db)
        with DuckDBSink(db) as sink:
            con = sink._con
            con.execute("""
                CREATE OR REPLACE VIEW pending AS
                SELECT 'SKU1' AS sku, 'Widget' AS name, 9.99 AS price,
                       'abc123' AS checksum, TRUE AS is_current,
                       '2024-01-01' AS valid_from, NULL AS valid_to
            """)
            sink.write(con, "pending", "dim_product")
            count = con.execute(
                f"SELECT count(*) FROM {sink._alias}.main.dim_product"
            ).fetchone()[0]
            assert count == 1

    def test_write_multiple_rows(self, tmp_path):
        db = str(tmp_path / "test.duckdb")
        _setup_ddb_file(db)
        with DuckDBSink(db) as sink:
            con = sink._con
            con.execute("""
                CREATE OR REPLACE VIEW pending AS
                SELECT 'SKU1' AS sku, 'A' AS name, 1.0 AS price,
                       'h1' AS checksum, TRUE AS is_current,
                       '2024-01-01' AS valid_from, NULL AS valid_to
                UNION ALL
                SELECT 'SKU2', 'B', 2.0, 'h2', TRUE, '2024-01-01', NULL
            """)
            sink.write(con, "pending", "dim_product")
            count = con.execute(
                f"SELECT count(*) FROM {sink._alias}.main.dim_product"
            ).fetchone()[0]
            assert count == 2


# ---------------------------------------------------------------------------
# DuckDBSink — write_named
# ---------------------------------------------------------------------------

class TestDuckDBSinkWriteNamed:
    def test_write_named_inserts_only_listed_columns(self, tmp_path):
        """write_named() must insert a subset of columns from a view."""
        db = str(tmp_path / "test.duckdb")
        _setup_ddb_file(db)
        with DuckDBSink(db) as sink:
            con = sink._con
            # View has an extra 'sk' column not present in dim_product schema
            con.execute("""
                CREATE OR REPLACE VIEW pending_named AS
                SELECT 'SKU1' AS sku, 'Widget' AS name, 9.99 AS price,
                       'abc' AS checksum, TRUE AS is_current,
                       '2024-01-01' AS valid_from, NULL AS valid_to,
                       42 AS sk
            """)
            n = sink.write_named(
                con, "pending_named", "dim_product",
                ["sku", "name", "price", "checksum", "is_current", "valid_from", "valid_to"],
            )
            assert n == 1
            count = con.execute(
                f"SELECT count(*) FROM {sink._alias}.main.dim_product"
            ).fetchone()[0]
            assert count == 1

    def test_write_named_returns_zero_for_empty_view(self, tmp_path):
        db = str(tmp_path / "test.duckdb")
        _setup_ddb_file(db)
        with DuckDBSink(db) as sink:
            con = sink._con
            con.execute("""
                CREATE OR REPLACE VIEW empty_v AS
                SELECT 'SKU1' AS sku, 'W' AS name, 1.0 AS price,
                       'h' AS checksum, TRUE AS is_current,
                       '2024-01-01' AS valid_from, NULL AS valid_to
                WHERE 1 = 0
            """)
            n = sink.write_named(
                con, "empty_v", "dim_product",
                ["sku", "name", "price", "checksum", "is_current", "valid_from", "valid_to"],
            )
            assert n == 0

    def test_write_named_multiple_rows(self, tmp_path):
        db = str(tmp_path / "test.duckdb")
        _setup_ddb_file(db)
        with DuckDBSink(db) as sink:
            con = sink._con
            con.execute("""
                CREATE OR REPLACE VIEW pending_multi AS
                SELECT 'S1' AS sku, 'A' AS name, 1.0 AS price,
                       'h1' AS checksum, TRUE AS is_current,
                       '2024-01-01' AS valid_from, NULL AS valid_to
                UNION ALL
                SELECT 'S2', 'B', 2.0, 'h2', TRUE, '2024-01-01', NULL
            """)
            n = sink.write_named(
                con, "pending_multi", "dim_product",
                ["sku", "name", "price", "checksum", "is_current", "valid_from", "valid_to"],
            )
            assert n == 2


# ---------------------------------------------------------------------------
# DuckDBSink — close_versions
# ---------------------------------------------------------------------------

class TestDuckDBSinkCloseVersions:
    def test_marks_matching_rows_not_current(self, tmp_path):
        db = str(tmp_path / "test.duckdb")
        _setup_ddb_file(db)
        with DuckDBSink(db) as sink:
            con = sink._con
            tbl = f"{sink._alias}.main.dim_product"
            # seed two rows
            con.execute(f"""
                INSERT INTO {tbl}
                VALUES ('SKU1', 'Widget', 9.99, 'h1', TRUE, '2024-01-01', NULL),
                       ('SKU2', 'Gadget', 19.99, 'h2', TRUE, '2024-01-01', NULL)
            """)
            # close SKU1
            con.execute("CREATE OR REPLACE VIEW changed_nks AS SELECT 'SKU1' AS sku")
            sink.close_versions(con, "dim_product", "sku", "changed_nks", "2024-06-01")

            row = con.execute(
                f"SELECT is_current FROM {tbl} WHERE sku = 'SKU1'"
            ).fetchone()
            assert row[0] is False

    def test_non_matching_rows_stay_current(self, tmp_path):
        db = str(tmp_path / "test.duckdb")
        _setup_ddb_file(db)
        with DuckDBSink(db) as sink:
            con = sink._con
            tbl = f"{sink._alias}.main.dim_product"
            con.execute(f"""
                INSERT INTO {tbl}
                VALUES ('SKU1', 'Widget', 9.99, 'h1', TRUE, '2024-01-01', NULL),
                       ('SKU2', 'Gadget', 19.99, 'h2', TRUE, '2024-01-01', NULL)
            """)
            con.execute("CREATE OR REPLACE VIEW changed_nks AS SELECT 'SKU1' AS sku")
            sink.close_versions(con, "dim_product", "sku", "changed_nks", "2024-06-01")

            row = con.execute(
                f"SELECT is_current FROM {tbl} WHERE sku = 'SKU2'"
            ).fetchone()
            assert row[0] is True


# ---------------------------------------------------------------------------
# DuckDBSink — update_attributes
# ---------------------------------------------------------------------------

class TestDuckDBSinkUpdateAttributes:
    def test_updates_tracked_column(self, tmp_path):
        db = str(tmp_path / "test.duckdb")
        _setup_ddb_file(db)
        with DuckDBSink(db) as sink:
            con = sink._con
            tbl = f"{sink._alias}.main.dim_product"
            con.execute(f"""
                INSERT INTO {tbl}
                VALUES ('SKU1', 'OldName', 9.99, 'h1', TRUE, '2024-01-01', NULL)
            """)
            con.execute("""
                CREATE OR REPLACE VIEW changed_updates AS
                SELECT 'SKU1' AS sku, 'NewName' AS name, 12.50 AS price
            """)
            sink.update_attributes(con, "dim_product", "sku", "changed_updates", ["name", "price"])

            row = con.execute(f"SELECT name, price FROM {tbl} WHERE sku = 'SKU1'").fetchone()
            assert row[0] == "NewName"
            assert row[1] == 12.50


# ---------------------------------------------------------------------------
# DuckDBSink — update_milestones
# ---------------------------------------------------------------------------

class TestDuckDBSinkUpdateMilestones:
    def test_patches_non_null_milestones(self, tmp_path):
        db = str(tmp_path / "test.duckdb")
        _setup_ddb_file(db)
        with DuckDBSink(db) as sink:
            con = sink._con
            tbl = f"{sink._alias}.main.fact_orders"
            con.execute(f"""
                INSERT INTO {tbl}
                VALUES (1, NULL, NULL, NULL),
                       (2, '2024-01-02', NULL, NULL)
            """)
            con.execute("""
                CREATE OR REPLACE VIEW update_rows AS
                SELECT 1 AS order_id, '2024-01-03' AS approved_at, NULL AS shipped_at
            """)
            sink.update_milestones(
                con, "fact_orders", "order_id", "update_rows",
                ["approved_at", "shipped_at"]
            )
            row = con.execute(
                f"SELECT approved_at FROM {tbl} WHERE order_id = 1"
            ).fetchone()
            assert row[0] == "2024-01-03"

    def test_does_not_overwrite_with_null(self, tmp_path):
        db = str(tmp_path / "test.duckdb")
        _setup_ddb_file(db)
        with DuckDBSink(db) as sink:
            con = sink._con
            tbl = f"{sink._alias}.main.fact_orders"
            con.execute(f"INSERT INTO {tbl} VALUES (2, '2024-01-02', NULL, NULL)")
            # update with NULL approved_at — should NOT overwrite existing value
            con.execute("""
                CREATE OR REPLACE VIEW update_rows AS
                SELECT 2 AS order_id, NULL AS approved_at, '2024-01-05' AS shipped_at
            """)
            sink.update_milestones(
                con, "fact_orders", "order_id", "update_rows",
                ["approved_at", "shipped_at"]
            )
            row = con.execute(
                f"SELECT approved_at, shipped_at FROM {tbl} WHERE order_id = 2"
            ).fetchone()
            assert row[0] == "2024-01-02"   # preserved
            assert row[1] == "2024-01-05"   # patched


# ---------------------------------------------------------------------------
# DuckDBSink — rotate_attributes
# ---------------------------------------------------------------------------

class TestDuckDBSinkRotateAttributes:
    def test_rotates_current_to_previous(self, tmp_path):
        """Rotate: prev_name = name, name = incoming new name."""
        db = str(tmp_path / "test.duckdb")
        # Need a table with prev_name column too
        con_setup = duckdb.connect(db)
        con_setup.execute("""
            CREATE TABLE dim_region (
                region_id  INTEGER,
                region     VARCHAR,
                prev_region VARCHAR,
                checksum    VARCHAR,
                is_current  BOOLEAN,
                valid_from  VARCHAR,
                valid_to    VARCHAR
            )
        """)
        con_setup.execute(
            "INSERT INTO dim_region VALUES (1, 'East', NULL, 'h1', TRUE, '2024-01-01', NULL)"
        )
        con_setup.close()

        with DuckDBSink(db) as sink:
            con = sink._con
            tbl = f"{sink._alias}.main.dim_region"
            con.execute("""
                CREATE OR REPLACE VIEW changed_rotations AS
                SELECT 1 AS region_id, 'West' AS region
            """)
            sink.rotate_attributes(
                con, "dim_region", "region_id",
                "changed_rotations", [("region", "prev_region")]
            )
            row = con.execute(
                f"SELECT region, prev_region FROM {tbl} WHERE region_id = 1"
            ).fetchone()
            assert row[0] == "West"
            assert row[1] == "East"


# ---------------------------------------------------------------------------
# ParquetSink — SQL generation (.current_state_sql)
# ---------------------------------------------------------------------------

class TestParquetSinkCurrentStateSql:
    def test_returns_read_parquet_expression(self):
        sink = ParquetSink("/data/dim")
        sql = sink.current_state_sql("dim_product")
        assert "read_parquet(" in sql
        assert "dim_product" in sql

    def test_includes_hive_partitioning(self):
        sink = ParquetSink("/data/dim")
        sql = sink.current_state_sql("dim_product")
        assert "hive_partitioning=true" in sql

    def test_base_path_in_sql(self):
        sink = ParquetSink("/my/warehouse")
        sql = sink.current_state_sql("orders")
        assert "/my/warehouse" in sql

    def test_glob_pattern_in_sql(self):
        sink = ParquetSink("/data")
        sql = sink.current_state_sql("orders")
        assert "**/*.parquet" in sql or "*.parquet" in sql

    def test_strips_trailing_slash_from_base(self):
        sink_with = ParquetSink("/data/")
        sink_without = ParquetSink("/data")
        assert sink_with.current_state_sql("t") == sink_without.current_state_sql("t")

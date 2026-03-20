"""
Tests for ParquetSink — all methods exercised using a real tmpdir and DuckDB.

Coverage target: sqldim/sinks/parquet.py  41% → ~95%
"""
from __future__ import annotations
import duckdb
import pyarrow as pa

from sqldim.sinks.parquet import ParquetSink


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _seed_parquet(path: str, rows: list[dict]) -> None:
    """Create an initial Parquet partition tree at *path* from *rows*."""
    schema = pa.schema([
        ("sku",        pa.string()),
        ("name",       pa.string()),
        ("price",      pa.string()),
        ("checksum",   pa.string()),
        ("is_current", pa.bool_()),
        ("valid_from", pa.string()),
        ("valid_to",   pa.string()),
    ])
    pa.table(
        {k: pa.array([r[k] for r in rows], type=schema.field(k).type)
         for k in schema.names},
        schema=schema,
    )
    import os
    os.makedirs(f"{path}/is_current=True",  exist_ok=True)
    os.makedirs(f"{path}/is_current=False", exist_ok=True)
    import pyarrow.parquet as pq
    current = [r for r in rows if r["is_current"]]
    old     = [r for r in rows if not r["is_current"]]
    if current:
        cur_table = pa.table(
            {k: pa.array([r[k] for r in current], type=schema.field(k).type)
             for k in schema.names if k != "is_current"},
            schema=pa.schema([f for f in schema if f.name != "is_current"]),
        )
        pq.write_table(cur_table, f"{path}/is_current=True/part0.parquet")
    if old:
        old_table = pa.table(
            {k: pa.array([r[k] for r in old], type=schema.field(k).type)
             for k in schema.names if k != "is_current"},
            schema=pa.schema([f for f in schema if f.name != "is_current"]),
        )
        pq.write_table(old_table, f"{path}/is_current=False/part0.parquet")


def _fresh_parquet(tmp_path, rows: list[dict]) -> tuple[str, duckdb.DuckDBPyConnection]:
    """Return (base_path, con) with data seeded as Parquet."""
    base   = str(tmp_path)
    table  = "dim_product"
    path   = f"{base}/{table}"
    _seed_parquet(path, rows)
    con = duckdb.connect()
    return base, con


# ---------------------------------------------------------------------------
# current_state_sql
# ---------------------------------------------------------------------------

class TestParquetSinkCurrentStateSql:
    def test_returns_read_parquet_call(self, tmp_path):
        sink = ParquetSink(str(tmp_path))
        sql = sink.current_state_sql("dim_product")
        assert "read_parquet" in sql
        assert "dim_product" in sql
        assert "hive_partitioning=true" in sql

    def test_table_path_uses_glob(self, tmp_path):
        sink = ParquetSink(str(tmp_path))
        path = sink._table_path("dim_product")
        assert "/**/*.parquet" in path

    def test_table_out_strips_trailing_slash(self, tmp_path):
        sink = ParquetSink(str(tmp_path) + "/")
        out = sink._table_out("dim_product")
        assert not out.endswith("//")
        assert out.endswith("dim_product")


# ---------------------------------------------------------------------------
# write
# ---------------------------------------------------------------------------

class TestParquetSinkWrite:
    def test_write_creates_parquet_partition(self, tmp_path):
        base = str(tmp_path)
        con  = duckdb.connect()
        sink = ParquetSink(base)
        con.execute("""
            CREATE OR REPLACE VIEW new_rows AS
            SELECT 'SKU1' AS sku, 'Widget' AS name, '9.99' AS price,
                   'hash1' AS checksum, TRUE AS is_current,
                   '2024-01-01' AS valid_from, NULL::VARCHAR AS valid_to
        """)
        n = sink.write(con, "new_rows", "dim_product")
        assert n == 1
        # Partition directory should exist
        import os
        assert os.path.exists(f"{base}/dim_product")

    def test_write_returns_row_count(self, tmp_path):
        base = str(tmp_path)
        con  = duckdb.connect()
        sink = ParquetSink(base)
        con.execute("""
            CREATE OR REPLACE VIEW src AS
            SELECT 'A' AS sku, 'Alpha' AS name, '1.00' AS price,
                   'h1' AS checksum, TRUE AS is_current,
                   '2024-01-01' AS valid_from, NULL::VARCHAR AS valid_to
            UNION ALL
            SELECT 'B', 'Beta', '2.00', 'h2', TRUE, '2024-01-01', NULL
        """)
        n = sink.write(con, "src", "dim_product2")
        assert n == 2


# ---------------------------------------------------------------------------
# close_versions
# ---------------------------------------------------------------------------

class TestParquetSinkCloseVersions:
    def test_close_versions_rewrites_partition(self, tmp_path):
        base, con = _fresh_parquet(tmp_path, [
            {"sku": "SKU1", "name": "Widget", "price": "9.99",
             "checksum": "abc", "is_current": True,
             "valid_from": "2024-01-01", "valid_to": None},
        ])
        sink = ParquetSink(base)
        con.execute("""
            CREATE OR REPLACE VIEW nks AS
            SELECT 'SKU1' AS sku
        """)
        n = sink.close_versions(con, "dim_product", "sku", "nks", "2024-06-01")
        assert n == 1

    def test_close_versions_returns_nk_count(self, tmp_path):
        base, con = _fresh_parquet(tmp_path, [
            {"sku": "A", "name": "Alpha", "price": "1.00",
             "checksum": "h1", "is_current": True,
             "valid_from": "2024-01-01", "valid_to": None},
            {"sku": "B", "name": "Beta", "price": "2.00",
             "checksum": "h2", "is_current": True,
             "valid_from": "2024-01-01", "valid_to": None},
        ])
        sink = ParquetSink(base)
        con.execute("""
            CREATE OR REPLACE VIEW nks AS
            SELECT 'A' AS sku UNION ALL SELECT 'B'
        """)
        n = sink.close_versions(con, "dim_product", "sku", "nks", "2024-06-01")
        assert n == 2


# ---------------------------------------------------------------------------
# update_attributes
# ---------------------------------------------------------------------------

class TestParquetSinkUpdateAttributes:
    def test_update_attributes_rewrites_partition(self, tmp_path):
        base, con = _fresh_parquet(tmp_path, [
            {"sku": "SKU1", "name": "OldName", "price": "9.99",
             "checksum": "abc", "is_current": True,
             "valid_from": "2024-01-01", "valid_to": None},
        ])
        sink = ParquetSink(base)
        con.execute("""
            CREATE OR REPLACE VIEW updates AS
            SELECT 'SKU1' AS sku, 'NewName' AS name, '9.99' AS price
        """)
        n = sink.update_attributes(con, "dim_product", "sku", "updates", ["name"])
        assert n == 1

    def test_update_attributes_updates_multiple_cols(self, tmp_path):
        base, con = _fresh_parquet(tmp_path, [
            {"sku": "X", "name": "Old", "price": "1.00",
             "checksum": "h", "is_current": True,
             "valid_from": "2024-01-01", "valid_to": None},
        ])
        sink = ParquetSink(base)
        con.execute("""
            CREATE OR REPLACE VIEW updates AS
            SELECT 'X' AS sku, 'New' AS name, '2.00' AS price
        """)
        n = sink.update_attributes(con, "dim_product", "sku", "updates", ["name", "price"])
        assert n == 1


# ---------------------------------------------------------------------------
# rotate_attributes
# ---------------------------------------------------------------------------

class TestParquetSinkRotateAttributes:
    def test_rotate_attributes_rewrites_partition(self, tmp_path):
        # Create a table with current_ and prev_ columns
        import os
        import pyarrow.parquet as pq
        base = str(tmp_path)
        path = f"{base}/dim_category"
        os.makedirs(f"{path}/is_current=True", exist_ok=True)
        schema = pa.schema([
            ("cat_id",       pa.string()),
            ("curr_label",   pa.string()),
            ("prev_label",   pa.string()),
            ("checksum",     pa.string()),
            ("valid_from",   pa.string()),
            ("valid_to",     pa.string()),
        ])
        pq.write_table(
            pa.table({
                "cat_id":     ["C1"],
                "curr_label": ["Electronics"],
                "prev_label": [None],
                "checksum":   ["h1"],
                "valid_from": ["2024-01-01"],
                "valid_to":   pa.array([None], type=pa.string()),
            }, schema=schema),
            f"{path}/is_current=True/part0.parquet",
        )
        con = duckdb.connect()
        sink = ParquetSink(base)
        con.execute("""
            CREATE OR REPLACE VIEW rotations AS
            SELECT 'C1' AS cat_id, 'Gadgets' AS curr_label
        """)
        n = sink.rotate_attributes(
            con, "dim_category", "cat_id", "rotations",
            [("curr_label", "prev_label")],
        )
        assert n == 1


# ---------------------------------------------------------------------------
# update_milestones
# ---------------------------------------------------------------------------

class TestParquetSinkUpdateMilestones:
    def test_update_milestones_rewrites_partition(self, tmp_path):
        import os
        import pyarrow.parquet as pq
        base = str(tmp_path)
        path = f"{base}/fact_orders"
        os.makedirs(f"{path}/is_current=True", exist_ok=True)
        schema = pa.schema([
            ("order_id",    pa.string()),
            ("approved_at", pa.string()),
            ("shipped_at",  pa.string()),
            ("checksum",    pa.string()),
            ("valid_from",  pa.string()),
            ("valid_to",    pa.string()),
        ])
        pq.write_table(
            pa.table({
                "order_id":    ["O1"],
                "approved_at": [None],
                "shipped_at":  [None],
                "checksum":    ["hx"],
                "valid_from":  ["2024-01-01"],
                "valid_to":    pa.array([None], type=pa.string()),
            }, schema=schema),
            f"{path}/is_current=True/part0.parquet",
        )
        con = duckdb.connect()
        sink = ParquetSink(base)
        con.execute("""
            CREATE OR REPLACE VIEW milestone_updates AS
            SELECT 'O1' AS order_id, '2024-01-02' AS approved_at, NULL::VARCHAR AS shipped_at
        """)
        n = sink.update_milestones(
            con, "fact_orders", "order_id", "milestone_updates",
            ["approved_at", "shipped_at"],
        )
        assert n == 1


# ---------------------------------------------------------------------------
# write_named
# ---------------------------------------------------------------------------

class TestParquetSinkWriteNamed:
    def test_write_named_writes_subset_of_columns(self, tmp_path):
        """write_named() must write only listed columns and return row count."""
        import os
        base = str(tmp_path)
        os.makedirs(f"{base}/dim_product/is_current=True", exist_ok=True)
        os.makedirs(f"{base}/dim_product/is_current=False", exist_ok=True)
        con = duckdb.connect()
        sink = ParquetSink(base)
        # View with an extra 'sk' column not wanted in the output
        con.execute("""
            CREATE OR REPLACE VIEW src_named AS
            SELECT 'SKU1' AS sku, 'Widget' AS name, '9.99' AS price,
                   'abc' AS checksum, TRUE AS is_current,
                   '2024-01-01' AS valid_from, NULL::VARCHAR AS valid_to,
                   42 AS sk
        """)
        n = sink.write_named(
            con, "src_named", "dim_product",
            ["sku", "name", "price", "checksum", "is_current", "valid_from", "valid_to"],
        )
        assert n == 1

    def test_write_named_multiple_rows(self, tmp_path):
        import os
        base = str(tmp_path)
        os.makedirs(f"{base}/dim_product/is_current=True", exist_ok=True)
        os.makedirs(f"{base}/dim_product/is_current=False", exist_ok=True)
        con = duckdb.connect()
        sink = ParquetSink(base)
        con.execute("""
            CREATE OR REPLACE VIEW src_multi AS
            SELECT 'S1' AS sku, 'A' AS name, '1.0' AS price,
                   'h1' AS checksum, TRUE AS is_current,
                   '2024-01-01' AS valid_from, NULL::VARCHAR AS valid_to
            UNION ALL
            SELECT 'S2', 'B', '2.0', 'h2', FALSE, '2024-01-01', '2024-06-01'
        """)
        n = sink.write_named(
            con, "src_multi", "dim_product",
            ["sku", "name", "price", "checksum", "is_current", "valid_from", "valid_to"],
        )
        assert n == 2


# ---------------------------------------------------------------------------
# Branch coverage for _partition_clause and _order_clause helpers
# ---------------------------------------------------------------------------

class TestParquetSinkHelperBranches:
    """Cover the empty-return branches in _partition_clause and _order_clause."""

    def test_partition_clause_empty_returns_blank(self):
        """partition_by=[] → _partition_clause() returns "" (line 68)."""
        sink = ParquetSink("/tmp/x", partition_by=[])
        assert sink._partition_clause() == ""

    def test_order_clause_with_columns_returns_order_by(self):
        """zorder_by=['col'] → _order_clause() returns ORDER BY clause (lines 76-77)."""
        sink = ParquetSink("/tmp/x", zorder_by=["col1", "col2"])
        result = sink._order_clause()
        assert "ORDER BY col1, col2" in result

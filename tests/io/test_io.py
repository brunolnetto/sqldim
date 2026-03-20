"""Tests for sqldim.io — clustering strategies and Compactor."""
from __future__ import annotations

import os
import tempfile
from unittest.mock import MagicMock

import duckdb

from sqldim.io import ClusteringStrategy, Compactor, SortBasedClustering
from sqldim.io.clustering import NoOpClustering


# ---------------------------------------------------------------------------
# ClusteringStrategy protocol
# ---------------------------------------------------------------------------

class TestClusteringStrategyProtocol:
    def test_sort_based_satisfies_protocol(self):
        assert isinstance(SortBasedClustering(), ClusteringStrategy)

    def test_noop_satisfies_protocol(self):
        assert isinstance(NoOpClustering(), ClusteringStrategy)


# ---------------------------------------------------------------------------
# SortBasedClustering
# ---------------------------------------------------------------------------

class TestSortBasedClustering:
    def setup_method(self):
        self.s = SortBasedClustering()

    def test_single_column(self):
        sql = self.s.cluster_sql("my_table", ["customer_id"])
        assert sql == "SELECT * FROM my_table ORDER BY customer_id"

    def test_multiple_columns(self):
        sql = self.s.cluster_sql("my_table", ["customer_id", "product_id"])
        assert sql == "SELECT * FROM my_table ORDER BY customer_id, product_id"

    def test_empty_columns_no_order_by(self):
        sql = self.s.cluster_sql("my_table", [])
        assert sql == "SELECT * FROM my_table"
        assert "ORDER BY" not in sql


# ---------------------------------------------------------------------------
# NoOpClustering
# ---------------------------------------------------------------------------

class TestNoOpClustering:
    def setup_method(self):
        self.s = NoOpClustering()

    def test_ignores_columns(self):
        sql = self.s.cluster_sql("my_table", ["customer_id", "product_id"])
        assert sql == "SELECT * FROM my_table"

    def test_empty_columns(self):
        sql = self.s.cluster_sql("my_table", [])
        assert sql == "SELECT * FROM my_table"


# ---------------------------------------------------------------------------
# Compactor
# ---------------------------------------------------------------------------

class TestCompactor:
    """Exercises Compactor.compact() — SQL generation and real round-trips."""

    def _mock_con(self, row_count: int = 10):
        """Return a mock DuckDB connection that returns *row_count* from the count query."""
        con = MagicMock()
        con.execute.return_value.fetchone.return_value = (row_count,)
        return con

    def _write_parquet_partition(self, tmp_path: str) -> str:
        """Write a partitioned Parquet table and return its root directory."""
        table_dir = os.path.join(tmp_path, "my_table", "is_current=1")
        os.makedirs(table_dir, exist_ok=True)
        part_path = os.path.join(table_dir, "part0.parquet")
        setup_con = duckdb.connect()
        setup_con.execute(
            f"COPY (SELECT i AS id, 'val_' || i AS label FROM range(1, 11) t(i)) "
            f"TO '{part_path}' (FORMAT parquet)"
        )
        setup_con.close()
        return os.path.join(tmp_path, "my_table")

    # -- SQL generation tests (no filesystem required) ----------------

    def test_compact_no_options_generates_plain_copy(self):
        con = self._mock_con()
        n = Compactor.compact(con, "/data/my_table")
        assert n == 10
        copy_sql = con.execute.call_args_list[0][0][0]
        assert "COPY" in copy_sql
        assert "ORDER BY" not in copy_sql
        assert "PARTITION_BY" not in copy_sql
        assert "OVERWRITE_OR_IGNORE true" in copy_sql

    def test_compact_zorder_by_generates_order_clause(self):
        con = self._mock_con()
        Compactor.compact(con, "/data/my_table", zorder_by=["customer_id", "product_id"])
        copy_sql = con.execute.call_args_list[0][0][0]
        assert "ORDER BY customer_id, product_id" in copy_sql

    def test_compact_partition_by_generates_partition_clause(self):
        con = self._mock_con()
        Compactor.compact(con, "/data/my_table", partition_by=["ingest_date"])
        copy_sql = con.execute.call_args_list[0][0][0]
        assert "PARTITION_BY (ingest_date)" in copy_sql

    def test_compact_overwrite_false_omits_overwrite_or_ignore(self):
        con = self._mock_con()
        Compactor.compact(con, "/data/my_table", overwrite=False)
        copy_sql = con.execute.call_args_list[0][0][0]
        assert "OVERWRITE_OR_IGNORE" not in copy_sql

    def test_compact_returns_row_count(self):
        con = self._mock_con(row_count=42)
        n = Compactor.compact(con, "/data/my_table")
        assert n == 42

    # -- Real round-trip test with DuckDB + Parquet -------------------

    def test_compact_real_parquet_with_partition_by(self):
        with tempfile.TemporaryDirectory() as tmp:
            table_path = self._write_parquet_partition(tmp)
            con = duckdb.connect()
            n = Compactor.compact(con, table_path, partition_by=["is_current"])
            assert n >= 10

"""Tests for sqldim.io — clustering strategies (Compactor: test_compaction.py)."""

from __future__ import annotations

from sqldim.io import ClusteringStrategy, SortBasedClustering
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

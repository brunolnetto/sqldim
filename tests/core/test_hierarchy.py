"""Tests for HierarchyMixin, strategies, and HierarchyRoller."""
import pytest
from sqldim.core.kimball.dimensions.hierarchy import (
    HierarchyMixin,
    AdjacencyListStrategy,
    MaterializedPathStrategy,
    ClosureTableStrategy,
    HierarchyRoller,
)


# ---------------------------------------------------------------------------
# HierarchyMixin.hierarchy_strategy — unknown name raises ValueError
# ---------------------------------------------------------------------------

class TestHierarchyMixinStrategy:
    def test_unknown_strategy_raises(self):
        class BadHierarchyDim(HierarchyMixin):
            __hierarchy_strategy__ = "nonexistent"

        with pytest.raises(ValueError, match="Unknown hierarchy strategy"):
            BadHierarchyDim.hierarchy_strategy()

    def test_adjacency_strategy_returned(self):
        class AdjDim(HierarchyMixin):
            __hierarchy_strategy__ = "adjacency"

        strategy = AdjDim.hierarchy_strategy()
        assert isinstance(strategy, AdjacencyListStrategy)

    def test_materialized_path_strategy_returned(self):
        class MpDim(HierarchyMixin):
            __hierarchy_strategy__ = "materialized_path"

        strategy = MpDim.hierarchy_strategy()
        assert isinstance(strategy, MaterializedPathStrategy)

    def test_closure_strategy_returned(self):
        class ClosureDim(HierarchyMixin):
            __hierarchy_strategy__ = "closure"

        strategy = ClosureDim.hierarchy_strategy()
        assert isinstance(strategy, ClosureTableStrategy)


# ---------------------------------------------------------------------------
# MaterializedPathStrategy.rollup_sql — covers line 445
# ---------------------------------------------------------------------------

class TestMaterializedPathStrategy:
    def test_rollup_sql_contains_hierarchy_path(self):
        sql = MaterializedPathStrategy().rollup_sql(
            fact_table="fact_sales",
            dim_table="product_dim",
            measure="revenue",
            level=2,
        )
        assert "hierarchy_path" in sql
        assert "fact_sales" in sql
        assert "product_dim" in sql
        assert "revenue" in sql

    def test_build_path_root_node(self):
        """parent_path=None (root node) → /node_id/ (line 480)"""
        path = MaterializedPathStrategy.build_path(None, 42)
        assert path == "/42/"

    def test_build_path_child_node(self):
        path = MaterializedPathStrategy.build_path("/1/5/", 12)
        assert path == "/1/5/12/"


# ---------------------------------------------------------------------------
# HierarchyRoller — explicit strategy override (lines 723-726)
# ---------------------------------------------------------------------------

class TestHierarchyRoller:
    def test_rollup_sql_with_explicit_strategy(self):
        """Passing strategy= uses it directly, skipping auto-detect."""
        sql = HierarchyRoller.rollup_sql(
            fact_table="sales_fact",
            dim_table="org_dim",
            measure="revenue",
            level=2,
            strategy=ClosureTableStrategy(),
        )
        assert "sales_fact" in sql
        assert "org_dim" in sql
        assert "revenue" in sql

    def test_rollup_sql_with_dim_cls(self):
        """dim_cls= auto-detects strategy from class attribute."""
        class OrgDim(HierarchyMixin):
            __hierarchy_strategy__ = "adjacency"

        sql = HierarchyRoller.rollup_sql(
            fact_table="sales_fact",
            dim_table="org_dim",
            measure="revenue",
            level=2,
            dim_cls=OrgDim,
        )
        assert "sales_fact" in sql

    def test_rollup_sql_default_strategy(self):
        """No strategy, no dim_cls → defaults to AdjacencyListStrategy."""
        sql = HierarchyRoller.rollup_sql(
            fact_table="f", dim_table="d", measure="m", level=1
        )
        assert "f" in sql
        assert "d" in sql


# ---------------------------------------------------------------------------
# OrgChartSource — teardown and snapshot (datasets/hierarchy.py lines 203-209)
# ---------------------------------------------------------------------------

class TestOrgChartSource:
    def test_teardown_drops_tables(self):
        import duckdb
        from sqldim.application.datasets.domains.hierarchy import OrgChartSource

        src = OrgChartSource()
        con = duckdb.connect()
        con.execute("CREATE TABLE org_dim (id INT)")
        con.execute("CREATE TABLE sales_fact (id INT)")
        con.execute("CREATE TABLE org_dim_closure (id INT)")
        src.teardown(con)  # should drop all three without error
        tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
        assert "org_dim" not in tables
        assert "sales_fact" not in tables
        assert "org_dim_closure" not in tables

    def test_snapshot_raises_not_implemented(self):
        from sqldim.application.datasets.domains.hierarchy import OrgChartSource

        src = OrgChartSource()
        with pytest.raises(NotImplementedError):
            src.snapshot()

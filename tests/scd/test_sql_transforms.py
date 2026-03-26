"""
Tests for SQLTransform and SQLTransformPipeline.

Coverage target: sqldim/processors/sql_transforms.py — 100%
"""

from __future__ import annotations

import duckdb

from sqldim.core.kimball.dimensions.scd.processors.sql_transforms import (
    SQLTransform,
    SQLTransformPipeline,
)


class TestSQLTransform:
    def test_as_select_expr_simple(self):
        t = SQLTransform("name", "upper(trim(name))")
        assert t.as_select_expr() == "upper(trim(name)) AS name"

    def test_as_select_expr_case_expression(self):
        expr = "CASE status WHEN '1' THEN 'active' ELSE 'inactive' END"
        t = SQLTransform("status", expr)
        assert t.as_select_expr() == f"{expr} AS status"

    def test_stores_column_and_expression(self):
        t = SQLTransform("col", "round(col::double, 2)")
        assert t.column == "col"
        assert t.expression == "round(col::double, 2)"


def _make_view(con: duckdb.DuckDBPyConnection) -> None:
    """Register a simple ``incoming`` view with three columns."""
    con.execute("""
        CREATE OR REPLACE VIEW incoming AS
        SELECT 'alice' AS name, '  hello  ' AS city, 99 AS score
    """)


class TestSQLTransformPipeline:
    def test_empty_pipeline_is_noop(self):
        con = duckdb.connect()
        _make_view(con)
        pipeline = SQLTransformPipeline([])
        pipeline.apply(con, "incoming")  # should not raise
        row = con.execute("SELECT name FROM incoming").fetchone()
        assert row[0] == "alice"

    def test_single_transform_rewrites_column(self):
        con = duckdb.connect()
        _make_view(con)
        pipeline = SQLTransformPipeline(
            [
                SQLTransform("name", "upper(name)"),
            ]
        )
        pipeline.apply(con, "incoming")
        row = con.execute("SELECT name, city FROM incoming").fetchone()
        assert row[0] == "ALICE"
        assert row[1] == "  hello  "  # untransformed

    def test_multiple_transforms_applied_in_order(self):
        con = duckdb.connect()
        _make_view(con)
        pipeline = SQLTransformPipeline(
            [
                SQLTransform("name", "upper(name)"),
                SQLTransform("city", "trim(city)"),
            ]
        )
        pipeline.apply(con, "incoming")
        row = con.execute("SELECT name, city FROM incoming").fetchone()
        assert row[0] == "ALICE"
        assert row[1] == "hello"

    def test_column_order_preserved(self):
        con = duckdb.connect()
        _make_view(con)
        pipeline = SQLTransformPipeline(
            [
                SQLTransform("score", "score * 2"),
            ]
        )
        pipeline.apply(con, "incoming")
        cols = [desc[0] for desc in con.execute("DESCRIBE incoming").fetchall()]
        assert cols == ["name", "city", "score"]

    def test_untransformed_columns_unchanged(self):
        con = duckdb.connect()
        _make_view(con)
        pipeline = SQLTransformPipeline(
            [
                SQLTransform("name", "lower(name)"),
            ]
        )
        pipeline.apply(con, "incoming")
        row = con.execute("SELECT score FROM incoming").fetchone()
        assert row[0] == 99

    def test_apply_uses_default_incoming_view(self):
        con = duckdb.connect()
        _make_view(con)
        pipeline = SQLTransformPipeline([SQLTransform("city", "trim(city)")])
        pipeline.apply(con)  # default source_view="incoming"
        row = con.execute("SELECT city FROM incoming").fetchone()
        assert row[0] == "hello"

    def test_transform_to_numeric_expression(self):
        con = duckdb.connect()
        con.execute("""
            CREATE OR REPLACE VIEW incoming AS
            SELECT 100 AS score, 'x' AS tag
        """)
        pipeline = SQLTransformPipeline(
            [
                SQLTransform("score", "score + 5"),
            ]
        )
        pipeline.apply(con, "incoming")
        row = con.execute("SELECT score FROM incoming").fetchone()
        assert row[0] == 105

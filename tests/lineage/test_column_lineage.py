"""Tests for sqldim.lineage.column — ColumnLineageFacet, extract_* functions."""

from __future__ import annotations

import sys
from unittest.mock import patch

import pytest

from sqldim.lineage.column import (
    ColumnLineageEntry,
    ColumnLineageFacet,
    extract_declared_lineage,
    extract_structural_lineage,
    extract_sql_lineage,
)


# ---------------------------------------------------------------------------
# ColumnLineageEntry
# ---------------------------------------------------------------------------


class TestColumnLineageEntry:
    def test_to_dict_shape(self):
        entry = ColumnLineageEntry(
            output_column="customer_sk",
            input_columns=["customer_id"],
            transform_description="Direct mapping",
            transform_sql=None,
            confidence="declared",
        )
        d = entry.to_dict()
        assert d["outputColumn"] == "customer_sk"
        assert d["inputColumns"] == ["customer_id"]
        assert d["transformDescription"] == "Direct mapping"
        assert d["transformSql"] is None
        assert d["confidence"] == "declared"

    def test_to_dict_with_transform_sql(self):
        entry = ColumnLineageEntry(
            output_column="full_name",
            input_columns=["first_name", "last_name"],
            transform_description="CONCAT(first_name, ' ', last_name)",
            transform_sql="CONCAT(first_name, ' ', last_name)",
            confidence="parsed",
        )
        d = entry.to_dict()
        assert d["transformSql"] == "CONCAT(first_name, ' ', last_name)"
        assert d["confidence"] == "parsed"


# ---------------------------------------------------------------------------
# ColumnLineageFacet
# ---------------------------------------------------------------------------


class TestColumnLineageFacet:
    def _make_facet(self):
        return ColumnLineageFacet(
            entries=[
                ColumnLineageEntry("customer_sk", ["customer_id"]),
                ColumnLineageEntry(
                    "order_total",
                    ["amount", "tax"],
                    transform_description="amount + tax",
                    confidence="parsed",
                ),
            ]
        )

    def test_to_dict_structure(self):
        facet = self._make_facet()
        d = facet.to_dict()
        assert d["_producer"] == "sqldim"
        assert "_schemaURL" in d
        assert len(d["fields"]) == 2
        assert d["fields"][0]["outputColumn"] == "customer_sk"

    def test_to_openlineage_fields_basic(self):
        facet = self._make_facet()
        fields = facet.to_openlineage_fields()
        assert len(fields) == 2
        first = fields[0]
        assert first["name"] == "customer_sk"
        assert len(first["inputFields"]) == 1
        assert first["inputFields"][0]["name"] == "customer_id"

    def test_to_openlineage_fields_transform_type_identity(self):
        facet = ColumnLineageFacet(
            entries=[
                ColumnLineageEntry(
                    "col_a", ["col_a"], transform_description="Direct mapping"
                ),
            ]
        )
        fields = facet.to_openlineage_fields()
        assert fields[0]["inputFields"][0]["transformType"] == "IDENTITY"

    def test_to_openlineage_fields_transform_type_custom(self):
        facet = ColumnLineageFacet(
            entries=[
                ColumnLineageEntry("col_b", ["a", "b"], transform_description="a + b"),
            ]
        )
        fields = facet.to_openlineage_fields()
        assert fields[0]["inputFields"][0]["transformType"] == "CUSTOM"

    def test_to_openlineage_fields_qualified_column(self):
        """Columns with table prefix should be split into namespace/name."""
        facet = ColumnLineageFacet(
            entries=[
                ColumnLineageEntry("total", ["orders.amount"]),
            ]
        )
        fields = facet.to_openlineage_fields()
        input_field = fields[0]["inputFields"][0]
        assert input_field["namespace"] == "orders"
        assert input_field["name"] == "amount"

    def test_to_openlineage_fields_unqualified_column(self):
        """Bare column names have empty namespace."""
        facet = ColumnLineageFacet(
            entries=[
                ColumnLineageEntry("total", ["amount"]),
            ]
        )
        fields = facet.to_openlineage_fields()
        input_field = fields[0]["inputFields"][0]
        assert input_field["namespace"] == ""
        assert input_field["name"] == "amount"

    def test_to_openlineage_fields_no_inputs(self):
        """Entry with no input columns produces a field with no inputFields key."""
        facet = ColumnLineageFacet(
            entries=[
                ColumnLineageEntry("computed", []),
            ]
        )
        fields = facet.to_openlineage_fields()
        assert "inputFields" not in fields[0]

    def test_empty_facet(self):
        facet = ColumnLineageFacet()
        assert facet.to_dict()["fields"] == []
        assert facet.to_openlineage_fields() == []


# ---------------------------------------------------------------------------
# extract_declared_lineage
# ---------------------------------------------------------------------------


class _FakeCol:
    """Minimal stand-in for a SQLAlchemy column."""

    def __init__(self, name, info=None):
        self.name = name
        self.info = info or {}


class _FakeTable:
    def __init__(self, cols):
        self.columns = cols


class _ModelWithLineage:
    pass


class _ModelWithoutTable:
    pass


class TestExtractDeclaredLineage:
    def test_returns_empty_when_no_table(self):
        facet = extract_declared_lineage(_ModelWithoutTable)
        assert facet.entries == []

    def test_single_source_column(self):
        col = _FakeCol("customer_sk", info={"source_column": "raw.customer_id"})
        _ModelWithLineage.__table__ = _FakeTable([col])
        facet = extract_declared_lineage(_ModelWithLineage)
        assert len(facet.entries) == 1
        assert facet.entries[0].output_column == "customer_sk"
        assert facet.entries[0].input_columns == ["raw.customer_id"]
        assert facet.entries[0].confidence == "declared"
        del _ModelWithLineage.__table__

    def test_source_columns_list(self):
        col = _FakeCol(
            "full_name",
            info={
                "source_columns": ["first_name", "last_name"],
                "transform_description": "concat",
            },
        )
        _ModelWithLineage.__table__ = _FakeTable([col])
        facet = extract_declared_lineage(_ModelWithLineage)
        assert len(facet.entries) == 1
        assert facet.entries[0].input_columns == ["first_name", "last_name"]
        assert facet.entries[0].transform_description == "concat"
        del _ModelWithLineage.__table__

    def test_no_source_info_skipped(self):
        col = _FakeCol("plain_col", info={})
        _ModelWithLineage.__table__ = _FakeTable([col])
        facet = extract_declared_lineage(_ModelWithLineage)
        assert facet.entries == []
        del _ModelWithLineage.__table__

    def test_default_transform_description(self):
        col = _FakeCol("col_a", info={"source_column": "src_col"})
        _ModelWithLineage.__table__ = _FakeTable([col])
        facet = extract_declared_lineage(_ModelWithLineage)
        assert facet.entries[0].transform_description == "Direct mapping"
        del _ModelWithLineage.__table__


# ---------------------------------------------------------------------------
# extract_structural_lineage
# ---------------------------------------------------------------------------


class TestExtractStructuralLineage:
    def test_returns_empty_when_no_table(self):
        facet = extract_structural_lineage(_ModelWithoutTable)
        assert facet.entries == []

    def test_fk_column_produces_inferred_entry(self):
        col = _FakeCol(
            "customer_sk", info={"foreign_key_target": "customer_dim.customer_id"}
        )
        _ModelWithLineage.__table__ = _FakeTable([col])
        facet = extract_structural_lineage(_ModelWithLineage)
        assert len(facet.entries) == 1
        entry = facet.entries[0]
        assert entry.output_column == "customer_sk"
        assert entry.input_columns == ["customer_dim.customer_id"]
        assert entry.confidence == "inferred"
        assert "customer_dim.customer_id" in entry.transform_description
        del _ModelWithLineage.__table__

    def test_non_fk_column_skipped(self):
        col = _FakeCol("amount", info={})
        _ModelWithLineage.__table__ = _FakeTable([col])
        facet = extract_structural_lineage(_ModelWithLineage)
        assert facet.entries == []
        del _ModelWithLineage.__table__

    def test_multiple_fk_columns(self):
        cols = [
            _FakeCol("customer_sk", info={"foreign_key_target": "customer_dim.id"}),
            _FakeCol("product_sk", info={"foreign_key_target": "product_dim.id"}),
            _FakeCol("amount", info={}),
        ]
        _ModelWithLineage.__table__ = _FakeTable(cols)
        facet = extract_structural_lineage(_ModelWithLineage)
        assert len(facet.entries) == 2
        names = {e.output_column for e in facet.entries}
        assert names == {"customer_sk", "product_sk"}
        del _ModelWithLineage.__table__


# ---------------------------------------------------------------------------
# extract_sql_lineage
# ---------------------------------------------------------------------------


class TestExtractSqlLineage:
    def test_raises_import_error_without_sqlglot(self):
        with patch.dict(sys.modules, {"sqlglot": None, "sqlglot.lineage": None}):
            with pytest.raises(ImportError, match="sqlglot"):
                extract_sql_lineage("SELECT id FROM t", "out_table")

    def test_parses_simple_select(self):
        pytest.importorskip("sqlglot")
        facet = extract_sql_lineage(
            "SELECT customer_id, order_total FROM staging_orders",
            "dim_orders",
        )
        cols = {e.output_column for e in facet.entries}
        assert "customer_id" in cols
        assert "order_total" in cols

    def test_all_entries_have_parsed_confidence(self):
        pytest.importorskip("sqlglot")
        facet = extract_sql_lineage(
            "SELECT a, b, a + b AS c FROM t",
            "out_table",
        )
        for entry in facet.entries:
            assert entry.confidence == "parsed"

    def test_returns_empty_on_bad_sql(self):
        pytest.importorskip("sqlglot")
        facet = extract_sql_lineage("NOT VALID SQL !!!###", "out_table")
        # Should return empty rather than raise
        assert isinstance(facet, ColumnLineageFacet)

    def test_returns_empty_when_parse_returns_none(self):
        pytest.importorskip("sqlglot")
        import sqlglot

        with patch.object(sqlglot, "parse_one", return_value=None):
            facet = extract_sql_lineage("SELECT 1", "out_table")
        assert isinstance(facet, ColumnLineageFacet)
        assert len(facet.entries) == 0

"""Tests for automatic indexing strategies across Kimball models.

Covers five concerns:
  1. Fact FK-column auto-indexing (via Field factory — no DB constraint by default)
  2. Graph edge endpoint auto-indexing (same mechanism)
  3. Bridge composite key unique index (via BridgeModel.__bridge_keys__)
  4. SCD2Mixin valid_from indexing for temporal range queries
  5. FK metadata stored in column.info["foreign_key_target"] for schema introspection
  6. opt-in constraint=True creates a real SA FK constraint

Note on FK constraints
-----------------------
sqldim's Field(foreign_key=...) does NOT create a DB-level REFERENCES constraint
by default.  The FK target is stored in column.info["foreign_key_target"] for
schema-graph introspection, and the column is auto-indexed.  This avoids
SQLAlchemy MetaData resolution errors in tests and removes enforcement overhead
for analytical workloads (DuckDB, Parquet, etc. don't support FK constraints).
Use Field(foreign_key=..., constraint=True) to explicitly opt-in.
"""
import pytest
from sqlalchemy import Index as SAIndex
from sqlmodel import SQLModel

from sqldim import DimensionModel, FactModel, BridgeModel, Field, VertexModel
from sqldim.core.kimball.fields import _UNSET
from sqldim.core.kimball.mixins import SCD2Mixin


# ── Helpers ────────────────────────────────────────────────────────────────────

def _get_indexes(model_class: type) -> set[str]:
    """Return set of index names from a SQLModel's mapped table."""
    return {idx.name for idx in model_class.__table__.indexes}


def _has_column_index(model_class: type, col_name: str) -> bool:
    """Return True if a single-column index exists on the given column."""
    col = model_class.__table__.c[col_name]
    # _UNSET sentinel may leak through from Field factory when index was not specified
    return bool(col.index) and col.index is not _UNSET


def _has_fk_constraint(model_class: type, col_name: str) -> bool:
    """Return True if *col_name* has a real SA ForeignKey constraint."""
    col = model_class.__table__.c[col_name]
    return bool(col.foreign_keys)


def _fk_target(model_class: type, col_name: str) -> str | None:
    """Return the FK target string from column.info, or None."""
    col = model_class.__table__.c[col_name]
    return col.info.get("foreign_key_target")


# ── Concern 1 & 2: FK auto-index (covers facts and graph edges) ───────────────
# No referenced dimension tables are defined here — this confirms that FK-like
# columns are indexed without requiring the target table to exist.

class OrderFact(FactModel, table=True):
    __tablename__ = "order_fact"
    id: int = Field(primary_key=True)
    customer_id: int = Field(foreign_key="customer.id")
    product_id: int = Field(foreign_key="product.id")
    amount: float = Field()


class EdgeWithEndpoints(FactModel, table=True):
    __tablename__ = "graph_edge"
    id: int = Field(primary_key=True)
    subject_id: int = Field(foreign_key="vertex.id")
    object_id: int = Field(foreign_key="vertex.id")


class ExplicitOptOut(FactModel, table=True):
    __tablename__ = "opt_out_fact"
    id: int = Field(primary_key=True)
    dim_id: int = Field(foreign_key="dim.id", index=False)


# ── Concern 6: opt-in FK constraint (target table must exist in same MetaData) ─

class ConstraintDim(DimensionModel, table=True):
    """Referenced table for the constraint=True test."""
    __tablename__ = "constraint_dim"
    id: int = Field(primary_key=True)
    code: str


class ConstraintFact(FactModel, table=True):
    __tablename__ = "constraint_fact"
    id: int = Field(primary_key=True)
    dim_id: int = Field(foreign_key="constraint_dim.id", constraint=True)


# ── Tests: auto-index on FK columns ──────────────────────────────────────────

def test_fk_auto_indexed_on_fact():
    """FK columns on fact tables are auto-indexed."""
    assert _has_column_index(OrderFact, "customer_id")
    assert _has_column_index(OrderFact, "product_id")


def test_fk_auto_indexed_on_graph_edge():
    assert _has_column_index(EdgeWithEndpoints, "subject_id")
    assert _has_column_index(EdgeWithEndpoints, "object_id")


def test_fk_index_opt_out():
    assert not _has_column_index(ExplicitOptOut, "dim_id")


def test_non_fk_not_auto_indexed():
    assert not _has_column_index(OrderFact, "amount")


# ── Concern 5: FK metadata in column.info — no DB constraint by default ──────

def test_fk_target_stored_in_column_info():
    """FK target string is stored in column.info, not as an SA constraint."""
    assert _fk_target(OrderFact, "customer_id") == "customer.id"
    assert _fk_target(OrderFact, "product_id") == "product.id"


def test_fk_no_db_constraint_by_default():
    """FK columns do NOT create SA ForeignKey constraints unless constraint=True."""
    assert not _has_fk_constraint(OrderFact, "customer_id")
    assert not _has_fk_constraint(OrderFact, "product_id")
    assert not _has_fk_constraint(EdgeWithEndpoints, "subject_id")
    assert not _has_fk_constraint(EdgeWithEndpoints, "object_id")


def test_fk_opt_out_no_constraint_and_no_index():
    """index=False also produces no FK constraint."""
    assert not _has_fk_constraint(ExplicitOptOut, "dim_id")


# ── Concern 6: constraint=True opt-in ────────────────────────────────────────

def test_constraint_true_creates_fk_constraint():
    """constraint=True passes the FK through to SQLModel, creating a real constraint."""
    assert _has_fk_constraint(ConstraintFact, "dim_id")


def test_constraint_true_still_indexed():
    """Columns with constraint=True are still auto-indexed."""
    assert _has_column_index(ConstraintFact, "dim_id")


def test_constraint_true_target_in_info():
    """constraint=True also persists FK target in column.info."""
    assert _fk_target(ConstraintFact, "dim_id") == "constraint_dim.id"


# ── Concern 3: Bridge composite key unique index ──────────────────────────────

class SimpleBridge(BridgeModel, table=True):
    __tablename__ = "simple_bridge"
    id: int = Field(primary_key=True)
    a_id: int
    b_id: int


class KeyedBridge(BridgeModel, table=True):
    __tablename__ = "keyed_bridge"
    __bridge_keys__ = ["sale_id", "rep_id"]
    id: int = Field(primary_key=True)
    sale_id: int
    rep_id: int


class KeyedBridgeWithExistingTableArgs(BridgeModel, table=True):
    __tablename__ = "keyed_bridge_ext"
    __bridge_keys__ = ["order_id", "tag_id"]
    id: int = Field(primary_key=True)
    order_id: int
    tag_id: int
    __table_args__ = ({"comment": "extended bridge"},)


def test_bridge_no_keys_no_composite_index():
    indexes = _get_indexes(SimpleBridge)
    assert not any("sale_id" in idx or "a_id" in idx for idx in indexes)


def test_bridge_keys_creates_composite_unique_index():
    indexes = _get_indexes(KeyedBridge)
    expected = "uq_keyed_bridge_sale_id_rep_id"
    assert expected in indexes
    idx = [i for i in KeyedBridge.__table__.indexes if i.name == expected][0]
    assert idx.unique is True
    assert len(idx.columns) == 2


def test_bridge_keys_merges_with_existing_table_args():
    indexes = _get_indexes(KeyedBridgeWithExistingTableArgs)
    expected = "uq_keyed_bridge_ext_order_id_tag_id"
    assert expected in indexes
    # Original dict opts preserved
    ta = KeyedBridgeWithExistingTableArgs.__table_args__
    assert isinstance(ta[-1], dict)
    assert ta[-1]["comment"] == "extended bridge"


def test_bridge_keys_dict_only_table_args_raises():
    with pytest.raises(TypeError, match="__table_args__ is a dict"):

        class BadBridge(BridgeModel, table=True):
            __tablename__ = "bad_bridge"
            __bridge_keys__ = ["x", "y"]
            id: int = Field(primary_key=True)
            x: int
            y: int
            __table_args__ = {"comment": "bad"}


# ── Concern 4: SCD2Mixin valid_from indexing ─────────────────────────────────

class SCD2Dim(DimensionModel, SCD2Mixin, table=True):
    __tablename__ = "scd2_dim"
    id: int = Field(primary_key=True)
    code: str


def test_valid_from_indexed():
    assert _has_column_index(SCD2Dim, "valid_from")


def test_valid_to_indexed():
    assert _has_column_index(SCD2Dim, "valid_to")


def test_is_current_indexed():
    assert _has_column_index(SCD2Dim, "is_current")


def test_checksum_indexed():
    assert _has_column_index(SCD2Dim, "checksum")


# ── Concern 7: DimensionModel __natural_key__ auto-index ─────────────────────

class SingleNKDim(DimensionModel, table=True):
    __tablename__ = "single_nk_dim"
    __natural_key__ = ["code"]
    id: int = Field(primary_key=True)
    code: str


class MultiNKDim(DimensionModel, table=True):
    __tablename__ = "multi_nk_dim"
    __natural_key__ = ["country_code", "product_code"]
    id: int = Field(primary_key=True)
    country_code: str
    product_code: str


class NoNKDim(DimensionModel, table=True):
    __tablename__ = "no_nk_dim"
    id: int = Field(primary_key=True)
    name: str


class NKVertex(VertexModel, table=True):
    __tablename__ = "nk_vertex"
    __natural_key__ = ["code"]
    __vertex_type__ = "item"
    id: int = Field(primary_key=True)
    code: str


class NKDimWithOpts(DimensionModel, table=True):
    """Dimension with __natural_key__ and an existing trailing-dict __table_args__."""
    __tablename__ = "nk_dim_with_opts"
    __natural_key__ = ["ref"]
    id: int = Field(primary_key=True)
    ref: str
    __table_args__ = ({"comment": "with options"},)


def test_natural_key_single_column_creates_named_index():
    """Single-column __natural_key__ gets a named ix_<table>_nk index."""
    assert "ix_single_nk_dim_nk" in _get_indexes(SingleNKDim)


def test_natural_key_single_column_is_non_unique():
    """Natural key index is NOT unique — SCD2 has multiple rows per NK."""
    idx = next(i for i in SingleNKDim.__table__.indexes if i.name == "ix_single_nk_dim_nk")
    assert not idx.unique


def test_natural_key_multi_column_creates_composite_index():
    """Multi-column __natural_key__ gets a composite index over all NK columns."""
    indexes = _get_indexes(MultiNKDim)
    assert "ix_multi_nk_dim_nk" in indexes
    idx = next(i for i in MultiNKDim.__table__.indexes if i.name == "ix_multi_nk_dim_nk")
    assert len(idx.columns) == 2
    assert not idx.unique


def test_natural_key_empty_no_nk_index():
    """Dimensions without __natural_key__ do not get a spurious ix_*_nk index."""
    assert "ix_no_nk_dim_nk" not in _get_indexes(NoNKDim)


def test_vertex_inherits_natural_key_auto_index():
    """VertexModel subclasses inherit DimensionModel natural-key indexing."""
    assert "ix_nk_vertex_nk" in _get_indexes(NKVertex)


def test_nk_merges_with_existing_table_args_dict():
    """NK index is injected before the trailing dict opts when __table_args__ ends in a dict."""
    indexes = _get_indexes(NKDimWithOpts)
    assert "ix_nk_dim_with_opts_nk" in indexes
    ta = NKDimWithOpts.__table_args__
    assert isinstance(ta[-1], dict)
    assert ta[-1]["comment"] == "with options"


def test_nk_dict_table_args_raises():
    """DimensionModel with a plain-dict __table_args__ and __natural_key__ raises TypeError."""
    with pytest.raises(TypeError, match="__table_args__ is a dict"):
        class BadNKDim(DimensionModel, table=True):
            __tablename__ = "bad_nk_dim_for_test"
            __natural_key__ = ["code"]
            id: int = Field(primary_key=True)
            code: str
            __table_args__ = {"comment": "bad"}

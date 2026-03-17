"""Tests for NarwhalsSKResolver — covers lines 68-71, 93-100, 112-118, 138,
153-158, 162-164, 168 in sqldim/processors/sk_resolver.py."""
import sys
import pytest
from datetime import date
from typing import Optional

import polars as pl
import narwhals as nw
from sqlalchemy.pool import StaticPool
from sqlmodel import Field, Session, SQLModel, create_engine

from sqldim import DimensionModel, FactModel, SCD2Mixin
from sqldim.core.kimball.dimensions.scd.processors.sk_resolver import NarwhalsSKResolver


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class SkResDim(DimensionModel, table=True):
    __tablename__ = "sk_res_dim"
    __natural_key__ = ["code"]
    id: Optional[int] = Field(default=None, primary_key=True)
    code: str


class SkResFact(FactModel, table=True):
    __tablename__ = "sk_res_fact"
    id: Optional[int] = Field(default=None, primary_key=True)
    dim_id: Optional[int] = Field(default=None, foreign_key="sk_res_dim.id")
    code: str = ""


class SkResSCD2Dim(DimensionModel, SCD2Mixin, table=True):
    __tablename__ = "sk_res_scd2_dim"
    __natural_key__ = ["code"]
    id: Optional[int] = Field(default=None, primary_key=True)
    code: str
    valid_from: date = Field(default=date(2020, 1, 1))
    valid_to: Optional[date] = Field(default=None, nullable=True)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def sk_engine():
    """Module-scoped engine — create_all runs once for all sk_resolver tests."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def sk_session(sk_engine):
    """Per-test session backed by the shared module-scoped engine."""
    with Session(sk_engine) as s:
        yield s


# ---------------------------------------------------------------------------
# Tests: resolve() — lines 68-71
# ---------------------------------------------------------------------------

def test_resolve_joins_and_returns_sk(sk_session):
    """resolve() left-joins frame with dimension lookup and returns SK column."""
    dim = SkResDim(code="X")
    sk_session.add(dim)
    sk_session.commit()
    sk_session.refresh(dim)

    resolver = NarwhalsSKResolver(sk_session)
    frame = nw.from_native(pl.DataFrame({"code": ["X", "MISSING"]}), eager_only=True)
    result = resolver.resolve(frame, SkResDim, "code", "dim_id")

    native = nw.to_native(result)
    assert "dim_id" in native.columns
    assert native["dim_id"][0] == dim.id
    assert native["dim_id"][1] is None  # missing key → null SK


def test_resolve_empty_dimension(sk_session):
    """resolve() with no dimension rows returns the frame with null SKs."""
    d1 = SkResDim(code="ONLY")
    sk_session.add(d1)
    sk_session.commit()
    sk_session.refresh(d1)

    resolver = NarwhalsSKResolver(sk_session)
    # Key that doesn't exist in dimension → null SK
    frame = nw.from_native(pl.DataFrame({"code": ["NOTEXIST"]}), eager_only=True)
    result = resolver.resolve(frame, SkResDim, "code", "dim_id")
    native = nw.to_native(result)
    assert native["dim_id"][0] is None


# ---------------------------------------------------------------------------
# Tests: resolve_all() — lines 93-100
# ---------------------------------------------------------------------------

def test_resolve_all_resolves_multiple_fks(sk_session):
    """resolve_all() resolves each FK in key_map in a single pass."""
    d1 = SkResDim(code="A")
    sk_session.add(d1)
    sk_session.commit()
    sk_session.refresh(d1)

    resolver = NarwhalsSKResolver(sk_session)
    frame = nw.from_native(pl.DataFrame({"code": ["A"]}), eager_only=True)
    key_map = {"dim_id": (SkResDim, "code")}
    result = resolver.resolve_all(frame, SkResFact, key_map)
    native = nw.to_native(result)
    assert "dim_id" in native.columns
    assert native["dim_id"][0] == d1.id


# ---------------------------------------------------------------------------
# Tests: _get_lookup() cache — lines 112-118
# ---------------------------------------------------------------------------

def test_get_lookup_caches_result(sk_session):
    """_get_lookup() returns the same object on repeated calls (cache hit)."""
    d1 = SkResDim(code="CACHE_TEST")
    sk_session.add(d1)
    sk_session.commit()

    resolver = NarwhalsSKResolver(sk_session)
    lookup1 = resolver._get_lookup(SkResDim, None, "code")
    lookup2 = resolver._get_lookup(SkResDim, None, "code")
    # Second call hits cache: same object
    assert lookup1 is lookup2


def test_resolve_with_is_current_model(sk_session):
    """resolve() with a model that has is_current and as_of=None filters to is_current=True (line 138)."""
    active = SkResSCD2Dim(code="ACTIVE", is_current=True)
    inactive = SkResSCD2Dim(code="OLD", is_current=False)
    sk_session.add_all([active, inactive])
    sk_session.commit()
    sk_session.refresh(active)

    resolver = NarwhalsSKResolver(sk_session)
    # as_of=None → uses is_current==True filter (line 138)
    frame = nw.from_native(pl.DataFrame({"code": ["ACTIVE", "OLD"]}), eager_only=True)
    result = resolver.resolve(frame, SkResSCD2Dim, "code", "dim_id")
    native = nw.to_native(result)
    # ACTIVE resolved; OLD is is_current=False so not in lookup → null
    assert native["dim_id"][0] == active.id
    assert native["dim_id"][1] is None


# ---------------------------------------------------------------------------
# Tests: _load_lookup as_of branch — line 138
# ---------------------------------------------------------------------------

def test_load_lookup_as_of_filters_by_date(sk_session):
    """_load_lookup with as_of filters with valid_from/valid_to (line 138)."""
    d1 = SkResSCD2Dim(
        code="AS_OF_RANGE",
        is_current=False,
        valid_from=date(2020, 1, 1),
        valid_to=date(2021, 12, 31),
    )
    sk_session.add(d1)
    sk_session.commit()

    resolver = NarwhalsSKResolver(sk_session)
    # as_of within [2020-01-01, 2021-12-31] → code must appear in lookup
    lookup_in = resolver._load_lookup(SkResSCD2Dim, as_of=date(2021, 6, 1), natural_key_col="code")
    codes_in = nw.to_native(lookup_in)["code"].to_list()
    assert "AS_OF_RANGE" in codes_in

    # as_of after valid_to → code must NOT appear in lookup
    lookup_out = resolver._load_lookup(SkResSCD2Dim, as_of=date(2023, 1, 1), natural_key_col="code")
    codes_out = nw.to_native(lookup_out)["code"].to_list() if len(lookup_out) else []
    assert "AS_OF_RANGE" not in codes_out


# ---------------------------------------------------------------------------
# Tests: _df_from_records static method — lines 153-158, 162-164
# ---------------------------------------------------------------------------

def test_df_from_records_empty_returns_polars_frame():
    """_df_from_records([]) returns a polars-backed narwhals frame (lines 153-158)."""
    result = NarwhalsSKResolver._df_from_records([], "code")
    assert "code" in result.columns
    assert "id" in result.columns
    assert len(result) == 0


def test_df_from_records_nonempty_returns_polars_frame():
    """_df_from_records([...]) returns a polars-backed narwhals frame (lines 162-164)."""
    records = [{"code": "A", "id": 1}, {"code": "B", "id": 2}]
    result = NarwhalsSKResolver._df_from_records(records, "code")
    assert len(result) == 2
    native_codes = nw.to_native(result)["code"].to_list()
    assert "A" in native_codes
    assert "B" in native_codes


def test_df_from_records_empty_no_polars(monkeypatch):
    """_df_from_records([]) falls back to pandas when polars is unavailable (lines 153-158)."""
    monkeypatch.setitem(sys.modules, "polars", None)
    result = NarwhalsSKResolver._df_from_records([], "code")
    assert "code" in result.columns
    assert len(result) == 0


def test_df_from_records_nonempty_no_polars(monkeypatch):
    """_df_from_records([...]) falls back to pandas when polars unavailable (lines 162-164)."""
    monkeypatch.setitem(sys.modules, "polars", None)
    records = [{"code": "X", "id": 10}]
    result = NarwhalsSKResolver._df_from_records(records, "code")
    assert len(result) == 1


# ---------------------------------------------------------------------------
# Tests: invalidate_cache() — line 168
# ---------------------------------------------------------------------------

def test_invalidate_cache_clears_all_entries(sk_session):
    """invalidate_cache() empties the lookup cache (line 168)."""
    resolver = NarwhalsSKResolver(sk_session)
    # Warm the cache
    resolver._get_lookup(SkResDim, None, "code")
    assert len(resolver._cache) > 0

    resolver.invalidate_cache()
    assert resolver._cache == {}

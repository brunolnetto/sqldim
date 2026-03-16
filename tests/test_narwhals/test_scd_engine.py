"""Tests for NarwhalsHashStrategy and NarwhalsSCDProcessor — Task 7.2."""
import hashlib
import sys
import pytest
from datetime import date, datetime, timezone
from typing import Optional

import narwhals as nw
import polars as pl
import pandas as pd
from sqlalchemy.pool import StaticPool
from sqlmodel import Field, Session, SQLModel, create_engine

from sqldim import DimensionModel, SCD2Mixin
from sqldim.processors.scd_engine import NarwhalsHashStrategy, NarwhalsSCDProcessor
from sqldim.processors.sk_resolver import NarwhalsSKResolver
from sqldim.scd.handler import SCDResult


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

NATURAL_KEY = ["customer_code"]
TRACK_COLS = ["name", "city"]


def _make_incoming_pl():
    return nw.from_native(pl.DataFrame({
        "customer_code": ["C1", "C2", "C3"],
        "name":          ["Alice", "Bob", "Carol"],
        "city":          ["NY",    "LA",  "SF"],
    }), eager_only=True)


def _make_current_pl():
    """C1 same as incoming. C2 different city → changed. C3 absent → new."""
    return nw.from_native(pl.DataFrame({
        "customer_code": ["C1", "C2"],
        "name":          ["Alice", "Bob"],
        "city":          ["NY",    "Boston"],
        "is_current":    [True,    True],
    }), eager_only=True)


def _python_checksum(record: dict, track_cols: list[str]) -> str:
    cols = sorted(track_cols)
    combined = "|".join(str(record.get(c, "")) for c in cols)
    return hashlib.md5(combined.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# NarwhalsHashStrategy.compute_checksums
# ---------------------------------------------------------------------------

class TestNarwhalsHashStrategy:

    def test_compute_checksums_adds_column(self):
        strategy = NarwhalsHashStrategy(NATURAL_KEY, TRACK_COLS)
        frame = _make_incoming_pl()
        result = strategy.compute_checksums(frame)
        assert "checksum" in result.columns

    def test_checksum_matches_python_hashstrategy(self):
        strategy = NarwhalsHashStrategy(NATURAL_KEY, TRACK_COLS)
        frame = _make_incoming_pl()
        result = strategy.compute_checksums(frame)
        rows = nw.to_native(result).to_dicts()
        for row in rows:
            expected = _python_checksum(row, TRACK_COLS)
            assert row["checksum"] == expected, f"Mismatch for {row}"

    def test_checksum_deterministic(self):
        strategy = NarwhalsHashStrategy(NATURAL_KEY, TRACK_COLS)
        frame = _make_incoming_pl()
        cs1 = nw.to_native(strategy.compute_checksums(frame)).to_dicts()
        cs2 = nw.to_native(strategy.compute_checksums(frame)).to_dicts()
        assert [r["checksum"] for r in cs1] == [r["checksum"] for r in cs2]

    def test_checksum_works_with_pandas(self):
        strategy = NarwhalsHashStrategy(NATURAL_KEY, TRACK_COLS)
        df = pd.DataFrame({
            "customer_code": ["C1"],
            "name": ["Alice"],
            "city": ["NY"],
        })
        frame = nw.from_native(df, eager_only=True)
        result = strategy.compute_checksums(frame)
        assert "checksum" in result.columns
        assert len(result) == 1

    # ---------------------------------------------------------------------------
    # find_changes
    # ---------------------------------------------------------------------------

    def test_find_changes_correct_classification(self):
        strategy = NarwhalsHashStrategy(NATURAL_KEY, TRACK_COLS)
        incoming = strategy.compute_checksums(_make_incoming_pl())
        current = strategy.compute_checksums(_make_current_pl())

        new, changed, unchanged = strategy.find_changes(incoming, current)

        new_codes = set(new["customer_code"].to_list())
        changed_codes = set(changed["customer_code"].to_list())
        unchanged_codes = set(unchanged["customer_code"].to_list())

        assert "C3" in new_codes        # brand new
        assert "C2" in changed_codes    # city changed
        assert "C1" in unchanged_codes  # same

    def test_find_changes_counts(self):
        strategy = NarwhalsHashStrategy(NATURAL_KEY, TRACK_COLS)
        incoming = strategy.compute_checksums(_make_incoming_pl())
        current = strategy.compute_checksums(_make_current_pl())
        new, changed, unchanged = strategy.find_changes(incoming, current)
        assert len(new) == 1
        assert len(changed) == 1
        assert len(unchanged) == 1

    def test_find_changes_no_checksum_current_in_output(self):
        strategy = NarwhalsHashStrategy(NATURAL_KEY, TRACK_COLS)
        incoming = strategy.compute_checksums(_make_incoming_pl())
        current = strategy.compute_checksums(_make_current_pl())
        new, changed, unchanged = strategy.find_changes(incoming, current)
        for frame in (new, changed, unchanged):
            assert "checksum_current" not in frame.columns

    def test_find_changes_all_new_when_current_empty(self):
        strategy = NarwhalsHashStrategy(NATURAL_KEY, TRACK_COLS)
        incoming = strategy.compute_checksums(_make_incoming_pl())
        # Use explicit dtypes so join keys match (polars infers Null for empty lists)
        empty_current = nw.from_native(
            pl.DataFrame({
                "customer_code": pl.Series([], dtype=pl.Utf8),
                "name": pl.Series([], dtype=pl.Utf8),
                "city": pl.Series([], dtype=pl.Utf8),
                "checksum": pl.Series([], dtype=pl.Utf8),
            }),
            eager_only=True,
        )
        new, changed, unchanged = strategy.find_changes(incoming, empty_current)
        assert len(new) == 3
        assert len(changed) == 0
        assert len(unchanged) == 0

    def test_find_changes_all_unchanged_when_identical(self):
        strategy = NarwhalsHashStrategy(NATURAL_KEY, TRACK_COLS)
        incoming = strategy.compute_checksums(_make_incoming_pl())
        # current = same data
        current = nw.from_native(pl.DataFrame({
            "customer_code": ["C1", "C2", "C3"],
            "name":          ["Alice", "Bob", "Carol"],
            "city":          ["NY",    "LA",  "SF"],
            "is_current":    [True, True, True],
        }), eager_only=True)
        current = strategy.compute_checksums(current)
        new, changed, unchanged = strategy.find_changes(incoming, current)
        assert len(new) == 0
        assert len(changed) == 0
        assert len(unchanged) == 3


# ---------------------------------------------------------------------------
# NarwhalsSCDProcessor
# ---------------------------------------------------------------------------

class TestNarwhalsSCDProcessor:

    def test_process_returns_scd_result(self):
        proc = NarwhalsSCDProcessor(NATURAL_KEY, TRACK_COLS)
        incoming = _make_incoming_pl()
        current = _make_current_pl()
        result = proc.process(incoming, current)
        assert isinstance(result, SCDResult)

    def test_process_counts_correct(self):
        proc = NarwhalsSCDProcessor(NATURAL_KEY, TRACK_COLS)
        result = proc.process(_make_incoming_pl(), _make_current_pl())
        assert result.inserted == 1   # C3 new
        assert result.versioned == 1  # C2 changed
        assert result.unchanged == 1  # C1 same

    def test_process_to_close_and_to_insert_attached(self):
        proc = NarwhalsSCDProcessor(NATURAL_KEY, TRACK_COLS)
        result = proc.process(_make_incoming_pl(), _make_current_pl())
        assert hasattr(result, "to_close")
        assert hasattr(result, "to_insert")

    def test_idempotency_second_run_all_unchanged(self):
        proc = NarwhalsSCDProcessor(NATURAL_KEY, TRACK_COLS)
        # Process once
        proc.process(_make_incoming_pl(), _make_current_pl())
        # Process again with same data as both incoming and current
        current_after = nw.from_native(pl.DataFrame({
            "customer_code": ["C1", "C2", "C3"],
            "name":          ["Alice", "Bob", "Carol"],
            "city":          ["NY",    "LA",  "SF"],
            "is_current":    [True, True, True],
        }), eager_only=True)
        result2 = proc.process(_make_incoming_pl(), current_after)
        assert result2.unchanged == 3
        assert result2.inserted == 0
        assert result2.versioned == 0

    def test_process_with_pandas_source(self):
        proc = NarwhalsSCDProcessor(NATURAL_KEY, TRACK_COLS)
        incoming_pd = pd.DataFrame({
            "customer_code": ["C1"],
            "name": ["Alice"],
            "city": ["NY"],
        })
        current_pd = pd.DataFrame({
            "customer_code": ["C1"],
            "name": ["Alice"],
            "city": ["Boston"],
            "is_current": [True],
        })
        incoming = nw.from_native(incoming_pd, eager_only=True)
        current = nw.from_native(current_pd, eager_only=True)
        result = proc.process(incoming, current)
        assert result.versioned == 1

    def test_process_custom_as_of(self):
        proc = NarwhalsSCDProcessor(NATURAL_KEY, TRACK_COLS)
        ts = datetime(2024, 1, 15, tzinfo=timezone.utc)
        result = proc.process(_make_incoming_pl(), _make_current_pl(), as_of=ts)
        assert result.inserted + result.versioned + result.unchanged == 3


# ---------------------------------------------------------------------------
# Migrated from test_coverage_100.py, test_coverage_final.py, test_coverage_gap.py
# ---------------------------------------------------------------------------

class TestNarwhalsHashStrategyMissingLine:
    def test_pandas_object_dtype_handling(self):
        """pandas dict-valued column has dtype=object; apply(json.dumps) called."""
        strategy = NarwhalsHashStrategy(
            natural_key=["id"],
            track_columns=["id", "data"],
        )
        df = pd.DataFrame({"id": [1, 2], "data": [{"key": "a"}, {"key": "b"}]})
        assert df["data"].dtype == "object"
        frame = nw.from_native(df, eager_only=True)
        result = strategy.compute_checksums(frame)
        assert "checksum" in result.columns
        checksums = nw.to_native(result)["checksum"].tolist()
        assert all(len(c) == 32 for c in checksums)


def test_narwhals_scd_processor_no_is_current_column():
    """NarwhalsSCDProcessor.process() handles current frame without is_current column."""
    proc = NarwhalsSCDProcessor(["code"], ["name"])
    incoming = nw.from_native(pd.DataFrame({"code": ["A"], "name": ["Alice"]}))
    current = nw.from_native(pd.DataFrame({"code": ["A"], "name": ["Bob"]}))
    result = proc.process(incoming, current)
    assert result.versioned == 1


class Cov100SCD2DimEngine(DimensionModel, SCD2Mixin, table=True):
    __tablename__ = "cov100_scd2_engine"
    __natural_key__ = ["code"]
    id: Optional[int] = Field(default=None, primary_key=True)
    code: str


@pytest.fixture
def scd2_engine_session():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        yield s
    engine.dispose()


def test_narwhals_sk_resolver_as_of_logic(scd2_engine_session):
    """NarwhalsSKResolver._load_lookup with as_of filters by valid_from/valid_to."""
    class SCD2DimLocal(DimensionModel, table=True):
        __tablename__ = "scd2_lookup_engine"
        id: Optional[int] = Field(default=None, primary_key=True)
        code: str
        is_current: bool = True
        valid_from: date = date(2020, 1, 1)
        valid_to: Optional[date] = None

    SQLModel.metadata.create_all(scd2_engine_session.get_bind())

    d1 = SCD2DimLocal(
        code="X", is_current=False,
        valid_from=date(2020, 1, 1), valid_to=date(2021, 1, 1)
    )
    scd2_engine_session.add(d1)
    scd2_engine_session.commit()

    resolver = NarwhalsSKResolver(scd2_engine_session)
    lookup = resolver._load_lookup(SCD2DimLocal, as_of=date(2020, 6, 1), natural_key_col="code")
    assert len(lookup) == 1

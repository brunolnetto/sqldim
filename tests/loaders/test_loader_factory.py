"""Tests for the inferred loader factory (FactModel.as_loader / DimensionModel.as_loader).

These tests verify that the correct Lazy loader is built from model metadata,
that field-type inference fills missing ClassVars, and that missing required
params raise clear TypeError messages.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import List, Optional
from unittest.mock import MagicMock

import pytest

from sqldim.core.kimball.models import DimensionModel, FactModel
from sqldim.core.kimball.mixins import SCD2Mixin
from sqldim.core.loaders._factory import build_lazy_loader
from sqldim.core.loaders.snapshot import LazyTransactionLoader, LazySnapshotLoader
from sqldim.core.loaders.accumulating import LazyAccumulatingLoader
from sqldim.core.loaders.cumulative import LazyCumulativeLoader
from sqldim.core.loaders.bitmask import LazyBitmaskLoader
from sqldim.core.loaders.array_metric import LazyArrayMetricLoader
from sqldim.core.loaders.edge_projection import LazyEdgeProjectionLoader
from sqldim.core.kimball.dimensions.scd.handler import SCDHandler
from sqldim.core.kimball.fields import Field


# ---------------------------------------------------------------------------
# Shared mock sink
# ---------------------------------------------------------------------------

@pytest.fixture
def sink():
    return MagicMock()


# ---------------------------------------------------------------------------
# Fact models used across tests
# ---------------------------------------------------------------------------

class BulkFact(FactModel):
    __strategy__ = "bulk"


class BulkDefaultFact(FactModel):
    pass  # __strategy__ = None → defaults to bulk


class SnapshotFact(FactModel):
    __strategy__ = "snapshot"


class AccumulatingFactExplicit(FactModel):
    __strategy__ = "accumulating"
    __match_column__ = "order_id"
    __milestones__ = ["approved_at", "shipped_at"]


class AccumulatingFactInferred(FactModel):
    """No ClassVars — milestones inferred from Optional[datetime] fields."""
    __strategy__ = "accumulating"
    __natural_key__ = ["order_id"]

    order_id: str = Field(default=None)
    approved_at: Optional[datetime] = Field(default=None)
    shipped_at: Optional[datetime] = Field(default=None)
    amount: float = Field(default=0.0)


class CumulativeFact(FactModel):
    __strategy__ = "cumulative"
    __partition_key__ = "player_id"
    __cumulative_column__ = "seasons"
    __metric_columns__ = ["pts", "ast"]


class CumulativeFactInferred(FactModel):
    """cumulative_column inferred from List[...] field."""
    __strategy__ = "cumulative"
    __natural_key__ = ["player_id"]

    player_id: str = Field(default=None)
    seasons: List[dict] = Field(default_factory=list)
    pts: int = Field(default=0)
    ast: int = Field(default=0)


class BitmaskFact(FactModel):
    __strategy__ = "bitmask"
    __partition_key__ = "user_id"
    __dates_column__ = "dates_active"
    __window_days__ = 28


class ArrayMetricFact(FactModel):
    __strategy__ = "array_metric"
    __partition_key__ = "user_id"
    __value_column__ = "revenue"
    __metric_name__ = "monthly_revenue"


class EdgeFact(FactModel):
    __strategy__ = "edge_projection"
    __subject_key__ = "player_id"
    __object_key__ = "game_id"


class UpsertFact(FactModel):
    __strategy__ = "upsert"


class MergeFact(FactModel):
    __strategy__ = "merge"


# ---------------------------------------------------------------------------
# Dimension model
# ---------------------------------------------------------------------------

class CustomerDim(DimensionModel, SCD2Mixin):
    __natural_key__ = ["code"]
    __scd_type__ = 2
    code: str = Field(default=None)
    city: str = Field(default=None)
    region: str = Field(default=None)


# ---------------------------------------------------------------------------
# Tests: FactModel.as_loader()
# ---------------------------------------------------------------------------

class TestBulkStrategy:
    def test_explicit_bulk(self, sink):
        loader = BulkFact.as_loader(sink)
        assert isinstance(loader, LazyTransactionLoader)

    def test_none_strategy_is_bulk(self, sink):
        loader = BulkDefaultFact.as_loader(sink)
        assert isinstance(loader, LazyTransactionLoader)


class TestSnapshotStrategy:
    def test_snapshot_no_date_at_construction(self, sink):
        """as_loader() without snapshot_date now succeeds; date is deferred to load()."""
        loader = SnapshotFact.as_loader(sink)
        assert isinstance(loader, LazySnapshotLoader)
        assert loader.snapshot_date is None

    def test_snapshot_requires_date_at_load(self, sink):
        """TypeError is raised at load() time when no snapshot_date was ever supplied."""
        loader = SnapshotFact.as_loader(sink)
        with pytest.raises(TypeError, match="snapshot_date"):
            loader.load(object(), "some_table")  # date not provided

    def test_snapshot_with_date_at_construction(self, sink):
        loader = SnapshotFact.as_loader(sink, snapshot_date="2024-03-31")
        assert isinstance(loader, LazySnapshotLoader)
        assert loader.snapshot_date == "2024-03-31"

    def test_snapshot_date_field_default(self, sink):
        loader = SnapshotFact.as_loader(sink, snapshot_date="2024-03-31")
        assert loader.date_field == "snapshot_date"

    def test_snapshot_model_cls_bound(self, sink):
        """Factory stores the model on the loader for table-free aload() calls."""
        loader = SnapshotFact.as_loader(sink)
        assert loader._model_cls is SnapshotFact


class TestAccumulatingStrategy:
    def test_explicit_classvars(self, sink):
        loader = AccumulatingFactExplicit.as_loader(sink)
        assert isinstance(loader, LazyAccumulatingLoader)
        assert loader.match_column == "order_id"
        assert loader.milestone_columns == ["approved_at", "shipped_at"]

    def test_inferred_from_fields(self, sink):
        loader = AccumulatingFactInferred.as_loader(sink)
        assert isinstance(loader, LazyAccumulatingLoader)
        assert loader.match_column == "order_id"
        assert "approved_at" in loader.milestone_columns
        assert "shipped_at" in loader.milestone_columns
        assert "amount" not in loader.milestone_columns  # float, not datetime

    def test_missing_match_column_raises(self, sink):
        class BadAccumulating(FactModel):
            __strategy__ = "accumulating"
            # no __match_column__, no __natural_key__

        with pytest.raises(TypeError, match="match_column|natural_key"):
            BadAccumulating.as_loader(sink)


class TestCumulativeStrategy:
    def test_explicit_classvars(self, sink):
        loader = CumulativeFact.as_loader(sink)
        assert isinstance(loader, LazyCumulativeLoader)
        assert loader.partition_key == "player_id"
        assert loader.cumulative_column == "seasons"
        assert loader.metric_columns == ["pts", "ast"]

    def test_inferred_list_field(self, sink):
        loader = CumulativeFactInferred.as_loader(sink)
        assert isinstance(loader, LazyCumulativeLoader)
        assert loader.partition_key == "player_id"
        assert loader.cumulative_column == "seasons"

    def test_missing_partition_key_raises(self, sink):
        class BadCumulative(FactModel):
            __strategy__ = "cumulative"
            __cumulative_column__ = "history"
            # no partition_key, no natural_key

        with pytest.raises(TypeError, match="partition_key|cumulative"):
            BadCumulative.as_loader(sink)


class TestBitmaskStrategy:
    def test_explicit_classvars(self, sink):
        loader = BitmaskFact.as_loader(sink, reference_date="2024-01-31")
        assert isinstance(loader, LazyBitmaskLoader)
        assert loader.partition_key == "user_id"
        assert loader.dates_column == "dates_active"
        assert loader.window_days == 28

    def test_requires_reference_date(self, sink):
        with pytest.raises(TypeError, match="reference_date"):
            BitmaskFact.as_loader(sink)


class TestArrayMetricStrategy:
    def test_explicit_classvars(self, sink):
        loader = ArrayMetricFact.as_loader(sink, month_start=date(2024, 1, 1))
        assert isinstance(loader, LazyArrayMetricLoader)
        assert loader.partition_key == "user_id"
        assert loader.value_column == "revenue"
        assert loader.metric_name == "monthly_revenue"

    def test_requires_month_start(self, sink):
        with pytest.raises(TypeError, match="month_start"):
            ArrayMetricFact.as_loader(sink)


class TestEdgeProjectionStrategy:
    def test_explicit_classvars(self, sink):
        loader = EdgeFact.as_loader(sink)
        assert isinstance(loader, LazyEdgeProjectionLoader)
        assert loader.subject_key == "player_id"
        assert loader.object_key == "game_id"

    def test_missing_keys_raises(self, sink):
        with pytest.raises(TypeError, match="object_key|subject_key"):
            class BadEdge(FactModel):
                __strategy__ = "edge_projection"
                __subject_key__ = "player_id"
                # __object_key__ missing — guard fires at class definition


class TestSessionOnlyStrategies:
    def test_upsert_raises(self, sink):
        with pytest.raises(TypeError, match="upsert.*session|DimensionalLoader"):
            UpsertFact.as_loader(sink)

    def test_merge_raises(self, sink):
        with pytest.raises(TypeError, match="merge.*session|DimensionalLoader"):
            MergeFact.as_loader(sink)


class TestUnknownStrategy:
    def test_raises_with_message(self, sink):
        class WeirdFact(FactModel):
            __strategy__ = "teleportation"

        with pytest.raises(TypeError, match="unknown strategy"):
            WeirdFact.as_loader(sink)


# ---------------------------------------------------------------------------
# Tests: DimensionModel.as_loader()
# ---------------------------------------------------------------------------

class TestDimensionAsLoader:
    def test_returns_scd_handler(self):
        from sqlalchemy.pool import StaticPool
        from sqlmodel import Session, create_engine, SQLModel as _SQLModel

        engine = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        _SQLModel.metadata.create_all(engine)
        with Session(engine) as session:
            handler = CustomerDim.as_loader(session)
            assert isinstance(handler, SCDHandler)
        engine.dispose()

    def test_infers_track_columns(self):
        from sqlalchemy.pool import StaticPool
        from sqlmodel import Session, create_engine, SQLModel as _SQLModel

        engine = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        _SQLModel.metadata.create_all(engine)
        with Session(engine) as session:
            handler = CustomerDim.as_loader(session)
            # Should include city and region, exclude SCD infrastructure + NK
            assert "city" in handler.track_columns
            assert "region" in handler.track_columns
            assert "code" not in handler.track_columns
            assert "valid_from" not in handler.track_columns
        engine.dispose()

    def test_explicit_track_columns_respected(self):
        from sqlalchemy.pool import StaticPool
        from sqlmodel import Session, create_engine, SQLModel as _SQLModel

        engine = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        _SQLModel.metadata.create_all(engine)
        with Session(engine) as session:
            handler = CustomerDim.as_loader(session, track_columns=["city"])
            assert handler.track_columns == ["city"]
        engine.dispose()


# ---------------------------------------------------------------------------
# _factory.py missing-required-ClassVar error paths (lines 128, 147)
# ---------------------------------------------------------------------------

class TestBitmaskMissingClassVars:
    def test_missing_partition_and_dates_column_raises(self, sink):
        """_factory.py line 128: bitmask without __partition_key__ AND __dates_column__
        and no natural_key for inference → TypeError."""
        class NoBitmaskKeys(FactModel):
            __strategy__ = "bitmask"
            # no __partition_key__, no __dates_column__, no __natural_key__

        with pytest.raises(TypeError, match="bitmask"):
            NoBitmaskKeys.as_loader(sink, reference_date="2024-01-31")


class TestArrayMetricMissingClassVars:
    def test_missing_partition_and_value_column_raises(self, sink):
        """_factory.py line 147: array_metric without __partition_key__ AND __value_column__
        and no natural_key for inference → TypeError."""
        class NoArrayMetricKeys(FactModel):
            __strategy__ = "array_metric"
            # no __partition_key__, no __value_column__, no __natural_key__

        with pytest.raises(TypeError, match="array_metric"):
            NoArrayMetricKeys.as_loader(sink, month_start=date(2024, 1, 1))


class TestEdgeProjectionMissingBothViaFactory:
    def test_missing_keys_with_natural_key_bypass_raises_at_factory(self, sink):
        """_factory.py line 164: model with __natural_key__ bypasses class-definition
        guard but factory still raises when both subject/object keys are absent."""
        class EdgeWithNK(FactModel):
            __strategy__ = "edge_projection"
            __natural_key__ = ["id"]  # bypasses __init_subclass__ guard
            # no __subject_key__, no __object_key__

        with pytest.raises(TypeError, match="edge_projection"):
            EdgeWithNK.as_loader(sink)


# ---------------------------------------------------------------------------
# _utils.py edge-case paths (lines 35, 46, 65-68, 110-112)
# ---------------------------------------------------------------------------

class TestUtilsInternals:
    def test_assert_not_dimension_raises_for_dim_model(self, sink):
        """_utils.py line 35: LazyTransactionLoader.load() rejects DimensionModel subclass."""
        from sqldim.core.loaders.snapshot import LazyTransactionLoader

        class PlainDim(DimensionModel):
            pass

        loader = LazyTransactionLoader(sink)
        with pytest.raises(TypeError, match="SCDHandler"):
            loader.load("data.parquet", PlainDim)

    def test_is_optional_datetime_bare_datetime(self):
        """_utils.py line 46: bare datetime and date return True."""
        from datetime import date, datetime
        from sqldim.core.loaders._utils import _is_optional_datetime

        assert _is_optional_datetime(datetime) is True
        assert _is_optional_datetime(date) is True

    def test_is_numeric_optional_int_and_float(self):
        """_utils.py lines 65-67: Optional[int] and Optional[float] are numeric."""
        from sqldim.core.loaders._utils import _is_numeric

        assert _is_numeric(Optional[int]) is True
        assert _is_numeric(Optional[float]) is True

    def test_is_numeric_non_numeric_returns_false(self):
        """_utils.py line 68: non-numeric types return False."""
        from sqldim.core.loaders._utils import _is_numeric

        assert _is_numeric(str) is False
        assert _is_numeric(list) is False

    def test_infer_loader_params_skips_field_with_no_annotation(self):
        """_utils.py lines 110-112: field with annotation=None falls back to
        __annotations__ dict; if still None the field is skipped."""
        from sqldim.core.loaders._utils import _infer_loader_params

        class FakeFieldInfo:
            annotation = None

        class MockModelNoAnnot:
            model_fields = {"x": FakeFieldInfo()}
            __natural_key__: list = []
            __annotations__: dict = {}  # fallback also None → continue

        result = _infer_loader_params(MockModelNoAnnot)
        assert isinstance(result, dict)
        # 'x' was entirely skipped — not classified as datetime, list, or numeric
        assert "milestones" not in result
        assert "cumulative_column" not in result
        assert "value_column" not in result

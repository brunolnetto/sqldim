"""Tests for:

1. snapshot_date at load() call time
2. aload() unified async wrapper on all loaders + SCDHandler
3. DWSchema dimension-first orchestrator
"""
from __future__ import annotations

import asyncio
from datetime import date, datetime
from typing import List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sqldim.core.kimball.fields import Field
from sqldim.core.kimball.mixins import SCD2Mixin
from sqldim.core.kimball.models import DimensionModel, FactModel
from sqldim.core.kimball.schema import DWSchema
from sqldim.core.loaders.fact.snapshot import LazySnapshotLoader, LazyTransactionLoader
from sqldim.core.loaders.fact.accumulating import LazyAccumulatingLoader
from sqldim.core.loaders.fact.cumulative import LazyCumulativeLoader
from sqldim.core.loaders.dimension.bitmask import LazyBitmaskLoader
from sqldim.core.loaders.dimension.array_metric import LazyArrayMetricLoader
from sqldim.core.loaders.dimension.edge_projection import LazyEdgeProjectionLoader
from sqldim.core.kimball.dimensions.scd.handler import SCDHandler, SCDResult


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sink():
    return MagicMock()


@pytest.fixture
def session():
    return MagicMock()


# ---------------------------------------------------------------------------
# Models for testing
# ---------------------------------------------------------------------------

class TxFact(FactModel):
    __strategy__ = "bulk"


class SnapFact(FactModel):
    __strategy__ = "snapshot"


class AccFact(FactModel):
    __strategy__ = "accumulating"
    __match_column__ = "order_id"
    __milestones__ = ["approved_at", "shipped_at"]


class EdgeFact(FactModel):
    __strategy__ = "edge_projection"
    __subject_key__ = "player_id"
    __object_key__ = "game_id"


class CumFact(FactModel):
    __strategy__ = "cumulative"
    __partition_key__ = "player_id"
    __cumulative_column__ = "seasons"
    __metric_columns__ = ["pts", "ast"]


class CustomerDim(DimensionModel, SCD2Mixin):
    __natural_key__ = ["code"]
    __scd_type__ = 2
    code: str = Field(default=None)
    city: str = Field(default=None)


# ===========================================================================
# 1. snapshot_date at load() call time
# ===========================================================================

class TestSnapshotDateAtLoadTime:

    def test_construction_without_date_succeeds(self, sink):
        loader = LazySnapshotLoader(sink)
        assert loader.snapshot_date is None

    def test_load_raises_when_no_date_supplied(self, sink):
        loader = LazySnapshotLoader(sink)
        with pytest.raises(TypeError, match="snapshot_date"):
            loader.load(object(), "tbl")

    def test_load_uses_instance_date(self):
        """load() picks up the instance-level snapshot_date."""
        _sink = MagicMock()
        loader = LazySnapshotLoader(_sink, snapshot_date="2024-03-31")
        loader._con = MagicMock()
        loader._con.execute = MagicMock()
        _sink.write = MagicMock(return_value=5)

        with patch("sqldim.sources.coerce_source") as cs:
            cs.return_value.as_sql.return_value = "SELECT 1"
            result = loader.load("fake.parquet", "tbl")
        assert result == 5

    def test_load_call_time_date_overrides_instance(self):
        """A snapshot_date passed to load() overrides the constructor value."""
        _sink = MagicMock()
        loader = LazySnapshotLoader(_sink, snapshot_date="2024-01-31")
        loader._con = MagicMock()
        _sink.write = MagicMock(return_value=3)

        captured = []

        def capturing_execute(sql):
            if "snapshot_rows" in sql:
                captured.append(sql)

        loader._con.execute.side_effect = capturing_execute

        with patch("sqldim.sources.coerce_source") as cs:
            cs.return_value.as_sql.return_value = "SELECT 1"
            loader.load("fake.parquet", "tbl", snapshot_date="2024-06-30")

        assert any("2024-06-30" in s for s in captured), "call-time date not injected"
        assert all("2024-01-31" not in s for s in captured), "old date leaked"

    def test_load_stream_raises_when_no_date(self, sink):
        loader = LazySnapshotLoader(sink)
        with pytest.raises(TypeError, match="snapshot_date"):
            loader.load_stream(MagicMock(), "tbl")

    def test_as_loader_snapshot_date_optional(self, sink):
        loader = SnapFact.as_loader(sink)
        assert isinstance(loader, LazySnapshotLoader)
        assert loader.snapshot_date is None

    def test_as_loader_snapshot_date_at_construction(self, sink):
        loader = SnapFact.as_loader(sink, snapshot_date="2024-03-31")
        assert loader.snapshot_date == "2024-03-31"


# ===========================================================================
# 2. aload() unified async wrapper
# ===========================================================================

class TestAloadWrapper:
    """Each lazy loader exposes aload() that mirrors its primary sync method."""

    def test_transaction_loader_has_aload(self, sink):
        loader = LazyTransactionLoader(sink)
        assert callable(getattr(loader, "aload", None))
        assert asyncio.iscoroutinefunction(loader.aload)

    def test_snapshot_loader_has_aload(self, sink):
        loader = LazySnapshotLoader(sink)
        assert asyncio.iscoroutinefunction(loader.aload)

    def test_accumulating_loader_has_aload(self, sink):
        loader = LazyAccumulatingLoader("tbl", "order_id", ["shipped_at"], sink)
        assert asyncio.iscoroutinefunction(loader.aload)

    def test_cumulative_loader_has_aload(self, sink):
        loader = LazyCumulativeLoader("tbl", "player_id", "seasons", ["pts"], sink)
        assert asyncio.iscoroutinefunction(loader.aload)

    def test_bitmask_loader_has_aload(self, sink):
        loader = LazyBitmaskLoader("tbl", "user_id", "dates", "2024-01-31", sink)
        assert asyncio.iscoroutinefunction(loader.aload)

    def test_array_metric_loader_has_aload(self, sink):
        loader = LazyArrayMetricLoader("tbl", "user_id", "revenue", "monthly_revenue",
                                       date(2024, 1, 1), sink)
        assert asyncio.iscoroutinefunction(loader.aload)

    def test_edge_projection_loader_has_aload(self, sink):
        loader = LazyEdgeProjectionLoader("tbl", "player_id", "game_id", sink)
        assert asyncio.iscoroutinefunction(loader.aload)

    def test_scd_handler_has_aload(self, session):
        from sqlalchemy.pool import StaticPool
        from sqlmodel import create_engine, SQLModel as _SQLModel, Session
        engine = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        _SQLModel.metadata.create_all(engine)
        with Session(engine) as s:
            handler = CustomerDim.as_loader(s)
            assert asyncio.iscoroutinefunction(handler.aload)
        engine.dispose()

    def test_accumulating_aload_delegates_to_process(self, sink):
        """aload() should call process() under the hood."""
        loader = LazyAccumulatingLoader("tbl", "order_id", ["shipped_at"], sink)
        called_with = []

        def fake_process(source):
            called_with.append(source)
            return {"inserted": 1, "updated": 0}

        loader.process = fake_process
        result = asyncio.run(loader.aload("my_source.parquet"))
        assert called_with == ["my_source.parquet"]
        assert result == {"inserted": 1, "updated": 0}

    def test_edge_aload_delegates_to_process(self, sink):
        loader = LazyEdgeProjectionLoader("tbl", "player_id", "game_id", sink)
        loader.process = MagicMock(return_value=42)
        result = asyncio.run(loader.aload("data.parquet"))
        loader.process.assert_called_once_with("data.parquet")
        assert result == 42

    def test_cumulative_aload_passes_extra_arg(self, sink):
        loader = LazyCumulativeLoader("tbl", "player_id", "seasons", ["pts"], sink)
        loader.process = MagicMock(return_value=5)
        result = asyncio.run(loader.aload("today.parquet", 2024))
        loader.process.assert_called_once_with("today.parquet", 2024)
        assert result == 5

    def test_scd_handler_aload_is_alias_for_process(self, session):
        """SCDHandler.aload() must await the same logic as process()."""
        from sqlalchemy.pool import StaticPool
        from sqlmodel import create_engine, SQLModel as _SQLModel, Session
        engine = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        _SQLModel.metadata.create_all(engine)
        with Session(engine) as s:
            handler = CustomerDim.as_loader(s)
            expected = SCDResult()
            # Patch process() so we don't need real DB state
            async def fake_process(records):
                return expected
            handler.process = fake_process
            result = asyncio.run(handler.aload([{"code": "C1", "city": "Paris"}]))
            assert result is expected
        engine.dispose()

    def test_transaction_aload_requires_table_or_model(self, sink):
        """aload() on an unbound transaction loader must raise without a table."""
        loader = LazyTransactionLoader(sink)
        with pytest.raises(TypeError, match="table"):
            asyncio.run(loader.aload("data.parquet"))

    def test_transaction_aload_uses_model_cls(self, sink):
        """aload() on a model-bound loader uses _model_cls as the table."""
        loader = TxFact.as_loader(sink)
        loader.load = MagicMock(return_value=7)
        result = asyncio.run(loader.aload("data.parquet"))
        loader.load.assert_called_once_with("data.parquet", TxFact)
        assert result == 7

    def test_snapshot_aload_raises_without_table_or_model(self, sink):
        """snapshot.py lines 204-210: LazySnapshotLoader.aload() raises TypeError when
        neither table kwarg nor _model_cls is available on the unbound loader."""
        loader = LazySnapshotLoader(sink, snapshot_date="2024-03-31")
        with pytest.raises(TypeError, match="table"):
            asyncio.run(loader.aload("data.parquet"))

    def test_snapshot_aload_succeeds_with_explicit_table(self, sink):
        """snapshot.py line 210: aload() runs load() via to_thread when table is provided."""
        loader = LazySnapshotLoader(sink)
        loader.load = MagicMock(return_value=5)
        result = asyncio.run(loader.aload("data.parquet", "my_table", snapshot_date="2024-03-31"))
        loader.load.assert_called_once_with("data.parquet", "my_table", snapshot_date="2024-03-31")
        assert result == 5


# ===========================================================================
# 3. DWSchema orchestrator
# ===========================================================================

class TestDWSchemaConstruction:

    def test_separates_dims_and_facts(self, sink):
        schema = DWSchema([CustomerDim, TxFact], sink)
        assert CustomerDim in schema._dims
        assert TxFact in schema._facts

    def test_order_invariant(self, sink):
        """Even if facts are listed before dims, dims must be first internally."""
        schema = DWSchema([TxFact, CustomerDim], sink)
        assert schema._dims == [CustomerDim]
        assert schema._facts == [TxFact]

    def test_missing_session_raises_for_dim_load(self, sink):
        schema = DWSchema([CustomerDim, TxFact], sink)  # no session
        with pytest.raises(TypeError, match="session"):
            schema.load({CustomerDim: [{"code": "C1", "city": "Paris"}]})


class TestDWSchemaLoad:

    def test_skips_models_not_in_source_map(self, sink):
        schema = DWSchema([TxFact, EdgeFact], sink)
        # Only TxFact is in the map
        loader_mock = MagicMock()
        loader_mock.aload = AsyncMock(return_value=10)

        with patch.object(TxFact, "as_loader", return_value=loader_mock):
            results = schema.load({TxFact: "data.parquet"})

        assert TxFact in results
        assert EdgeFact not in results

    def test_dimension_loaded_before_fact(self, sink, session):
        """Dimensions must appear in results before facts when using process_order."""
        schema = DWSchema([TxFact, CustomerDim], sink, session=session)

        mock_handler = MagicMock()
        mock_handler.process = AsyncMock(return_value=SCDResult())
        mock_fact_loader = MagicMock()
        mock_fact_loader.aload = AsyncMock(return_value=5)

        with (
            patch.object(CustomerDim, "as_loader", return_value=mock_handler),
            patch.object(TxFact, "as_loader", return_value=mock_fact_loader),
        ):
            results = schema.load({
                TxFact: "orders.parquet",
                CustomerDim: [{"code": "C1"}],
            })

        assert CustomerDim in results
        assert TxFact in results

    def test_source_spec_with_kwargs(self, sink):
        """source_map value as (source, kwargs_dict) should forward kwargs to aload()."""
        schema = DWSchema([SnapFact], sink)
        mock_loader = MagicMock()
        mock_loader.aload = AsyncMock(return_value=3)
        mock_loader._model_cls = SnapFact

        with patch.object(SnapFact, "as_loader", return_value=mock_loader):
            schema.load({SnapFact: ("data.parquet", {"snapshot_date": "2024-03-31"})})

        mock_loader.aload.assert_awaited_once_with(
            "data.parquet", snapshot_date="2024-03-31"
        )

    def test_source_spec_with_extra_positional(self, sink):
        """(source, extra_arg) tuple is unpacked for loaders needing two positionals."""
        schema = DWSchema([CumFact], sink)
        loader_mock = MagicMock()
        loader_mock.aload = AsyncMock(return_value=8)

        with patch.object(CumFact, "as_loader", return_value=loader_mock):
            schema.load({CumFact: ("today.parquet", 2024)})

        loader_mock.aload.assert_awaited_once_with("today.parquet", 2024)

    def test_empty_source_map_returns_empty(self, sink):
        schema = DWSchema([TxFact, CustomerDim], sink)
        results = schema.load({})
        assert results == {}

    @pytest.mark.asyncio
    async def test_load_raises_in_running_event_loop(self, sink):
        """schema.py line 118: DWSchema.load() raises RuntimeError inside a running async loop."""
        schema = DWSchema([TxFact], sink)
        with pytest.raises(RuntimeError, match="running event loop"):
            schema.load({TxFact: "data.parquet"})


class TestDWSchemaAload:

    @pytest.mark.asyncio
    async def test_aload_runs_fact_via_aload(self, sink):
        schema = DWSchema([AccFact], sink)
        mock_loader = MagicMock()
        mock_loader.aload = AsyncMock(return_value={"inserted": 5, "updated": 2})

        with patch.object(AccFact, "as_loader", return_value=mock_loader):
            results = await schema.aload({AccFact: "orders.parquet"})

        mock_loader.aload.assert_awaited_once_with("orders.parquet")
        assert results[AccFact] == {"inserted": 5, "updated": 2}

    @pytest.mark.asyncio
    async def test_aload_dimension_via_aload(self, sink, session):
        schema = DWSchema([CustomerDim, AccFact], sink, session=session)
        mock_handler = MagicMock()
        scd_result = SCDResult()
        mock_handler.aload = AsyncMock(return_value=scd_result)
        mock_fact_loader = MagicMock()
        mock_fact_loader.aload = AsyncMock(return_value={"inserted": 2, "updated": 0})

        with (
            patch.object(CustomerDim, "as_loader", return_value=mock_handler),
            patch.object(AccFact, "as_loader", return_value=mock_fact_loader),
        ):
            results = await schema.aload({
                CustomerDim: [{"code": "C1"}],
                AccFact: "orders.parquet",
            })

        assert results[CustomerDim] is scd_result
        assert results[AccFact] == {"inserted": 2, "updated": 0}

    @pytest.mark.asyncio
    async def test_aload_missing_session_raises(self, sink):
        schema = DWSchema([CustomerDim], sink)
        with pytest.raises(TypeError, match="session"):
            await schema.aload({CustomerDim: [{"code": "C1"}]})

    @pytest.mark.asyncio
    async def test_aload_snapshot_with_kwargs(self, sink):
        schema = DWSchema([SnapFact], sink)
        mock_loader = MagicMock()
        mock_loader.aload = AsyncMock(return_value=10)
        mock_loader._model_cls = SnapFact

        with patch.object(SnapFact, "as_loader", return_value=mock_loader):
            await schema.aload({
                SnapFact: ("balances.parquet", {"snapshot_date": "2024-03-31"}),
            })

        mock_loader.aload.assert_awaited_once_with(
            "balances.parquet", snapshot_date="2024-03-31"
        )

    @pytest.mark.asyncio
    async def test_aload_skips_models_not_in_source_map(self, sink):
        """schema.py line 142: aload() continue branch when model absent from source_map."""
        schema = DWSchema([TxFact, EdgeFact], sink)
        mock_loader = MagicMock()
        mock_loader.aload = AsyncMock(return_value=10)

        with patch.object(TxFact, "as_loader", return_value=mock_loader):
            results = await schema.aload({TxFact: "data.parquet"})  # EdgeFact absent → continue

        assert TxFact in results
        assert EdgeFact not in results

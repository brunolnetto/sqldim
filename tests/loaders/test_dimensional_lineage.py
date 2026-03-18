"""Tests for DimensionalLoader lineage integration — mocked DB, real lineage."""
import io
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sqldim.core.kimball.models import DimensionModel, FactModel


# ---------------------------------------------------------------------------
# Minimal stub models (no real DB)
# ---------------------------------------------------------------------------

class StubDim(DimensionModel):
    __tablename__ = "stub_dim"
    name: str = ""

    class Config:
        arbitrary_types_allowed = True


class StubFact(FactModel):
    __tablename__ = "stub_fact"
    dim_id: int = 0
    amount: float = 0.0

    class Config:
        arbitrary_types_allowed = True


def _make_loader(*, emitter=None):
    """Create a DimensionalLoader with a fully mocked session."""
    from sqldim.core.loaders.dimensional import DimensionalLoader

    mock_session = MagicMock()
    loader = DimensionalLoader(
        mock_session,
        [StubDim, StubFact],
        lineage_emitter=emitter,
    )
    return loader, mock_session


def _make_emitter():
    from sqldim.lineage import ConsoleLineageEmitter
    buf = io.StringIO()
    return ConsoleLineageEmitter(stream=buf), buf


# ---------------------------------------------------------------------------
# No lineage emitter → no events
# ---------------------------------------------------------------------------

class TestNoLineage:
    @pytest.mark.asyncio
    async def test_no_crash_without_emitter(self):
        loader, session = _make_loader()
        loader.register(StubDim, [{"name": "a"}])

        with patch.object(loader, "_load_dimension", new_callable=AsyncMock):
            await loader.run()
        # No assertion — just ensures no crash


# ---------------------------------------------------------------------------
# Lineage emission — successful load
# ---------------------------------------------------------------------------

class TestLineageSuccessfulLoad:
    @pytest.mark.asyncio
    async def test_pipeline_start_and_complete(self):
        emitter, buf = _make_emitter()
        loader, session = _make_loader(emitter=emitter)
        loader.register(StubDim, [{"name": "a"}])

        with patch.object(loader, "_load_dimension", new_callable=AsyncMock):
            await loader.run()

        events = [json.loads(l) for l in buf.getvalue().strip().split("\n")]
        job_names = [e["job"]["name"] for e in events]
        assert "load.pipeline" in job_names
        pipeline_events = [e for e in events if e["job"]["name"] == "load.pipeline"]
        assert pipeline_events[0]["eventType"] == "START"
        assert pipeline_events[-1]["eventType"] == "COMPLETE"
        # Same run_id across all events
        run_ids = {e["run"]["runId"] for e in events}
        assert len(run_ids) == 1

    @pytest.mark.asyncio
    async def test_per_model_start_and_complete(self):
        emitter, buf = _make_emitter()
        loader, session = _make_loader(emitter=emitter)
        loader.register(StubDim, [{"name": "a"}])
        loader.register(StubFact, [{"dim_id": 1, "amount": 10.0}], key_map={})

        with patch.object(loader, "_load_dimension", new_callable=AsyncMock), \
             patch.object(loader, "_execute_fact_strategy", new_callable=AsyncMock):
            await loader.run()

        events = [json.loads(l) for l in buf.getvalue().strip().split("\n")]
        model_events = [e for e in events if e["job"]["name"].startswith("load.Stub")]
        # Each model gets START + COMPLETE = 2 events
        assert len(model_events) == 4
        # Dimension loads first
        assert model_events[0]["job"]["name"] == "load.StubDim"
        assert model_events[0]["eventType"] == "START"
        assert model_events[1]["job"]["name"] == "load.StubDim"
        assert model_events[1]["eventType"] == "COMPLETE"

    @pytest.mark.asyncio
    async def test_model_type_in_facets(self):
        emitter, buf = _make_emitter()
        loader, session = _make_loader(emitter=emitter)
        loader.register(StubDim, [{"name": "a"}])

        with patch.object(loader, "_load_dimension", new_callable=AsyncMock):
            await loader.run()

        events = [json.loads(l) for l in buf.getvalue().strip().split("\n")]
        dim_start = next(e for e in events if e["job"]["name"] == "load.StubDim" and e["eventType"] == "START")
        assert dim_start["run"]["facets"]["model_type"] == "dimension"
        assert dim_start["run"]["facets"]["rows"] == 1

    @pytest.mark.asyncio
    async def test_loaded_models_in_pipeline_complete(self):
        emitter, buf = _make_emitter()
        loader, session = _make_loader(emitter=emitter)
        loader.register(StubDim, [{"name": "a"}])

        with patch.object(loader, "_load_dimension", new_callable=AsyncMock):
            await loader.run()

        events = [json.loads(l) for l in buf.getvalue().strip().split("\n")]
        pipeline_complete = next(
            e for e in events
            if e["job"]["name"] == "load.pipeline" and e["eventType"] == "COMPLETE"
        )
        assert "StubDim" in pipeline_complete["run"]["facets"]["loaded_models"]
        assert len(pipeline_complete["outputs"]) == 1
        assert pipeline_complete["outputs"][0]["name"] == "StubDim"


# ---------------------------------------------------------------------------
# Lineage emission — failed load
# ---------------------------------------------------------------------------

class TestLineageFailedLoad:
    @pytest.mark.asyncio
    async def test_model_fail_emits_fail_event(self):
        emitter, buf = _make_emitter()
        loader, session = _make_loader(emitter=emitter)
        loader.register(StubDim, [{"name": "a"}])

        with patch.object(loader, "_load_dimension", new_callable=AsyncMock, side_effect=RuntimeError("DB down")):
            with pytest.raises(RuntimeError, match="DB down"):
                await loader.run()

        events = [json.loads(l) for l in buf.getvalue().strip().split("\n")]
        dim_events = [e for e in events if e["job"]["name"] == "load.StubDim"]
        assert any(e["eventType"] == "FAIL" for e in dim_events)

    @pytest.mark.asyncio
    async def test_pipeline_fail_on_model_failure(self):
        emitter, buf = _make_emitter()
        loader, session = _make_loader(emitter=emitter)
        loader.register(StubDim, [{"name": "a"}])

        with patch.object(loader, "_load_dimension", new_callable=AsyncMock, side_effect=RuntimeError("DB down")):
            with pytest.raises(RuntimeError):
                await loader.run()

        events = [json.loads(l) for l in buf.getvalue().strip().split("\n")]
        pipeline_events = [e for e in events if e["job"]["name"] == "load.pipeline"]
        assert pipeline_events[-1]["eventType"] == "FAIL"

    @pytest.mark.asyncio
    async def test_loaded_models_excludes_failed(self):
        emitter, buf = _make_emitter()
        loader, session = _make_loader(emitter=emitter)
        loader.register(StubDim, [{"name": "a"}])

        with patch.object(loader, "_load_dimension", new_callable=AsyncMock, side_effect=RuntimeError("boom")):
            with pytest.raises(RuntimeError):
                await loader.run()

        events = [json.loads(l) for l in buf.getvalue().strip().split("\n")]
        pipeline_fail = next(
            e for e in events
            if e["job"]["name"] == "load.pipeline" and e["eventType"] == "FAIL"
        )
        assert "StubDim" not in pipeline_fail["run"]["facets"]["loaded_models"]

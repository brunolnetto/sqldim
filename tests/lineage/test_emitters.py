"""Tests for lineage emitters — ConsoleLineageEmitter, OpenLineageEmitter guard."""
import io
import json
from unittest.mock import MagicMock

import pytest

from sqldim.lineage import (
    LineageEvent,
    LineageEmitter,
    ConsoleLineageEmitter,
    RunState,
    OpenLineageEmitter,
)
from sqldim.lineage.events import DatasetRef


# ---------------------------------------------------------------------------
# Protocol compliance
# ---------------------------------------------------------------------------

class TestProtocolCompliance:
    def test_console_emitter_is_lineage_emitter(self):
        assert isinstance(ConsoleLineageEmitter(), LineageEmitter)


# ---------------------------------------------------------------------------
# ConsoleLineageEmitter
# ---------------------------------------------------------------------------

class TestConsoleLineageEmitter:
    def test_writes_event_as_json_line(self):
        buf = io.StringIO()
        emitter = ConsoleLineageEmitter(stream=buf)
        ev = LineageEvent(job_name="load.dim_customer", state=RunState.COMPLETE)
        emitter.emit(ev)
        lines = buf.getvalue().strip().split("\n")
        assert len(lines) == 1
        data = json.loads(lines[0])
        assert data["eventType"] == "COMPLETE"
        assert data["job"]["name"] == "load.dim_customer"

    def test_includes_inputs_and_outputs(self):
        buf = io.StringIO()
        emitter = ConsoleLineageEmitter(stream=buf)
        ev = LineageEvent(
            job_name="x",
            inputs=[DatasetRef("bronze", "raw")],
            outputs=[DatasetRef("silver", "dim")],
        )
        emitter.emit(ev)
        data = json.loads(buf.getvalue().strip())
        assert len(data["inputs"]) == 1
        assert data["inputs"][0]["name"] == "raw"
        assert len(data["outputs"]) == 1
        assert data["outputs"][0]["name"] == "dim"

    def test_multiple_events_multiple_lines(self):
        buf = io.StringIO()
        emitter = ConsoleLineageEmitter(stream=buf)
        emitter.emit(LineageEvent(job_name="a", state=RunState.START))
        emitter.emit(LineageEvent(job_name="a", state=RunState.COMPLETE))
        lines = buf.getvalue().strip().split("\n")
        assert len(lines) == 2
        assert json.loads(lines[0])["eventType"] == "START"
        assert json.loads(lines[1])["eventType"] == "COMPLETE"

    def test_run_id_preserved(self):
        buf = io.StringIO()
        emitter = ConsoleLineageEmitter(stream=buf)
        ev = LineageEvent(run_id="custom_run_id", job_name="x")
        emitter.emit(ev)
        data = json.loads(buf.getvalue().strip())
        assert data["run"]["runId"] == "custom_run_id"

    def test_broken_stream_does_not_raise(self):
        emitter = ConsoleLineageEmitter(stream=None)
        emitter._stream = _BrokenStream()
        emitter.emit(LineageEvent(job_name="x"))
        # No exception — we got here safely


class _BrokenStream:
    def write(self, data):
        raise OSError("broken pipe")

    def flush(self):
        raise OSError("broken pipe")


# ---------------------------------------------------------------------------
# OpenLineageEmitter — ImportError guard
# ---------------------------------------------------------------------------

class TestOpenLineageImportGuard:
    def test_raises_import_error_without_dep(self):
        with pytest.raises(ImportError, match="pip install sqldim\\[lineage\\]"):
            OpenLineageEmitter()


# ---------------------------------------------------------------------------
# OpenLineageEmitter — mocked SDK
# ---------------------------------------------------------------------------

class TestOpenLineageEmitterMocked:
    """Cover lines 81-123 of emitter.py with a mocked openlineage package."""

    @staticmethod
    def _build_mocks():
        """Build a complete mock openlineage module tree."""
        from unittest.mock import MagicMock

        mock_ol = MagicMock()
        # RunState enum — attribute access returns the string value
        for state in ("START", "RUNNING", "COMPLETE", "FAIL", "ABORT"):
            setattr(mock_ol.client.run.RunState, state, state)
        # Callable constructor mocks (instances, not classes)
        mock_ol.client.OpenLineageClient = MagicMock()
        mock_ol.client.run.RunEvent = MagicMock()
        mock_ol.client.run.Run = MagicMock()
        mock_ol.client.run.Job = MagicMock()
        mock_ol.client.run.Dataset = MagicMock()
        return mock_ol

    @staticmethod
    def _patch_context(mock_ol):
        """Return (patch_dict_cm, module) for the emitter."""
        import importlib
        import sys
        from unittest.mock import patch

        modules = {
            "openlineage": mock_ol,
            "openlineage.client": mock_ol.client,
            "openlineage.client.run": mock_ol.client.run,
        }
        return (
            patch.dict(sys.modules, modules, clear=False),
            importlib.import_module("sqldim.lineage.emitter"),
        )

    def test_init_stores_namespace_and_creates_client(self):
        mock_ol = self._build_mocks()
        mock_ol.client.OpenLineageClient.return_value = MagicMock()
        ctx, mod = self._patch_context(mock_ol)
        with ctx:
            import importlib
            importlib.reload(mod)
            emitter = mod.OpenLineageEmitter(url="http://marquez:5000", namespace="prod")

        assert emitter._namespace == "prod"
        mock_ol.client.OpenLineageClient.assert_called_once_with(url="http://marquez:5000")

    def test_emit_translates_event_to_run_event(self):
        mock_ol = self._build_mocks()
        ctx, mod = self._patch_context(mock_ol)
        with ctx:
            import importlib
            importlib.reload(mod)
            emitter = mod.OpenLineageEmitter(namespace="sqldim")
            emitter.emit(LineageEvent(
                run_id="run-abc",
                job_name="bronze_to_silver",
                inputs=[DatasetRef(name="bronze.orders", namespace="sqldim")],
                outputs=[DatasetRef(name="silver.orders", namespace="sqldim")],
                state=RunState.COMPLETE,
            ))

        # MagicMock kwargs are NOT stored as attrs — inspect call_args instead
        run_event_call = mock_ol.client.run.RunEvent.call_args.kwargs
        assert run_event_call["eventType"] == "COMPLETE"
        # Job was called with namespace + name
        job_call = mock_ol.client.run.Job.call_args.kwargs
        assert job_call["namespace"] == "sqldim"
        assert job_call["name"] == "bronze_to_silver"
        # Dataset called once for inputs, once for outputs
        assert mock_ol.client.run.Dataset.call_count == 2

    def test_emit_maps_all_run_states(self):
        mock_ol = self._build_mocks()
        ctx, mod = self._patch_context(mock_ol)
        with ctx:
            import importlib
            importlib.reload(mod)
            emitter = mod.OpenLineageEmitter()
            for state in (RunState.START, RunState.RUNNING, RunState.COMPLETE, RunState.FAIL, RunState.ABORT):
                emitter.emit(LineageEvent(job_name="x", state=state))

        assert mock_ol.client.run.RunEvent.call_count == 5

    def test_emit_uses_event_namespace_when_set(self):
        """If the event has its own namespace, it overrides the emitter default."""
        mock_ol = self._build_mocks()
        ctx, mod = self._patch_context(mock_ol)
        with ctx:
            import importlib
            importlib.reload(mod)
            emitter = mod.OpenLineageEmitter(namespace="default_ns")
            emitter.emit(LineageEvent(
                job_name="x",
                state=RunState.COMPLETE,
                namespace="custom_ns",
            ))

        job_call = mock_ol.client.run.Job.call_args.kwargs
        assert job_call["namespace"] == "custom_ns"

    def test_emit_passes_facets_to_run(self):
        """Event facets should be forwarded to the Run facets."""
        mock_ol = self._build_mocks()
        ctx, mod = self._patch_context(mock_ol)
        with ctx:
            import importlib
            importlib.reload(mod)
            emitter = mod.OpenLineageEmitter()
            emitter.emit(LineageEvent(
                job_name="x",
                state=RunState.COMPLETE,
                facets={"rows": 100, "duration_ms": 42},
            ))

        run_call = mock_ol.client.run.Run.call_args.kwargs
        assert run_call["facets"] == {"rows": 100, "duration_ms": 42}

    def test_emit_handles_empty_inputs_outputs(self):
        """Events with no inputs/outputs should produce empty lists."""
        mock_ol = self._build_mocks()
        ctx, mod = self._patch_context(mock_ol)
        with ctx:
            import importlib
            importlib.reload(mod)
            emitter = mod.OpenLineageEmitter()
            emitter.emit(LineageEvent(job_name="x", state=RunState.START))

        run_event_call = mock_ol.client.run.RunEvent.call_args.kwargs
        assert run_event_call["inputs"] == []
        assert run_event_call["outputs"] == []

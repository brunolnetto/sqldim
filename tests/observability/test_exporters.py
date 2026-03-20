"""Tests for observability exporters — ConsoleExporter, protocol compliance, OTLP ImportError."""
import io
import json
from unittest.mock import MagicMock

import pytest

from sqldim.observability import (
    PipelineSpan,
    SpanStatus,
    MetricSample,
    MetricKind,
    ConsoleExporter,
    SpanExporter,
    MetricExporter,
)
from sqldim.observability.exporters import OTLPSpanExporter, OTLPMetricExporter


# ---------------------------------------------------------------------------
# Protocol compliance
# ---------------------------------------------------------------------------

class TestProtocolCompliance:
    def test_console_exporter_is_span_exporter(self):
        assert isinstance(ConsoleExporter(), SpanExporter)

    def test_console_exporter_is_metric_exporter(self):
        assert isinstance(ConsoleExporter(), MetricExporter)


# ---------------------------------------------------------------------------
# ConsoleExporter — spans
# ---------------------------------------------------------------------------

class TestConsoleExporterSpans:
    def test_writes_span_as_json_line(self):
        buf = io.StringIO()
        exp = ConsoleExporter(stream=buf)
        span = PipelineSpan(name="test", status=SpanStatus.OK, duration_s=0.1)
        exp.export(span)
        lines = buf.getvalue().strip().split("\n")
        assert len(lines) == 1
        data = json.loads(lines[0])
        assert data["type"] == "span"
        assert data["name"] == "test"
        assert data["status"] == "ok"
        assert data["duration_s"] == 0.1

    def test_span_with_attributes(self):
        buf = io.StringIO()
        exp = ConsoleExporter(stream=buf)
        span = PipelineSpan(name="x", attributes={"layer": "bronze", "rows": 500})
        exp.export(span)
        data = json.loads(buf.getvalue().strip())
        assert data["attributes"]["layer"] == "bronze"
        assert data["attributes"]["rows"] == 500

    def test_span_error_included(self):
        buf = io.StringIO()
        exp = ConsoleExporter(stream=buf)
        span = PipelineSpan(
            name="fail", status=SpanStatus.ERROR, error="timeout"
        )
        exp.export(span)
        data = json.loads(buf.getvalue().strip())
        assert data["status"] == "error"
        assert data["error"] == "timeout"

    def test_span_timestamps_serialised(self):
        from datetime import datetime, timezone
        buf = io.StringIO()
        exp = ConsoleExporter(stream=buf)
        now = datetime.now(timezone.utc)
        span = PipelineSpan(name="t", start_time=now, end_time=now)
        exp.export(span)
        data = json.loads(buf.getvalue().strip())
        assert data["start_time"] is not None
        assert data["end_time"] is not None

    def test_multiple_spans_multiple_lines(self):
        buf = io.StringIO()
        exp = ConsoleExporter(stream=buf)
        exp.export(PipelineSpan(name="a"))
        exp.export(PipelineSpan(name="b"))
        lines = buf.getvalue().strip().split("\n")
        assert len(lines) == 2
        assert json.loads(lines[0])["name"] == "a"
        assert json.loads(lines[1])["name"] == "b"


# ---------------------------------------------------------------------------
# ConsoleExporter — metrics
# ---------------------------------------------------------------------------

class TestConsoleExporterMetrics:
    def test_writes_metric_as_json_line(self):
        buf = io.StringIO()
        exp = ConsoleExporter(stream=buf)
        sample = MetricSample(name="rows", value=100.0, kind=MetricKind.COUNTER)
        exp.export(sample)
        data = json.loads(buf.getvalue().strip())
        assert data["type"] == "metric"
        assert data["name"] == "rows"
        assert data["value"] == 100.0
        assert data["kind"] == "counter"

    def test_metric_with_labels(self):
        buf = io.StringIO()
        exp = ConsoleExporter(stream=buf)
        sample = MetricSample(
            name="latency", value=42.0, kind=MetricKind.HISTOGRAM,
            labels={"pipeline": "orders"},
        )
        exp.export(sample)
        data = json.loads(buf.getvalue().strip())
        assert data["labels"]["pipeline"] == "orders"

    def test_metric_timestamp_serialised(self):
        buf = io.StringIO()
        exp = ConsoleExporter(stream=buf)
        sample = MetricSample(name="x", value=1.0, kind=MetricKind.GAUGE)
        exp.export(sample)
        data = json.loads(buf.getvalue().strip())
        assert data["timestamp"] is not None


# ---------------------------------------------------------------------------
# ConsoleExporter — resilience
# ---------------------------------------------------------------------------

class TestConsoleExporterResilience:
    def test_broken_stream_does_not_raise(self):
        """Even if the stream raises, the exporter swallows the error."""
        exp = ConsoleExporter(stream=None)
        # Patching _stream to a broken object after init
        exp._stream = _BrokenStream()
        exp.export(PipelineSpan(name="x"))
        exp.export(MetricSample(name="y", value=1.0, kind=MetricKind.GAUGE))
        # No exception — we got here safely


class _BrokenStream:
    def write(self, data):
        raise OSError("broken pipe")

    def flush(self):
        raise OSError("broken pipe")


# ---------------------------------------------------------------------------
# OTLP exporters — ImportError guard
# ---------------------------------------------------------------------------

class TestOTLPImportGuards:
    def test_otlp_span_exporter_raises_without_dep(self):
        """OTLPSpanExporter raises ImportError with install hint when OTel is missing."""
        with pytest.raises(ImportError, match="pip install sqldim\\[otel\\]"):
            OTLPSpanExporter(endpoint="http://localhost:4318")

    def test_otlp_metric_exporter_raises_without_dep(self):
        """OTLPMetricExporter raises ImportError with install hint when OTel is missing."""
        with pytest.raises(ImportError, match="pip install sqldim\\[otel\\]"):
            OTLPMetricExporter(endpoint="http://localhost:4318")




# ---------------------------------------------------------------------------
# OTLPSpanExporter — mocked OTel SDK
# ---------------------------------------------------------------------------

class _FakeOtelSpan:
    """Real class so __enter__/__exit__ resolve correctly (no MagicMock dunder issues)."""

    def __init__(self):
        self.attributes: list[tuple[str, object]] = []
        self.status: tuple | None = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def set_attribute(self, key: str, value: object):
        self.attributes.append((key, value))

    def set_status(self, *args):
        self.status = args


class TestOTLPSpanExporterMocked:
    """Cover lines 81-107 of exporters.py with a mocked opentelemetry package."""

    @staticmethod
    def _build_span_mocks():
        """Return (patch_dict_cm, mod_ref, mock_otel, mock_trace, mock_provider_cls)."""
        import importlib
        import sys
        from unittest.mock import patch

        mock_otel = MagicMock()
        mock_trace = MagicMock()
        mock_provider_cls = MagicMock()
        mock_processor_cls = MagicMock()
        mock_resource_cls = MagicMock()

        mock_trace.StatusCode = MagicMock()
        mock_trace.StatusCode.ERROR = "ERROR"
        mock_otel.trace = mock_trace
        mock_otel.sdk.trace.TracerProvider = mock_provider_cls
        mock_otel.sdk.trace.export.BatchSpanProcessor = mock_processor_cls
        mock_otel.sdk.trace.export.OTLPSpanExporter = MagicMock()
        mock_otel.sdk.resources.Resource = mock_resource_cls

        modules = {
            "opentelemetry": mock_otel,
            "opentelemetry.sdk": mock_otel.sdk,
            "opentelemetry.sdk.trace": mock_otel.sdk.trace,
            "opentelemetry.sdk.trace.export": mock_otel.sdk.trace.export,
            "opentelemetry.sdk.resources": mock_otel.sdk.resources,
        }
        mod_ref = importlib.import_module("sqldim.observability.exporters")
        return (
            patch.dict(sys.modules, modules, clear=False),
            mod_ref,
            mock_otel,
            mock_trace,
            mock_provider_cls,
        )

    def test_init_creates_provider_and_tracer(self):
        import importlib
        ctx, mod_ref, mock_otel, mock_trace, mock_provider_cls = self._build_span_mocks()
        with ctx:
            importlib.reload(mod_ref)
            mod_ref.OTLPSpanExporter(endpoint="http://localhost:9999")

        mock_otel.sdk.resources.Resource.create.assert_called_once()
        mock_provider_cls.return_value.add_span_processor.assert_called_once()
        mock_trace.get_tracer.assert_called_once_with("sqldim.pipeline")

    def test_export_creates_otel_span_with_attributes(self):
        import importlib
        ctx, mod_ref, mock_otel, mock_trace, _ = self._build_span_mocks()

        fake_span = _FakeOtelSpan()
        mock_trace.get_tracer.return_value.start_as_current_span.return_value = fake_span

        with ctx:
            importlib.reload(mod_ref)
            exp = mod_ref.OTLPSpanExporter()
            exp.export(PipelineSpan(
                name="scd_process",
                attributes={"table": "dim_customer", "rows": 500},
            ))

        assert ("table", "dim_customer") in fake_span.attributes
        assert ("rows", 500) in fake_span.attributes

    def test_export_sets_error_status_on_error_span(self):
        import importlib
        ctx, mod_ref, mock_otel, mock_trace, _ = self._build_span_mocks()

        fake_span = _FakeOtelSpan()
        mock_trace.get_tracer.return_value.start_as_current_span.return_value = fake_span

        with ctx:
            importlib.reload(mod_ref)
            exp = mod_ref.OTLPSpanExporter()
            exp.export(PipelineSpan(
                name="fail_step",
                status=SpanStatus.ERROR,
                error="connection timeout",
            ))

        assert fake_span.status is not None


# ---------------------------------------------------------------------------
# OTLPMetricExporter — mocked OTel SDK
# ---------------------------------------------------------------------------

class TestOTLPMetricExporterMocked:
    """Cover lines 120-154 of exporters.py with a mocked opentelemetry package."""

    @staticmethod
    def _build_metric_mocks():
        """Return (patch_dict_cm, mod_ref, mock_meter, counter, histogram, updown)."""
        import importlib
        import sys
        from unittest.mock import patch

        # Build flat independent mocks per module path to avoid parent-child spec issues
        mock_otel = MagicMock(name="opentelemetry")
        mock_sdk = MagicMock(name="opentelemetry.sdk")
        mock_sdk_metrics = MagicMock(name="opentelemetry.sdk.metrics")
        mock_sdk_metrics_export = MagicMock(name="opentelemetry.sdk.metrics.export")
        mock_sdk_resources = MagicMock(name="opentelemetry.sdk.resources")
        mock_metrics_mod = MagicMock(name="opentelemetry.metrics")

        mock_meter = MagicMock(name="meter")
        mock_counter = MagicMock(name="counter")
        mock_histogram = MagicMock(name="histogram")
        mock_updown = MagicMock(name="updown")

        mock_meter.create_counter.return_value = mock_counter
        mock_meter.create_histogram.return_value = mock_histogram
        mock_meter.create_up_down_counter.return_value = mock_updown
        mock_metrics_mod.get_meter.return_value = mock_meter

        mock_sdk_metrics.MeterProvider = MagicMock(name="MeterProvider")
        mock_sdk_metrics_export.PeriodicExportingMetricReader = MagicMock(name="PeriodicExportingMetricReader")
        mock_sdk_metrics_export.OTLPMetricExporter = MagicMock(name="OTLPMetricExporter")
        mock_sdk_resources.Resource = MagicMock(name="Resource")

        modules = {
            "opentelemetry": mock_otel,
            "opentelemetry.sdk": mock_sdk,
            "opentelemetry.sdk.metrics": mock_sdk_metrics,
            "opentelemetry.sdk.metrics.export": mock_sdk_metrics_export,
            "opentelemetry.sdk.resources": mock_sdk_resources,
        }
        # Wire otel.metrics so 'from opentelemetry import metrics' works
        mock_otel.metrics = mock_metrics_mod

        mod_ref = importlib.import_module("sqldim.observability.exporters")
        return (
            patch.dict(sys.modules, modules, clear=False),
            mod_ref,
            mock_metrics_mod,
            mock_meter,
            mock_counter,
            mock_histogram,
            mock_updown,
        )

    def test_init_creates_meter_provider(self):
        import importlib
        ctx, mod_ref, mock_metrics_mod, _, _, _, _ = self._build_metric_mocks()
        with ctx:
            importlib.reload(mod_ref)
            mod_ref.OTLPMetricExporter(endpoint="http://localhost:9999")

        mock_metrics_mod.get_meter.assert_called_once_with("sqldim.pipeline")

    def test_export_counter_creates_and_records(self):
        import importlib
        ctx, mod_ref, _, mock_meter, mock_counter, _, _ = self._build_metric_mocks()
        with ctx:
            importlib.reload(mod_ref)
            exp = mod_ref.OTLPMetricExporter()
            exp.export(MetricSample(
                name="sqldim.rows_inserted", value=42.0,
                kind=MetricKind.COUNTER, labels={"table": "orders"},
            ))

        mock_meter.create_counter.assert_called_once_with("sqldim.rows_inserted")
        mock_counter.record.assert_called_once_with(42.0, attributes={"table": "orders"})

    def test_export_histogram_creates_and_records(self):
        import importlib
        ctx, mod_ref, _, mock_meter, _, mock_histogram, _ = self._build_metric_mocks()
        with ctx:
            importlib.reload(mod_ref)
            exp = mod_ref.OTLPMetricExporter()
            exp.export(MetricSample(
                name="sqldim.duration", value=0.34, kind=MetricKind.HISTOGRAM,
            ))

        mock_meter.create_histogram.assert_called_once_with("sqldim.duration")
        mock_histogram.record.assert_called_once_with(0.34, attributes={})

    def test_export_gauge_uses_up_down_counter(self):
        import importlib
        ctx, mod_ref, _, mock_meter, _, _, mock_updown = self._build_metric_mocks()
        with ctx:
            importlib.reload(mod_ref)
            exp = mod_ref.OTLPMetricExporter()
            exp.export(MetricSample(
                name="sqldim.backlog", value=1200.0,
                kind=MetricKind.GAUGE, labels={"topic": "events"},
            ))

        mock_meter.create_up_down_counter.assert_called_once_with("sqldim.backlog")
        mock_updown.record.assert_called_once_with(1200.0, attributes={"topic": "events"})

    def test_export_reuses_instrument_for_same_metric(self):
        """Second export of the same name+kind should reuse the instrument."""
        import importlib
        ctx, mod_ref, _, mock_meter, mock_counter, _, _ = self._build_metric_mocks()
        with ctx:
            importlib.reload(mod_ref)
            exp = mod_ref.OTLPMetricExporter()
            exp.export(MetricSample(name="rows", value=1.0, kind=MetricKind.COUNTER))
            exp.export(MetricSample(name="rows", value=2.0, kind=MetricKind.COUNTER))

        mock_meter.create_counter.assert_called_once()
        assert mock_counter.record.call_count == 2


# ---------------------------------------------------------------------------
# OTelCollector integration with exporters
# ---------------------------------------------------------------------------

class TestCollectorWithExporter:
    def test_collector_forwards_spans_to_exporter(self):
        from sqldim.observability import OTelCollector

        buf = io.StringIO()
        exporter = ConsoleExporter(stream=buf)
        col = OTelCollector(span_exporter=exporter)

        with col.start_span("hello"):
            pass

        lines = buf.getvalue().strip().split("\n")
        assert len(lines) == 1
        assert json.loads(lines[0])["name"] == "hello"
        # In-memory store also works
        assert len(col.spans()) == 1

    def test_collector_forwards_metrics_to_exporter(self):
        from sqldim.observability import OTelCollector

        buf = io.StringIO()
        exporter = ConsoleExporter(stream=buf)
        col = OTelCollector(metric_exporter=exporter)

        col.record_metric(
            MetricSample(name="rows", value=42.0, kind=MetricKind.COUNTER)
        )

        data = json.loads(buf.getvalue().strip())
        assert data["name"] == "rows"
        assert len(col.metrics()) == 1

    def test_collector_without_exporter_still_in_memory(self):
        from sqldim.observability import OTelCollector

        col = OTelCollector()
        with col.start_span("x"):
            pass
        col.record_metric(MetricSample("y", 1.0, MetricKind.GAUGE))
        assert len(col.spans()) == 1
        assert len(col.metrics()) == 1

    def test_collector_error_span_still_exported(self):
        from sqldim.observability import OTelCollector

        buf = io.StringIO()
        exporter = ConsoleExporter(stream=buf)
        col = OTelCollector(span_exporter=exporter)

        with pytest.raises(ValueError):
            with col.start_span("boom"):
                raise ValueError("kaboom")

        data = json.loads(buf.getvalue().strip())
        assert data["status"] == "error"
        assert "kaboom" in data["error"]

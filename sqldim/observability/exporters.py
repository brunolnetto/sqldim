"""Span and metric exporters — pluggable backends for OTel-compatible telemetry.

All exporters implement the :class:`SpanExporter` and :class:`MetricExporter`
protocols.  The :class:`ConsoleExporter` ships as a zero-dependency default;
:class:`OTLPSpanExporter` requires the ``[otel]`` optional dependency group.
"""
from __future__ import annotations

import json
import sys
from typing import Protocol, runtime_checkable

from sqldim.observability.metrics import MetricSample
from sqldim.observability.span import PipelineSpan


# ---------------------------------------------------------------------------
# Protocols
# ---------------------------------------------------------------------------

@runtime_checkable
class SpanExporter(Protocol):
    """Protocol for exporting :class:`PipelineSpan` instances."""

    def export(self, span: PipelineSpan) -> None: ...


@runtime_checkable
class MetricExporter(Protocol):
    """Protocol for exporting :class:`MetricSample` instances."""

    def export(self, sample: MetricSample) -> None: ...


# ---------------------------------------------------------------------------
# Console exporter (zero dependencies)
# ---------------------------------------------------------------------------

class ConsoleExporter:
    """Writes spans and metrics as JSON lines to *stream* (default ``stderr``).

    This is the zero-dependency fallback — no OTel SDK required.
    Implements both :class:`SpanExporter` and :class:`MetricExporter`
    via type-based dispatch.
    """

    def __init__(self, *, stream=None) -> None:
        self._stream = stream or sys.stderr

    def export(self, item: PipelineSpan | MetricSample) -> None:
        """Dispatch to the correct serializer based on *item* type."""
        if isinstance(item, PipelineSpan):
            self._write(_span_to_dict(item))
        else:
            self._write(_metric_to_dict(item))

    # -- internal --

    def _write(self, obj: dict) -> None:
        try:
            self._stream.write(json.dumps(obj, default=str) + "\n")
            self._stream.flush()
        except Exception:  # noqa: BLE001 — never crash the pipeline
            pass


# ---------------------------------------------------------------------------
# OTLP exporter (optional dependency)
# ---------------------------------------------------------------------------

class OTLPSpanExporter:
    """Exports spans via the OpenTelemetry OTLP gRPC/HTTP exporter.

    Requires the ``[otel]`` optional dependency group.  Raises
    :exc:`ImportError` with a helpful message if the SDK is not installed.
    """

    def __init__(self, endpoint: str = "http://localhost:4317") -> None:
        try:
            from opentelemetry import trace  # noqa: F401
            from opentelemetry.sdk.trace import TracerProvider
            from opentelemetry.sdk.trace.export import (
                BatchSpanProcessor,
                OTLPSpanExporter as _OTLPSpanExporter,
            )
            from opentelemetry.sdk.resources import Resource
        except ImportError:
            raise ImportError(
                "The [otel] optional dependency group is required for OTLP "
                "export.  Install it with:  pip install sqldim[otel]"
            ) from None

        resource = Resource.create({"service.name": "sqldim"})
        provider = TracerProvider(resource=resource)
        provider.add_span_processor(
            BatchSpanProcessor(_OTLPSpanExporter(endpoint=endpoint))
        )
        trace.set_tracer_provider(provider)
        self._trace = trace
        self._tracer = trace.get_tracer("sqldim.pipeline")

    def export(self, span: PipelineSpan) -> None:
        with self._tracer.start_as_current_span(span.name) as otel_span:
            for k, v in span.attributes.items():
                otel_span.set_attribute(k, v)
            if span.status.value == "error":
                otel_span.set_status(self._trace.StatusCode.ERROR, span.error)


class OTLPMetricExporter:
    """Exports metrics via the OpenTelemetry OTLP metric exporter.

    Requires the ``[otel]`` optional dependency group.  Raises
    :exc:`ImportError` with a helpful message if the SDK is not installed.
    """

    def __init__(self, endpoint: str = "http://localhost:4317") -> None:
        try:
            from opentelemetry import metrics  # noqa: F401
            from opentelemetry.sdk.metrics import MeterProvider
            from opentelemetry.sdk.metrics.export import (
                PeriodicExportingMetricReader,
                OTLPMetricExporter as _OTLPMetricExporter,
            )
            from opentelemetry.sdk.resources import Resource
        except ImportError:
            raise ImportError(
                "The [otel] optional dependency group is required for OTLP "
                "metric export.  Install it with:  pip install sqldim[otel]"
            ) from None

        resource = Resource.create({"service.name": "sqldim"})
        reader = PeriodicExportingMetricReader(
            _OTLPMetricExporter(endpoint=endpoint)
        )
        provider = MeterProvider(resource=resource, metric_readers=[reader])
        metrics.set_meter_provider(provider)
        self._meter = metrics.get_meter("sqldim.pipeline")
        self._instruments: dict[str, object] = {}

    def export(self, sample: MetricSample) -> None:
        key = f"{sample.name}:{sample.kind.value}"
        if key not in self._instruments:
            self._instruments[key] = self._create_instrument(sample)
        instrument = self._instruments[key]
        instrument.record(sample.value, attributes=sample.labels)

    def _create_instrument(self, sample: MetricSample) -> object:
        if sample.kind.value == "counter":
            return self._meter.create_counter(sample.name)
        if sample.kind.value == "histogram":
            return self._meter.create_histogram(sample.name)
        # gauge — use up-down counter as closest OTel primitive
        return self._meter.create_up_down_counter(sample.name)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _span_to_dict(span: PipelineSpan) -> dict:
    return {
        "type": "span",
        "name": span.name,
        "status": span.status.value,
        "duration_s": span.duration_s,
        "start_time": span.start_time.isoformat() if span.start_time else None,
        "end_time": span.end_time.isoformat() if span.end_time else None,
        "attributes": span.attributes,
        "error": span.error,
    }


def _metric_to_dict(sample: MetricSample) -> dict:
    return {
        "type": "metric",
        "name": sample.name,
        "value": sample.value,
        "kind": sample.kind.value,
        "labels": sample.labels,
        "timestamp": sample.timestamp.isoformat() if sample.timestamp else None,
    }

"""In-memory OTel-compatible collector with pluggable exporters."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Generator

from sqldim.observability.exporters import SpanExporter, MetricExporter
from sqldim.observability.metrics import MetricSample
from sqldim.observability.span import PipelineSpan, SpanStatus


class OTelCollector:
    """Stores spans and metrics in-memory; zero external dependencies.

    Optionally accepts exporter callables that are invoked every time a span
    or metric is recorded.  Pass ``None`` (default) for pure in-memory
    behaviour suitable for testing.

    Parameters
    ----------
    span_exporter:
        Optional :class:`~sqldim.observability.exporters.SpanExporter`
        called after each span is finalised.
    metric_exporter:
        Optional :class:`~sqldim.observability.exporters.MetricExporter`
        called after each metric is recorded.
    """

    def __init__(
        self,
        *,
        span_exporter: SpanExporter | None = None,
        metric_exporter: MetricExporter | None = None,
    ) -> None:
        self._spans: list[PipelineSpan] = []
        self._metrics: list[MetricSample] = []
        self._span_exporter = span_exporter
        self._metric_exporter = metric_exporter

    @contextmanager
    def start_span(
        self, name: str, **attributes: Any
    ) -> Generator[PipelineSpan, None, None]:
        """Context manager that records a :class:`PipelineSpan`.

        Keyword arguments become span attributes.  On exception the span
        status is set to ``ERROR`` and the exception is re-raised.
        """
        span = PipelineSpan(name=name, attributes=dict(attributes))
        span.start_time = datetime.now(timezone.utc)
        try:
            yield span
        except Exception as exc:  # noqa: BLE001
            span.status = SpanStatus.ERROR
            span.error = str(exc)
            raise
        finally:
            span.end_time = datetime.now(timezone.utc)
            span.duration_s = (span.end_time - span.start_time).total_seconds()
            self._spans.append(span)
            if self._span_exporter is not None:
                self._span_exporter.export(span)

    def record_metric(self, sample: MetricSample) -> None:
        """Append a :class:`MetricSample` to the in-memory store and export."""
        self._metrics.append(sample)
        if self._metric_exporter is not None:
            self._metric_exporter.export(sample)

    def spans(self) -> list[PipelineSpan]:
        """Return all recorded spans in insertion order."""
        return list(self._spans)

    def metrics(self) -> list[MetricSample]:
        """Return all recorded metrics in insertion order."""
        return list(self._metrics)

    def clear(self) -> None:
        """Reset all recorded spans and metrics."""
        self._spans.clear()
        self._metrics.clear()

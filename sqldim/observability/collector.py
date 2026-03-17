"""In-memory OTel-compatible collector — swap for a real SDK in production."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Generator

from sqldim.observability.metrics import MetricSample
from sqldim.observability.span import PipelineSpan, SpanStatus


class OTelCollector:
    """Stores spans and metrics in-memory; zero external dependencies.

    Intended for testing, local development, and environments where a real
    OTel SDK is not available.  Replace with an SDK-backed implementation
    by subclassing or injecting at the call-site.
    """

    def __init__(self) -> None:
        self._spans: list[PipelineSpan] = []
        self._metrics: list[MetricSample] = []

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

    def record_metric(self, sample: MetricSample) -> None:
        """Append a :class:`MetricSample` to the in-memory store."""
        self._metrics.append(sample)

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

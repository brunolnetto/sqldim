"""Ergonomic metric instrument wrappers around :class:`OTelCollector`.

These are thin convenience classes — not a full OTel SDK replacement.
Use them when you want named, reusable metric handles:

    counter = Counter(collector, "rows_loaded", labels={"pipeline": "orders"})
    counter.increment(50)
"""
from __future__ import annotations

from sqldim.observability.collector import OTelCollector
from sqldim.observability.metrics import MetricKind, MetricSample


class Counter:
    """Monotonically increasing counter instrument.

    Parameters
    ----------
    collector:
        Target :class:`OTelCollector`.
    name:
        Metric name.
    labels:
        Static labels attached to every recorded sample.
    """

    def __init__(
        self,
        collector: OTelCollector,
        name: str,
        labels: dict[str, str] | None = None,
    ) -> None:
        self._collector = collector
        self._name = name
        self._labels = labels or {}

    def increment(self, amount: float = 1.0) -> None:
        """Record an increment of *amount* (default ``1.0``)."""
        self._collector.record_metric(
            MetricSample(
                name=self._name,
                value=amount,
                kind=MetricKind.COUNTER,
                labels=self._labels,
            )
        )


class Gauge:
    """Point-in-time gauge instrument.

    Parameters
    ----------
    collector:
        Target :class:`OTelCollector`.
    name:
        Metric name.
    labels:
        Static labels attached to every recorded sample.
    """

    def __init__(
        self,
        collector: OTelCollector,
        name: str,
        labels: dict[str, str] | None = None,
    ) -> None:
        self._collector = collector
        self._name = name
        self._labels = labels or {}

    def set(self, value: float) -> None:
        """Record the current *value* of the gauge."""
        self._collector.record_metric(
            MetricSample(
                name=self._name,
                value=value,
                kind=MetricKind.GAUGE,
                labels=self._labels,
            )
        )


class Histogram:
    """Distribution-tracking histogram instrument.

    Parameters
    ----------
    collector:
        Target :class:`OTelCollector`.
    name:
        Metric name.
    labels:
        Static labels attached to every recorded sample.
    """

    def __init__(
        self,
        collector: OTelCollector,
        name: str,
        labels: dict[str, str] | None = None,
    ) -> None:
        self._collector = collector
        self._name = name
        self._labels = labels or {}

    def record(self, value: float) -> None:
        """Record a single observation *value*."""
        self._collector.record_metric(
            MetricSample(
                name=self._name,
                value=value,
                kind=MetricKind.HISTOGRAM,
                labels=self._labels,
            )
        )

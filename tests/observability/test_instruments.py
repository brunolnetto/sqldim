"""Tests for metric instrument wrappers — Counter, Gauge, Histogram."""

from sqldim.observability import (
    OTelCollector,
    MetricKind,
    Counter,
    Gauge,
    Histogram,
)


# ---------------------------------------------------------------------------
# Counter
# ---------------------------------------------------------------------------


class TestCounter:
    def test_increment_default_amount(self):
        col = OTelCollector()
        c = Counter(col, "rows_loaded")
        c.increment()
        assert len(col.metrics()) == 1
        assert col.metrics()[0].value == 1.0
        assert col.metrics()[0].kind is MetricKind.COUNTER

    def test_increment_custom_amount(self):
        col = OTelCollector()
        c = Counter(col, "rows_loaded")
        c.increment(50)
        assert col.metrics()[0].value == 50.0

    def test_multiple_increments(self):
        col = OTelCollector()
        c = Counter(col, "requests")
        c.increment(10)
        c.increment(5)
        assert len(col.metrics()) == 2
        assert col.metrics()[0].value == 10.0
        assert col.metrics()[1].value == 5.0

    def test_labels_attached(self):
        col = OTelCollector()
        c = Counter(col, "rows", labels={"pipeline": "orders"})
        c.increment()
        assert col.metrics()[0].labels["pipeline"] == "orders"


# ---------------------------------------------------------------------------
# Gauge
# ---------------------------------------------------------------------------


class TestGauge:
    def test_set_value(self):
        col = OTelCollector()
        g = Gauge(col, "active_connections")
        g.set(42.0)
        assert len(col.metrics()) == 1
        assert col.metrics()[0].value == 42.0
        assert col.metrics()[0].kind is MetricKind.GAUGE

    def test_set_overwrites(self):
        col = OTelCollector()
        g = Gauge(col, "temperature")
        g.set(20.0)
        g.set(25.0)
        assert len(col.metrics()) == 2
        assert col.metrics()[0].value == 20.0
        assert col.metrics()[1].value == 25.0

    def test_labels_attached(self):
        col = OTelCollector()
        g = Gauge(col, "cpu", labels={"host": "db-01"})
        g.set(0.75)
        assert col.metrics()[0].labels["host"] == "db-01"


# ---------------------------------------------------------------------------
# Histogram
# ---------------------------------------------------------------------------


class TestHistogram:
    def test_record_value(self):
        col = OTelCollector()
        h = Histogram(col, "latency_ms")
        h.record(12.5)
        assert len(col.metrics()) == 1
        assert col.metrics()[0].value == 12.5
        assert col.metrics()[0].kind is MetricKind.HISTOGRAM

    def test_multiple_observations(self):
        col = OTelCollector()
        h = Histogram(col, "latency_ms")
        h.record(1.0)
        h.record(5.0)
        h.record(10.0)
        assert len(col.metrics()) == 3
        values = [m.value for m in col.metrics()]
        assert values == [1.0, 5.0, 10.0]

    def test_labels_attached(self):
        col = OTelCollector()
        h = Histogram(col, "duration", labels={"op": "scd_merge"})
        h.record(3.14)
        assert col.metrics()[0].labels["op"] == "scd_merge"


# ---------------------------------------------------------------------------
# Cross-instrument
# ---------------------------------------------------------------------------


class TestCrossInstruments:
    def test_different_instruments_same_collector(self):
        col = OTelCollector()
        c = Counter(col, "rows")
        g = Gauge(col, "active")
        h = Histogram(col, "latency")

        c.increment(100)
        g.set(5)
        h.record(2.5)

        assert len(col.metrics()) == 3
        kinds = {m.kind for m in col.metrics()}
        assert kinds == {MetricKind.COUNTER, MetricKind.GAUGE, MetricKind.HISTOGRAM}

    def test_instruments_with_exporter(self):
        import io
        import json
        from sqldim.observability import ConsoleExporter

        buf = io.StringIO()
        exporter = ConsoleExporter(stream=buf)
        col = OTelCollector(metric_exporter=exporter)

        c = Counter(col, "rows")
        c.increment(10)

        lines = buf.getvalue().strip().split("\n")
        data = json.loads(lines[0])
        assert data["type"] == "metric"
        assert data["name"] == "rows"
        assert data["value"] == 10.0

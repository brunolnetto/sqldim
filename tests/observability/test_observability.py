"""Tests for Observability — PipelineSpan, MetricSample, OTelCollector."""
import time
import pytest

from sqldim.observability import (
    PipelineSpan,
    SpanStatus,
    MetricSample,
    MetricKind,
    OTelCollector,
)


# ---------------------------------------------------------------------------
# SpanStatus
# ---------------------------------------------------------------------------

class TestSpanStatus:
    def test_ok_and_error_exist(self):
        assert SpanStatus.OK
        assert SpanStatus.ERROR

    def test_distinct_values(self):
        assert SpanStatus.OK != SpanStatus.ERROR


# ---------------------------------------------------------------------------
# PipelineSpan
# ---------------------------------------------------------------------------

class TestPipelineSpan:
    def test_has_required_fields(self):
        span = PipelineSpan(name="ingest")
        assert span.name == "ingest"
        assert span.status is SpanStatus.OK
        assert span.duration_s == 0.0
        assert span.attributes == {}
        assert span.error is None
        assert span.start_time is None
        assert span.end_time is None

    def test_set_error_status(self):
        span = PipelineSpan(name="ingest", status=SpanStatus.ERROR, error="timeout")
        assert span.status is SpanStatus.ERROR
        assert span.error == "timeout"

    def test_set_attributes(self):
        span = PipelineSpan(name="x", attributes={"layer": "bronze", "rows": 100})
        assert span.attributes["layer"] == "bronze"
        assert span.attributes["rows"] == 100


# ---------------------------------------------------------------------------
# MetricKind
# ---------------------------------------------------------------------------

class TestMetricKind:
    def test_all_kinds_exist(self):
        assert MetricKind.GAUGE
        assert MetricKind.COUNTER
        assert MetricKind.HISTOGRAM

    def test_distinct(self):
        assert MetricKind.GAUGE != MetricKind.COUNTER
        assert MetricKind.COUNTER != MetricKind.HISTOGRAM


# ---------------------------------------------------------------------------
# MetricSample
# ---------------------------------------------------------------------------

class TestMetricSample:
    def test_basic_fields(self):
        s = MetricSample(name="row_count", value=1000.0, kind=MetricKind.COUNTER)
        assert s.name == "row_count"
        assert s.value == 1000.0
        assert s.kind is MetricKind.COUNTER
        assert s.labels == {}
        assert s.timestamp is not None

    def test_with_labels(self):
        s = MetricSample(
            name="latency_p99",
            value=42.5,
            kind=MetricKind.HISTOGRAM,
            labels={"pipeline": "orders", "layer": "silver"},
        )
        assert s.labels["pipeline"] == "orders"

    def test_timestamp_is_recent(self):
        from datetime import datetime, timezone
        before = datetime.now(timezone.utc)
        s = MetricSample(name="x", value=1.0, kind=MetricKind.GAUGE)
        after = datetime.now(timezone.utc)
        assert before <= s.timestamp <= after


# ---------------------------------------------------------------------------
# OTelCollector — span context manager
# ---------------------------------------------------------------------------

class TestOTelCollectorSpans:
    def test_span_context_manager_records_span(self):
        col = OTelCollector()
        with col.start_span("my_pipeline") as span:
            assert span.name == "my_pipeline"
        assert len(col.spans()) == 1
        assert col.spans()[0].name == "my_pipeline"

    def test_span_status_ok_by_default(self):
        col = OTelCollector()
        with col.start_span("ok_span"):
            pass
        assert col.spans()[0].status is SpanStatus.OK

    def test_span_records_duration(self):
        col = OTelCollector()
        with col.start_span("timed"):
            time.sleep(0.01)
        assert col.spans()[0].duration_s >= 0.01

    def test_span_records_start_and_end_times(self):
        col = OTelCollector()
        with col.start_span("t"):
            pass
        span = col.spans()[0]
        assert span.start_time is not None
        assert span.end_time is not None
        assert span.end_time >= span.start_time

    def test_span_error_on_exception(self):
        col = OTelCollector()
        with pytest.raises(ValueError):
            with col.start_span("bad"):
                raise ValueError("boom")
        assert col.spans()[0].status is SpanStatus.ERROR
        assert "boom" in col.spans()[0].error

    def test_span_attributes_set(self):
        col = OTelCollector()
        with col.start_span("tagged", layer="bronze", rows=500):
            pass
        assert col.spans()[0].attributes["layer"] == "bronze"
        assert col.spans()[0].attributes["rows"] == 500

    def test_multiple_spans_accumulated(self):
        col = OTelCollector()
        with col.start_span("a"):
            pass
        with col.start_span("b"):
            pass
        assert len(col.spans()) == 2
        assert col.spans()[0].name == "a"
        assert col.spans()[1].name == "b"


# ---------------------------------------------------------------------------
# OTelCollector — metrics
# ---------------------------------------------------------------------------

class TestOTelCollectorMetrics:
    def test_record_metric(self):
        col = OTelCollector()
        s = MetricSample(name="rows", value=100.0, kind=MetricKind.COUNTER)
        col.record_metric(s)
        assert len(col.metrics()) == 1
        assert col.metrics()[0].name == "rows"

    def test_multiple_metrics(self):
        col = OTelCollector()
        col.record_metric(MetricSample("a", 1.0, MetricKind.GAUGE))
        col.record_metric(MetricSample("b", 2.0, MetricKind.COUNTER))
        assert len(col.metrics()) == 2

    def test_clear_resets_all(self):
        col = OTelCollector()
        with col.start_span("x"):
            pass
        col.record_metric(MetricSample("m", 1.0, MetricKind.GAUGE))
        col.clear()
        assert col.spans() == []
        assert col.metrics() == []


# ---------------------------------------------------------------------------
# _print_fallback — observability showcase helper
# ---------------------------------------------------------------------------

class TestPrintFallback:
    def test_fetchall_success_prints_rows(self, capsys):
        from sqldim.examples.features.observability.showcase import _print_fallback
        from unittest.mock import MagicMock
        rel = MagicMock()
        rel.fetchall.return_value = [("row1",), ("row2",)]
        _print_fallback(rel, Exception("ignored"))
        out = capsys.readouterr().out
        assert "row1" in out
        assert "row2" in out

    def test_fetchall_raises_prints_query_error(self, capsys):
        from sqldim.examples.features.observability.showcase import _print_fallback
        from unittest.mock import MagicMock
        rel = MagicMock()
        rel.fetchall.side_effect = RuntimeError("db gone")
        _print_fallback(rel, Exception("original error"))
        out = capsys.readouterr().out
        assert "query error" in out
        assert "original error" in out

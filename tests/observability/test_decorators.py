"""Tests for @traced and @metered decorators."""
import pytest

from sqldim.observability import (
    OTelCollector,
    SpanStatus,
    MetricKind,
    traced,
    metered,
)


# ---------------------------------------------------------------------------
# @traced — sync functions
# ---------------------------------------------------------------------------

class TestTracedSync:
    def test_records_span_on_success(self):
        col = OTelCollector()

        @traced(col)
        def add(a, b):
            return a + b

        result = add(2, 3)
        assert result == 5
        assert len(col.spans()) == 1
        assert "add" in col.spans()[0].name
        assert col.spans()[0].status is SpanStatus.OK

    def test_records_span_on_exception(self):
        col = OTelCollector()

        @traced(col)
        def fail():
            raise RuntimeError("oops")

        with pytest.raises(RuntimeError, match="oops"):
            fail()

        assert len(col.spans()) == 1
        assert col.spans()[0].status is SpanStatus.ERROR
        assert "oops" in col.spans()[0].error

    def test_custom_span_name(self):
        col = OTelCollector()

        @traced(col, name="custom.pipeline")
        def foo():
            pass

        foo()
        assert col.spans()[0].name == "custom.pipeline"

    def test_preserves_function_name(self):
        col = OTelCollector()

        @traced(col)
        def my_loader():
            pass

        assert my_loader.__name__ == "my_loader"

    def test_span_records_duration(self):
        import time
        col = OTelCollector()

        @traced(col)
        def slow():
            time.sleep(0.01)

        slow()
        assert col.spans()[0].duration_s >= 0.01


# ---------------------------------------------------------------------------
# @traced — async functions
# ---------------------------------------------------------------------------

class TestTracedAsync:
    @pytest.mark.asyncio
    async def test_records_span_on_success(self):
        col = OTelCollector()

        @traced(col)
        async def async_add(a, b):
            return a + b

        result = await async_add(2, 3)
        assert result == 5
        assert len(col.spans()) == 1
        assert col.spans()[0].status is SpanStatus.OK

    @pytest.mark.asyncio
    async def test_records_span_on_exception(self):
        col = OTelCollector()

        @traced(col)
        async def async_fail():
            raise RuntimeError("async oops")

        with pytest.raises(RuntimeError, match="async oops"):
            await async_fail()

        assert len(col.spans()) == 1
        assert col.spans()[0].status is SpanStatus.ERROR


# ---------------------------------------------------------------------------
# @metered — sync functions
# ---------------------------------------------------------------------------

class TestMeteredSync:
    def test_records_metric_after_call(self):
        col = OTelCollector()

        @metered(col, kind=MetricKind.COUNTER)
        def process():
            pass

        process()
        assert len(col.metrics()) == 1
        assert "process" in col.metrics()[0].name
        assert col.metrics()[0].value == 1.0
        assert col.metrics()[0].kind is MetricKind.COUNTER

    def test_custom_metric_name(self):
        col = OTelCollector()

        @metered(col, name="rows_loaded", kind=MetricKind.COUNTER)
        def load():
            pass

        load()
        assert col.metrics()[0].name == "rows_loaded"

    def test_static_labels(self):
        col = OTelCollector()

        @metered(col, kind=MetricKind.GAUGE, labels={"pipeline": "orders"})
        def run():
            pass

        run()
        assert col.metrics()[0].labels["pipeline"] == "orders"

    def test_custom_value(self):
        col = OTelCollector()

        @metered(col, kind=MetricKind.HISTOGRAM, value=42.0)
        def run():
            pass

        run()
        assert col.metrics()[0].value == 42.0

    def test_callable_value_records_return_value(self):
        col = OTelCollector()

        @metered(col, kind=MetricKind.COUNTER, value=lambda r: r)
        def count_rows():
            return 150

        count_rows()
        assert col.metrics()[0].value == 150.0

    def test_preserves_function_name(self):
        col = OTelCollector()

        @metered(col)
        def my_processor():
            pass

        assert my_processor.__name__ == "my_processor"

    def test_metric_not_recorded_on_exception(self):
        col = OTelCollector()

        @metered(col, kind=MetricKind.COUNTER)
        def fail():
            raise RuntimeError("boom")

        with pytest.raises(RuntimeError):
            fail()

        assert len(col.metrics()) == 0


# ---------------------------------------------------------------------------
# @metered — async functions
# ---------------------------------------------------------------------------

class TestMeteredAsync:
    @pytest.mark.asyncio
    async def test_records_metric_after_call(self):
        col = OTelCollector()

        @metered(col, kind=MetricKind.COUNTER)
        async def async_process():
            pass

        await async_process()
        assert len(col.metrics()) == 1
        assert col.metrics()[0].kind is MetricKind.COUNTER

    @pytest.mark.asyncio
    async def test_callable_value_with_async(self):
        col = OTelCollector()

        @metered(col, kind=MetricKind.HISTOGRAM, value=lambda r: r)
        async def async_count():
            return 99

        await async_count()
        assert col.metrics()[0].value == 99.0


# ---------------------------------------------------------------------------
# Combined @traced + @metered
# ---------------------------------------------------------------------------

class TestCombinedDecorators:
    def test_traced_and_metered_together(self):
        col = OTelCollector()

        @traced(col)
        @metered(col, name="pipeline.runs", kind=MetricKind.COUNTER)
        def pipeline():
            return "done"

        result = pipeline()
        assert result == "done"
        assert len(col.spans()) == 1
        assert len(col.metrics()) == 1
        assert col.metrics()[0].name == "pipeline.runs"

    def test_traced_and_metered_with_exporter(self):
        import io
        import json
        from sqldim.observability import ConsoleExporter

        buf = io.StringIO()
        exporter = ConsoleExporter(stream=buf)
        col = OTelCollector(
            span_exporter=exporter,
            metric_exporter=exporter,
        )

        @traced(col, name="full.pipeline")
        @metered(col, name="full.pipeline.count", kind=MetricKind.COUNTER)
        def run():
            pass

        run()

        lines = buf.getvalue().strip().split("\n")
        types = [json.loads(l)["type"] for l in lines]
        # Order: metric recorded first (inner decorator), then span (outer)
        assert "metric" in types
        assert "span" in types

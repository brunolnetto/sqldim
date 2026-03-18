"""sqldim.observability — in-memory OTel-compatible pipeline instrumentation."""
from sqldim.observability.span import PipelineSpan, SpanStatus
from sqldim.observability.metrics import MetricSample, MetricKind
from sqldim.observability.collector import OTelCollector
from sqldim.observability.exporters import (
    SpanExporter,
    MetricExporter,
    ConsoleExporter,
    OTLPSpanExporter,
    OTLPMetricExporter,
)
from sqldim.observability.decorators import traced, metered
from sqldim.observability.instruments import Counter, Gauge, Histogram

__all__ = [
    "PipelineSpan",
    "SpanStatus",
    "MetricSample",
    "MetricKind",
    "OTelCollector",
    "SpanExporter",
    "MetricExporter",
    "ConsoleExporter",
    "OTLPSpanExporter",
    "OTLPMetricExporter",
    "traced",
    "metered",
    "Counter",
    "Gauge",
    "Histogram",
]

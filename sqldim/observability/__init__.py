"""sqldim.observability — in-memory OTel-compatible pipeline instrumentation."""
from sqldim.observability.span import PipelineSpan, SpanStatus
from sqldim.observability.metrics import MetricSample, MetricKind
from sqldim.observability.collector import OTelCollector

__all__ = [
    "PipelineSpan",
    "SpanStatus",
    "MetricSample",
    "MetricKind",
    "OTelCollector",
]

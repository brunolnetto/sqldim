"""
observability domain Dataset — real execution telemetry from OTelCollector.

Unlike the other 12 domains that generate synthetic OLTP data,
this dataset runs every other domain pipeline with instrumentation
and materialises the captured telemetry (spans + metrics) as tables.

    import duckdb
    from sqldim.application.datasets.domains.observability.dataset import observability_dataset

    con = duckdb.connect()
    observability_dataset.setup(con)      # runs ALL domain pipelines
    con.sql("SELECT * FROM pipeline_spans").show()
"""

from sqldim.application.datasets.dataset import Dataset
from sqldim.application.datasets.domains.observability.sources import (
    MetricSampleSource,
    PipelineSpanSource,
)

observability_dataset = Dataset(
    "observability",
    [
        (PipelineSpanSource(), "pipeline_spans"),
        (MetricSampleSource(), "metric_samples"),
    ],
)

DATASET_METADATA = {
    "name": "observability",
    "title": "Pipeline Observability",
    "description": "Real execution spans and metrics captured by running all domain pipelines with OTelCollector instrumentation.",
    "dataset_attr": "observability_dataset",
}

__all__ = ["observability_dataset", "DATASET_METADATA"]

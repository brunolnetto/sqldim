"""
telemetry domain Dataset — devices, page events, pipeline spans, and metrics.

Instantiate and use with a DuckDB connection:

    import duckdb
    from sqldim.application.datasets.domains.telemetry.dataset import telemetry_dataset

    con = duckdb.connect()
    telemetry_dataset.setup(con)

    for table, rows in telemetry_dataset.snapshots().items():
        print(f"{table}: {len(rows)} rows")

    telemetry_dataset.teardown(con)
"""

from sqldim.application.datasets.dataset import Dataset
from sqldim.application.datasets.domains.telemetry.sources import (
    DevicesSource,
    EventsSource,
    PipelineSpanSource,
    MetricSampleSource,
)

telemetry_dataset = Dataset(
    "telemetry",
    [
        (DevicesSource(), "devices"),
        (EventsSource(), "page_events"),
        (PipelineSpanSource(), "pipeline_spans"),
        (MetricSampleSource(), "metric_samples"),
    ],
)

DATASET_METADATA = {
    "name": "telemetry",
    "title": "Telemetry (devices, events & pipeline metrics)",
    "description": "User devices, page events, pipeline execution spans, and OTel metric samples",
    "dataset_attr": "telemetry_dataset",
}

__all__ = ["telemetry_dataset", "DATASET_METADATA"]

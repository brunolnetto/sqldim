"""The DGM model the telemetry dataset declares (§2.6).

Written from ``sources/``: ``devices`` is a dimension; each ``page_events``
row is one page view that names its device (``device_id``) and carries no
measure, so it is a factless fact.  Its ``user_id`` names no table of the
dataset and is a property.  ``pipeline_spans`` and ``metric_samples`` are
facts with a measure each (``duration_s``, ``value``) and no foreign key.
"""

from __future__ import annotations

from sqldim.application.datasets.model_decl import (
    ASSESSED_KINDS,
    INTEGER,
    REAL,
    STRING,
    TIMESTAMP,
    table_node,
    verb_fk,
)
from sqldim.core.query.dgm.annotations import FactlessFact
from sqldim.core.query.dgm.model import DGMModel, NodeKind

__all__ = ["build_model"]


def build_model() -> DGMModel:
    return DGMModel(
        node_types=(
            table_node(
                "Device",
                NodeKind.DIM,
                "devices",
                ("device_id",),
                {"browser_type": STRING, "os_type": STRING, "device_type": STRING},
            ),
            table_node(
                "PageEvent",
                NodeKind.FACT,
                "page_events",
                ("event_id",),
                {
                    "url": STRING,
                    "referrer": STRING,
                    "user_id": INTEGER,
                    "host": STRING,
                    "event_time": TIMESTAMP,
                },
                event_time="event_time",
            ),
            table_node(
                "PipelineSpan",
                NodeKind.FACT,
                "pipeline_spans",
                ("span_id",),
                {
                    "name": STRING,
                    "status": STRING,
                    "duration_s": REAL,
                    "start_time": STRING,  # ISO timestamps stored as VARCHAR
                    "end_time": STRING,
                    "error": STRING,
                    "pipeline": STRING,
                    "domain": STRING,
                },
            ),
            table_node(
                "MetricSample",
                NodeKind.FACT,
                "metric_samples",
                ("sample_id",),
                {
                    "name": STRING,
                    "value": REAL,
                    "kind": STRING,
                    "label_key": STRING,
                    "label_value": STRING,
                    "timestamp": STRING,
                    "domain": STRING,
                },
            ),
        ),
        edge_types=(verb_fk("Device", "PageEvent", "generated", "device_id"),),
        sigma=(FactlessFact("PageEvent"),),
        sigma_kinds=ASSESSED_KINDS,
    )

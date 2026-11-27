"""The DGM model the telemetry dataset declares (§2.6).

Written from ``sources/``: ``devices`` is a dimension; each ``page_events``
row is one page view that names its device (``device_id``) and carries no
measure, so it is a factless fact.  Its ``user_id`` names no table of the
dataset and is a property.  ``pipeline_spans`` and ``metric_samples`` are
facts with a measure each (``duration_s``, ``value``) and no foreign key.

Grain (§2.4): a page view, a span and a metric sample are each one row per
event.  No dimension holds verb edge types into two fact types, so nothing is
Conformed; there is no bridge edge type.

Additivity (§2.4): ``duration_s`` is ADDITIVE; ``value`` mixes gauges,
counter readings and histogram samples of different metrics, NON_ADDITIVE;
``user_id`` identifies a user with no node type, Degenerate.
"""

from __future__ import annotations

from sqldim.application.datasets.model_decl import (
    ASSESSED_KINDS,
    measures,
    INTEGER,
    REAL,
    STRING,
    TIMESTAMP,
    table_node,
    verb_fk,
)
from sqldim.core.query.dgm.annotations import FactlessFact, Grain, GrainKind
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
        sigma=(
            FactlessFact("PageEvent"),
            Grain("PageEvent", GrainKind.EVENT),
            Grain("PipelineSpan", GrainKind.EVENT),
            Grain("MetricSample", GrainKind.EVENT),
            *measures("PageEvent", degenerate=("user_id",)),
            *measures("PipelineSpan", additive=("duration_s",)),
            *measures("MetricSample", non_additive=("value",)),
        ),
        sigma_kinds=ASSESSED_KINDS,
    )

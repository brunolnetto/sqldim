"""Lineage event model — inspired by OpenLineage RunEvent but zero-dependency."""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class RunState(str, Enum):
    """Lifecycle states for a pipeline run, mirroring OpenLineage ``RunState``."""

    START = "START"
    RUNNING = "RUNNING"
    COMPLETE = "COMPLETE"
    FAIL = "FAIL"
    ABORT = "ABORT"


@dataclass
class DatasetRef:
    """Reference to a dataset (table, file, topic) participating in a lineage event.

    Parameters
    ----------
    namespace:
        Logical namespace (e.g. ``"sqldim.bronze"``).
    name:
        Dataset identifier (e.g. ``"raw_orders"``).
    facets:
        Optional extra metadata (schema, stats, etc.).
    """

    namespace: str
    name: str
    facets: dict[str, Any] = field(default_factory=dict)


@dataclass
class LineageEvent:
    """A single lineage event describing a pipeline run's relationship to datasets.

    Designed to be serialisable to JSON and compatible with OpenLineage's
    ``RunEvent`` shape when converted by :class:`OpenLineageEmitter`.

    Parameters
    ----------
    run_id:
        Unique identifier for the run this event belongs to.
    job_name:
        Human-readable job identifier (e.g. ``"load.dim_customer"``).
    namespace:
        Top-level namespace for the job (e.g. ``"sqldim"``).
    state:
        Current :class:`RunState` of the run.
    inputs:
        Datasets consumed by this job.
    outputs:
        Datasets produced by this job.
    event_time:
        When this event was created (defaults to now).
    facets:
        Optional run-level metadata.
    """

    run_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    job_name: str = ""
    namespace: str = "sqldim"
    state: RunState = RunState.START
    inputs: list[DatasetRef] = field(default_factory=list)
    outputs: list[DatasetRef] = field(default_factory=list)
    event_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    facets: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialise to a plain dict suitable for JSON encoding."""
        return {
            "eventType": self.state.value,
            "eventTime": self.event_time.isoformat(),
            "run": {
                "runId": self.run_id,
                "facets": self.facets,
            },
            "job": {
                "namespace": self.namespace,
                "name": self.job_name,
            },
            "inputs": [
                {"namespace": d.namespace, "name": d.name, "facets": d.facets}
                for d in self.inputs
            ],
            "outputs": [
                {"namespace": d.namespace, "name": d.name, "facets": d.facets}
                for d in self.outputs
            ],
        }

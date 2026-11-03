"""Telemetry domain — drift event façade.

Re-exports source-specific OLTP mutation functions from their
respective source modules so that ``artifacts/drift.json`` references
resolve unchanged.
"""

from sqldim.application.datasets.domains.telemetry.events.devices import (
    apply_mobile_migration,
    apply_os_migration,
)
from sqldim.application.datasets.domains.telemetry.events.pipeline_spans import (
    apply_error_spike,
)
from sqldim.application.datasets.domains.telemetry.events.metric_samples import (
    apply_latency_degradation,
)

__all__ = [
    "apply_mobile_migration",
    "apply_os_migration",
    "apply_error_spike",
    "apply_latency_degradation",
]

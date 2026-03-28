"""Metric-samples OLTP drift events for the observability domain.

With real telemetry data the synthetic events are no longer
needed.  The function is retained as a no-op so that existing
references (e.g. from ``drift.json``) do not break.
"""

from __future__ import annotations

import duckdb


def apply_latency_degradation(
    con: duckdb.DuckDBPyConnection,  # noqa: ARG001
    *,
    factor: float = 3.0,  # noqa: ARG001
) -> None:
    """No-op — real telemetry already captures actual metric values."""


__all__ = ["apply_latency_degradation"]

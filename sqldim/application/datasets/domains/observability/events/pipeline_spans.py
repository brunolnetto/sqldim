"""Pipeline-spans OLTP drift events for the observability domain.

With real telemetry data the synthetic events are no longer
needed.  The function is retained as a no-op so that existing
references (e.g. from ``drift.json``) do not break.
"""

from __future__ import annotations

import duckdb


def apply_error_spike(con: duckdb.DuckDBPyConnection) -> None:  # noqa: ARG001
    """No-op — real telemetry already reflects actual pipeline errors."""


__all__ = ["apply_error_spike"]


__all__ = ["apply_error_spike"]

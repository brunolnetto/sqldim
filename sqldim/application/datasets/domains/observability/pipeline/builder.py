"""Observability pipeline builder — real execution telemetry.

:func:`build_pipeline` runs every other domain pipeline under an
:class:`~sqldim.observability.OTelCollector` and materialises the
captured spans and metrics as OLTP tables in the supplied DuckDB
connection.

Usage
-----
    import duckdb
    from sqldim.application.datasets.domains.observability.pipeline.builder import build_pipeline

    con = duckdb.connect(":memory:")
    build_pipeline(con)
    # tables now available: ``pipeline_spans``, ``metric_samples``
"""

from __future__ import annotations

import duckdb

#: OLTP tables created by :func:`build_pipeline`.
TABLE_NAMES: list[str] = ["pipeline_spans", "metric_samples"]


def build_pipeline(con: duckdb.DuckDBPyConnection, *, seed: int = 42) -> None:
    """Populate the **observability** OLTP tables in *con*.

    Runs the 12 other domain pipelines with instrumentation, then
    writes captured :class:`PipelineSpan` / :class:`MetricSample`
    rows into ``pipeline_spans`` and ``metric_samples``.
    """
    # Ensure the collector uses the requested seed.
    from sqldim.application.datasets.domains.observability.sources._collector import (
        _reset_cache,
    )

    _reset_cache()

    from sqldim.application.datasets.domains.observability.dataset import (
        observability_dataset,
    )

    observability_dataset.setup(con)


__all__ = ["build_pipeline", "TABLE_NAMES"]

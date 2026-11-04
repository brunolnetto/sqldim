"""Flat OLTP pipeline builder for the telemetry domain.

There is no medallion layer — :func:`build_pipeline` populates the OLTP source
tables directly in the supplied DuckDB connection.

Usage
-----
    import duckdb
    from sqldim.application.datasets.domains.telemetry.pipeline.builder import build_pipeline

    con = duckdb.connect(":memory:")
    build_pipeline(con)
    # tables now available: ``devices``, ``page_events``, ``pipeline_spans``, ``metric_samples``
"""

from __future__ import annotations

import duckdb

#: OLTP tables created by :func:`build_pipeline`.
TABLE_NAMES: list[str] = ["devices", "page_events", "pipeline_spans", "metric_samples"]


def build_pipeline(con: duckdb.DuckDBPyConnection, *, seed: int = 42) -> None:
    """Populate the **telemetry** OLTP tables in *con*.

    Runs the other domain pipelines with instrumentation, then writes
    captured :class:`PipelineSpan` / :class:`MetricSample` rows alongside
    the synthetic ``devices`` and ``page_events`` tables.
    """
    from sqldim.application.datasets.domains.telemetry.sources._collector import (
        _reset_cache,
    )

    _reset_cache()

    from sqldim.application.datasets.domains.telemetry.dataset import (
        telemetry_dataset,
    )

    telemetry_dataset.setup(con)


__all__ = ["build_pipeline", "TABLE_NAMES"]

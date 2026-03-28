"""Flat OLTP pipeline builder for the user_activity domain.

There is no medallion layer — :func:`build_pipeline` populates the OLTP source
tables directly in the supplied DuckDB connection.

Usage
-----
    import duckdb
    from sqldim.application.datasets.domains.user_activity.pipeline.builder import build_pipeline

    con = duckdb.connect(":memory:")
    build_pipeline(con)
    # tables now available: ``devices``, ``page_events``
"""

from __future__ import annotations

import duckdb

#: OLTP tables created by :func:`build_pipeline`.
TABLE_NAMES: list[str] = ['devices', 'page_events']


def build_pipeline(con: duckdb.DuckDBPyConnection, *, seed: int = 42) -> None:
    """Populate the **user_activity** OLTP tables in *con*.

    Tables created: ``devices``, ``page_events``.
    """
    from sqldim.application.datasets.domains.user_activity.dataset import user_activity_dataset

    user_activity_dataset.setup(con)


__all__ = ["build_pipeline", "TABLE_NAMES"]

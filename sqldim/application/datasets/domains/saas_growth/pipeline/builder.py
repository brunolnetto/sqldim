"""Flat OLTP pipeline builder for the saas_growth domain.

There is no medallion layer — :func:`build_pipeline` populates the OLTP source
tables directly in the supplied DuckDB connection.

Usage
-----
    import duckdb
    from sqldim.application.datasets.domains.saas_growth.pipeline.builder import build_pipeline

    con = duckdb.connect(":memory:")
    build_pipeline(con)
    # tables now available: ``saas_users``, ``saas_sessions``
"""

from __future__ import annotations

import duckdb

#: OLTP tables created by :func:`build_pipeline`.
TABLE_NAMES: list[str] = ['saas_users', 'saas_sessions']


def build_pipeline(con: duckdb.DuckDBPyConnection, *, seed: int = 42) -> None:
    """Populate the **saas_growth** OLTP tables in *con*.

    Tables created: ``saas_users``, ``saas_sessions``.
    """
    from sqldim.application.datasets.domains.saas_growth.dataset import saas_growth_dataset

    saas_growth_dataset.setup(con)


__all__ = ["build_pipeline", "TABLE_NAMES"]

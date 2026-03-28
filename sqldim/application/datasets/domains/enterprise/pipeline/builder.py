"""Flat OLTP pipeline builder for the enterprise domain.

There is no medallion layer — :func:`build_pipeline` populates the OLTP source
tables directly in the supplied DuckDB connection.

Usage
-----
    import duckdb
    from sqldim.application.datasets.domains.enterprise.pipeline.builder import build_pipeline

    con = duckdb.connect(":memory:")
    build_pipeline(con)
    # tables now available: ``employees``, ``accounts``
"""

from __future__ import annotations

import duckdb

#: OLTP tables created by :func:`build_pipeline`.
TABLE_NAMES: list[str] = ['employees', 'accounts']


def build_pipeline(con: duckdb.DuckDBPyConnection, *, seed: int = 42) -> None:
    """Populate the **enterprise** OLTP tables in *con*.

    Tables created: ``employees``, ``accounts``.
    """
    from sqldim.application.datasets.domains.enterprise.dataset import enterprise_dataset

    enterprise_dataset.setup(con)


__all__ = ["build_pipeline", "TABLE_NAMES"]

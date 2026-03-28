"""Flat OLTP pipeline builder for the hierarchy domain.

There is no medallion layer — :func:`build_pipeline` populates the OLTP source
tables directly in the supplied DuckDB connection.

Usage
-----
    import duckdb
    from sqldim.application.datasets.domains.hierarchy.pipeline.builder import build_pipeline

    con = duckdb.connect(":memory:")
    build_pipeline(con)
    # tables now available: ``org_dim``, ``sales_fact``, ``org_dim_closure``
"""

from __future__ import annotations

import duckdb

#: OLTP tables created by :func:`build_pipeline`.
TABLE_NAMES: list[str] = ['org_dim', 'sales_fact', 'org_dim_closure']


def build_pipeline(con: duckdb.DuckDBPyConnection, *, seed: int = 42) -> None:
    """Populate the **hierarchy** OLTP tables in *con*.

    Tables created: ``org_dim``, ``sales_fact``, ``org_dim_closure``.
    """
    from sqldim.application.datasets.domains.hierarchy.dataset import hierarchy_dataset

    hierarchy_dataset.setup(con)


__all__ = ["build_pipeline", "TABLE_NAMES"]

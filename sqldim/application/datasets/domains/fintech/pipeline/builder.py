"""Flat OLTP pipeline builder for the fintech domain.

There is no medallion layer — :func:`build_pipeline` populates the OLTP source
tables directly in the supplied DuckDB connection.

Usage
-----
    import duckdb
    from sqldim.application.datasets.domains.fintech.pipeline.builder import build_pipeline

    con = duckdb.connect(":memory:")
    build_pipeline(con)
    # tables now available: ``accounts``, ``counterparties``, ``transactions``
"""

from __future__ import annotations

import duckdb

#: OLTP tables created by :func:`build_pipeline`.
TABLE_NAMES: list[str] = ['accounts', 'counterparties', 'transactions']


def build_pipeline(con: duckdb.DuckDBPyConnection, *, seed: int = 42) -> None:
    """Populate the **fintech** OLTP tables in *con*.

    Tables created: ``accounts``, ``counterparties``, ``transactions``.
    """
    from sqldim.application.datasets.domains.fintech.dataset import fintech_dataset

    fintech_dataset.setup(con)


__all__ = ["build_pipeline", "TABLE_NAMES"]

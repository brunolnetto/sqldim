"""Flat OLTP pipeline builder for the ecommerce domain.

There is no medallion layer — :func:`build_pipeline` populates the OLTP source
tables directly in the supplied DuckDB connection.

Usage
-----
    import duckdb
    from sqldim.application.datasets.domains.ecommerce.pipeline.builder import build_pipeline

    con = duckdb.connect(":memory:")
    build_pipeline(con)
    # tables now available: ``customers``, ``products``, ``stores``, ``orders``
"""

from __future__ import annotations

import duckdb

#: OLTP tables created by :func:`build_pipeline`.
TABLE_NAMES: list[str] = ['customers', 'products', 'stores', 'orders']


def build_pipeline(con: duckdb.DuckDBPyConnection, *, seed: int = 42) -> None:
    """Populate the **ecommerce** OLTP tables in *con*.

    Tables created: ``customers``, ``products``, ``stores``, ``orders``.
    """
    from sqldim.application.datasets.domains.ecommerce.dataset import ecommerce_dataset

    ecommerce_dataset.setup(con)


__all__ = ["build_pipeline", "TABLE_NAMES"]

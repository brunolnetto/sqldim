"""Flat OLTP pipeline builder for the supply_chain domain.

There is no medallion layer — :func:`build_pipeline` populates the OLTP source
tables directly in the supplied DuckDB connection.

Usage
-----
    import duckdb
    from sqldim.application.datasets.domains.supply_chain.pipeline.builder import build_pipeline

    con = duckdb.connect(":memory:")
    build_pipeline(con)
    # tables now available: ``suppliers``, ``warehouses``, ``skus``, ``receipts``
"""

from __future__ import annotations

import duckdb

#: OLTP tables created by :func:`build_pipeline`.
TABLE_NAMES: list[str] = ['suppliers', 'warehouses', 'skus', 'receipts']


def build_pipeline(con: duckdb.DuckDBPyConnection, *, seed: int = 42) -> None:
    """Populate the **supply_chain** OLTP tables in *con*.

    Tables created: ``suppliers``, ``warehouses``, ``skus``, ``receipts``.
    """
    from sqldim.application.datasets.domains.supply_chain.dataset import supply_chain_dataset

    supply_chain_dataset.setup(con)


__all__ = ["build_pipeline", "TABLE_NAMES"]

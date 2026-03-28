"""Flat OLTP pipeline builder for the dgm domain.

There is no medallion layer — :func:`build_pipeline` populates the OLTP source
tables directly in the supplied DuckDB connection.

Usage
-----
    import duckdb
    from sqldim.application.datasets.domains.dgm.pipeline.builder import build_pipeline

    con = duckdb.connect(":memory:")
    build_pipeline(con)
    # tables now available: ``dgm_showcase_customer``, ``dgm_showcase_product``, ``dgm_showcase_segment``, ``dgm_showcase_sale``, ``dgm_showcase_prod_seg``
"""

from __future__ import annotations

import duckdb

#: OLTP tables created by :func:`build_pipeline`.
TABLE_NAMES: list[str] = ['dgm_showcase_customer', 'dgm_showcase_product', 'dgm_showcase_segment', 'dgm_showcase_sale', 'dgm_showcase_prod_seg']


def build_pipeline(con: duckdb.DuckDBPyConnection, *, seed: int = 42) -> None:
    """Populate the **dgm** OLTP tables in *con*.

    Tables created: ``dgm_showcase_customer``, ``dgm_showcase_product``, ``dgm_showcase_segment``, ``dgm_showcase_sale``, ``dgm_showcase_prod_seg``.
    """
    from sqldim.application.datasets.domains.dgm.dataset import dgm_dataset

    dgm_dataset.setup(con)


__all__ = ["build_pipeline", "TABLE_NAMES"]

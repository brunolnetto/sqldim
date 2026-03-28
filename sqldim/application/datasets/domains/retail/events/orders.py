"""Order-source drift events for the retail medallion domain."""

from __future__ import annotations

import duckdb

from sqldim.application.datasets.domains.retail.pipeline.builder import (
    rebuild_from_bronze,
)


def apply_price_increase(
    con: duckdb.DuckDBPyConnection,
    *,
    factor: float = 1.20,
) -> None:
    """Raise all product prices by *factor* (default +20 %).

    **OLTP mutation** — updates ``raw_orders.unit_price`` and
    ``raw_products.price`` (the source-of-truth tables), then triggers a full
    silver + gold rebuild so the NL agent's gold tables reflect the repriced
    data.
    """
    con.execute(f"UPDATE raw_orders   SET unit_price = ROUND(unit_price * {factor}, 2)")
    con.execute(f"UPDATE raw_products SET price      = ROUND(price      * {factor}, 2)")
    rebuild_from_bronze(con)


__all__ = ["apply_price_increase"]

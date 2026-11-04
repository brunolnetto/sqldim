"""Product-source OLTP drift events for the commerce domain."""

from __future__ import annotations

import duckdb

from sqldim.application.datasets.domains.commerce.pipeline.builder import (
    rebuild_from_bronze,
)


def apply_price_increase(
    con: duckdb.DuckDBPyConnection,
    *,
    factor: float = 1.20,
) -> None:
    """Raise all product ``price`` values by *factor* (default +20 %).

    **OLTP mutation** — updates ``raw_products.price`` in place, then
    rebuilds silver/gold layers so the gold tables reflect the new prices.
    """
    con.execute(f"UPDATE raw_products SET price = ROUND(price * {factor}, 2)")
    rebuild_from_bronze(con)


def apply_stock_out(
    con: duckdb.DuckDBPyConnection,
    *,
    category: str = "gadgets",
) -> None:
    """Discount all products in *category* to a clearance price of 1.00.

    **OLTP mutation** — updates ``raw_products.price = 1.00`` for the affected
    category, then rebuilds silver/gold so the change is visible in gold tables.
    """
    con.execute(
        "UPDATE raw_products SET price = 1.00 WHERE category = ?",
        [category],
    )
    rebuild_from_bronze(con)


__all__ = ["apply_price_increase", "apply_stock_out"]

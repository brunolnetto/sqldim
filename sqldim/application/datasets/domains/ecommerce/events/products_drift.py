"""Product-source OLTP drift events for the ecommerce domain."""

from __future__ import annotations

import duckdb


def apply_price_increase(
    con: duckdb.DuckDBPyConnection,
    *,
    factor: float = 1.20,
) -> None:
    """Raise all product ``unit_price`` values by *factor* (default +20 %).

    **OLTP mutation** — updates ``products.unit_price`` in place.  Orders
    already placed are unaffected (their ``unit_price_at_order`` was snapshotted
    at placement time), which is the realistic ecommerce behaviour.
    """
    con.execute(
        f"UPDATE products SET unit_price = ROUND(unit_price * {factor}, 2)"
    )


def apply_stock_out(
    con: duckdb.DuckDBPyConnection,
    *,
    category: str = "Electronics",
) -> None:
    """Zero out ``stock_units`` for all products in *category*.

    **OLTP mutation** — updates ``products.stock_units = 0`` for the affected
    category.  Visible to any NL query that reads stock availability.
    """
    con.execute(
        "UPDATE products SET stock_units = 0 WHERE category = ?",
        [category],
    )


__all__ = ["apply_price_increase", "apply_stock_out"]

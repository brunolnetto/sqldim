"""Warehouse-source drift events for the supply_chain domain."""

from __future__ import annotations

import duckdb


def apply_warehouse_closure(
    con: duckdb.DuckDBPyConnection,
    *,
    region: str = "EMEA",
) -> None:
    """Deactivate all warehouses in *region*.

    **OLTP mutation** — sets ``warehouses.is_active = FALSE`` for every
    warehouse whose ``region`` matches.  Active-warehouse count queries will
    reflect the reduction immediately.
    """
    con.execute(
        "UPDATE warehouses SET is_active = FALSE WHERE region = ?",
        [region],
    )


__all__ = ["apply_warehouse_closure"]

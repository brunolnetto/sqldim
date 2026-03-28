"""Customer-source OLTP drift events for the ecommerce domain."""

from __future__ import annotations

import duckdb


def apply_loyalty_upgrade(con: duckdb.DuckDBPyConnection) -> None:
    """Bulk-upgrade bronze→silver and silver→gold loyalty tiers.

    **OLTP mutation** — updates ``customers.loyalty_tier`` for all customers
    currently on bronze or silver.  The aggregate distribution of tiers visible
    to the NL agent changes immediately.
    """
    con.execute("""
        UPDATE customers
        SET loyalty_tier = CASE
            WHEN loyalty_tier = 'bronze' THEN 'silver'
            WHEN loyalty_tier = 'silver' THEN 'gold'
            ELSE loyalty_tier
        END
        WHERE loyalty_tier IN ('bronze', 'silver')
    """)


__all__ = ["apply_loyalty_upgrade"]

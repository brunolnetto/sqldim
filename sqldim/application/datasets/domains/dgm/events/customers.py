"""Customer-source OLTP drift events for the dgm domain."""

from __future__ import annotations

import duckdb


def apply_segment_upgrade(con: duckdb.DuckDBPyConnection) -> None:
    """Upgrade all customers from region 'west' to a different segment.

    **OLTP mutation** — representative change to the customer segment column.
    """
    con.execute("""
        UPDATE dgm_showcase_customer
        SET segment = 'premium'
        WHERE region = 'west'
    """)


__all__ = ["apply_segment_upgrade"]

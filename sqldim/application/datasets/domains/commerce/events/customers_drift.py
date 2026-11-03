"""Customer-source OLTP drift events for the commerce domain."""

from __future__ import annotations

import duckdb

from sqldim.application.datasets.domains.commerce.pipeline.builder import (
    rebuild_from_bronze,
)


def apply_loyalty_upgrade(con: duckdb.DuckDBPyConnection) -> None:
    """Bulk-upgrade trial→standard and standard→premium customer segments.

    **OLTP mutation** — updates ``raw_customers.segment`` for all customers
    currently on trial or standard, then rebuilds silver/gold layers so the
    aggregate distribution of segments visible to the NL agent changes.
    """
    con.execute("""
        UPDATE raw_customers
        SET segment = CASE
            WHEN segment = 'trial'    THEN 'standard'
            WHEN segment = 'standard' THEN 'premium'
            ELSE segment
        END
        WHERE segment IN ('trial', 'standard')
    """)
    rebuild_from_bronze(con)


__all__ = ["apply_loyalty_upgrade"]

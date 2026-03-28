"""Customer-source drift events for the retail medallion domain."""

from __future__ import annotations

import duckdb

from sqldim.application.datasets.domains.retail.pipeline.builder import (
    rebuild_from_bronze,
)


def apply_segment_upgrade(con: duckdb.DuckDBPyConnection) -> None:
    """Bulk-upgrade all trial→standard and standard→premium customers.

    **OLTP mutation** — updates ``raw_customers.segment`` in place (a single
    SQL CASE expression so each row is upgraded exactly once based on its
    current value).  After the update, :func:`rebuild_from_bronze` recreates
    the full silver + gold pipeline, including the SCD2 ``silver_dim_customer``
    history, from the mutated source data.
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


__all__ = ["apply_segment_upgrade"]

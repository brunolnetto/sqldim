"""Sales-fact-source OLTP drift events for the hierarchy domain."""

from __future__ import annotations

import duckdb


def apply_revenue_surge(
    con: duckdb.DuckDBPyConnection,
    *,
    factor: float = 1.20,
) -> None:
    """Increase all sales_fact revenue by *factor* (default +20 %).

    **OLTP mutation** — updates ``sales_fact.revenue`` in place for all rows.
    Total and per-employee revenue queries will reflect the increase immediately.
    """
    con.execute(
        "UPDATE sales_fact SET revenue = ROUND(revenue * ?, 2)",
        [factor],
    )


__all__ = ["apply_revenue_surge"]

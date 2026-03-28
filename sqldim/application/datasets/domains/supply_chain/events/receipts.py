"""Receipt-source drift events for the supply_chain domain."""

from __future__ import annotations

import duckdb


def apply_cost_shock(
    con: duckdb.DuckDBPyConnection,
    *,
    factor: float = 1.30,
) -> None:
    """Raise all receipt unit costs by *factor* (default +30 %).

    **OLTP mutation** — updates ``receipts.unit_cost_usd`` in place for every
    row.  Aggregate cost queries (total spend, avg cost per category) will
    reflect the increase immediately.
    """
    con.execute(
        "UPDATE receipts SET unit_cost_usd = ROUND(unit_cost_usd * ?, 2)",
        [factor],
    )


__all__ = ["apply_cost_shock"]

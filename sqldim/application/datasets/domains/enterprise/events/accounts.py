"""Account-source drift events for the enterprise domain."""

from __future__ import annotations

import duckdb


def apply_balance_adjustment(
    con: duckdb.DuckDBPyConnection,
    *,
    factor: float = 1.05,
) -> None:
    """Apply an interest credit to all accounts (default +5 %).

    **OLTP mutation** — updates ``accounts.balance`` in place for every row.
    Total and average balance queries will reflect the increase immediately.
    """
    con.execute(
        "UPDATE accounts SET balance = ROUND(balance * ?, 2)",
        [factor],
    )


__all__ = ["apply_balance_adjustment"]

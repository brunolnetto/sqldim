"""Transaction-source drift events for the fintech domain."""

from __future__ import annotations

import duckdb


def apply_rate_surge(
    con: duckdb.DuckDBPyConnection,
    *,
    factor: float = 1.15,
) -> None:
    """Raise all payment transaction amounts by *factor* (default +15 %).

    **OLTP mutation** — updates ``transactions.amount_usd`` in place for every
    row where ``txn_type = 'payment'``.  The aggregate average and total
    payment volume visible to the NL agent changes immediately.
    """
    con.execute(
        "UPDATE transactions"
        " SET amount_usd = ROUND(amount_usd * ?, 2)"
        " WHERE txn_type = 'payment'",
        [factor],
    )


__all__ = ["apply_rate_surge"]

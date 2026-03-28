"""Account-source drift events for the fintech domain."""

from __future__ import annotations

import duckdb


def apply_risk_downgrade(con: duckdb.DuckDBPyConnection) -> None:
    """Bulk-downgrade medium-risk accounts to high-risk.

    **OLTP mutation** — updates ``accounts.risk_tier = 'high'`` for every
    account currently rated ``'medium'``.  The risk-tier distribution visible
    to NL queries shifts toward high-risk immediately.
    """
    con.execute(
        "UPDATE accounts SET risk_tier = 'high' WHERE risk_tier = 'medium'"
    )


__all__ = ["apply_risk_downgrade"]

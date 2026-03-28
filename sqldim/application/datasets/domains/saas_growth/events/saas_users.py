"""SaaS-user-source drift events for the saas_growth domain."""

from __future__ import annotations

import duckdb


def apply_free_tier_churn(con: duckdb.DuckDBPyConnection) -> None:
    """Mark all free-tier users as churned.

    **OLTP mutation** — updates ``saas_users.plan_tier = 'churned'`` for every
    user currently on the ``'free'`` tier.  The aggregate tier distribution
    visible to the NL agent changes immediately.
    """
    con.execute(
        "UPDATE saas_users SET plan_tier = 'churned' WHERE plan_tier = 'free'"
    )


def apply_plan_upgrade(con: duckdb.DuckDBPyConnection) -> None:
    """Bulk-upgrade all pro-tier users to enterprise.

    **OLTP mutation** — updates ``saas_users.plan_tier = 'enterprise'`` for
    every user currently on the ``'pro'`` tier.  The aggregate tier counts
    visible to NL queries change immediately.
    """
    con.execute(
        "UPDATE saas_users SET plan_tier = 'enterprise' WHERE plan_tier = 'pro'"
    )


__all__ = ["apply_free_tier_churn", "apply_plan_upgrade"]

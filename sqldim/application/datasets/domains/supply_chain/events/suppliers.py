"""Supplier-source drift events for the supply_chain domain."""

from __future__ import annotations

import duckdb


def apply_supplier_disruption(
    con: duckdb.DuckDBPyConnection,
    *,
    country_code: str = "CN",
) -> None:
    """Halve the reliability score of all suppliers from *country_code*.

    **OLTP mutation** — updates ``suppliers.reliability_score`` in place.
    The average supplier reliability visible to NL queries will drop
    immediately for the affected country.
    """
    con.execute(
        "UPDATE suppliers"
        " SET reliability_score = ROUND(reliability_score * 0.4, 3)"
        " WHERE country_code = ?",
        [country_code],
    )


__all__ = ["apply_supplier_disruption"]

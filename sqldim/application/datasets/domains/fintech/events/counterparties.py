"""Counterparty-source drift events for the fintech domain."""

from __future__ import annotations

import duckdb


def apply_sanctions_wave(con: duckdb.DuckDBPyConnection) -> None:
    """Flag all counterparties with high-risk country codes as sanctioned.

    **OLTP mutation** — sets ``counterparties.is_sanctioned = TRUE`` for every
    counterparty whose ``country_code`` is in a hard-coded high-risk list.
    Before the event, some may already be sanctioned; afterwards the total
    sanctioned count rises (or stays the same if all were already flagged).
    """
    con.execute("""
        UPDATE counterparties
        SET is_sanctioned = TRUE
        WHERE country_code IN ('RU', 'IR', 'KP', 'SY', 'CU')
    """)


__all__ = ["apply_sanctions_wave"]

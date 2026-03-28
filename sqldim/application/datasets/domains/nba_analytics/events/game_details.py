"""Game-details-source drift events for the nba_analytics domain."""

from __future__ import annotations

import duckdb


def apply_three_point_era(con: duckdb.DuckDBPyConnection) -> None:
    """Boost three-point attempt rates for all game_details rows.

    **OLTP mutation** — increases ``fg3a`` (three-point attempts) by 3,
    adjusts ``fg3m`` proportionally (×1.25), and adds the extra 3-point
    makes to ``pts`` (3 pts each) for every player-game row.  Average
    three-point statistics and total points visible to NL queries will
    rise immediately, simulating a league-wide shift toward three-point
    shooting.
    """
    con.execute("""
        UPDATE game_details
        SET fg3a = fg3a + 3,
            pts  = pts + 3 * (CAST(ROUND(fg3m * 1.25) AS INTEGER) - fg3m),
            fg3m = CAST(ROUND(fg3m * 1.25) AS INTEGER)
    """)


__all__ = ["apply_three_point_era"]

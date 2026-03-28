"""Games-source drift events for the nba_analytics domain."""

from __future__ import annotations

import duckdb


def apply_home_court_bump(
    con: duckdb.DuckDBPyConnection,
    *,
    bonus: int = 5,
) -> None:
    """Add *bonus* points to every home team's final score.

    **OLTP mutation** — increments ``games.pts_home`` by *bonus* for all rows
    and sets ``home_team_wins = 1`` for any game where the home team now leads.
    Home-win percentage and average home score will increase.
    """
    con.execute("UPDATE games SET pts_home = pts_home + ?", [bonus])
    con.execute("""
        UPDATE games
        SET home_team_wins = CASE
            WHEN pts_home > pts_away THEN 1
            ELSE 0
        END
    """)


__all__ = ["apply_home_court_bump"]

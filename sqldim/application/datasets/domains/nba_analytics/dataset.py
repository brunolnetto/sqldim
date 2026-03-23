"""
nba_analytics domain Dataset — FK-ordered collection of all OLTP sources.

Instantiate and use with a DuckDB connection:

    import duckdb
    from sqldim.application.datasets.domains.nba_analytics.dataset import nba_analytics_dataset

    con = duckdb.connect()
    nba_analytics_dataset.setup(con)

    for table, rows in nba_analytics_dataset.snapshots().items():
        print(f"{table}: {len(rows)} rows")

    nba_analytics_dataset.teardown(con)
"""
from sqldim.application.datasets.dataset import Dataset
from sqldim.application.datasets.domains.nba_analytics.sources import (
    PlayerSeasonsSource,
    TeamsSource,
    GamesSource,
    GameDetailsSource,
)   

_games_src = GamesSource(n=200, seed=42)

nba_analytics_dataset = Dataset(
    "nba_analytics",
    [
        # FK dependency order: teams first, then player-seasons (independent),
        # then games (FK → teams), then game_details (FK → games + teams)
        (TeamsSource(), "teams"),
        (PlayerSeasonsSource(n=50, seed=42), "player_seasons"),
        (_games_src, "games"),
        (GameDetailsSource(_games_src, seed=42), "game_details"),
    ],
)

__all__ = ["nba_analytics_dataset"]

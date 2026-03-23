"""sources sub-package for the nba_analytics domain — exposes all source classes."""
from sqldim.application.datasets.domains.nba_analytics.sources.player_seasons import PlayerSeasonsSource
from sqldim.application.datasets.domains.nba_analytics.sources.teams import TeamsSource
from sqldim.application.datasets.domains.nba_analytics.sources.games import GamesSource
from sqldim.application.datasets.domains.nba_analytics.sources.game_details import GameDetailsSource

__all__ = [
    "PlayerSeasonsSource",
    "TeamsSource",
    "GamesSource",
    "GameDetailsSource",
]

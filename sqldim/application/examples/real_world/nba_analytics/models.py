"""
NBA Analytics — sqldim Phase 6 & Bootcamp examples.

Demonstrates dual-paradigm: the same Player/Team/Game models power both
dimensional (GROUP BY scoring_class) and graph (neighbors via PlaysAgainstEdge)
queries through a single library.
"""

from __future__ import annotations

from datetime import date
from enum import Enum
from typing import List, Optional
from pydantic import BaseModel
from sqlmodel import Field, Column, JSON

from sqldim import DimensionModel, FactModel, SCD2Mixin
from sqldim.core.graph import VertexModel, EdgeModel
from sqldim.core.kimball.mixins import CumulativeMixin


# ---------------------------------------------------------------------------
# Composite Types & Enums
# ---------------------------------------------------------------------------


class SeasonStats(BaseModel):
    """Composite type for seasonal stats history."""

    season: int
    pts: float
    ast: float
    reb: float
    weight: int


class ScoringClass(str, Enum):
    bad = "bad"
    average = "average"
    good = "good"
    star = "star"


# ---------------------------------------------------------------------------
# Vertex & Dimension models (Dual-Paradigm)
# ---------------------------------------------------------------------------


class Player(VertexModel, CumulativeMixin, table=True):
    """
    Cumulative player table with seasonal history array.
    Reproduces players.sql and projects as a graph Vertex.
    """

    __natural_key__ = ["player_name"]
    __scd_type__ = 1
    __vertex_type__ = "player"
    __vertex_properties__ = ["player_name", "scoring_class", "is_active"]

    id: Optional[int] = Field(default=None, primary_key=True)
    player_name: str
    current_season: int
    scoring_class: ScoringClass = ScoringClass.bad
    is_active: bool = True

    # history stored as JSON array of structs
    seasons: List[SeasonStats] = Field(default_factory=list, sa_column=Column(JSON))


class Team(VertexModel, table=True):
    __natural_key__ = ["abbreviation"]
    __scd_type__ = 1
    __vertex_type__ = "team"

    id: Optional[int] = Field(default=None, primary_key=True)
    team_id: int
    abbreviation: str
    city: str


# ---------------------------------------------------------------------------
# Fact & Edge models
# ---------------------------------------------------------------------------


class Game(FactModel, VertexModel, table=True):
    """Periodic snapshot fact for games. Reproduces games.sql."""

    __grain__ = "one row per game"
    __vertex_type__ = "game"
    __natural_key__ = ["game_id"]

    id: Optional[int] = Field(default=None, primary_key=True)
    game_id: int
    game_date: date
    home_team_id: int
    visitor_team_id: int
    pts_home: float = 0.0
    pts_visitor: float = 0.0


class PlaysInEdge(EdgeModel, table=True):
    """Directed edge projected from GameDetail. Reproduces player_game_edges.sql."""

    __tablename__ = "plays_in_edge"
    __edge_type__ = "plays_in"
    __subject__ = Player
    __object__ = Game
    __directed__ = True
    __grain__ = "player_game"

    id: Optional[int] = Field(default=None, primary_key=True)
    subject_id: int = Field(foreign_key="player.id")
    object_id: int = Field(foreign_key="game.id")

    start_position: Optional[str] = None
    pts: float = 0.0
    team_id: int = 0
    team_abbreviation: str = ""


class PlaysAgainstEdge(EdgeModel, table=True):
    """Undirected self-referential edge. Reproduces player_player_edges.sql."""

    __tablename__ = "plays_against_edge"
    __edge_type__ = "plays_against"
    __subject__ = Player
    __object__ = Player
    __directed__ = False
    __grain__ = "player_pair"

    id: Optional[int] = Field(default=None, primary_key=True)
    subject_id: int = Field(foreign_key="player.id")
    object_id: int = Field(foreign_key="player.id")

    num_games: int = 0
    left_points: float = 0.0
    right_points: float = 0.0


# ---------------------------------------------------------------------------
# SCD Tracking Model
# ---------------------------------------------------------------------------


class PlayerSCD(DimensionModel, SCD2Mixin, table=True):
    """SCD Type 2 version of Player. Reproduces players_scd_table.sql."""

    __natural_key__ = ["player_name"]
    __scd_type__ = 2

    id: Optional[int] = Field(default=None, primary_key=True)
    player_name: str
    scoring_class: ScoringClass
    is_active: bool
    # Override period types to support integer seasons
    valid_from: int = Field(default=0)
    valid_to: Optional[int] = Field(default=None, nullable=True)

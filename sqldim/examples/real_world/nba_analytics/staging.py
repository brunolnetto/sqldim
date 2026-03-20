"""NBA analytics staging table: raw player-season data.

:class:`PlayerSeasons` is the source staging table that mirrors the
``player_seasons.sql`` CTE from the original SQL data-engineering
esercises, used as the starting point for the cumulative array and
SCD-2 pipeline showcases.
"""

from typing import Optional
from sqlmodel import SQLModel, Field


class PlayerSeasons(SQLModel, table=True):
    """
    Raw staging table — source data before dimensional transformation.
    Reproduces player_seasons.sql.
    """

    __tablename__ = "player_seasons"

    player_name: str = Field(primary_key=True)
    season: int = Field(primary_key=True)
    age: Optional[int] = None
    height: Optional[str] = None
    weight: Optional[int] = None
    college: Optional[str] = None
    country: Optional[str] = None
    draft_year: Optional[str] = None
    draft_round: Optional[str] = None
    draft_number: Optional[str] = None
    gp: Optional[float] = None
    pts: Optional[float] = None
    reb: Optional[float] = None
    ast: Optional[float] = None
    netrtg: Optional[float] = None
    oreb_pct: Optional[float] = None
    dreb_pct: Optional[float] = None
    usg_pct: Optional[float] = None
    ts_pct: Optional[float] = None
    ast_pct: Optional[float] = None

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

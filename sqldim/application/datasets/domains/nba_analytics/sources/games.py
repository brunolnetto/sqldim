"""GamesSource — nba_analytics domain source.

Exact schema matches the ``games`` table from the NBA postgres dump:

    game_date_est    DATE
    game_id          BIGINT   PRIMARY KEY
    game_status_text VARCHAR
    home_team_id     BIGINT   FK → teams.team_id
    visitor_team_id  BIGINT   FK → teams.team_id
    season           INTEGER
    team_id_home     BIGINT
    pts_home         DOUBLE
    fg_pct_home      DOUBLE
    ft_pct_home      DOUBLE
    fg3_pct_home     DOUBLE
    ast_home         INTEGER
    reb_home         INTEGER
    team_id_away     BIGINT
    pts_away         DOUBLE
    fg_pct_away      DOUBLE
    ft_pct_away      DOUBLE
    fg3_pct_away     DOUBLE
    ast_away         INTEGER
    reb_away         INTEGER
    home_team_wins   INTEGER  (0 or 1)

FK pool is drawn from :data:`NBA_TEAM_IDS` (teams 1-30) so ``games``
and ``teams`` share consistent surrogate key space.
"""

from __future__ import annotations

import random
from datetime import date, timedelta

import duckdb

from sqldim.application.datasets.base import BaseSource, DatasetFactory, SourceProvider
from sqldim.application.datasets.domains.nba_analytics.sources.teams import (
    NBA_TEAM_IDS,
)

# ── DDL ────────────────────────────────────────────────────────────────────────

_GAMES_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    game_date_est    DATE,
    game_id          BIGINT NOT NULL PRIMARY KEY,
    game_status_text VARCHAR,
    home_team_id     BIGINT,
    visitor_team_id  BIGINT,
    season           INTEGER,
    team_id_home     BIGINT,
    pts_home         DOUBLE,
    fg_pct_home      DOUBLE,
    ft_pct_home      DOUBLE,
    fg3_pct_home     DOUBLE,
    ast_home         INTEGER,
    reb_home         INTEGER,
    team_id_away     BIGINT,
    pts_away         DOUBLE,
    fg_pct_away      DOUBLE,
    ft_pct_away      DOUBLE,
    fg3_pct_away     DOUBLE,
    ast_away         INTEGER,
    reb_away         INTEGER,
    home_team_wins   INTEGER
)
"""

_COLUMNS = (
    "game_date_est, game_id, game_status_text, home_team_id, visitor_team_id, "
    "season, team_id_home, pts_home, fg_pct_home, ft_pct_home, fg3_pct_home, "
    "ast_home, reb_home, team_id_away, pts_away, fg_pct_away, ft_pct_away, "
    "fg3_pct_away, ast_away, reb_away, home_team_wins"
)

_SEASONS = [2020, 2021, 2022]


def _rand_pct(lo: float, hi: float) -> float:
    return round(random.uniform(lo, hi), 3)


def _game_row(
    game_id: int,
    game_date: date,
    home_id: int,
    away_id: int,
    season: int,
) -> str:
    pts_h = round(random.uniform(95, 130), 1)
    pts_a = round(random.uniform(95, 130), 1)
    wins  = 1 if pts_h > pts_a else 0
    row = (
        f"DATE '{game_date}', "
        f"{game_id}, "
        f"'Final', "
        f"{home_id}, {away_id}, {season}, "
        f"{home_id}, "
        f"{pts_h}, {_rand_pct(0.40, 0.58)}, {_rand_pct(0.65, 0.90)}, {_rand_pct(0.30, 0.42)}, "
        f"{random.randint(18, 32)}, {random.randint(38, 56)}, "
        f"{away_id}, "
        f"{pts_a}, {_rand_pct(0.40, 0.58)}, {_rand_pct(0.65, 0.90)}, {_rand_pct(0.30, 0.42)}, "
        f"{random.randint(18, 32)}, {random.randint(38, 56)}, "
        f"{wins}"
    )
    return f"({row})"


# ── Source class ───────────────────────────────────────────────────────────────


@DatasetFactory.register("nba_games")
class GamesSource(BaseSource):
    """
    Synthetic NBA game-results fact source with FK references to TeamsSource.

    Generates ``n`` games across 3 seasons (2020-2022), pairing teams
    randomly from the 30-team pool so that every team plays a realistic
    mix of home and away games.

    game_id values start at 22200001 to mirror real NBA game IDs.

    OLTP schema::

        game_date_est    DATE
        game_id          BIGINT  PK
        game_status_text VARCHAR
        home_team_id     BIGINT  FK → teams.team_id
        visitor_team_id  BIGINT  FK → teams.team_id
        season           INTEGER
        pts_home/away    DOUBLE
        ( … additional box-score aggregates … )
        home_team_wins   INTEGER

    Example
    -------
    ::

        import duckdb
        from sqldim.application.datasets.domains.nba_analytics.sources import (
            TeamsSource, GamesSource,
        )

        con = duckdb.connect()
        TeamsSource().setup(con, "teams")
        src = GamesSource(n=200, seed=7)
        src.setup(con, "games")
        print(con.execute("SELECT season, AVG(pts_home) FROM games GROUP BY 1").fetchall())
    """

    DIM_DDL = _GAMES_DDL

    provider = SourceProvider(
        name="NBA Stats API — Games",
        url="https://www.nba.com/stats",
        description=(
            "Game-level results and efficiency stats for every NBA regular-season game."
        ),
    )

    def __init__(self, n: int = 200, seed: int = 42) -> None:
        random.seed(seed)
        self._rows: list[str] = []
        self._game_ids: list[int] = []
        self._team_pairs: list[tuple[int, int]] = []   # (home_id, away_id) per game

        start_date = date(2020, 10, 20)
        team_ids   = list(NBA_TEAM_IDS)

        for i in range(n):
            home, away = random.sample(team_ids, 2)
            season     = random.choice(_SEASONS)
            game_date  = start_date + timedelta(days=random.randint(0, 730))
            game_id    = 22200001 + i
            self._rows.append(_game_row(game_id, game_date, home, away, season))
            self._game_ids.append(game_id)
            self._team_pairs.append((home, away))

    def snapshot(self):
        from sqldim.sources import SQLSource

        rows_sql = ", ".join(self._rows)
        return SQLSource(f"SELECT * FROM (VALUES {rows_sql}) AS t({_COLUMNS})")

    def setup(self, con: duckdb.DuckDBPyConnection, table: str) -> None:
        con.execute(_GAMES_DDL.format(table=table))
        con.execute(f"INSERT INTO {table} SELECT * FROM ({self.snapshot().as_sql(con)})")

    @property
    def game_ids(self) -> list[int]:
        """Game IDs generated — passed to GameDetailsSource for FK consistency."""
        return list(self._game_ids)

    @property
    def team_pairs(self) -> list[tuple[int, int]]:
        """(home_id, away_id) per game — same order as game_ids."""
        return list(self._team_pairs)

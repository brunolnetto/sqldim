"""GameDetailsSource — nba_analytics domain source.

Exact schema matches the ``game_details`` table from the NBA postgres dump:

    game_id            BIGINT   FK → games.game_id
    team_id            BIGINT   FK → teams.team_id
    team_abbreviation  VARCHAR
    team_city          VARCHAR
    player_id          INTEGER
    player_name        VARCHAR
    nickname           VARCHAR
    start_position     VARCHAR  ('G', 'F', 'C', or '' for bench)
    comment            VARCHAR  (e.g. 'DNP - Coach's Decision')
    min                VARCHAR  (minutes-seconds formatted as 'MM:SS')
    fgm                INTEGER
    fga                INTEGER
    fg_pct             DOUBLE
    fg3m               INTEGER
    fg3a               INTEGER
    fg3_pct            DOUBLE
    ftm                INTEGER
    fta                INTEGER
    ft_pct             DOUBLE
    oreb               INTEGER
    dreb               INTEGER
    reb                INTEGER
    ast                INTEGER
    stl                INTEGER
    blk                INTEGER
    to_                INTEGER  (turnovers — aliased to avoid ``TO`` SQL keyword)
    pf                 INTEGER
    pts                DOUBLE
    plus_minus         DOUBLE

Each game produces 22 player rows: 2 teams × 11 players (5 starters + 6 bench),
of which 2 bench players receive a DNP comment to mirror real-world data.

FK consistency
--------------
Pass the ``GamesSource`` instance to ``GameDetailsSource.__init__`` so that
``game_id`` / ``team_id`` / ``team_abbreviation`` / ``team_city`` values are
drawn from the same ID pool as ``GamesSource``.
"""

from __future__ import annotations

import random

import duckdb
from faker import Faker

from sqldim.application.datasets.base import BaseSource, DatasetFactory, SourceProvider
from sqldim.application.datasets.domains.nba_analytics.sources.teams import (
    NBA_ABBREV,
    NBA_CITY,
    NBA_TEAM_IDS,
)

# ── Position / DNP vocabulary ──────────────────────────────────────────────────

_STARTERS = [("G", ""), ("G", ""), ("F", ""), ("F", ""), ("C", "")]
_DNP_MSGS = [
    "DNP - Coach's Decision",
    "DNP - Injury/Illness",
    "DNP - Rest",
]

# ── DDL ────────────────────────────────────────────────────────────────────────

_GAME_DETAILS_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    game_id           BIGINT,
    team_id           BIGINT,
    team_abbreviation VARCHAR,
    team_city         VARCHAR,
    player_id         INTEGER,
    player_name       VARCHAR,
    nickname          VARCHAR,
    start_position    VARCHAR,
    comment           VARCHAR,
    min               VARCHAR,
    fgm               INTEGER,
    fga               INTEGER,
    fg_pct            DOUBLE,
    fg3m              INTEGER,
    fg3a              INTEGER,
    fg3_pct           DOUBLE,
    ftm               INTEGER,
    fta               INTEGER,
    ft_pct            DOUBLE,
    oreb              INTEGER,
    dreb              INTEGER,
    reb               INTEGER,
    ast               INTEGER,
    stl               INTEGER,
    blk               INTEGER,
    to_               INTEGER,
    pf                INTEGER,
    pts               DOUBLE,
    plus_minus        DOUBLE,
    PRIMARY KEY (game_id, team_id, player_id)
)
"""

_COLUMNS = (
    "game_id, team_id, team_abbreviation, team_city, player_id, player_name, "
    "nickname, start_position, comment, min, fgm, fga, fg_pct, fg3m, fg3a, "
    "fg3_pct, ftm, fta, ft_pct, oreb, dreb, reb, ast, stl, blk, to_, pf, "
    "pts, plus_minus"
)


def _esc(s: str) -> str:
    return s.replace("'", "''")


def _mins(dnp: bool) -> str:
    if dnp:
        return ""
    mins = random.randint(4, 38)
    secs = random.randint(0, 59)
    return f"{mins:02d}:{secs:02d}"


def _player_row(
    game_id: int,
    team_id: int,
    player_id: int,
    player_name: str,
    position: str,
    dnp_comment: str,
    plus_minus_base: float,
) -> str:
    dnp = bool(dnp_comment)
    min_str = _mins(dnp)
    abbrev = NBA_ABBREV.get(team_id, "UNK")
    city = NBA_CITY.get(team_id, "Unknown")

    if dnp:
        # DNP row — all stats NULL / 0
        return (
            f"({game_id}, {team_id}, '{abbrev}', '{_esc(city)}', "
            f"{player_id}, '{_esc(player_name)}', '', "
            f"'{position}', '{_esc(dnp_comment)}', '', "
            f"0, 0, 0.0, 0, 0, 0.0, 0, 0, 0.0, "
            f"0, 0, 0, 0, 0, 0, 0, 0, 0.0, 0.0)"
        )

    fgm = random.randint(0, 12)
    fga = max(fgm, random.randint(fgm, fgm + 8))
    fg3m = random.randint(0, min(fgm, 5))
    fg3a = max(fg3m, random.randint(fg3m, fg3m + 4))
    ftm = random.randint(0, 8)
    fta = max(ftm, random.randint(ftm, ftm + 3))
    oreb = random.randint(0, 4)
    dreb = random.randint(0, 8)

    fg_pct = round(fgm / fga, 3) if fga else 0.0
    fg3_pct = round(fg3m / fg3a, 3) if fg3a else 0.0
    ft_pct = round(ftm / fta, 3) if fta else 0.0
    pts = float(fgm * 2 + fg3m + ftm)
    pm = round(plus_minus_base + random.uniform(-8, 8), 1)

    return (
        f"({game_id}, {team_id}, '{abbrev}', '{_esc(city)}', "
        f"{player_id}, '{_esc(player_name)}', '', "
        f"'{position}', '', '{min_str}', "
        f"{fgm}, {fga}, {fg_pct}, "
        f"{fg3m}, {fg3a}, {fg3_pct}, "
        f"{ftm}, {fta}, {ft_pct}, "
        f"{oreb}, {dreb}, {oreb + dreb}, "
        f"{random.randint(0, 9)}, "
        f"{random.randint(0, 3)}, {random.randint(0, 2)}, "
        f"{random.randint(0, 4)}, {random.randint(0, 5)}, "
        f"{pts}, {pm})"
    )


def _generate_player_rows(
    game_id: int,
    team_id: int,
    players: list,
    n_players_per_team: int,
    pm_base: float,
) -> list[str]:
    n_dnp = 2
    rows: list[str] = []
    for j, (pid, pname) in enumerate(players):
        if j < 5:
            pos, comment = _STARTERS[j]
        elif j < n_players_per_team - n_dnp:
            pos, comment = "", ""
        else:
            pos = ""
            comment = random.choice(_DNP_MSGS)
        rows.append(_player_row(game_id, team_id, pid, pname, pos, comment, pm_base))
    return rows


# ── Source class ───────────────────────────────────────────────────────────────


@DatasetFactory.register("nba_game_details")
class GameDetailsSource(BaseSource):
    """
    Synthetic NBA player box-score source (``game_details``).

    Generates 22 player rows per game — 2 teams × 11 players each
    (5 starters + 4 bench + 2 DNP bench players) — with FK-consistent
    ``game_id``, ``team_id``, ``team_abbreviation``, and ``team_city``
    values drawn from the ``GamesSource`` instance passed at construction.

    Using ``GameDetailsSource`` together with ``GamesSource``::

        from sqldim.application.datasets.domains.nba_analytics.sources import (
            GamesSource, GameDetailsSource,
        )

        games_src   = GamesSource(n=200, seed=42)
        details_src = GameDetailsSource(games_src, seed=42)

    OLTP schema::

        game_id  team_id  team_abbreviation  team_city  player_id  player_name
        start_position  comment  min  fgm fga fg_pct fg3m fg3a fg3_pct
        ftm fta ft_pct  oreb dreb reb  ast stl blk to_ pf  pts  plus_minus

    Example
    -------
    ::

        import duckdb
        from sqldim.application.datasets.domains.nba_analytics.sources import (
            TeamsSource, GamesSource, GameDetailsSource,
        )

        con = duckdb.connect()
        gs  = GamesSource(n=50, seed=0)
        ds  = GameDetailsSource(gs, seed=0)
        TeamsSource().setup(con, "teams")
        gs.setup(con, "games")
        ds.setup(con, "game_details")
        top = con.execute(
            \"\"\"
            SELECT player_name, SUM(pts) AS total_pts
            FROM game_details GROUP BY 1 ORDER BY 2 DESC LIMIT 5
            \"\"\"
        ).fetchall()
        print(top)
    """

    DIM_DDL = _GAME_DETAILS_DDL

    provider = SourceProvider(
        name="NBA Stats API — Box Scores",
        url="https://www.nba.com/stats",
        description=(
            "Player-level in-game statistics for every appearance in every NBA game."
        ),
    )

    def __init__(
        self,
        games_source,  # GamesSource instance
        seed: int = 42,
        n_players_per_team: int = 11,
    ) -> None:
        random.seed(seed)
        fake = Faker()
        Faker.seed(seed)

        self._rows: list[str] = []
        player_id_counter = 1

        # Per-team player pool so the same player appears in multiple games
        team_player_pool: dict[int, list[tuple[int, str]]] = {}
        for team_id in NBA_TEAM_IDS:
            team_player_pool[team_id] = [
                (player_id_counter + j, fake.name()) for j in range(n_players_per_team)
            ]
            player_id_counter += n_players_per_team

        for game_id, (home_id, away_id) in zip(
            games_source.game_ids, games_source.team_pairs
        ):
            # Determine home/away plus-minus direction
            pm_sign = random.choice([1, -1])

            for team_id, pm_base in [
                (home_id, pm_sign * 4.0),
                (away_id, -pm_sign * 4.0),
            ]:
                players = team_player_pool[team_id]
                self._rows.extend(
                    _generate_player_rows(
                        game_id, team_id, players, n_players_per_team, pm_base
                    )
                )

    def snapshot(self):
        from sqldim.sources import SQLSource

        rows_sql = ", ".join(self._rows)
        return SQLSource(f"SELECT * FROM (VALUES {rows_sql}) AS t({_COLUMNS})")

    def setup(self, con: duckdb.DuckDBPyConnection, table: str) -> None:
        con.execute(_GAME_DETAILS_DDL.format(table=table))
        con.execute(
            f"INSERT INTO {table} SELECT * FROM ({self.snapshot().as_sql(con)})"
        )

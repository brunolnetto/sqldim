"""TeamsSource — nba_analytics domain source.

Exact schema matches the ``teams`` table from the NBA postgres dump.
Static fixture data lives in :mod:`._teams_data` to keep this module
focused on database-interaction logic.

Exported constants (re-exported from ``._teams_data`` for backwards
compatibility):
    NBA_TEAM_IDS : list[int]
    NBA_ABBREV   : dict[int, str]
    NBA_CITY     : dict[int, str]
"""

from __future__ import annotations

import duckdb

from sqldim.application.datasets.base import BaseSource, DatasetFactory, SourceProvider
from sqldim.application.datasets.domains.nba_analytics.sources._teams_data import (
    _COACHING_CHANGES,
    _COLUMNS,
    _TEAMS,
    _TEAMS_DDL,
    _esc,  # noqa: F401
    _team_to_row,
    NBA_ABBREV,  # noqa: F401 — re-exported for backwards compat
    NBA_CITY,  # noqa: F401 — re-exported for backwards compat
    NBA_TEAM_IDS,  # noqa: F401 — re-exported for backwards compat
)


# ── Source class ───────────────────────────────────────────────────────────────


@DatasetFactory.register("nba_teams")
class TeamsSource(BaseSource):
    """
    Static NBA team-dimension source — 30 rows matching the ``teams``
    table from the NBA postgres dump.

    The event batch simulates mid-season coaching changes, making this
    source a natural SCD-2 driver for a dim_team table.

    OLTP schema::

        league_id           BIGINT
        team_id             BIGINT   PRIMARY KEY
        min_year / max_year INTEGER
        abbreviation        VARCHAR  (3 chars)
        nickname            VARCHAR
        yearfounded         INTEGER
        city                VARCHAR
        arena               VARCHAR
        arenacapacity       INTEGER
        owner               VARCHAR
        generalmanager      VARCHAR
        headcoach           VARCHAR
        dleagueaffiliation  VARCHAR

    Example
    -------
    ::

        import duckdb
        from sqldim.application.datasets.domains.nba_analytics.sources import TeamsSource

        con = duckdb.connect()
        src = TeamsSource()
        src.setup(con, "teams")
        con.execute("INSERT INTO teams " + src.snapshot().sql)
        print(con.execute("SELECT abbreviation, headcoach FROM teams LIMIT 5").fetchall())
    """

    DIM_DDL = _TEAMS_DDL

    provider = SourceProvider(
        name="NBA Stats API — Teams",
        url="https://www.nba.com/stats",
        description=(
            "Official NBA team registry: 30 franchises with arena, "
            "management and coaching staff data."
        ),
    )

    def snapshot(self):
        from sqldim.sources import SQLSource

        rows = ", ".join(_team_to_row(t) for t in _TEAMS)
        return SQLSource(f"SELECT * FROM (VALUES {rows}) AS t({_COLUMNS})")

    def event_batch(self, n: int = 1):
        """
        Coaching-change batch — returns updated rows for teams that had a
        head-coach change mid-season.  Use this as the ``updated`` batch
        for an SCD-2 dim_team pipeline.
        """
        from sqldim.sources import SQLSource

        rows = []
        for t in _TEAMS:
            team_id = t[0]
            if team_id in _COACHING_CHANGES:
                new_coach = _COACHING_CHANGES[team_id]
                updated = list(t)
                updated[9] = new_coach  # headcoach is index 9
                rows.append(_team_to_row(tuple(updated)))

        sql = f"SELECT * FROM (VALUES {', '.join(rows)}) AS t({_COLUMNS})"
        return SQLSource(sql)

    def setup(self, con: duckdb.DuckDBPyConnection, table: str) -> None:
        con.execute(_TEAMS_DDL.format(table=table))
        con.execute(
            f"INSERT INTO {table} SELECT * FROM ({self.snapshot().as_sql(con)})"
        )

    @property
    def teams(self) -> list[tuple]:
        """Raw team tuples — useful for FK seeding in other sources."""
        return list(_TEAMS)

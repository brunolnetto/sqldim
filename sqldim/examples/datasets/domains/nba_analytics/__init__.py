"""
sqldim/examples/datasets/nba_analytics.py
====================================================
Faker-backed OLTP simulator for the NBA Analytics real-world example.

Provides synthetic player-season data matching the ``PlayerSeasons`` staging
schema (``staging.py``).  Feeds the sqldim cumulative + SCD2 + graph pipeline
demonstrated in ``showcase.py``.

OLTP flow::

    PlayerSeasonsSource                 (this module — synthetic OLTP)
        │  .snapshot()                  full initial extract
        │  .event_batch(n=1)            season stats update batch
        ▼
    sqldim processors                   (LazySCDProcessor / CumulativeLoader)
        ▼
    player_seasons staging table        (DuckDB)
        ▼
    Player (cumulative) + PlayerSCD (SCD2)

Public interface (matches all BaseSource subclasses):
    setup(con, table)     — create the sqldim-managed target DDL
    teardown(con, table)  — drop the target table
    snapshot()            — SQLSource of the full initial season data
    event_batch(n=1)      — SQLSource of next-season stats updates
"""

from __future__ import annotations

import random


from sqldim.examples.datasets.base import (
    DatasetFactory,
    SchematicSource,
    SourceProvider,
)
from sqldim.examples.datasets.schema import (
    ChangeRule,
    DatasetSpec,
    EntitySchema,
    EventSpec,
    FieldSpec,
)

# ── Vocabulary ────────────────────────────────────────────────────────────────

_COLLEGES = [
    "Duke",
    "Kentucky",
    "UNC",
    "Kansas",
    "Michigan",
    "UCLA",
    "Indiana",
    "Gonzaga",
    "Syracuse",
    "Arizona",
    "Louisville",
]
_COUNTRIES = [
    "USA",
    "USA",
    "USA",
    "France",
    "Nigeria",
    "Australia",
    "Canada",
    "Spain",
    "Serbia",
    "Germany",
]
_HEIGHTS = ["6-1", "6-3", "6-5", "6-6", "6-7", "6-9", "6-10", "6-11", "7-0"]

# ── DatasetSpec ───────────────────────────────────────────────────────────────

_PLAYER_SEASONS_SPEC = DatasetSpec(
    name="player_seasons",
    schemas={
        "source": EntitySchema(
            name="player_seasons",
            fields=[
                # Composite PK: (player_name, season)
                FieldSpec("player_name", "VARCHAR", kind="faker", method="name"),
                FieldSpec("season", "INTEGER", kind="randint", low=2000, high=2023),
                FieldSpec("age", "INTEGER", kind="randint", low=19, high=38),
                FieldSpec("height", "VARCHAR", kind="choices", choices=_HEIGHTS),
                FieldSpec("weight", "INTEGER", kind="randint", low=185, high=290),
                FieldSpec("college", "VARCHAR", kind="choices", choices=_COLLEGES),
                FieldSpec("country", "VARCHAR", kind="choices", choices=_COUNTRIES),
                FieldSpec(
                    "draft_year",
                    "VARCHAR",
                    kind="randint",
                    low=1995,
                    high=2020,
                    post=lambda v, f, i: str(v),
                ),
                FieldSpec(
                    "draft_round",
                    "VARCHAR",
                    kind="choices",
                    choices=["1", "1", "1", "2"],
                ),
                FieldSpec(
                    "draft_number",
                    "VARCHAR",
                    kind="randint",
                    low=1,
                    high=60,
                    post=lambda v, f, i: str(v),
                ),
                FieldSpec("gp", "DOUBLE", kind="randint", low=20, high=82),
                FieldSpec(
                    "pts", "DOUBLE", kind="uniform", low=2.0, high=32.0, precision=1
                ),
                FieldSpec(
                    "reb", "DOUBLE", kind="uniform", low=1.0, high=14.0, precision=1
                ),
                FieldSpec(
                    "ast", "DOUBLE", kind="uniform", low=0.5, high=11.0, precision=1
                ),
                FieldSpec(
                    "netrtg",
                    "DOUBLE",
                    kind="uniform",
                    low=-15.0,
                    high=20.0,
                    precision=1,
                ),
                FieldSpec(
                    "oreb_pct",
                    "DOUBLE",
                    kind="uniform",
                    low=0.02,
                    high=0.22,
                    precision=3,
                ),
                FieldSpec(
                    "dreb_pct",
                    "DOUBLE",
                    kind="uniform",
                    low=0.05,
                    high=0.35,
                    precision=3,
                ),
                FieldSpec(
                    "usg_pct",
                    "DOUBLE",
                    kind="uniform",
                    low=0.10,
                    high=0.38,
                    precision=3,
                ),
                FieldSpec(
                    "ts_pct", "DOUBLE", kind="uniform", low=0.40, high=0.75, precision=3
                ),
                FieldSpec(
                    "ast_pct",
                    "DOUBLE",
                    kind="uniform",
                    low=0.05,
                    high=0.55,
                    precision=3,
                ),
            ],
        ),
    },
    events=EventSpec(
        changes=[
            # Next season: advance the season counter and vary game stats
            ChangeRule(
                field="season",
                condition=lambda i, row: True,
                mutate=lambda old, row, fake: old + 1,
            ),
            ChangeRule(
                field="pts",
                condition=lambda i, row: random.random() < 0.8,
                mutate=lambda old, row, fake: round(
                    old * random.uniform(0.85, 1.15), 1
                ),
            ),
            ChangeRule(
                field="reb",
                condition=lambda i, row: random.random() < 0.8,
                mutate=lambda old, row, fake: round(
                    old * random.uniform(0.85, 1.15), 1
                ),
            ),
            ChangeRule(
                field="ast",
                condition=lambda i, row: random.random() < 0.8,
                mutate=lambda old, row, fake: round(
                    old * random.uniform(0.85, 1.15), 1
                ),
            ),
            ChangeRule(
                field="gp",
                condition=lambda i, row: random.random() < 0.5,
                mutate=lambda old, row, fake: min(
                    82, max(20, int(old) + random.randint(-10, 10))
                ),
            ),
        ],
        timestamp_field=None,
    ),
)


# ── Source class ──────────────────────────────────────────────────────────────


@DatasetFactory.register("player_seasons")
class PlayerSeasonsSource(SchematicSource):
    """
    Synthetic NBA player-seasons OLTP source.

    Matches the ``PlayerSeasons`` SQLModel staging schema.  Use with
    ``LazySCDProcessor`` or ``CumulativeLoader`` to build the analytical layer.

    Example
    -------
    ::

        import duckdb
        from sqldim.examples.real_world.nba_analytics.dataset import PlayerSeasonsSource
        from sqldim.core.kimball.dimensions.scd.processors.scd_engine import LazySCDProcessor

        con = duckdb.connect()
        src = PlayerSeasonsSource(n=100, seed=7)
        src.setup(con, "player_seasons")
        proc = LazySCDProcessor("player_name", ["pts", "reb", "ast"], ...)
        proc.process(src.snapshot(), "player_seasons")
    """

    _spec = _PLAYER_SEASONS_SPEC

    provider = SourceProvider(
        name="NBA Stats API",
        url="https://www.nba.com/stats",
        description=(
            "Official NBA player-season performance data. "
            "Real deployment would pull via nba_api or similar."
        ),
    )

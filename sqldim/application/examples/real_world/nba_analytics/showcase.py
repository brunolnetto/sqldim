"""
sqldim Showcase: NBA Analytics — Full Warehouse Pipeline
=========================================================

Demonstrates sqldim's complete analytical toolkit applied to a multi-table
NBA dataset that mirrors the real Postgres schema (40 MB dump → synthetic
in-memory equivalent, no files bundled).

Data flow
---------
Source layer (DuckDB):
    teams (30)           ← TeamsSource
    player_seasons (50)  ← PlayerSeasonsSource
    games (200)          ← GamesSource
    game_details (4 400) ← GameDetailsSource (22 rows per game)

Analytical demonstrations
-------------------------
1. Staging layer     — load all four sources into a single in-memory DuckDB
2. Home court edge   — aggregate ``home_team_wins`` per team with RANK
3. Top scorer        — QUALIFY window function (top-1 scorer per team)
4. Cumulative arrays — LazyCumulativeLoader builds a seasons-history array
5. Co-player graph   — LazyEdgeProjectionLoader projects a player↔player
                       co-occurrence network from game_details
6. Coaching SCD-2    — event batch simulates mid-season coaching changes
                       (printed diff; full SCDHandler demo in ecommerce/)
"""

from __future__ import annotations

import duckdb

from sqldim.application.datasets.domains.nba_analytics.sources import (
    GameDetailsSource,
    GamesSource,
    PlayerSeasonsSource,
    TeamsSource,
)
from sqldim.core.loaders.dimension.edge_projection import LazyEdgeProjectionLoader
from sqldim.core.loaders.fact.cumulative import LazyCumulativeLoader
from sqldim.application.examples.utils import section, banner

# ── in-memory sink (same DuckDB connection — no file path needed) ───────────


class _InMemorySink:
    """Minimal SinkAdapter backed by the shared in-memory DuckDB connection."""

    def current_state_sql(self, table_name: str) -> str:
        return f"SELECT * FROM {table_name}"

    def write(
        self,
        con: "duckdb.DuckDBPyConnection",
        view_name: str,
        table_name: str,
        batch_size: int = 100_000,
    ) -> int:
        n = con.execute(f"SELECT count(*) FROM {view_name}").fetchone()[0]
        try:
            con.execute(f"INSERT INTO {table_name} BY NAME SELECT * FROM {view_name}")
        except Exception:
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {view_name}")
        return n

# ── helpers ────────────────────────────────────────────────────────────────────


def _print_table(headers: list[str], rows: list[tuple], max_rows: int = 10) -> None:
    widths = [
        max(len(h), max((len(str(r[i])) for r in rows[:max_rows]), default=0))
        for i, h in enumerate(headers)
    ]
    sep = "  " + "  ".join("-" * w for w in widths)
    print("  " + "  ".join(h.ljust(w) for h, w in zip(headers, widths)))
    print(sep)
    for row in rows[:max_rows]:
        print("  " + "  ".join(str(v).ljust(w) for v, w in zip(row, widths)))
    if len(rows) > max_rows:  # pragma: no cover
        print(f"  … ({len(rows) - max_rows} more rows)")


# ── Stage 1: staging layer ─────────────────────────────────────────────────────


def demo_staging_layer(con: duckdb.DuckDBPyConnection) -> None:
    """Load all four sources and report row counts."""
    with section("1. Loading the NBA Staging Layer"):
        games_src   = GamesSource(n=200, seed=42)
        details_src = GameDetailsSource(games_src, seed=42)
        ps_src      = PlayerSeasonsSource(n=50, seed=42)

        # TeamsSource.setup() creates AND populates (static dimension)
        TeamsSource().setup(con, "teams")

        # PlayerSeasonsSource uses DIM_DDL (SCD-augmented); for staging use CREATE AS SELECT
        con.execute(f"CREATE TABLE player_seasons AS SELECT * FROM ({ps_src.snapshot().as_sql(con)})")

        # GamesSource / GameDetailsSource also self-populate in setup()
        games_src.setup(con, "games")
        details_src.setup(con, "game_details")

        for tbl in ("teams", "player_seasons", "games", "game_details"):
            n = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            print(f"  {tbl:<16}  {n:>6} rows")


# ── Stage 2: home court advantage ─────────────────────────────────────────────


def demo_home_court_advantage(con: duckdb.DuckDBPyConnection) -> None:
    """
    Team-level home win rates — GROUP BY + JOIN + ORDER BY.
    Shows which teams have the strongest home court edge.
    """
    with section("2. Home Court Advantage (aggregate per team)"):
        rows = con.execute("""
            SELECT
                t.abbreviation                              AS team,
                COUNT(*)                                    AS home_games,
                SUM(g.home_team_wins)                       AS home_wins,
                ROUND(AVG(g.home_team_wins) * 100, 1)       AS win_pct,
                ROUND(AVG(g.pts_home), 1)                   AS avg_pts
            FROM games g
            JOIN teams t ON t.team_id = g.home_team_id
            GROUP BY t.abbreviation
            ORDER BY win_pct DESC
            LIMIT 8
        """).fetchall()
        _print_table(["team", "home_games", "home_wins", "win_%", "avg_pts"], rows)


# ── Stage 3: QUALIFY — top scorer per team ────────────────────────────────────


def demo_qualify_top_scorer(con: duckdb.DuckDBPyConnection) -> None:
    """
    QUALIFY window function — selects the single top-scoring player per
    team across all games, without requiring a CTE or subquery.
    """
    with section("3. QUALIFY — Top Scorer per Team"):
        rows = con.execute("""
            SELECT
                t.abbreviation          AS team,
                gd.player_name          AS top_scorer,
                SUM(gd.pts)             AS total_pts,
                COUNT(DISTINCT gd.game_id) AS games_played,
                ROUND(SUM(gd.pts) / NULLIF(COUNT(DISTINCT gd.game_id), 0), 1) AS ppg
            FROM game_details gd
            JOIN teams t ON t.team_id = gd.team_id
            WHERE gd.start_position <> ''
            GROUP BY t.abbreviation, gd.player_name
            QUALIFY RANK() OVER (
                PARTITION BY t.abbreviation
                ORDER BY SUM(gd.pts) DESC
            ) = 1
            ORDER BY total_pts DESC
            LIMIT 10
        """).fetchall()
        _print_table(["team", "top_scorer", "total_pts", "gp", "ppg"], rows)


# ── Stage 4: cumulative player history ────────────────────────────────────────


def demo_cumulative_arrays(con: duckdb.DuckDBPyConnection) -> None:
    """
    LazyCumulativeLoader — builds a cumulative seasons-history array per
    player.  All merging happens in DuckDB SQL (FULL OUTER JOIN +
    ``list_append`` + ``struct_pack``) — zero Python element-wise loops.
    """
    with section("4. Cumulative Player History (LazyCumulativeLoader)"):
        con.execute("""
            CREATE TABLE IF NOT EXISTS player_seasons_cumulated (
                player_name             VARCHAR,
                seasons                 STRUCT(pts DOUBLE, reb DOUBLE, ast DOUBLE, period VARCHAR)[],
                is_active               BOOLEAN,
                years_since_last_active INTEGER,
                current_season          INTEGER,
                PRIMARY KEY (player_name)
            )
        """)

        sink = _InMemorySink()
        loader = LazyCumulativeLoader(
            table="player_seasons_cumulated",
            partition_key="player_name",
            cumulative_column="seasons",
            metric_columns=["pts", "reb", "ast"],
            sink=sink,
            con=con,
        )
        n = loader.process("player_seasons", target_period=2020)
        print(f"  Season 2020 → {n} player records written")

        sample = con.execute("""
            SELECT
                player_name,
                list_count(seasons)     AS seasons_in_array,
                current_season
            FROM player_seasons_cumulated
            ORDER BY player_name
            LIMIT 6
        """).fetchall()
        _print_table(["player_name", "seasons_in_array", "current_season"], sample)


# ── Stage 5: co-player graph ───────────────────────────────────────────────────


def demo_coplayer_graph(con: duckdb.DuckDBPyConnection) -> None:
    """
    LazyEdgeProjectionLoader — self-join on game_id builds a
    player↔player co-occurrence graph from game_details.
    Two players are linked if they appeared in the same game for the
    same team.
    """
    with section("5. Co-player Graph (LazyEdgeProjectionLoader)"):
        sink = _InMemorySink()
        loader = LazyEdgeProjectionLoader(
            table="graph_coplayer",
            subject_key="player_id",
            object_key="player_id",
            self_join=True,
            self_join_key="game_id",
            sink=sink,
            con=con,
        )
        n_edges = loader.process("game_details")

        print(f"  Projected {n_edges} co-player edges from game_details")

        top = con.execute("""
            SELECT subject_id, COUNT(*) AS degree
            FROM graph_coplayer
            GROUP BY subject_id
            ORDER BY degree DESC
            LIMIT 5
        """).fetchall()
        _print_table(["player_id", "co_player_degree"], top)


# ── Stage 6: coaching changes SCD-2 preview ───────────────────────────────────


def demo_coaching_changes(con: duckdb.DuckDBPyConnection) -> None:
    """
    TeamsSource.event_batch() models mid-season coaching changes.
    Demonstrate the before/after delta — in production this feeds an
    SCDHandler to produce versioned dim_team rows.
    """
    with section("6. Coaching Changes (SCD-2 event batch)"):
        src = TeamsSource()

        original = {row[4]: row[12] for row in con.execute(src.snapshot().as_sql(con)).fetchall()}
        updated  = {row[4]: row[12] for row in con.execute(src.event_batch().as_sql(con)).fetchall()}

        changed = [
            (abbr, original[abbr], updated[abbr])
            for abbr in updated
            if updated[abbr] != original.get(abbr)
        ]
        _print_table(["team", "outgoing_coach", "new_coach"], changed)
        print(f"\n  {len(changed)} coaching change(s) in this event batch.")
        print("  → Feed into SCDHandler(TeamDim, session, track_columns=['headcoach'])")
        print("     to generate versioned SCD-2 rows for dim_team.")


# ── Entry point ────────────────────────────────────────────────────────────────


EXAMPLE_METADATA = {
    "name": "nba",
    "title": "NBA Analytics",
    "description": (
        "Multi-table star schema: teams + seasons + games + box scores. "
        "Showcases cumulative arrays, QUALIFY, graph projection, and SCD-2 coaching changes."
    ),
    "entry_point": "run_showcase",
}


async def run_showcase() -> None:
    """Run the full NBA analytics showcase in a single in-memory DuckDB session."""
    banner(
        "NBA Analytics — Full Warehouse Pipeline",
        "sqldim  ·  cumulative arrays  ·  QUALIFY  ·  graph projection  ·  SCD-2",
    )

    con = duckdb.connect()

    demo_staging_layer(con)
    demo_home_court_advantage(con)
    demo_qualify_top_scorer(con)
    demo_cumulative_arrays(con)
    demo_coplayer_graph(con)
    demo_coaching_changes(con)

    con.close()
    print("\nNBA showcase complete.\n")


if __name__ == "__main__":  # pragma: no cover
    import asyncio
    asyncio.run(run_showcase())

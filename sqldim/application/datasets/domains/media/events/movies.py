"""Movies-source OLTP drift events for the media domain."""

from __future__ import annotations

import duckdb


def apply_new_release(
    con: duckdb.DuckDBPyConnection,
    *,
    movie_id: int = 999,
    title: str = "New Blockbuster",
    released_at: str = "2024-06-01",
) -> None:
    """Insert cast rows for a new film featuring all existing actors.

    **OLTP mutation** — inserts one ``movies`` row per actor currently in the
    ``actors`` table.  Total cast/movie counts visible to NL queries increase.
    """
    con.execute(f"""
        INSERT INTO movies (actor_id, actor_name, movie_id, title, released_at)
        SELECT id, name, {movie_id}, '{title}', DATE '{released_at}'
        FROM actors
    """)


__all__ = ["apply_new_release"]

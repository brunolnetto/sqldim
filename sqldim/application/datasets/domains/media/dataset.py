"""
media domain Dataset — FK-ordered collection of all OLTP sources.

Instantiate and use with a DuckDB connection:

    import duckdb
    from sqldim.application.datasets.domains.media.dataset import media_dataset

    con = duckdb.connect()
    media_dataset.setup(con)

    for table, rows in media_dataset.snapshots().items():
        print(f"{table}: {len(rows)} rows")

    media_dataset.teardown(con)
"""

import duckdb

from sqldim.application.datasets.base import BaseSource
from sqldim.application.datasets.dataset import Dataset
from sqldim.application.datasets.domains.media.sources import (
    MoviesSource,
    _MOVIES_SPEC,
)


# ── Thin wrapper sources that expose MoviesSource data via the standard API ──

_movies_src = MoviesSource(n_actors=8, n_movies=5, seed=42)


class _CastSource(BaseSource):
    """Cast rows (actor_id, actor_name, movie_id, title, released_at)."""

    DIM_DDL = _MOVIES_SPEC.cast.oltp_ddl()

    def snapshot(self):
        return _movies_src.cast_snapshot()


class _ActorsSource(BaseSource):
    """Actors lookup table (id, name)."""

    DIM_DDL = _MOVIES_SPEC.actors.oltp_ddl()

    def snapshot(self):
        from sqldim.sources import SQLSource

        rows = ", ".join(
            f"({a['id']}, '{a['name'].replace(chr(39), chr(39)*2)}')"
            for a in _movies_src.actors
        )
        return SQLSource(f"SELECT * FROM (VALUES {rows}) AS t(id, name)")


media_dataset = Dataset(
    "media",
    [
        (_ActorsSource(), "actors"),
        (_CastSource(), "movies"),
    ],
)

DATASET_METADATA = {
    "name": "media",
    "title": "Film / cast database",
    "description": "Actors, movies, and cast relationships for film analytics",
    "dataset_attr": "media_dataset",
}

__all__ = ["media_dataset", "DATASET_METADATA"]

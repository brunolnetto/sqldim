"""
entertainment domain Dataset — films/cast (movies) + BookClub social reading network.

Instantiate and use with a DuckDB connection:

    import duckdb
    from sqldim.application.datasets.domains.entertainment.dataset import entertainment_dataset

    con = duckdb.connect()
    entertainment_dataset.setup(con)
    # tables: actors, movies,
    #         pessoa, livro, genero, leitura, conexao_social,
    #         livro_genero, pessoa_preferencia
    entertainment_dataset.teardown(con)
"""

from sqldim.application.datasets.base import BaseSource
from sqldim.application.datasets.dataset import Dataset
from sqldim.application.datasets.domains.entertainment.sources import (
    MoviesSource,
    SocialNetworkSource,
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
            f"({a['id']}, '{a['name'].replace(chr(39), chr(39) * 2)}')"
            for a in _movies_src.actors
        )
        return SQLSource(f"SELECT * FROM (VALUES {rows}) AS t(id, name)")


entertainment_dataset = Dataset(
    "entertainment",
    [
        (_ActorsSource(), "actors"),
        (_CastSource(), "movies"),
        (SocialNetworkSource(), "social_network"),
    ],
)

DATASET_METADATA = {
    "name": "entertainment",
    "title": "Entertainment & social reading (films + BookClub)",
    "description": (
        "Combined film cast database and BookClub social reading network. "
        "Film side: actors, movies, and cast relationships. "
        "Book club side: 10 readers, 8 books, 7 genres, social connections, "
        "reading reviews, and recommendation graph."
    ),
    "dataset_attr": "entertainment_dataset",
}

__all__ = ["entertainment_dataset", "DATASET_METADATA"]

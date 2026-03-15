"""
sqldim/examples/datasets/media.py
===================================
Media domain: Movie cast data for graph-projection examples.

``MoviesSource`` simulates a film-production OLTP database where each row
in the ``cast`` table links an actor to a movie.  sqldim's
``LazyEdgeProjectionLoader`` projects this bipartite (actor × movie) table
into an actor–actor co-occurrence graph.

OLTP → Graph pipeline::

    cast (OLTP)  ──►  LazyEdgeProjectionLoader  ──►  graph_coactor (edges)
"""
from __future__ import annotations

import random
from typing import Any

import duckdb
from faker import Faker

from sqldim.examples.datasets.base import BaseSource, DatasetFactory, SourceProvider
from sqldim.examples.datasets.schema import DatasetSpec, EntitySchema, FieldSpec


def _esc(s: str) -> str:
    return s.replace("'", "''")


# ── Schema declarations ───────────────────────────────────────────────────────

_MOVIES_SPEC = DatasetSpec("movies", {
    "cast": EntitySchema(
        name="cast",
        fields=[
            FieldSpec("actor_id",    "INTEGER"),
            FieldSpec("actor_name",  "VARCHAR"),
            FieldSpec("movie_id",    "INTEGER"),
            FieldSpec("title",       "VARCHAR"),
            FieldSpec("released_at", "DATE"),
        ],
    ),
    "edge": EntitySchema(
        name="edge",
        fields=[
            FieldSpec("subject_id", "INTEGER"),
            FieldSpec("object_id",  "INTEGER"),
        ],
    ),
    "actors": EntitySchema(
        name="actors",
        fields=[
            FieldSpec("id",   "INTEGER"),
            FieldSpec("name", "VARCHAR"),
        ],
    ),
})


@DatasetFactory.register("movies")
class MoviesSource(BaseSource):
    """
    OLTP movie-production cast table — feeds ``LazyEdgeProjectionLoader``.

    The cast table is bipartite: actor ↔ movie.  When projected via a
    self-join on ``movie_id``, it becomes an actor–actor co-occurrence graph
    where two actors are linked if they appeared in the same film.

    Event batches model new film releases adding fresh actors to the graph.

    OLTP schema (cast table)::

        actor_id    INTEGER  -- FK to actors
        actor_name  VARCHAR
        movie_id    INTEGER  -- FK to movies
        title       VARCHAR
        released_at DATE

    Graph edge target (EDGE_DDL)::

        subject_id  INTEGER  -- actor A
        object_id   INTEGER  -- actor B (shares a film with A)
    """

    provider = SourceProvider(
        name="Film / media database (TMDb / IMDb datasets)",
        description="Movie cast and crew data for graph-projection examples.",
        url="https://developer.themoviedb.org/docs",
        auth_required=True,
        requires=["tmdbv3api"],
    )

    @property
    def OLTP_DDL(self) -> str:  # noqa: N802
        return _MOVIES_SPEC.cast.oltp_ddl()

    @property
    def EDGE_DDL(self) -> str:  # noqa: N802
        return _MOVIES_SPEC.edge.oltp_ddl()

    @property
    def ACTORS_DDL(self) -> str:  # noqa: N802
        return _MOVIES_SPEC.actors.oltp_ddl()

    def _actor_name(self, actor_id: int) -> str:
        return next(a["name"] for a in self._actors if a["id"] == actor_id)

    def _build_cast(self) -> list[dict[str, Any]]:
        actor_ids = [a["id"] for a in self._actors]
        cast: list[dict[str, Any]] = []
        for movie in self._movies:
            size   = random.randint(2, min(4, len(actor_ids)))
            chosen = random.sample(actor_ids, size)
            for actor_id in chosen:
                cast.append({
                    "actor_id":   actor_id,
                    "actor_name": self._actor_name(actor_id),
                    "movie_id":   movie["id"],
                    "title":      movie["title"],
                    "released_at": f"2023-{random.randint(1,12):02d}-01",
                })
        return cast

    def __init__(self, n_actors: int = 8, n_movies: int = 5, seed: int = 42) -> None:
        fake = Faker()
        Faker.seed(seed)
        random.seed(seed)

        adjectives = ["Dark", "Last", "Final", "Silent", "Lost", "Hidden", "Rising", "Broken"]
        nouns      = ["Horizon", "Signal", "Protocol", "Echo", "Threshold", "Cipher", "Vector", "Meridian"]

        self._actors: list[dict[str, Any]] = [
            {"id": i + 1, "name": fake.name()}
            for i in range(n_actors)
        ]
        self._movies: list[dict[str, Any]] = [
            {"id": i + 1, "title": f"{random.choice(adjectives)} {random.choice(nouns)}"}
            for i in range(n_movies)
        ]

        self._cast = self._build_cast()

        actor_ids       = [a["id"] for a in self._actors if a["id"] <= n_actors]
        new_actor_id    = n_actors + 1
        new_movie_id    = n_movies + 1
        new_actor_name  = fake.name()
        new_movie_title = f"{random.choice(adjectives)} {random.choice(nouns)}"
        self._actors.append({"id": new_actor_id, "name": new_actor_name})
        self._movies.append({"id": new_movie_id, "title": new_movie_title})
        hub_actor = random.choice(actor_ids[:3])
        self._events1: list[dict[str, Any]] = [
            {
                "actor_id":   hub_actor,
                "actor_name": self._actor_name(hub_actor),
                "movie_id":   new_movie_id,
                "title":      new_movie_title,
                "released_at": "2024-06-15",
            },
            {
                "actor_id":   new_actor_id,
                "actor_name": new_actor_name,
                "movie_id":   new_movie_id,
                "title":      new_movie_title,
                "released_at": "2024-06-15",
            },
        ]

    # ── Target lifecycle ──────────────────────────────────────────────────

    def setup(
        self,
        con: duckdb.DuckDBPyConnection,
        edge_table: str = "graph_coactor",
        actors_table: str = "actors",
    ) -> None:
        """Create the edge-projection table + actors lookup (both empty)."""
        con.execute(_MOVIES_SPEC.edge.oltp_ddl(edge_table))
        con.execute(_MOVIES_SPEC.actors.oltp_ddl(actors_table))
        rows = ", ".join(
            f"({a['id']}, '{_esc(a['name'])}')" for a in self._actors
        )
        con.execute(f"INSERT INTO {actors_table} VALUES {rows}")

    def teardown(
        self,
        con: duckdb.DuckDBPyConnection,
        edge_table: str = "graph_coactor",
        actors_table: str = "actors",
    ) -> None:
        con.execute(f"DROP TABLE IF EXISTS {edge_table}")
        con.execute(f"DROP TABLE IF EXISTS {actors_table}")

    # ── OLTP source factories ─────────────────────────────────────────────

    def snapshot(self):
        """Full cast table snapshot (alias for cast_snapshot())."""
        return self.cast_snapshot()

    def cast_snapshot(self):
        """Full cast table snapshot (all films and actors)."""
        from sqldim.sources import SQLSource
        return SQLSource(self._to_sql(self._cast))

    def new_releases(self, n: int = 1):
        """New film cast rows arriving as an event (expands the graph)."""
        from sqldim.sources import SQLSource
        if n == 1:
            return SQLSource(self._to_sql(self._events1))
        raise ValueError(f"MoviesSource has 1 event batch (requested n={n})")

    def _to_sql(self, rows: list[dict[str, Any]]) -> str:
        return " UNION ALL ".join(
            f"SELECT {r['actor_id']} AS actor_id,"
            f" '{_esc(r['actor_name'])}' AS actor_name,"
            f" {r['movie_id']} AS movie_id,"
            f" '{_esc(r['title'])}' AS title"
            for r in rows
        )

    # ── Inspection ────────────────────────────────────────────────────────

    def actor_map(self) -> dict[int, str]:
        return {a["id"]: a["name"] for a in self._actors}

    @property
    def actors(self) -> list[dict[str, Any]]:
        return list(self._actors)

    @property
    def movies(self) -> list[dict[str, Any]]:
        return list(self._movies)

    @property
    def cast(self) -> list[dict[str, Any]]:
        return list(self._cast)

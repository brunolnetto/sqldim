"""
Graph — Example 13
===================

13. Movie co-actor network — actors sharing films become undirected graph edges.
    N-hop paths and degree centrality via ``DuckDBTraversalEngine``.

OLTP → Graph pipeline:
  MoviesSource.cast_snapshot()  → bipartite (actor × movie) cast table
  LazyEdgeProjectionLoader      → self-join on movie_id → actor–actor edges
  MoviesSource.new_releases(1)  → new film expands the graph (event batch)
  DuckDBTraversalEngine         → neighbor queries, paths, degree centrality

Run:
    PYTHONPATH=. python -m sqldim.application.examples.features.graph.showcase
"""

from __future__ import annotations

import os
import types

import duckdb

from sqldim.core.loaders.dimension.edge_projection import LazyEdgeProjectionLoader
from sqldim.core.graph.traversal import DuckDBTraversalEngine
from sqldim.sinks import DuckDBSink

from sqldim.application.datasets.domains.media import MoviesSource
from sqldim.application.examples.features.utils import make_tmp_db


def _tmp_db() -> str:
    return make_tmp_db()


def _make_edge_model(table: str, directed: bool = False) -> type:
    return types.SimpleNamespace(__tablename__=table, __directed__=directed)  # type: ignore


def _setup_graph(path: str, src: MoviesSource) -> dict:
    """Project cast → co-actor edges; return actor_map and edge_count."""
    setup_con = duckdb.connect(path)
    src.setup(setup_con, edge_table="graph_coactor", actors_table="actors")
    setup_con.close()

    with DuckDBSink(path) as sink:
        loader = LazyEdgeProjectionLoader(
            table="graph_coactor",
            subject_key="actor_id",
            object_key="actor_id",
            self_join=True,
            self_join_key="movie_id",
            sink=sink,
            con=sink._con,
        )
        edge_count = loader.process(src.cast_snapshot())

    con = duckdb.connect()
    con.execute(f"ATTACH '{path}' AS g (READ_ONLY)")
    con.execute("""
        CREATE OR REPLACE TABLE graph_coactor AS
        SELECT subject_id, object_id FROM g.main.graph_coactor
    """)
    actor_map = {
        row[0]: row[1]
        for row in con.execute("SELECT id, name FROM g.main.actors").fetchall()
    }
    con.execute("DETACH g")
    return {"con": con, "actor_map": actor_map, "edge_count": edge_count}


def _print_paths(engine, edge_model, actor_map: dict, ranked: list) -> None:
    if len(ranked) < 2:
        return
    src_id, src_name = ranked[0]
    dst_id, dst_name = ranked[-1]
    hop_paths = engine.paths(edge_model, start_id=src_id, target_id=dst_id, max_hops=4)
    print(f"\n  Paths from {src_name} → {dst_name} (max 4 hops):")
    if hop_paths:  # pragma: no cover
        for hop_path in hop_paths:  # pragma: no cover
            named = " → ".join(
                actor_map.get(i, f"#{i}") for i in hop_path
            )  # pragma: no cover
            print(f"    {named}")  # pragma: no cover
    else:
        print("    (no path within max_hops)")


# ── Example 13 ────────────────────────────────────────────────────────────────


def example_13_movie_coactor_network() -> None:
    """
    Build an undirected actor↔actor co-occurrence graph from film cast data,
    then answer graph queries using recursive CTEs.

    Step A — edge projection (LazyEdgeProjectionLoader):
      cast(actor_id, movie_id) → SELF JOIN on movie_id → (subject_id, object_id)

    Step B — graph traversal (DuckDBTraversalEngine):
      neighbors(), degree(), paths()

    Step C — event expansion:
      A new film release adds fresh cast rows; the graph grows.
    """
    print("\n── Example 13: Movie Co-actor Network ──────────────────────────")

    src = MoviesSource(n_actors=8, n_movies=5, seed=42)
    path = _tmp_db()
    g = _setup_graph(path, src)
    con, actor_map, edge_count = g["con"], g["actor_map"], g["edge_count"]
    print(f"  Initial projection: {edge_count} co-actor edges")

    engine = DuckDBTraversalEngine(con)
    edge_model = _make_edge_model("graph_coactor", directed=False)

    start_id = min(actor_map)
    neighbors = engine.neighbors(edge_model, start_id=start_id)
    print(f"\n  {actor_map[start_id]}'s co-actors (1 hop):")
    for nid in neighbors:
        print(f"    → {actor_map.get(nid, f'actor#{nid}')}")

    print("\n  Degree centrality:")
    for actor_id, name in sorted(actor_map.items()):
        print(f"    {name:<26} degree={engine.degree(edge_model, start_id=actor_id)}")

    new_actor = max(src.actors, key=lambda a: a["id"])
    print(f"\n  New release event: actor '{new_actor['name']}' added to graph")

    ranked = sorted(
        actor_map.items(),
        key=lambda kv: engine.degree(edge_model, start_id=kv[0]),
        reverse=True,
    )
    _print_paths(engine, edge_model, actor_map, ranked)

    src.teardown(con, edge_table="graph_coactor", actors_table="actors")
    con.close()
    os.unlink(path)


# ── Entry point ───────────────────────────────────────────────────────────────


EXAMPLE_METADATA = {
    "name": "graph",
    "title": "Graph Analytics",
    "description": "Example 13: co-actor network with N-hop traversal + degree centrality",
    "entry_point": "run_showcase",
}


def run_showcase() -> None:
    print("Graph Showcase")
    print("==============")
    example_13_movie_coactor_network()
    print("\nDone.\n")


if __name__ == "__main__":  # pragma: no cover
    run_showcase()

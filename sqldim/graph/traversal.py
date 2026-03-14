"""
Recursive CTE traversal engine — SQL generation for graph queries.
Supports PostgreSQL and DuckDB dialects.
"""
from __future__ import annotations

from typing import Any, Literal, TYPE_CHECKING

if TYPE_CHECKING:
    from sqldim.models.graph import EdgeModel


class TraversalEngine:
    """
    Generates SQL strings for graph traversal operations.

    All methods return parameterised SQL using :param_name syntax
    (SQLAlchemy / asyncpg style).  The returned strings are for
    inspection and can be executed via session.execute(text(sql), params).

    Performance note
    ----------------
    Recursive CTEs degrade past ~5 hops on large graphs.
    For analytical use-cases the recommended limit is 1–3 hops.
    """

    def neighbors_sql(
        self,
        edge_model: type["EdgeModel"],
        start_id: int,
        direction: Literal["out", "in", "both"] = "both",
        filters: dict[str, Any] | None = None,
    ) -> str:
        """
        Single-hop neighbor lookup — returns peer vertex IDs.

        For directed edges the WHERE clause uses subject_id/object_id
        according to *direction*.  For undirected edges (``__directed__ =
        False``) direction is always treated as "both".
        """
        table = edge_model.__tablename__  # type: ignore[attr-defined]
        directed: bool = getattr(edge_model, "__directed__", True)

        if not directed:
            direction = "both"

        if direction == "out":
            select_col = "object_id"
            where_clause = f"subject_id = {start_id}"
        elif direction == "in":
            select_col = "subject_id"
            where_clause = f"object_id = {start_id}"
        else:  # both
            # Return all connected vertices; union out + in directions
            parts = [
                f"SELECT object_id AS neighbor_id FROM {table} WHERE subject_id = {start_id}",
                f"SELECT subject_id AS neighbor_id FROM {table} WHERE object_id = {start_id}",
            ]
            if filters:
                filter_sql = _build_filters(filters)
                parts = [p + f" AND {filter_sql}" for p in parts]
            return "\nUNION\n".join(parts)

        filter_sql = f" AND {_build_filters(filters)}" if filters else ""
        return f"SELECT {select_col} AS neighbor_id FROM {table} WHERE {where_clause}{filter_sql}"

    def paths_sql(
        self,
        edge_model: type["EdgeModel"],
        start_id: int,
        target_id: int,
        max_hops: int = 3,
    ) -> str:
        """
        Multi-hop path finding via a ``WITH RECURSIVE`` CTE.

        Cycle prevention: a vertex already in the accumulated path array
        is excluded from further expansion.

        Limitations
        -----------
        - Array syntax (``path || id``) is PostgreSQL / DuckDB compatible.
        - SQLite does not support recursive CTEs with arrays.
        """
        table = edge_model.__tablename__  # type: ignore[attr-defined]
        directed: bool = getattr(edge_model, "__directed__", True)

        if directed:
            follow_join = f"JOIN {table} e ON e.subject_id = t.current_id"
            next_id = "e.object_id"
        else:
            # Undirected: follow both directions
            follow_join = (
                f"JOIN {table} e ON e.subject_id = t.current_id OR e.object_id = t.current_id"
            )
            next_id = f"CASE WHEN e.subject_id = t.current_id THEN e.object_id ELSE e.subject_id END"

        return f"""WITH RECURSIVE traversal(current_id, path, depth) AS (
    -- Base case: start at source vertex
    SELECT {start_id}, ARRAY[{start_id}], 0

    UNION ALL

    -- Recursive case: follow edges one hop at a time
    SELECT {next_id},
           t.path || {next_id},
           t.depth + 1
    FROM traversal t
    {follow_join}
    WHERE t.depth < {max_hops}
      AND NOT ({next_id} = ANY(t.path))  -- cycle prevention
)
SELECT path
FROM traversal
WHERE current_id = {target_id}"""

    def aggregate_sql(
        self,
        edge_model: type["EdgeModel"],
        start_id: int,
        measure: str,
        agg: str,
        direction: Literal["out", "in", "both"] = "both",
    ) -> str:
        """
        Aggregate a numeric measure across all edges incident to a vertex.
        """
        table = edge_model.__tablename__  # type: ignore[attr-defined]
        directed: bool = getattr(edge_model, "__directed__", True)

        agg_upper = agg.upper()
        if not directed:
            direction = "both"

        if direction == "out":
            where_clause = f"subject_id = {start_id}"
        elif direction == "in":
            where_clause = f"object_id = {start_id}"
        else:
            where_clause = f"subject_id = {start_id} OR object_id = {start_id}"

        return f"SELECT {agg_upper}({measure}) AS result FROM {table} WHERE {where_clause}"

    def degree_sql(
        self,
        edge_model: type["EdgeModel"],
        start_id: int,
        direction: Literal["out", "in", "both"] = "both",
    ) -> str:
        """Return COUNT of edges for degree calculation."""
        return self.aggregate_sql(edge_model, start_id, "*", "count", direction)


# ---------------------------------------------------------------------------
# DuckDBTraversalEngine — executes generated SQL via DuckDB
# ---------------------------------------------------------------------------


class DuckDBTraversalEngine(TraversalEngine):
    """
    ``TraversalEngine`` that executes its generated SQL directly via a
    DuckDB connection instead of returning query strings.

    Intended for use with the lazy sink architecture: the edge table lives
    in DuckDB (or is registered via ``sink.current_state_sql``) and all
    traversal is expressed as SQL inside the engine.

    Usage::

        with DuckDBSink("/tmp/graph.duckdb") as sink:
            engine = DuckDBTraversalEngine(sink._con)
            neighbors = engine.neighbors(PlayerEdge, start_id=23)
            paths     = engine.paths(PlayerEdge, start_id=23, target_id=6)
            degree    = engine.degree(PlayerEdge, start_id=23, direction="out")
    """

    def __init__(self, con=None):
        import duckdb as _duckdb

        self._con = con or _duckdb.connect()

    # ── Execute helpers ───────────────────────────────────────────────────

    def neighbors(
        self,
        edge_model: type["EdgeModel"],
        start_id: int,
        direction: Literal["out", "in", "both"] = "both",
        filters: dict[str, Any] | None = None,
    ) -> list[int]:
        """Execute neighbors query and return a list of neighbor IDs."""
        sql = self.neighbors_sql(edge_model, start_id, direction, filters)
        return [row[0] for row in self._con.execute(sql).fetchall()]

    def paths(
        self,
        edge_model: type["EdgeModel"],
        start_id: int,
        target_id: int,
        max_hops: int = 3,
    ) -> list[list[int]]:
        """Execute paths query and return a list of path arrays."""
        sql = self.paths_sql(edge_model, start_id, target_id, max_hops)
        return [row[0] for row in self._con.execute(sql).fetchall()]

    def aggregate(
        self,
        edge_model: type["EdgeModel"],
        start_id: int,
        measure: str,
        agg: str,
        direction: Literal["out", "in", "both"] = "both",
    ):
        """Execute aggregation query and return the scalar result."""
        sql = self.aggregate_sql(edge_model, start_id, measure, agg, direction)
        return self._con.execute(sql).fetchone()[0]

    def degree(
        self,
        edge_model: type["EdgeModel"],
        start_id: int,
        direction: Literal["out", "in", "both"] = "both",
    ) -> int:
        """Return the degree (edge count) for *start_id*."""
        sql = self.degree_sql(edge_model, start_id, direction)
        return self._con.execute(sql).fetchone()[0]

    # ── View-based API (composable with lazy loaders) ─────────────────────

    def register_neighbor_view(
        self,
        edge_model: type["EdgeModel"],
        start_id: int,
        view_name: str = "neighbors",
        direction: Literal["out", "in", "both"] = "both",
        filters: dict[str, Any] | None = None,
    ) -> str:
        """
        Register the neighbor query as a DuckDB VIEW for downstream joins.
        Returns the view name.
        """
        sql = self.neighbors_sql(edge_model, start_id, direction, filters)
        self._con.execute(f"CREATE OR REPLACE VIEW {view_name} AS {sql}")
        return view_name

    def register_paths_view(
        self,
        edge_model: type["EdgeModel"],
        start_id: int,
        target_id: int,
        view_name: str = "graph_paths",
        max_hops: int = 3,
    ) -> str:
        """
        Register the paths query as a DuckDB VIEW.
        Returns the view name.
        """
        sql = self.paths_sql(edge_model, start_id, target_id, max_hops)
        self._con.execute(f"CREATE OR REPLACE VIEW {view_name} AS {sql}")
        return view_name


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _build_filters(filters: dict[str, Any]) -> str:
    """Convert a simple {col: val} dict into a SQL AND clause."""
    clauses = []
    for col, val in filters.items():
        if isinstance(val, str):
            clauses.append(f"{col} = '{val}'")
        elif val is None:
            clauses.append(f"{col} IS NULL")
        else:
            clauses.append(f"{col} = {val}")
    return " AND ".join(clauses)

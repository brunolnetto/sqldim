"""
Recursive CTE traversal engine — SQL generation for graph queries.
Supports PostgreSQL and DuckDB dialects.
"""

from __future__ import annotations

from typing import Any, Literal, TYPE_CHECKING
from datetime import date, datetime

if TYPE_CHECKING:
    from sqldim.core.graph.models import EdgeModel

from sqldim.core.graph._impl._traversal_base import (  # noqa: F401
    TraversalEngine,
    _temporal_filter_clause,
    _build_filters,
    _direction_where,
    _as_of_str,
    _conditional_filter,
)


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
        weighted: bool = False,
    ):
        """Execute aggregation query and return the scalar result."""
        sql = self.aggregate_sql(
            edge_model, start_id, measure, agg, direction, weighted=weighted
        )
        return self._con.execute(sql).fetchone()[0]

    def neighbors_at(
        self,
        edge_model: type["EdgeModel"],
        vertex_model: type,
        start_id: int,
        as_of: date | datetime | None = None,
        direction: Literal["out", "in", "both"] = "both",
        filters: dict[str, Any] | None = None,
    ) -> list[int]:
        """
        Execute a temporal neighbor query and return a list of neighbor IDs.

        Delegates to :meth:`neighbors_sql_at` for SQL generation, then
        executes the query against the DuckDB connection.
        """
        sql = self.neighbors_sql_at(
            edge_model, vertex_model, start_id, as_of, direction, filters
        )
        return [row[0] for row in self._con.execute(sql).fetchall()]

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

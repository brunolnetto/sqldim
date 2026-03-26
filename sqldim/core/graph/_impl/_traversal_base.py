"""TraversalEngine abstract base and SQL helper functions.

Extracted from traversal.py to keep file sizes manageable.
``DuckDBTraversalEngine`` lives in :mod:`sqldim.core.graph.traversal` and
inherits from :class:`TraversalEngine` defined here.
"""

from __future__ import annotations

from typing import Any, Literal, TYPE_CHECKING
from datetime import date, datetime

if TYPE_CHECKING:
    from sqldim.core.graph.models import EdgeModel


def _temporal_filter_clause(
    vertex_cls: type, as_of: date | datetime | None, alias: str = "v"
) -> str:
    """
    Generate a temporal WHERE/JOIN predicate for SCD2 vertex models.

    Returns an empty string for SCD Type 1 (or untyped) vertices, since
    those have no versioning history and need no temporal filter.

    For SCD Type 2 vertices with ``as_of`` provided the predicate filters
    to the single version active at that instant:
        effective_from <= as_of AND (effective_to > as_of OR effective_to IS NULL)
    """
    scd_type = getattr(vertex_cls, "__scd_type__", 1)
    if scd_type != 2 or as_of is None:
        return ""
    as_of_str = as_of.isoformat() if hasattr(as_of, "isoformat") else str(as_of)
    return (
        f" AND {alias}.effective_from <= '{as_of_str}'"
        f" AND ({alias}.effective_to > '{as_of_str}' OR {alias}.effective_to IS NULL)"
    )


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


def _direction_where(direction: str, start_id: int) -> str:
    """Return the bare WHERE predicate for *direction* and *start_id*."""
    if direction == "out":
        return f"subject_id = {start_id}"
    if direction == "in":
        return f"object_id = {start_id}"
    return f"subject_id = {start_id} OR object_id = {start_id}"


def _as_of_str(as_of: date | datetime) -> str:
    """Normalise *as_of* to an ISO-format date/datetime string."""
    return as_of.isoformat() if hasattr(as_of, "isoformat") else str(as_of)


def _conditional_filter(filters: "dict[str, Any] | None") -> str:
    """Return an ' AND <filter_sql>' fragment when *filters* are supplied."""
    return f" AND {_build_filters(filters)}" if filters else ""


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

    def _both_direction_sql(
        self,
        table: str,
        start_id: int,
        filters: dict[str, Any] | None,
    ) -> str:
        parts = [
            f"SELECT object_id AS neighbor_id FROM {table} WHERE subject_id = {start_id}",
            f"SELECT subject_id AS neighbor_id FROM {table} WHERE object_id = {start_id}",
        ]
        if filters:
            filter_sql = _build_filters(filters)
            parts = [p + f" AND {filter_sql}" for p in parts]
        return "\nUNION\n".join(parts)

    def neighbors_sql(
        self,
        edge_model: type["EdgeModel"],
        start_id: int,
        direction: Literal["out", "in", "both"] = "both",
        filters: dict[str, Any] | None = None,
    ) -> str:
        """
        Single-hop neighbor lookup — returns peer vertex IDs.
        """
        table: str = edge_model.__tablename__  # type: ignore[attr-defined, assignment]
        directed: bool = getattr(edge_model, "__directed__", True)
        if not directed:
            direction = "both"
        if direction == "out":
            select_col, where_clause = "object_id", f"subject_id = {start_id}"
        elif direction == "in":
            select_col, where_clause = "subject_id", f"object_id = {start_id}"
        else:
            return self._both_direction_sql(table, start_id, filters)
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
        table: str = edge_model.__tablename__  # type: ignore[attr-defined, assignment]
        directed: bool = getattr(edge_model, "__directed__", True)

        if directed:
            follow_join = f"JOIN {table} e ON e.subject_id = t.current_id"
            next_id = "e.object_id"
        else:
            # Undirected: follow both directions
            follow_join = f"JOIN {table} e ON e.subject_id = t.current_id OR e.object_id = t.current_id"
            next_id = "CASE WHEN e.subject_id = t.current_id THEN e.object_id ELSE e.subject_id END"

        return f"""WITH RECURSIVE traversal(current_id, path, depth) AS (
    -- Base case: start at source vertex
    SELECT {start_id}, [{start_id}]::INTEGER[], 0

    UNION ALL

    -- Recursive case: follow edges one hop at a time
    SELECT {next_id},
           list_append(t.path, {next_id}),
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
        weighted: bool = False,
    ) -> str:
        """
        Aggregate a numeric measure across all edges incident to a vertex.

        Parameters
        ----------
        weighted:
            When ``True`` and ``measure != "*"``, the expression becomes
            ``AGG(measure * weight)`` to support bridge-table allocation
            semantics (prevents double-counting in multi-valued dimensions).
            ``COUNT(*)`` is never affected by this flag.
        """
        table: str = edge_model.__tablename__  # type: ignore[attr-defined, assignment]
        directed: bool = getattr(edge_model, "__directed__", True)

        agg_upper = agg.upper()
        if not directed:
            direction = "both"

        where_clause = _direction_where(direction, start_id)

        # Weighted expression: multiply measure by weight column, except for COUNT(*)
        if weighted and measure != "*":
            agg_expr = f"{agg_upper}({measure} * weight)"
        else:
            agg_expr = f"{agg_upper}({measure})"

        return f"SELECT {agg_expr} AS result FROM {table} WHERE {where_clause}"

    # ------------------------------------------------------------------
    # Temporal (SCD-aware) traversal
    # ------------------------------------------------------------------

    def _build_temporal_sql(
        self,
        edge_table: str,
        vertex_table: str,
        temporal_cond: str,
        start_id: int,
        filter_sql: str,
        direction: str,
    ) -> str:
        """Build the SCD2-aware neighbour SQL for a resolved *direction*."""
        out = (
            f"SELECT e.object_id AS neighbor_id"
            f" FROM {edge_table} e"
            f" JOIN {vertex_table} v ON v.id = e.object_id AND {temporal_cond}"
            f" WHERE e.subject_id = {start_id}{filter_sql}"
        )
        if direction == "out":
            return out
        inbound = (
            f"SELECT e.subject_id AS neighbor_id"
            f" FROM {edge_table} e"
            f" WHERE e.object_id = {start_id}{filter_sql}"
        )
        if direction == "in":
            return inbound
        return f"{out}\nUNION\n{inbound}"

    def neighbors_sql_at(
        self,
        edge_model: type["EdgeModel"],
        vertex_model: type,
        start_id: int,
        as_of: date | datetime | None = None,
        direction: Literal["out", "in", "both"] = "both",
        filters: dict[str, Any] | None = None,
    ) -> str:
        """
        Single-hop neighbor lookup with temporal SCD2 filtering via a JOIN.

        When ``as_of`` is provided and ``vertex_model.__scd_type__ == 2``,
        the returned SQL JOINs to the vertex table and restricts to the
        version active at ``as_of``.  For SCD Type 1 (or no ``as_of``),
        this degrades to a plain :meth:`neighbors_sql` call.

        Parameters
        ----------
        vertex_model:
            The neighbor vertex class — used to inspect ``__scd_type__``
            and ``__tablename__``.
        as_of:
            A ``datetime.date`` / ``datetime.datetime`` (or ISO string).
            ``None`` → no temporal filter (preserves current behaviour).
        """
        scd_type = getattr(vertex_model, "__scd_type__", 1)
        if as_of is None or scd_type != 2:
            return self.neighbors_sql(edge_model, start_id, direction, filters)

        edge_table: str = edge_model.__tablename__  # type: ignore[attr-defined, assignment]
        vertex_table: str = vertex_model.__tablename__  # type: ignore[attr-defined, assignment]
        if not getattr(edge_model, "__directed__", True):
            direction = "both"

        as_of_str_val = _as_of_str(as_of)
        temporal_cond = (
            f"v.effective_from <= '{as_of_str_val}'"
            f" AND (v.effective_to > '{as_of_str_val}' OR v.effective_to IS NULL)"
        )
        return self._build_temporal_sql(
            edge_table,
            vertex_table,
            temporal_cond,
            start_id,
            _conditional_filter(filters),
            direction,
        )

    def degree_sql(
        self,
        edge_model: type["EdgeModel"],
        start_id: int,
        direction: Literal["out", "in", "both"] = "both",
    ) -> str:
        """Return COUNT of edges for degree calculation."""
        return self.aggregate_sql(edge_model, start_id, "*", "count", direction)

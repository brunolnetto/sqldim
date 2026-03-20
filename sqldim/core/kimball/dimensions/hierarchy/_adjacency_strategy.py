"""Hierarchy strategy: AdjacencyListStrategy.

Extracted from _hierarchy_strategies.py to keep file sizes manageable.
"""

from __future__ import annotations


class AdjacencyListStrategy:
    """
    Default hierarchy strategy — stores only ``parent_id``.

    All traversal queries use ``WITH RECURSIVE`` CTEs which are natively
    supported by DuckDB and PostgreSQL.

    Pros
    ----
    * Minimal storage overhead — no additional columns or tables
    * Simple inserts and updates
    * Natural fit for DuckDB's recursive CTE engine (reuses ``TraversalEngine``)
    * Most SCD2-compatible strategy (``parent_id`` is a plain tracked column)

    Cons
    ----
    * Ancestor/descendant queries are O(depth) per call
    * No pre-computed rollup paths
    """

    def ancestors_sql(
        self,
        table: str,
        *,
        id_col: str = "id",
        node_id: int | str,
        max_depth: int = -1,
        parent_col: str = "parent_id",
    ) -> str:
        depth_guard = f"AND h.depth < {max_depth}" if max_depth > 0 else ""
        return f"""
WITH RECURSIVE ancestors AS (
    SELECT {id_col}, {parent_col}, 0 AS depth
    FROM {table}
    WHERE {id_col} = {node_id!r}

    UNION ALL

    SELECT d.{id_col}, d.{parent_col}, h.depth + 1
    FROM {table} d
    JOIN ancestors h ON d.{id_col} = h.{parent_col}
    WHERE h.{parent_col} IS NOT NULL {depth_guard}
)
SELECT * FROM ancestors
WHERE {id_col} != {node_id!r}
ORDER BY depth
""".strip()

    def descendants_sql(
        self,
        table: str,
        *,
        id_col: str = "id",
        node_id: int | str,
        max_depth: int = -1,
        parent_col: str = "parent_id",
    ) -> str:
        depth_guard = f"AND h.depth < {max_depth}" if max_depth > 0 else ""
        return f"""
WITH RECURSIVE descendants AS (
    SELECT {id_col}, {parent_col}, 0 AS depth
    FROM {table}
    WHERE {id_col} = {node_id!r}

    UNION ALL

    SELECT d.{id_col}, d.{parent_col}, h.depth + 1
    FROM {table} d
    JOIN descendants h ON d.{parent_col} = h.{id_col}
    {depth_guard}
)
SELECT * FROM descendants
WHERE {id_col} != {node_id!r}
ORDER BY depth
""".strip()

    def rollup_sql(
        self,
        fact_table: str,
        dim_table: str,
        measure: str,
        level: int,
        *,
        fact_fk: str = "dim_id",
        id_col: str = "id",
        parent_col: str = "parent_id",
        level_col: str = "hierarchy_level",
        agg: str = "SUM",
    ) -> str:
        """
        Aggregate *measure* up to *level* in the hierarchy.

        Uses a recursive CTE to climb from leaf nodes to the target level,
        then joins the fact table and groups by the ancestor at that level.

        Parameters
        ----------
        level:
            Target hierarchy depth to roll up to (0 = root, 1 = first child
            level, etc.).  Rows at depths deeper than *level* are aggregated
            into their ancestor at exactly *level*.
        """
        return f"""
WITH RECURSIVE rollup_tree AS (
    SELECT
        {id_col}         AS leaf_id,
        {id_col}         AS ancestor_id,
        {level_col}      AS ancestor_level,
        {parent_col}     AS parent_id
    FROM {dim_table}

    UNION ALL

    SELECT
        r.leaf_id,
        d.{id_col}       AS ancestor_id,
        d.{level_col}    AS ancestor_level,
        d.{parent_col}   AS parent_id
    FROM rollup_tree r
    JOIN {dim_table} d ON d.{id_col} = r.parent_id
    WHERE r.ancestor_level > {level}
),
at_level AS (
    SELECT leaf_id, ancestor_id
    FROM rollup_tree
    WHERE ancestor_level = {level}
)
SELECT
    al.ancestor_id,
    {agg}(f.{measure}) AS {measure}_{agg.lower()}
FROM {fact_table} f
JOIN at_level al ON f.{fact_fk} = al.leaf_id
GROUP BY al.ancestor_id
ORDER BY al.ancestor_id
""".strip()


# ---------------------------------------------------------------------------
# MaterializedPathStrategy
# ---------------------------------------------------------------------------

"""Hierarchy strategy: MaterializedPathStrategy.

Extracted from _hierarchy_strategies.py to keep file sizes manageable.
"""

from __future__ import annotations


class MaterializedPathStrategy:
    """
    Hierarchy strategy using a materialized path string in ``hierarchy_path``.

    Stores the full ancestor path as a delimited string, e.g.
    ``"/1/5/12/42/"`` where each segment is a surrogate key.  Enables
    ancestor and descendant queries using SQL ``LIKE`` without recursion.

    Pros
    ----
    * O(1) ancestor/descendant lookup — substring match
    * Works in any SQL dialect (no recursive CTE required)
    * Simple depth calculation (count path separators)

    Cons
    ----
    * ``hierarchy_path`` must be recomputed when SCD2 versions are created
      (paths reference surrogate keys that change between versions)
    * Subtree moves require updating the path for all descendants — O(subtree)
    * Path length is bounded by column size

    SCD2 note
    ---------
    Path maintenance is the responsibility of the loader.  When a new SCD2
    version is created for a dimension row, the ``hierarchy_path`` on the new
    version must be rebuilt to use new surrogate keys for ancestors that also
    have new versions at the point-in-time.
    """

    _SEP: str = "/"

    def ancestors_sql(
        self,
        table: str,
        *,
        id_col: str = "id",
        node_id: int | str,
        max_depth: int = -1,
        parent_col: str = "parent_id",
    ) -> str:
        # Fetch the path of *node_id*, then find all rows whose path is a
        # **prefix** of the target node's path (i.e., they are ancestors).
        depth_filter = (
            f"AND hierarchy_level >= (SELECT hierarchy_level - {max_depth} FROM {table} WHERE {id_col} = {node_id!r})"
            if max_depth > 0
            else ""
        )
        return f"""
SELECT anc.*
FROM {table} node
JOIN {table} anc
    ON node.hierarchy_path LIKE anc.hierarchy_path || '%'
    AND anc.{id_col} != node.{id_col}
WHERE node.{id_col} = {node_id!r}
  {depth_filter}
ORDER BY anc.hierarchy_level
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
        depth_filter = (
            f"AND desc_rows.hierarchy_level <= (SELECT hierarchy_level + {max_depth} FROM {table} WHERE {id_col} = {node_id!r})"
            if max_depth > 0
            else ""
        )
        return f"""
SELECT desc_rows.*
FROM {table} node
JOIN {table} desc_rows
    ON desc_rows.hierarchy_path LIKE node.hierarchy_path || '%'
    AND desc_rows.{id_col} != node.{id_col}
WHERE node.{id_col} = {node_id!r}
  {depth_filter}
ORDER BY desc_rows.hierarchy_level
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
        return f"""
WITH ancestors_at_level AS (
    SELECT leaf.{id_col} AS leaf_id, anc.{id_col} AS ancestor_id
    FROM {dim_table} leaf
    JOIN {dim_table} anc
        ON leaf.hierarchy_path LIKE anc.hierarchy_path || '%'
        AND anc.{level_col} = {level}
)
SELECT
    al.ancestor_id,
    {agg}(f.{measure}) AS {measure}_{agg.lower()}
FROM {fact_table} f
JOIN ancestors_at_level al ON f.{fact_fk} = al.leaf_id
GROUP BY al.ancestor_id
ORDER BY al.ancestor_id
""".strip()

    @staticmethod
    def build_path(parent_path: str | None, node_id: int | str, sep: str = "/") -> str:
        """Construct the hierarchy path string for a new row.

        Parameters
        ----------
        parent_path:
            The ``hierarchy_path`` of the parent row, e.g. ``"/1/5/"``.
            ``None`` for root nodes.
        node_id:
            The surrogate key of the new row.

        Returns
        -------
        str
            The concatenated path, e.g. ``"/1/5/12/"``.
        """
        if parent_path is None:
            return f"{sep}{node_id}{sep}"
        return f"{parent_path}{node_id}{sep}"


# ---------------------------------------------------------------------------
# ClosureTableStrategy
# ---------------------------------------------------------------------------

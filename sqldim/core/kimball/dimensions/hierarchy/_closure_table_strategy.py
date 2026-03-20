"""Hierarchy strategy: ClosureTableStrategy.

Extracted from _hierarchy_strategies.py to keep file sizes manageable.
"""

from __future__ import annotations


class ClosureTableStrategy:
    """
    Hierarchy strategy using a separate closure bridge table.

    Stores **all** ancestor-descendant pairs with their depth in a separate
    table named ``{dim_table}_closure`` (configurable via ``closure_table``
    parameter).

    SQL assumed for the closure table::

        CREATE TABLE {dim_table}_closure (
            ancestor_id   INTEGER NOT NULL,
            descendant_id INTEGER NOT NULL,
            depth         INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (ancestor_id, descendant_id)
        );

    A row ``(ancestor=5, descendant=42, depth=2)`` means node 42 is two
    levels below node 5.  Every node has a self-referencing row with
    ``depth=0``.

    Pros
    ----
    * O(1) ancestor/descendant lookup — pure JOIN, no recursion
    * Supports multi-parent hierarchies
    * Depth-filtered queries are simple predicates

    Cons
    ----
    * O(n²) storage for deep/wide hierarchies
    * Closure table must be rebuilt/maintained on inserts, deletes, and moves
    * SCD2 creates new versions of ancestor rows — closure rows referencing
      old surrogate keys may become stale (must be rebuilt alongside SCD2
      processing)

    SCD2 note
    ---------
    Closure table maintenance during SCD2 processing is the responsibility
    of the loader.  Rows referencing old surrogate keys of updated ancestors
    should be updated to point to the new (current) surrogate key, or the
    closure table should be fully rebuilt after each SCD2 batch.
    """

    def _closure_table(self, dim_table: str, closure_table: str | None = None) -> str:
        return closure_table or f"{dim_table}_closure"

    def ancestors_sql(
        self,
        table: str,
        *,
        id_col: str = "id",
        node_id: int | str,
        max_depth: int = -1,
        parent_col: str = "parent_id",
        closure_table: str | None = None,
    ) -> str:
        ct = self._closure_table(table, closure_table)
        depth_filter = f"AND c.depth <= {max_depth}" if max_depth > 0 else ""
        return f"""
SELECT d.*
FROM {ct} c
JOIN {table} d ON d.{id_col} = c.ancestor_id
WHERE c.descendant_id = {node_id!r}
  AND c.depth > 0
  {depth_filter}
ORDER BY c.depth
""".strip()

    def descendants_sql(
        self,
        table: str,
        *,
        id_col: str = "id",
        node_id: int | str,
        max_depth: int = -1,
        parent_col: str = "parent_id",
        closure_table: str | None = None,
    ) -> str:
        ct = self._closure_table(table, closure_table)
        depth_filter = f"AND c.depth <= {max_depth}" if max_depth > 0 else ""
        return f"""
SELECT d.*
FROM {ct} c
JOIN {table} d ON d.{id_col} = c.descendant_id
WHERE c.ancestor_id = {node_id!r}
  AND c.depth > 0
  {depth_filter}
ORDER BY c.depth
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
        closure_table: str | None = None,
    ) -> str:
        ct = self._closure_table(dim_table, closure_table)
        return f"""
WITH ancestors_at_level AS (
    SELECT c.descendant_id AS leaf_id, anc.{id_col} AS ancestor_id
    FROM {ct} c
    JOIN {dim_table} anc ON anc.{id_col} = c.ancestor_id
    WHERE anc.{level_col} = {level}
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
    def build_closure_sql(
        dim_table: str,
        *,
        id_col: str = "id",
        parent_col: str = "parent_id",
        closure_table: str | None = None,
    ) -> str:
        """Return SQL to (re)build the closure table from scratch.

        Uses a recursive CTE to enumerate all ancestor-descendant pairs.
        Suitable for initial population and full rebuilds after SCD2 batches.

        Returns
        -------
        str
            A ``INSERT INTO … SELECT`` statement that populates or replaces
            the closure table.
        """
        ct = closure_table or f"{dim_table}_closure"
        return f"""
-- First ensure the closure table exists (DDL not generated here — create it separately)
DELETE FROM {ct};
INSERT INTO {ct} (ancestor_id, descendant_id, depth)
WITH RECURSIVE pairs AS (
    SELECT {id_col} AS ancestor_id, {id_col} AS descendant_id, 0 AS depth
    FROM {dim_table}

    UNION ALL

    SELECT p.ancestor_id, d.{id_col} AS descendant_id, p.depth + 1
    FROM {dim_table} d
    JOIN pairs p ON d.{parent_col} = p.descendant_id
    WHERE d.{parent_col} IS NOT NULL
)
SELECT DISTINCT ancestor_id, descendant_id, depth FROM pairs
""".strip()


# ---------------------------------------------------------------------------
# HierarchyRoller — declarative rollup API
# ---------------------------------------------------------------------------

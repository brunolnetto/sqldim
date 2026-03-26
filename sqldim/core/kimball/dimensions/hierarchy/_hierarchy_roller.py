"""HierarchyRoller — declarative measure rollup over hierarchy levels.

Extracted from hierarchy.py to keep file sizes manageable.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqldim.core.kimball.dimensions.hierarchy._adjacency_strategy import (
    AdjacencyListStrategy,
)

if TYPE_CHECKING:
    from sqldim.core.kimball.dimensions.hierarchy import HierarchyStrategy


class HierarchyRoller:
    """
    Declarative measure rollup over a hierarchy dimension.

    Generates the appropriate rollup SQL for any strategy without exposing
    recursive CTE details to the caller.

    Example
    -------
    .. code-block:: python

        # Roll up revenue to region level (depth 2)
        sql = HierarchyRoller.rollup_sql(
            fact_table="sales_fact",
            dim_table="org_dim",
            measure="revenue",
            level=2,
        )
        result = con.execute(sql).fetchdf()

        # With a specific strategy override
        sql = HierarchyRoller.rollup_sql(
            fact_table="sales_fact",
            dim_table="org_dim",
            measure="revenue",
            level=2,
            strategy=ClosureTableStrategy(),
        )
    """

    @staticmethod
    def rollup_sql(
        fact_table: str,
        dim_table: str,
        measure: str,
        level: int,
        *,
        strategy: HierarchyStrategy | None = None,
        fact_fk: str = "dim_id",
        id_col: str = "id",
        parent_col: str = "parent_id",
        level_col: str = "hierarchy_level",
        agg: str = "SUM",
        dim_cls: type | None = None,
    ) -> str:
        """
        Return SQL to aggregate *measure* at *fact_table* up to *level* in
        the hierarchy defined by *dim_table*.

        Parameters
        ----------
        fact_table:
            Name of the fact table to aggregate.
        dim_table:
            Name of the hierarchy dimension table.
        measure:
            Column name in *fact_table* to aggregate.
        level:
            Target hierarchy depth (0 = root).
        strategy:
            Strategy instance to use.  When ``None``, resolves from
            ``dim_cls.__hierarchy_strategy__`` (defaults to adjacency list).
        fact_fk:
            Column in *fact_table* that is the FK to *dim_table*.
        agg:
            SQL aggregation function (``"SUM"``, ``"COUNT"``, ``"MAX"``, …).
        dim_cls:
            Optional class with ``__hierarchy_strategy__`` to auto-select
            the strategy.
        """
        if strategy is None:
            if dim_cls is not None:
                strategy = dim_cls.hierarchy_strategy()  # type: ignore[union-attr, attr-defined]
            else:
                strategy = AdjacencyListStrategy()

        return strategy.rollup_sql(
            fact_table=fact_table,
            dim_table=dim_table,
            measure=measure,
            level=level,
            fact_fk=fact_fk,
            id_col=id_col,
            parent_col=parent_col,
            level_col=level_col,
            agg=agg,
        )

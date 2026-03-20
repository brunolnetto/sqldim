"""
Recursive Hierarchy Dimensions — ADR implementation.

Provides ``HierarchyMixin`` for adding parent-child support to any
``DimensionModel``, a ``HierarchyStrategy`` protocol, three concrete strategy implementations
(in :mod:`._hierarchy_strategies`), and ``HierarchyRoller``
(in :mod:`._hierarchy_roller`).

Hierarchy is treated as a **dimension-level concern**, not a graph-level
concern.  The graph module handles cross-entity relationships; hierarchy
handles intra-entity parent-child relationships where both parent and child
are the same dimension model.

Usage
-----
.. code-block:: python

    from sqldim.core.kimball.dimensions.hierarchy import (
        HierarchyMixin,
        AdjacencyListStrategy,
        MaterializedPathStrategy,
        ClosureTableStrategy,
        HierarchyRoller,
    )

    class OrgDimension(HierarchyMixin, DimensionModel, table=True):
        __tablename__ = "org_dim"
        __natural_key__ = ["employee_code"]
        __hierarchy_strategy__ = "adjacency"

        id: int = Field(primary_key=True)
        employee_code: str
        name: str
        # parent_id and hierarchy_level are contributed by HierarchyMixin

    # Rollup revenue to regional level (depth 2)
    sql = HierarchyRoller.rollup_sql(
        fact_table="sales_fact",
        dim_table="org_dim",
        measure="revenue",
        level=2,
    )
"""

from __future__ import annotations

from typing import Optional, Protocol, runtime_checkable


# ---------------------------------------------------------------------------
# HierarchyMixin
# ---------------------------------------------------------------------------


class HierarchyMixin:
    """
    Mixin that adds parent-child support columns to a ``DimensionModel``.

    Adds these columns (as class-level defaults compatible with SQLModel /
    Pydantic):

    * ``parent_id`` — nullable self-referential FK to the same table
    * ``hierarchy_level`` — depth from root (0 = root node)
    * ``hierarchy_path`` — materialized path string (populated when using
      ``MaterializedPathStrategy``; ``None`` otherwise)

    Also uses ``__hierarchy_strategy__`` class var to declare which physical
    storage strategy to use (``"adjacency"`` | ``"materialized_path"`` |
    ``"closure"``).  Defaults to ``"adjacency"``.

    SCD2 interaction
    ----------------
    * **adjacency**: ``parent_id`` is a tracked column — hierarchy changes
      create new SCD2 versions.  Point-in-time hierarchy trees can be
      reconstructed with ``WHERE effective_from <= :as_of``.
    * **materialized_path**: ``hierarchy_path`` must be recomputed when
      SCD2 creates new versions (paths reference surrogate keys that change).
    * **closure table**: the closure bridge must be rebuilt when new SCD2
      versions are created.

    ``"adjacency"`` is the most SCD-compatible strategy and is the default.
    """

    # Physical strategy selector — override in subclass
    __hierarchy_strategy__: str = "adjacency"

    # These are declared as class-level annotations so that SQLModel picks
    # them up during table creation when the mixin is combined with a
    # concrete model.  The actual type annotation must match the SQLModel
    # field declaration style used in the concrete subclass.

    # NOTE: Concrete subclass must declare `id: int = Field(primary_key=True)`
    # before combining with this mixin.

    parent_id: Optional[int] = None  # Self-referential FK; annotated in subclass
    hierarchy_level: int = 0  # Depth from root
    hierarchy_path: Optional[str] = None  # Populated by MaterializedPathStrategy

    @classmethod
    def hierarchy_strategy(cls) -> "HierarchyStrategy":
        """Return the configured strategy instance."""
        strategy_name = getattr(cls, "__hierarchy_strategy__", "adjacency")
        _strategies: dict[str, HierarchyStrategy] = {
            "adjacency": AdjacencyListStrategy(),
            "materialized_path": MaterializedPathStrategy(),
            "closure": ClosureTableStrategy(),
        }
        if strategy_name not in _strategies:
            raise ValueError(
                f"Unknown hierarchy strategy {strategy_name!r}. "
                f"Valid options: {list(_strategies.keys())}"
            )
        return _strategies[strategy_name]

    @classmethod
    def ancestors_sql(
        cls,
        node_id: int | str,
        *,
        max_depth: int = -1,
        id_col: str = "id",
        parent_col: str = "parent_id",
    ) -> str:
        """Return SQL to find all ancestors of *node_id*.

        Delegates to the configured :class:`HierarchyStrategy`.
        """
        table = getattr(cls, "__tablename__", cls.__name__.lower())
        return cls.hierarchy_strategy().ancestors_sql(
            table,
            id_col=id_col,
            node_id=node_id,
            max_depth=max_depth,
            parent_col=parent_col,
        )

    @classmethod
    def descendants_sql(
        cls,
        node_id: int | str,
        *,
        max_depth: int = -1,
        id_col: str = "id",
        parent_col: str = "parent_id",
    ) -> str:
        """Return SQL to find all descendants of *node_id*.

        Delegates to the configured :class:`HierarchyStrategy`.
        """
        table = getattr(cls, "__tablename__", cls.__name__.lower())
        return cls.hierarchy_strategy().descendants_sql(
            table,
            id_col=id_col,
            node_id=node_id,
            max_depth=max_depth,
            parent_col=parent_col,
        )


# ---------------------------------------------------------------------------
# HierarchyStrategy protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class HierarchyStrategy(Protocol):
    """
    Protocol for hierarchy physical storage strategies.

    Concrete implementations:

    * :class:`AdjacencyListStrategy` — simple ``parent_id``; recursive CTEs
    * :class:`MaterializedPathStrategy` — ``hierarchy_path`` for O(1) lookups
    * :class:`ClosureTableStrategy` — separate bridge table for full O(1) traversal

    All SQL methods return DuckDB-compatible SQL strings that can be executed
    directly or embedded in higher-level queries.
    """

    def ancestors_sql(
        self,
        table: str,
        *,
        id_col: str,
        node_id: int | str,
        max_depth: int = -1,
        parent_col: str = "parent_id",
    ) -> str:
        """SQL returning all ancestors of *node_id*."""
        ...

    def descendants_sql(
        self,
        table: str,
        *,
        id_col: str,
        node_id: int | str,
        max_depth: int = -1,
        parent_col: str = "parent_id",
    ) -> str:
        """SQL returning all descendants of *node_id*."""
        ...

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
        """SQL to aggregate *measure* up to *level* in the hierarchy."""
        ...


# ---------------------------------------------------------------------------
# AdjacencyListStrategy
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Strategy implementations and HierarchyRoller (re-exported from sub-modules)
# ---------------------------------------------------------------------------
from sqldim.core.kimball.dimensions.hierarchy._hierarchy_strategies import (  # noqa: E402, F401
    AdjacencyListStrategy,
    MaterializedPathStrategy,
    ClosureTableStrategy,
)
from sqldim.core.kimball.dimensions.hierarchy._hierarchy_roller import (  # noqa: E402, F401
    HierarchyRoller,
)

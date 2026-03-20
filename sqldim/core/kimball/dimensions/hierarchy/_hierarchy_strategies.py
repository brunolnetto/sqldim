"""Hierarchy strategy implementations — re-export shim.

The three concrete strategies are each in their own module.
Import them from here for backward compatibility.
"""

from sqldim.core.kimball.dimensions.hierarchy._adjacency_strategy import (
    AdjacencyListStrategy,
)  # noqa: F401
from sqldim.core.kimball.dimensions.hierarchy._materialized_path_strategy import (
    MaterializedPathStrategy,
)  # noqa: F401
from sqldim.core.kimball.dimensions.hierarchy._closure_table_strategy import (
    ClosureTableStrategy,
)  # noqa: F401

__all__ = ["AdjacencyListStrategy", "MaterializedPathStrategy", "ClosureTableStrategy"]

"""Fluent dimensional query builders.

Exports :class:`DimensionalQuery` for SQLAlchemy-backed chained ``.join()`` /
``.filter()`` building and :class:`DGMQuery` (``Query``) for direct DuckDB
SQL-string generation.
"""

from sqldim.core.query.builder import (
    SemanticError,
    DimensionalQuery,
)
from sqldim.core.query.dgm import DGMQuery, Query

__all__ = ["SemanticError", "DimensionalQuery", "DGMQuery", "Query"]

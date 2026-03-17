"""Fluent dimensional query builder with DuckDB and SQLAlchemy backends.

Exports :class:`DimensionalQuery` for chained ``.join()`` / ``.filter()``
building and :class:`DuckDBDimensionalQuery` for direct execution.
"""
from sqldim.core.query.builder import SemanticError, DimensionalQuery, DuckDBDimensionalQuery

__all__ = ["SemanticError", "DimensionalQuery", "DuckDBDimensionalQuery"]

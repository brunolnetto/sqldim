"""Fluent dimensional query builder with SQLAlchemy backend.

Re-exports :class:`DimensionalQuery` and :class:`~sqldim.exceptions.SemanticError`.
:class:`DGMQuery` (formerly ``DuckDBDimensionalQuery``) is the recommended builder
for DuckDB-compatible dimensional SQL; import it from :mod:`sqldim.core.query.dgm`.
"""

from __future__ import annotations

from sqldim.core.query._dimensional_query import SemanticError, DimensionalQuery  # noqa: F401

__all__ = ["SemanticError", "DimensionalQuery"]

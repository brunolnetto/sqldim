"""SemanticError and SQLAlchemy-backed DimensionalQuery.

Extracted from builder.py to keep file sizes manageable.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from sqlmodel import Session, select
from sqlalchemy import func

from sqldim.core.kimball.models import DimensionModel, FactModel


class SemanticError(Exception):
    """Raised when a query violates dimensional modeling semantics."""

    pass


class DimensionalQuery:
    """SQLAlchemy-backed fluent query builder for star-schema dimensional analytics."""

    def __init__(self, fact: type[FactModel]):
        self._fact = fact
        self._group_by: list[Any] = []
        self._measures: list[Any] = []
        self._filters: list[Any] = []
        self._as_of: date | None = None
        # Infer dimension joins from SchemaGraph
        self._dim_joins: dict[str, type[DimensionModel]] = {}

    def by(self, *attributes: Any) -> "DimensionalQuery":
        """Group by one or more dimension attributes."""
        for attr in attributes:
            self._group_by.append(attr)
        return self

    def where(self, condition: Any) -> "DimensionalQuery":
        """Add a filter predicate."""
        self._filters.append(condition)
        return self

    def sum(self, measure: Any) -> "DimensionalQuery":
        """Aggregate a measure using SUM. Raises SemanticError for non-additive measures."""
        col_info = self._get_column_info(measure)
        if (
            col_info.get("additive") is False
            or col_info.get("additive") == "non_additive"
        ):
            raise SemanticError(
                "Cannot SUM a non-additive measure. Use avg() or a scalar expression instead."
            )
        self._measures.append(func.sum(measure).label(f"sum_{measure.key}"))
        return self

    def avg(self, measure: Any) -> "DimensionalQuery":
        """Aggregate a measure using AVG."""
        self._measures.append(func.avg(measure).label(f"avg_{measure.key}"))
        return self

    def count(self) -> "DimensionalQuery":
        """Add a COUNT(*) to the query."""
        self._measures.append(func.count().label("count"))
        return self

    def as_of(self, point_in_time: date) -> "DimensionalQuery":
        """
        Apply point-in-time SCD2 filtering — resolves all dimension joins
        to their valid version as of the given date instead of is_current=True.
        """
        self._as_of = point_in_time
        return self

    def _get_column_info(self, col: Any) -> dict[str, Any]:
        """Extract dimensional metadata from a SQLAlchemy column."""
        try:
            return col.info or {}
        except AttributeError:
            return {}

    def _build_dim_join(
        self, fact_model: type[FactModel], dim_model: type[DimensionModel], fk_col: Any
    ):
        """Build SCD2-aware join conditions."""
        join_cond = [fk_col == dim_model.id]
        if self._as_of:
            join_cond.append(dim_model.valid_from <= self._as_of)
            join_cond.append(
                dim_model.valid_to.is_(None) | (dim_model.valid_to > self._as_of)
            )
        else:
            join_cond.append(dim_model.is_current)
        return join_cond

    def _build(self) -> Any:
        """Compile the fluent query into a SQLAlchemy select()."""

        select_cols = self._group_by + self._measures
        if not select_cols:
            raise SemanticError(
                "Query must have at least one .by() attribute or .sum()/.avg()/.count() measure."
            )

        stmt = select(*select_cols).select_from(self._fact)
        joined_dims: set = set()
        for attr in self._group_by:
            stmt = self._auto_join_dim(stmt, attr, joined_dims)
        for f in self._filters:
            stmt = stmt.where(f)
        if self._group_by:
            stmt = stmt.group_by(*self._group_by)
        return stmt

    def _find_fk_col(self, dim_model: type) -> Any:
        for col in self._fact.__table__.columns:
            col_dim = col.info.get("dimension") if col.info else None
            if col_dim == dim_model:
                return getattr(self._fact, col.name)
        return None

    def _auto_join_dim(self, stmt: Any, attr: Any, joined_dims: set) -> Any:
        from sqlalchemy import and_

        dim_model = attr.class_
        if dim_model in joined_dims or not issubclass(dim_model, DimensionModel):
            return stmt
        fk_col = self._find_fk_col(dim_model)
        if fk_col is not None:
            join_conds = self._build_dim_join(self._fact, dim_model, fk_col)
            stmt = stmt.join(dim_model, and_(*join_conds))
            joined_dims.add(dim_model)
        return stmt

    async def execute(self, session: Session) -> list[Any]:
        stmt = self._build()
        result = session.exec(stmt)
        return result.all()


__all__ = ["SemanticError", "DimensionalQuery"]

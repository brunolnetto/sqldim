from __future__ import annotations
from datetime import date
from typing import Any, Dict, List, Optional, Type, TYPE_CHECKING
from sqlmodel import Session, select
from sqlalchemy import func

from sqldim.core.models import DimensionModel, FactModel
from sqldim.core.graph import SchemaGraph

class SemanticError(Exception):
    """Raised when a query violates dimensional modeling semantics."""
    pass

class DimensionalQuery:
    def __init__(self, fact: Type[FactModel]):
        self._fact = fact
        self._group_by: List[Any] = []
        self._measures: List[Any] = []
        self._filters: List[Any] = []
        self._as_of: Optional[date] = None
        # Infer dimension joins from SchemaGraph
        self._dim_joins: Dict[str, Type[DimensionModel]] = {}

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
        if col_info.get("additive") is False or col_info.get("additive") == "non_additive":
            raise SemanticError(
                f"Cannot SUM a non-additive measure. Use avg() or a scalar expression instead."
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

    def _get_column_info(self, col: Any) -> Dict[str, Any]:
        """Extract dimensional metadata from a SQLAlchemy column."""
        try:
            return col.info or {}
        except AttributeError:
            return {}

    def _build_dim_join(self, fact_model: Type[FactModel], dim_model: Type[DimensionModel], fk_col: Any):
        """Build SCD2-aware join conditions."""
        join_cond = [fk_col == dim_model.id]
        if self._as_of:
            join_cond.append(dim_model.valid_from <= self._as_of)
            join_cond.append(
                (dim_model.valid_to > self._as_of) | (dim_model.valid_to == None)
            )
        else:
            join_cond.append(dim_model.is_current == True)
        return join_cond

    def _build(self) -> Any:
        """Compile the fluent query into a SQLAlchemy select()."""
        from sqlalchemy import and_

        select_cols = self._group_by + self._measures
        if not select_cols:
            raise SemanticError("Query must have at least one .by() attribute or .sum()/.avg()/.count() measure.")

        stmt = select(*select_cols).select_from(self._fact)

        # Auto-join required dimensions based on group_by attributes
        joined_dims = set()
        for attr in self._group_by:
            # Derive the dimension model from the attribute's parent class
            dim_model = attr.class_
            if dim_model not in joined_dims and issubclass(dim_model, DimensionModel):
                # Find the FK column linking fact to this dimension
                for col in self._fact.__table__.columns:
                    col_dim = col.info.get("dimension") if col.info else None
                    if col_dim == dim_model:
                        fk_col = getattr(self._fact, col.name)
                        join_conds = self._build_dim_join(self._fact, dim_model, fk_col)
                        stmt = stmt.join(dim_model, and_(*join_conds))
                        joined_dims.add(dim_model)
                        break

        # Apply filters
        for f in self._filters:
            stmt = stmt.where(f)

        # Apply group by
        if self._group_by:
            stmt = stmt.group_by(*self._group_by)

        return stmt

    async def execute(self, session: Session) -> List[Any]:
        stmt = self._build()
        result = session.exec(stmt)
        return result.all()

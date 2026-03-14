from __future__ import annotations
from datetime import date
from typing import Any, Dict, List, Optional, Type, TYPE_CHECKING
from sqlmodel import Session, select
from sqlalchemy import func

from sqldim.core.models import DimensionModel, FactModel
from sqldim.core.graph import SchemaGraph


# ---------------------------------------------------------------------------
# DuckDBDimensionalQuery — lazy, SQL-string-based, no SQLAlchemy in hot path
# ---------------------------------------------------------------------------


class DuckDBDimensionalQuery:
    """
    Fluent query builder that emits DuckDB SQL strings.

    Mirrors the ``DimensionalQuery`` interface but works entirely with
    plain table/view names and SQL strings — no SQLAlchemy ORM objects.

    The built SQL can be:

    * Executed immediately via ``execute(con)``.
    * Registered as a DuckDB VIEW for downstream composition via ``as_view(con, name)``.

    Usage::

        q = (
            DuckDBDimensionalQuery("fact_sales")
            .join_dim("dim_product", fact_fk="product_id")
            .join_dim("dim_customer", fact_fk="customer_id")
            .by("dim_product.category", "dim_customer.region")
            .sum("revenue")
            .where("dim_product.category != 'Returns'")
            .as_of("2024-03-31")
        )
        rows = q.execute(con)
        # — or —
        q.as_view(con, "sales_summary")
    """

    def __init__(self, fact_table: str):
        self._fact_table   = fact_table
        self._group_by:    list[str] = []
        self._measures:    list[str] = []
        self._filters:     list[str] = []
        self._joins:       list[tuple[str, str, str]] = []  # (dim_table, fact_fk, dim_pk)
        self._as_of:       str | None = None

    # ── Fluent builders ───────────────────────────────────────────────────

    def by(self, *attributes: str) -> "DuckDBDimensionalQuery":
        """GROUP BY one or more column expressions."""
        self._group_by.extend(attributes)
        return self

    def where(self, condition: str) -> "DuckDBDimensionalQuery":
        """Add a WHERE predicate (raw SQL string)."""
        self._filters.append(condition)
        return self

    def sum(self, measure: str) -> "DuckDBDimensionalQuery":
        col = measure.rsplit(".", 1)[-1]  # strip table qualifier for alias
        self._measures.append(f"SUM({measure}) AS sum_{col}")
        return self

    def avg(self, measure: str) -> "DuckDBDimensionalQuery":
        col = measure.rsplit(".", 1)[-1]
        self._measures.append(f"AVG({measure}) AS avg_{col}")
        return self

    def count(self) -> "DuckDBDimensionalQuery":
        self._measures.append("COUNT(*) AS count")
        return self

    def as_of(self, point_in_time: str) -> "DuckDBDimensionalQuery":
        """
        Point-in-time SCD2 filtering.  Resolves dimension joins to the
        version active on *point_in_time* (ISO date string).
        """
        self._as_of = point_in_time
        return self

    def join_dim(
        self,
        dim_table: str,
        fact_fk: str,
        dim_pk: str = "id",
    ) -> "DuckDBDimensionalQuery":
        """
        Declare a dimension join.

        * **dim_table** — table/view name (must be registered in DuckDB)
        * **fact_fk** — FK column in the fact table
        * **dim_pk** — PK column in the dimension table (default ``id``)
        """
        self._joins.append((dim_table, fact_fk, dim_pk))
        return self

    # ── SQL compilation ───────────────────────────────────────────────────

    def to_sql(self) -> str:
        """Compile the fluent chain to a DuckDB SQL string."""
        select_parts = list(self._group_by) + list(self._measures)
        if not select_parts:
            raise SemanticError(
                "Query must have at least one .by() attribute or .sum()/.avg()/.count() measure."
            )

        lines: list[str] = [f"SELECT {', '.join(select_parts)}"]
        lines.append(f"FROM {self._fact_table} f")

        for dim_table, fact_fk, dim_pk in self._joins:
            alias = f"d_{dim_table}"
            if self._as_of:
                lines.append(
                    f"LEFT JOIN {dim_table} {alias}"
                    f" ON f.{fact_fk} = {alias}.{dim_pk}"
                    f" AND {alias}.valid_from <= '{self._as_of}'::DATE"
                    f" AND ({alias}.valid_to IS NULL OR {alias}.valid_to > '{self._as_of}'::DATE)"
                )
            else:
                lines.append(
                    f"LEFT JOIN {dim_table} {alias}"
                    f" ON f.{fact_fk} = {alias}.{dim_pk}"
                    f" AND {alias}.is_current = TRUE"
                )

        if self._filters:
            lines.append("WHERE " + " AND ".join(self._filters))

        if self._group_by:
            lines.append("GROUP BY " + ", ".join(self._group_by))

        return "\n".join(lines)

    def as_view(self, con, view_name: str) -> str:
        """
        Register the compiled SQL as a DuckDB VIEW.
        Returns the view name for downstream composition.
        """
        con.execute(f"CREATE OR REPLACE VIEW {view_name} AS\n{self.to_sql()}")
        return view_name

    def execute(self, con) -> list:
        """Execute the query and return result rows."""
        return con.execute(self.to_sql()).fetchall()

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

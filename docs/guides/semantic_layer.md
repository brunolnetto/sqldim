# Guide: Semantic Query Layer

`DimensionalQuery` builds an SQLAlchemy query over a fact and its SCD2
dimensions, so that you do not write the point-in-time join by hand.

## The point-in-time join

In an SCD2 schema, joining a fact to a dimension version needs a date range:

```sql
SELECT ... FROM facts f
JOIN dimensions d ON f.dim_id = d.dim_id
AND f.event_date BETWEEN d.valid_from AND COALESCE(d.valid_to, '9999-12-31')
```

## `DimensionalQuery`

`DimensionalQuery(Fact)` joins each dimension that a `.by(...)` attribute
belongs to through the fact column declared with `Field(dimension=...)`.
Without `.as_of(...)` it joins the current version (`is_current`); with
`.as_of(day)` it joins the version valid on that day. `.sum()`, `.avg()` and
`.count()` add measures, `.where()` adds a filter, and
`await query.execute(session)` runs it.

```python
import asyncio
from datetime import date, datetime
from typing import Optional

from sqlmodel import Session, SQLModel, create_engine

from sqldim import DimensionModel, FactModel, Field, SCD2Mixin
from sqldim.core.query import DimensionalQuery, SemanticError


class ProductDim(DimensionModel, SCD2Mixin, table=True):
    __tablename__ = "dim_product_sem"
    __natural_key__ = ["sku"]

    id: Optional[int] = Field(default=None, primary_key=True, surrogate_key=True)
    sku: str
    category: str


class SalesFact(FactModel, table=True):
    __tablename__ = "fact_sales_sem"

    id: Optional[int] = Field(default=None, primary_key=True)
    product_id: int = Field(foreign_key="dim_product_sem.id", dimension=ProductDim)
    amount: float
    unit_price: float = Field(additive=False)


engine = create_engine("sqlite://")
SQLModel.metadata.create_all(engine, tables=[ProductDim.__table__, SalesFact.__table__])
with Session(engine) as session:
    session.add(ProductDim(id=1, sku="A", category="Toys", valid_from=datetime(2023, 1, 1),
                           valid_to=datetime(2024, 6, 1), is_current=False, checksum="x"))
    session.add(ProductDim(id=2, sku="A", category="Games", valid_from=datetime(2024, 6, 1),
                           is_current=True, checksum="y"))
    session.add(SalesFact(product_id=1, amount=10.0, unit_price=5.0))
    session.add(SalesFact(product_id=2, amount=7.0, unit_price=7.0))
    session.commit()

    current = DimensionalQuery(SalesFact).by(ProductDim.category).sum(SalesFact.amount)
    print(asyncio.run(current.execute(session)))  # [('Games', 7.0)]
    past = (
        DimensionalQuery(SalesFact)
        .by(ProductDim.category)
        .sum(SalesFact.amount)
        .as_of(date(2024, 1, 1))
    )
    print(asyncio.run(past.execute(session)))     # [('Toys', 10.0)]
    try:
        DimensionalQuery(SalesFact).sum(SalesFact.unit_price)
    except SemanticError as exc:
        print("refused:", exc)  # refused: Cannot SUM a non-additive measure. …
```

## What it checks, and what it does not

- **SUM of a non-additive measure** is refused with `SemanticError` when the
  column is declared `Field(additive=False)`. Nothing else about additivity
  is enforced: `avg()` accepts any column.
- **The point-in-time filter** applies to the joins made for `.by()`
  attributes, on `valid_from <= day < valid_to`. It compares with the fact's
  dimension key only; it does not read an event date from the fact.
- **Not implemented:** grain or fan-trap validation, and role-playing
  dimensions (two foreign keys to the same dimension). The DGM query layer
  (`DGMQuery`, see [Dimensional Graph Model](../features/dimensional_graph_model.md))
  has its own typing rules.

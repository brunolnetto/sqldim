# Pattern: Hybrid SCD (Typed Columns + JSON)

Most products of a `Product` dimension share `name` and `sku`, but a few carry
their own attributes (`fabric_density` for textiles, `octane_rating` for
fuel). A hundred nullable columns is messy; a JSON blob alone loses typing
and easy filtering.

## The hybrid model

Keep the dense attributes as typed columns and the sparse ones in a JSON
`properties` column. When `properties` is a tracked column, a change inside
it creates a new SCD2 version. Declare the JSON column with SQLModel's
`Field(sa_column=...)`: sqldim's `Field` adds column metadata of its own and
cannot be combined with `sa_column`.

```python
import asyncio
from typing import Any, Optional

from sqlmodel import JSON, Column, Field, Session, SQLModel, create_engine, select

from sqldim import DimensionModel, SCD2Mixin, SCDHandler


class ProductDim(DimensionModel, SCD2Mixin, table=True):
    __tablename__ = "dim_product_hybrid"
    __natural_key__ = ["sku"]

    id: Optional[int] = Field(default=None, primary_key=True)
    sku: str
    name: str
    properties: dict[str, Any] = Field(
        default_factory=dict, sa_column=Column(JSON, info={"scd": 2})
    )


engine = create_engine("sqlite://")
SQLModel.metadata.create_all(engine, tables=[ProductDim.__table__])
with Session(engine) as session:
    handler = SCDHandler(ProductDim, session, track_columns=["name", "properties"])
    load = lambda props: asyncio.run(  # noqa: E731
        handler.process([{"sku": "T-1", "name": "Tent", "properties": props}])
    )
    load({"fabric_density": 210, "colour": "green"})
    same = load({"colour": "green", "fabric_density": 210})  # keys reordered
    changed = load({"fabric_density": 300, "colour": "green"})
    print(same.unchanged, changed.versioned)  # 1 1
    rows = session.exec(select(ProductDim).order_by(ProductDim.id)).all()
    print([(r.properties["fabric_density"], r.is_current) for r in rows])
    # [(210, False), (300, True)]
```

## Why this works

1. **Deterministic hashing.** The checksum serialises JSON with sorted keys,
   so reordering the keys of `properties` is not a change.
2. **Typed core, flexible rest.** The typed columns can be indexed and
   filtered as usual; what you index inside the JSON (a GIN index on
   PostgreSQL, for example) is your database's choice, not sqldim's.
3. **New attributes do not break the load.** A key that appears in the source
   JSON is a change to `properties`, not a schema change.

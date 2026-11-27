# Getting Started with sqldim

This tutorial builds a small star schema: it defines the models, loads data,
records a slowly-changing dimension change and queries the result. Every block
below runs as written, in order, in one Python session. The test
`tests/test_doc_examples.py` runs them.

## What you'll build

A small e-commerce data warehouse with:
- **User** — a slowly-changing dimension (SCD Type 2)
- **Product** — a second SCD Type 2 dimension
- **Order** — a transaction fact linking users and products

## 1. Install sqldim

```bash
pip install sqldim
```

The core install includes SQLModel, DuckDB and Narwhals. This tutorial stores
the tables in SQLite through SQLModel, so it needs no other database.

## 2. Define your models

```python
from typing import Optional

from sqldim import DimensionModel, FactModel, SCD2Mixin, Field


class User(DimensionModel, SCD2Mixin, table=True):
    """Customer dimension — tracks plan changes over time."""
    __tablename__ = "dim_user"
    __natural_key__ = ["email"]

    id: Optional[int] = Field(default=None, primary_key=True, surrogate_key=True)
    email: str
    name: str
    plan_tier: str          # free, pro, enterprise — tracked by SCD2
    country: str


class Product(DimensionModel, SCD2Mixin, table=True):
    """Product dimension — a price change is a new version too."""
    __tablename__ = "dim_product"
    __natural_key__ = ["sku"]

    id: Optional[int] = Field(default=None, primary_key=True, surrogate_key=True)
    sku: str
    name: str
    category: str
    price: float


class Order(FactModel, table=True):
    """Transaction fact — one row per order."""
    __tablename__ = "fact_order"
    __natural_key__ = ["order_id"]

    id: Optional[int] = Field(default=None, primary_key=True)
    order_id: str
    user_id: int = Field(foreign_key="dim_user.id", dimension=User)
    product_id: int = Field(foreign_key="dim_product.id", dimension=Product)
    quantity: int
    amount: float
    ordered_at: str
```

### What just happened?

- **`DimensionModel`** and **`FactModel`** are SQLModel tables with sqldim's
  schema metadata. The surrogate key is the `id` column you declare.
- **`SCD2Mixin`** adds `valid_from`, `valid_to`, `is_current` and `checksum`.
  A change to a tracked attribute creates a new row instead of updating the
  old one.
- **`__natural_key__`** names the column(s) that identify an entity. They are
  used for change detection and surrogate-key resolution.
- **`Field(..., dimension=User)`** marks a fact column as a foreign key to a
  dimension.

## 3. Load data

```python
import asyncio

from sqlmodel import Session, SQLModel, create_engine
from sqldim import DimensionalLoader

engine = create_engine("sqlite:///ecommerce.db")
SQLModel.metadata.create_all(
    engine, tables=[User.__table__, Product.__table__, Order.__table__]
)

with Session(engine) as session:
    loader = DimensionalLoader(session, models=[User, Product, Order])
    loader.register(User, source=[
        {"email": "alice@example.com", "name": "Alice", "plan_tier": "free",  "country": "US"},
        {"email": "bob@example.com",   "name": "Bob",   "plan_tier": "pro",   "country": "UK"},
    ])
    loader.register(Product, source=[
        {"sku": "WR-001", "name": "Wireless Mouse",  "category": "Electronics", "price": 29.99},
        {"sku": "KB-001", "name": "Mech Keyboard",   "category": "Electronics", "price": 89.99},
    ])
    loader.register(Order, source=[
        {"order_id": "ORD-1", "user_id": "alice@example.com", "product_id": "WR-001", "quantity": 1, "amount": 29.99, "ordered_at": "2026-01-15"},
        {"order_id": "ORD-2", "user_id": "bob@example.com",   "product_id": "KB-001", "quantity": 1, "amount": 89.99, "ordered_at": "2026-01-16"},
        {"order_id": "ORD-3", "user_id": "alice@example.com", "product_id": "KB-001", "quantity": 2, "amount": 179.98, "ordered_at": "2026-01-20"},
    ], key_map={
        "user_id":    (User,    "email"),
        "product_id": (Product, "sku"),
    })
    asyncio.run(loader.run())
```

### What happens during `loader.run()`?

1. **Dimensions first.** Every registered dimension is loaded before any
   fact. This is not a dependency analysis: dimensions always come first.
2. **SCD processing.** Each User row is compared by checksum with the current
   version of the same natural key.
3. **Surrogate-key resolution.** `key_map` maps each fact column to a
   dimension and natural key. `user_id = "alice@example.com"` becomes the `id`
   of Alice's current version.
4. **Fact insert.** The resolved fact rows are inserted.

## 4. See the SCD in action

Load the users again with Alice's plan changed:

```python
with Session(engine) as session:
    loader = DimensionalLoader(session, models=[User])
    loader.register(User, source=[
        {"email": "alice@example.com", "name": "Alice", "plan_tier": "pro",  "country": "US"},  # upgraded
        {"email": "bob@example.com",   "name": "Bob",   "plan_tier": "pro",  "country": "UK"},  # unchanged
    ])
    asyncio.run(loader.run())
```

## 5. Query your star schema

```python
from sqlalchemy import text

with Session(engine) as session:
    for row in session.exec(text(
        "SELECT id, email, plan_tier, valid_to IS NULL AS open_ended, is_current "
        "FROM dim_user ORDER BY id"
    )):
        print(row)
    for row in session.exec(text("""
        SELECT u.name, u.plan_tier, SUM(o.amount) AS total_spent
        FROM fact_order o
        JOIN dim_user u ON o.user_id = u.id
        GROUP BY u.name, u.plan_tier
        ORDER BY u.name
    """)):
        print(row)
```

It prints:

```text
(1, 'alice@example.com', 'free', 0, 0)
(2, 'bob@example.com', 'pro', 1, 1)
(3, 'alice@example.com', 'pro', 1, 1)
('Alice', 'free', 209.97)
('Bob', 'pro', 89.99)
```

Alice has two rows: version 1 (`free`) is closed (`valid_to` set,
`is_current = 0`) and version 3 (`pro`) is current. Bob was unchanged, so no
new row was created. Alice's orders still point at version 1, the version
that was current when they were loaded, so the revenue query reports them
under `free`.

## Next steps

Now that you have the basics, explore:

- **[Hybrid SCD Pattern](./patterns/hybrid_scd.md)** — mix typed columns with versioned JSONB for flexible metadata
- **[Vectorized ETL](./guides/vectorized_etl.md)** — use Narwhals/DataFrames for million-row loads instead of dicts
- **[Dual-Paradigm Graph](./patterns/dual_paradigm.md)** — traverse your star schema as a graph
- **[Data Contracts](./features/data_contracts.md)** — enforce schema rules, SLAs, and freshness checks
- **[Medallion Architecture](./features/medallion_layers.md)** — orchestrate Bronze → Silver → Gold pipelines
- **[Capabilities](./reference/capabilities.md)** — what the DGM layer implements today

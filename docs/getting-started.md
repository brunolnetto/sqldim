# Getting Started with sqldim

This tutorial walks you through building a complete star schema from scratch: defining models, loading data, and understanding what happens under the hood.

## What you'll build

A simple e-commerce data warehouse with:
- **User** — a slowly-changing dimension (SCD Type 2)
- **Product** — a reference dimension
- **Order** — a transaction fact linking users and products

## 1. Install sqldim

```bash
pip install sqldim
```

This installs the core: SQLModel, DuckDB, Narwhals, and Alembic. You don't need to configure anything — DuckDB works out of the box as a file-based database.

## 2. Define your models

Create a file called `models.py`:

```python
from sqlmodel import Field as SQLField
from sqldim import DimensionModel, FactModel, SCD2Mixin, Field

class User(DimensionModel, SCD2Mixin, table=True):
    """Customer dimension — tracks plan changes over time."""
    __tablename__ = "dim_user"
    __natural_key__ = ["email"]

    email: str
    name: str
    plan_tier: str          # free, pro, enterprise — tracked by SCD2
    country: str

class Product(DimensionModel, table=True):
    """Product reference dimension — no versioning needed."""
    __tablename__ = "dim_product"
    __natural_key__ = ["sku"]

    sku: str
    name: str
    category: str
    price: float

class Order(FactModel, table=True):
    """Transaction fact — one row per order."""
    __tablename__ = "fact_order"
    __natural_key__ = ["order_id"]

    order_id: str
    user_email: str         # foreign key → User.email
    product_sku: str        # foreign key → Product.sku
    quantity: int
    amount: float
    ordered_at: str
```

### What just happened?

- **`DimensionModel`** adds surrogate key (`id`), grain validation, and schema metadata
- **`SCD2Mixin`** adds `valid_from`, `valid_to`, `is_current`, and `checksum` columns — any change to a User's attributes creates a new row instead of updating in place
- **`FactModel`** adds grain validation for facts
- **`__natural_key__`** tells sqldim which column(s) uniquely identify each row — this is used for change detection and surrogate key resolution

## 3. Load data

Create a file called `load.py`:

```python
from sqlmodel import Session, create_engine
from models import User, Product, Order
from sqldim import DimensionalLoader

engine = create_engine("duckdb:///ecommerce.duckdb")

with Session(engine) as session:
    loader = DimensionalLoader(session, models=[User, Product, Order])

    # Load dimensions first (or let topological ordering handle it)
    loader.register(User, source=[
        {"email": "alice@example.com", "name": "Alice", "plan_tier": "free",  "country": "US"},
        {"email": "bob@example.com",   "name": "Bob",   "plan_tier": "pro",   "country": "UK"},
    ])

    loader.register(Product, source=[
        {"sku": "WR-001", "name": "Wireless Mouse",  "category": "Electronics", "price": 29.99},
        {"sku": "KB-001", "name": "Mech Keyboard",   "category": "Electronics", "price": 89.99},
    ])

    # Load facts — key_map tells sqldim how to resolve foreign keys
    loader.register(Order, source=[
        {"order_id": "ORD-1", "user_email": "alice@example.com", "product_sku": "WR-001", "quantity": 1, "amount": 29.99, "ordered_at": "2026-01-15"},
        {"order_id": "ORD-2", "user_email": "bob@example.com",   "product_sku": "KB-001", "quantity": 1, "amount": 89.99, "ordered_at": "2026-01-16"},
        {"order_id": "ORD-3", "user_email": "alice@example.com", "product_sku": "KB-001", "quantity": 2, "amount": 179.98, "ordered_at": "2026-01-20"},
    ], key_map={
        "user_email":  (User,    "email"),
        "product_sku": (Product, "sku"),
    })

    await loader.run()
```

### What happens during `loader.run()`?

1. **Topological sort** — sqldim detects that `Order` depends on `User` and `Product`, so dimensions load first
2. **SCD processing** — for each User row, it computes a checksum of the tracked columns and compares against existing rows. If Alice upgrades from "free" to "pro" next load, a new row is inserted and the old one is closed (`valid_to` set, `is_current = FALSE`)
3. **Surrogate key resolution** — for each Order, `user_email` and `product_sku` are replaced with the actual `id` values from the dimension tables
4. **Fact insert** — the resolved fact rows are inserted

## 4. See the SCD in action

Run the loader again with Alice's plan changed:

```python
loader.register(User, source=[
    {"email": "alice@example.com", "name": "Alice", "plan_tier": "pro",  "country": "US"},  # upgraded!
    {"email": "bob@example.com",   "name": "Bob",   "plan_tier": "pro",  "country": "UK"},  # no change
])

await loader.run()
```

Now query `dim_user`:

| id | email | name | plan_tier | country | valid_from | valid_to | is_current |
|----|-------|------|-----------|---------|------------|----------|------------|
| 1  | alice@example.com | Alice | free | US | 2026-01-15 | 2026-01-17 | FALSE |
| 2  | bob@example.com | Bob | pro | UK | 2026-01-16 | 9999-12-31 | TRUE |
| 3  | alice@example.com | Alice | pro | US | 2026-01-17 | 9999-12-31 | TRUE |

Alice has two rows — her full history is preserved. Bob was unchanged, so no new row was created.

## 5. Query your star schema

```python
from sqlmodel import select

with Session(engine) as session:
    # Total revenue by user (current version only)
    stmt = """
        SELECT u.name, u.plan_tier, SUM(o.amount) as total_spent
        FROM fact_order o
        JOIN dim_user u ON o.user_email = u.email
        WHERE u.is_current = TRUE
        GROUP BY u.name, u.plan_tier
    """
    result = session.exec(stmt)
    for row in result:
        print(row)
```

## Next steps

Now that you have the basics, explore:

- **[Hybrid SCD Pattern](./patterns/hybrid_scd.md)** — mix typed columns with versioned JSONB for flexible metadata
- **[Vectorized ETL](./guides/vectorized_etl.md)** — use Narwhals/DataFrames for million-row loads instead of dicts
- **[Dual-Paradigm Graph](./patterns/dual_paradigm.md)** — traverse your star schema as a graph (e.g., "find all users who bought products in the same category")
- **[Data Contracts](./features/data_contracts.md)** — enforce schema rules, SLAs, and freshness checks
- **[Medallion Architecture](./features/medallion_layers.md)** — orchestrate Bronze → Silver → Gold pipelines

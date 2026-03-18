# sqldim

> SQL-first dimensional modelling for Python — star schemas meet graph traversals.

`sqldim` lets you define Kimball-style dimensions and facts once, then use them as both **relational tables** (for BI) and **graph structures** (for network analysis) — all powered by vectorized ETL under the hood.

No row-by-row loops. No separate graph database. Just SQLModel classes and DataFrames.

## Why sqldim?

If you've ever built a star schema in Python, you know the pain: SCD logic scattered across scripts, row-by-row change detection that crawls on large datasets, and graph queries that need a whole separate database. `sqldim` solves all three:

| Problem | sqldim approach |
|---------|----------------|
| Slow SCD processing | Vectorized change detection via Narwhals (Polars/Pandas/DuckDB) |
| Manual load ordering | Topological dependency resolution — dimensions always load before facts |
| Graph needs separate DB | Dual-paradigm: dimensions → vertices, facts → edges, traversed via Recursive CTEs |
| Messy incremental loads | Purpose-built loaders: Cumulative, Bitmask, Activity, Edge Projection |

## Installation

```bash
# Core (DuckDB + Narwhals included)
pip install sqldim

# With streaming sources
pip install "sqldim[streaming]"

# Specific extras
pip install "sqldim[kafka]"       # Kafka source
pip install "sqldim[kinesis]"      # Kinesis source
pip install "sqldim[cdc]"          # Change Data Capture via Kafka
```

Requires **Python 3.11+**.

## Quick Start

```python
from sqlmodel import Session, create_engine
from sqldim import DimensionModel, FactModel, SCD2Mixin, DimensionalLoader

# 1. Define a slowly-changing dimension
class User(DimensionModel, SCD2Mixin, table=True):
    __natural_key__ = ["email"]
    email: str
    name: str
    plan_tier: str

# 2. Define a transaction fact
class Order(FactModel, table=True):
    __natural_key__ = ["order_id"]
    order_id: str
    user_email: str      # resolved against User.email
    amount: float
    ordered_at: str

# 3. Load data — topological order handled automatically
engine = create_engine("duckdb:///my_dw.duckdb")

with Session(engine) as session:
    loader = DimensionalLoader(session, models=[User, Order])

    loader.register(User, source=[
        {"email": "alice@example.com", "name": "Alice", "plan_tier": "free"},
        {"email": "bob@example.com",   "name": "Bob",   "plan_tier": "pro"},
    ])

    loader.register(Order, source=[
        {"order_id": "ORD-1", "user_email": "alice@example.com", "amount": 29.99, "ordered_at": "2026-01-15"},
        {"order_id": "ORD-2", "user_email": "bob@example.com",   "amount": 49.99, "ordered_at": "2026-01-16"},
    ], key_map={"user_email": (User, "email")})

    await loader.run()
```

That's it — `User` gets full SCD Type 2 tracking (valid_from, valid_to, is_current), `Order` has its foreign key resolved to User's surrogate key, and dimensions always load before facts.

## What else can it do?

- **🔄 SCD Types 1, 2, 3, 6** — pick the right strategy per attribute, or mix them with the [Hybrid SCD pattern](./docs/patterns/hybrid_scd.md)
- **🕸️ Graph traversals** — project dimensions as vertices and facts as edges, then traverse with `TraversalEngine` using Recursive CTEs ([Dual-Paradigm guide](./docs/patterns/dual_paradigm.md))
- **🧊 Open table formats** — sink to Apache Iceberg and Delta Lake natively
- **⏳ Semantic time-travel** — fluent query builder with automatic `AS OF` join resolution
- **📊 Bitmask retention** — L7/L28 activity tracking in 32-bit integers ([guide](./docs/guides/vectorized_etl.md))
- **🏗️ Medallion architecture** — Bronze → Silver → Gold orchestration with `MedallionRegistry`
- **✅ Data contracts** — schema enforcement, SLA checks, and freshness rules ([spec](./docs/features/data_contracts.md))

## Documentation

| Topic | Link |
|-------|------|
| **Getting Started** | [Tutorial](./docs/getting-started.md) |
| Architecture overview | [Overview](./docs/architecture/overview.md) |
| Lazy loaders (DuckDB) | [Guide](./docs/guides/lazy_loaders.md) |
| Vectorized ETL deep-dive | [Guide](./docs/guides/vectorized_etl.md) |
| Dual-paradigm graph modelling | [Pattern](./docs/patterns/dual_paradigm.md) |
| Hybrid SCD pattern | [Pattern](./docs/patterns/hybrid_scd.md) |
| **Fact types** (Kimball) | [Reference](./docs/reference/fact_types.md) |
| **Sinks** | [Reference](./docs/reference/sinks.md) |
| **CLI** | [Reference](./docs/reference/cli.md) |
| **Exceptions** | [Reference](./docs/reference/exceptions.md) |
| Data contracts & governance | [Spec](./docs/features/data_contracts.md) |
| Medallion layers | [Feature](./docs/features/medallion_layers.md) |
| Big data scaling | [Guide](./docs/guides/big_data.md) |
| Semantic layer | [Guide](./docs/guides/semantic_layer.md) |

## Contributing

We welcome contributions! See [CONTRIBUTING.md](./CONTRIBUTING.md) for setup instructions, coding standards, and the PR workflow.

## License

This project is developed by [Bruno L. Netto](https://github.com/brunolnetto).


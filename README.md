# sqldim

`sqldim` is a high-performance Python toolkit for **Unified Dimensional & Graph Modeling**. Built on top of `SQLModel`, it allows you to define complex Kimball-native structures that power both BI star schemas and network relationship traversals in a single, consistent model.

## Core Pillars

- **🚀 Vectorized ETL**: Powered by **Narwhals**. Handles millions of rows with single-pass joins using Polars, Pandas, or DuckDB backends.
- **🔄 Automated SCD**: Built-in support for Type 1, 2, 3, and 6. Vectorized change detection eliminates row-by-row Python loops.
- **🕸️ Dual-Paradigm Graph**: Project your dimensions as Vertices and facts as Edges. Traverse networks using Recursive CTEs without leaving your SQL database.
- **⏳ Semantic Time-Travel**: Fluent query builder with automatic `AS OF` join resolution for point-in-time analysis.
- **🧊 Open Table Ready**: Natively compatible with Apache Iceberg and Delta Lake via PyArrow/Narwhals integration.

## Key Features

- **Hybrid SCD**: Combine typed columns with versioned JSONB property bags for flexible, heterogeneous metadata.
- **Cumulative History**: Replicate the "Array-of-Structs" pattern for dense longitudinal data (e.g., player seasons).
- **Bitmask Activity**: Ultra-low footprint user retention tracking (L7/L28 metrics) encoded in 32-bit integers.
- **Topological Loading**: Automatic dependency resolution ensures dimensions load before facts.

## Quick Start

```python
from sqldim import DimensionModel, SCD2Mixin, Field

class User(DimensionModel, SCD2Mixin, table=True):
    __natural_key__ = ["email"]
    email: str
    plan_tier: str
    
# Loading is vectorized automatically if you pass a DataFrame
loader.register(User, source=polars_df)
await loader.run()
```

## Documentation
See the `/docs` folder for detailed guides on:
- [Vectorized ETL](./docs/guides/vectorized_etl.md)
- [Dual-Paradigm Graphing](./docs/guides/graph_extension.md)
- [Hybrid SCD Pattern](./docs/patterns/hybrid_scd.md)
- [Bitmask Retention](./docs/index.md)

## Examples
Check out our real-world showcases:
- `examples/nba_analytics/`: Longitudinal career analysis.
- `examples/user_activity/`: High-velocity retention tracking.
- `examples/saas_growth/`: Viral referral networks.

---
🌸 *Built with love by SylphAI*

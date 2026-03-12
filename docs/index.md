# sqldim: Unified Dimensional & Graph Modeling

`sqldim` is a high-performance Python toolkit that bridges the gap between traditional **Kimball Dimensional Modeling** and modern **Graph Analytics**. It allows you to use the same `SQLModel` definitions to power both your BI star schema and your network traversal queries.

## The Pain Points We Solve

1. **The "Two-Schema" Tax**: Usually, you need a SQL DB for reporting and a Graph DB for relationships. `sqldim` gives you both using recursive CTEs on your existing tables.
2. **SCD Hell**: Implementing Slowly Changing Dimensions (Type 2) is error-prone. `sqldim` provides a vectorized, automated SCD engine that handles versioning out of the box.
3. **Rigid Star Schemas**: Traditional dimensions are too flat. Our **Hybrid SCD** allows you to keep core attributes in columns while storing flexible, versioned metadata in JSONB.
4. **Point-in-Time Join Logic**: Writing `AS OF` joins manually is a nightmare. Our **Semantic Query Layer** handles historical state resolution automatically.

## Core Pillars

- [**Pillar 1: Dimensional Core**](./guides/dimensional_core.md) - SCD Type 1, 2, 3, and 6 implementation.
- [**Pillar 2: Graph Extension**](./guides/graph_extension.md) - Vertex/Edge projection and Recursive CTE traversal.
- [**Pillar 3: Vectorized ETL (Narwhals)**](./guides/vectorized_etl.md) - Blazing fast loading using Polars, Pandas, DuckDB, or PySpark.
- [**Pillar 4: Semantic Layer**](./guides/semantic_layer.md) - Fluent query builder with automatic point-in-time resolution.
- [**Pillar 5: Open Table Formats**](./guides/iceberg_support.md) - Native integration with Apache Iceberg and Delta Lake.

## Pattern Catalog
- [Hybrid SCD (Columns + JSONB)](./patterns/hybrid_scd.md)
- [The Dual-Paradigm Model (Dim + Vertex)](./patterns/dual_paradigm.md)
- [Junk Dimensions for Sparse Metadata](./patterns/junk_dimensions.md)

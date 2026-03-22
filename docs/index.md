# sqldim: Unified Dimensional & Graph Modeling

`sqldim` is a high-performance Python toolkit that bridges **Kimball Dimensional
Modeling** and **Graph Analytics** using the same `SQLModel` definitions to power
both BI star schemas and network traversal queries.

## Navigation

| Section | Contents |
|---|---|
| **[Getting Started](./getting-started.md)** | New-user tutorial — install, define models, load data |
| [Motivation](./motivation/dimensional_modelling.md) | Why dimensional modelling and DGM — conceptual foundations |
| [Architecture](./architecture/overview.md) | Design choices — Medallion, Kimball, graph projection, quality design |
| [Features](./features/data_contracts.md) | Per-feature reference: contracts, observability, DGM query algebra, graph, medallion, notifications |
| **[Guides](./guides/lazy_loaders.md)** | How-to guides: lazy loaders, vectorised ETL, semantic layer |
| **[Reference](./reference/api-surface.md)** | Full public API surface, fact types, sinks, CLI, exceptions, datasets |
| [Patterns](./patterns/dual_paradigm.md) | Good practices: hybrid SCD, dual-paradigm modelling |
| [Development](./development/theoretical.md) | Theory, deep research, design rationale |

---

## The Pain Points We Solve

1. **The "Two-Schema" Tax** — `sqldim` gives you SQL reporting AND graph relationships
   from the same schema using recursive CTEs.
2. **SCD Hell** — vectorised, automated SCD engine handles Types 1, 2, 3, and 6.
3. **Rigid Star Schemas** — Hybrid SCD stores flexible versioned metadata in JSONB
   alongside typed columns.
4. **Point-in-Time Join Logic** — the Semantic Query Layer resolves `AS OF` joins
   automatically.

---

## Core Pillars

- **Dimensional Core** — SCD Types 1–6, surrogate-key resolution, grain validation.
- **Dimensional Graph Model (DGM)** — three-band query algebra (`B1 ∘ B2? ∘ B3?`) unifying dimensional analysis with graph modelling.
  See [features/dimensional_graph_model.md](./features/dimensional_graph_model.md).
- **Graph Extension** — vertex/edge projection, recursive-CTE traversal, `SchemaGraph`.
- **Vectorised ETL** — Narwhals adapters for Polars, Pandas, DuckDB, PySpark.
  See [guides/vectorized_etl.md](./guides/vectorized_etl.md).
- **Semantic Layer** — fluent query builder with automatic point-in-time resolution.
  See [guides/semantic_layer.md](./guides/semantic_layer.md).
- **Open Table Formats** — Apache Iceberg and Delta Lake sinks and sources.
  See [guides/big_data.md](./guides/big_data.md).

---

## Pattern Catalog

- [Hybrid SCD (Columns + JSONB)](./patterns/hybrid_scd.md)
- [The Dual-Paradigm Model (Dim + Vertex)](./patterns/dual_paradigm.md)

---

## Motivation

- [Dimensional Modelling](./motivation/dimensional_modelling.md) — Kimball foundations and why star schemas matter
- [Dimensional Graph Modelling](./motivation/dimensional_graph_modelling.md) — how DGM unifies analytics and graph traversal

---

## Reference Index

| Reference | Description |
|---|---|
| [API Surface](./reference/api-surface.md) | Every public name exported from `sqldim` |
| [Fact Types](./reference/fact_types.md) | Transaction, snapshot, accumulating, cumulative, activity |
| [SCD Processors](./reference/scd_processors.md) | SCD engine internals and configuration |
| [Sinks](./reference/sinks.md) | DuckDB, PostgreSQL, Parquet, Delta, MotherDuck, Iceberg |
| [Sources](./reference/sources.md) | SourceAdapter and built-in adapters |
| [Loader](./reference/loader.md) | DimensionalLoader and lazy loader variants |
| [Dimensions](./reference/dimensions.md) | DateDimension, TimeDimension, prebuilt helpers |
| [Schema Graph](./reference/schema_graph.md) | FK-graph introspection |
| [Lineage](./reference/lineage.md) | Column-level lineage tracking |
| [Session](./reference/session.md) | AsyncDimensionalSession |
| [Config](./reference/config.md) | SqldimConfig and environment variables |
| [Exceptions](./reference/exceptions.md) | Full exception hierarchy |
| [CLI](./reference/cli.md) | Command-line interface |
| [Example Datasets](./reference/datasets.md) | BaseSource, Dataset, domain catalogue |

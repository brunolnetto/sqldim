# sqldim: Unified Dimensional & Graph Modeling

`sqldim` is a high-performance Python toolkit that bridges **Kimball Dimensional
Modeling** and **Graph Analytics** using the same `SQLModel` definitions to power
both BI star schemas and network traversal queries.

## Navigation

| Section | Contents |
|---|---|
| **[Getting Started](./getting-started.md)** | New-user tutorial — install, define models, load data |
| [Architecture](./architecture/overview.md) | Design choices — Medallion, Kimball, graph projection, quality design |
| [Features](./features/data_contracts.md) | Per-feature reference: contracts, observability, DGM query algebra, graph, medallion, notifications |
| **[Guides](./guides/lazy_loaders.md)** | How-to guides: lazy loaders, vectorised ETL, semantic layer |
| **[Reference](./reference/fact_types.md)** | API reference: fact types, sinks, CLI, exceptions |
| [Guides (big data)](./guides/big_data.md) | How-to: big-data scaling, vectorised ETL, semantic layer |
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

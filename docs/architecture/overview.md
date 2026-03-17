# Architecture Overview

`sqldim` is built around two orthogonal ideas that share the same SQLModel schema:

* **Kimball Dimensional Modeling** — star/snowflake schemas, SCDs, and surrogate keys for BI and analytical workloads.
* **Graph Analytics** — the same tables projected as directed graphs with recursive-CTE traversal for relationship queries.

---

## Medallion Layers

Data flows through three ordered tiers.  Each dataset belongs to exactly one layer and may only be promoted to the immediately next one.

```
BRONZE → SILVER → GOLD
```

| Layer  | Content                                               | Build Order (Silver only)              |
|--------|-------------------------------------------------------|----------------------------------------|
| Bronze | Raw ingested data — append-only, schema-on-read       | —                                      |
| Silver | Cleansed, conformed, SCD-managed dimensional models   | DIMENSION → FACT → BRIDGE → GRAPH      |
| Gold   | Business-ready aggregates and semantic views          | —                                      |

See [`features/medallion_layers.md`](../features/medallion_layers.md) for the full API and `SilverBuildOrder`.

---

## Dimensional Modeling

`sqldim` follows Kimball's four-step design process:

1. **Grain declaration** — `FactModel.__grain__` names the atomic event.
2. **Dimension identification** — `DimensionModel` subclasses with `__natural_key__`.
3. **Fact measurement** — additive, semi-additive, or non-additive columns on `FactModel`.
4. **Bridge tables** — `BridgeModel` for many-to-many joins with allocation weights.

### SCD Types

| Type | Class / Mixin      | Behaviour                                                  |
|------|--------------------|------------------------------------------------------------|
| 1    | `SCD1` (default)   | Overwrite changed attributes in place                      |
| 2    | `SCD2Mixin`        | Expire existing row, insert new version (`valid_from/to`)  |
| 3    | `SCD3Mixin`        | Keep previous value in an alternate column                 |
| 6    | `SCD2Mixin` + JSONB| Full history in versioned rows + flexible metadata sidecar |

---

## Graph Projection

Any `DimensionModel` is automatically a **vertex**; any `FactModel` that references two dimensions is automatically an **edge**.  `SchemaGraph` exposes the full graph without a separate store.

```
SchemaGraph(dimensions=[CustomerDim, ProductDim], facts=[OrderFact])
# gives you: vertices = {CustomerDim, ProductDim}
#            edges    = {OrderFact → (CustomerDim, ProductDim)}
```

`TraversalEngine` runs depth-first / breadth-first walks as recursive CTEs directly inside DuckDB.

See [`features/dependency_graph.md`](../features/dependency_graph.md) for the full traversal API.

---

## Data Quality Design

Quality is enforced at multiple levels, each with a distinct responsibility:

| Layer            | Mechanism                          | When                        |
|------------------|------------------------------------|-----------------------------|
| Schema contracts | `DataContract` / `ColumnSpec`      | At ingestion / layer-promotion |
| SQL validation   | `ContractEngine` + `Rule` classes  | On every pipeline run       |
| Quality gates    | `QualityGate`                      | As a pipeline step-guard    |
| Observability    | `ObservabilityCollector` / spans   | Continuous runtime metrics  |
| Notifications    | `NotificationRouter` + channels    | On threshold breaches       |

See [`features/data_contracts.md`](../features/data_contracts.md) and [`features/observability.md`](../features/observability.md).

---

## Storage I/O

All writes go through a `Sink` that speaks `INSERT/MERGE` SQL; all reads go through a `Source` that returns a lazy view name (not data).  The Python process is a coordinator — data stays in the engine.

```
Source (SQL reference) → Processor (SQL transform) → Sink (SQL write)
```

Supported sinks: DuckDB, MotherDuck, PostgreSQL, Parquet, Delta Lake, Apache Iceberg.
Supported sources: DuckDB, PostgreSQL, CSV, Parquet, Delta, Iceberg, Kafka, Kinesis, DLT.

---

## Package Domain Map

```
sqldim/
├── core/                  # Base models, mixins, fields, SCD foundations
│   └── dimensions/        # Prebuilt dimension tables (Date, Time, Junk)
├── graph/                 # Graph models, SchemaGraph, traversal
├── scd/                   # SCD handlers, detection, audit, backfill
│   └── processors/        # Narwhals + lazy DuckDB SCD processors
├── loaders/               # Dimensional, accumulating, snapshot loaders
├── sources/               # Data source adapters
├── sinks/                 # Storage sink adapters
├── contracts/             # Data contracts engine (schema + SQL validation)
├── observability/         # Spans, metrics, collector
├── notifications/         # Event routing and alerting channels
├── medallion/             # Layer enum, SilverBuildOrder
├── migrations/            # Schema migration generation and guards
└── query/                 # Fluent semantic query builder
```

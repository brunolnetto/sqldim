# Reference: Public API Surface

All names below are importable directly from `sqldim`:

```python
from sqldim import DimensionModel, SCD2Mixin, DimensionalLoader, DuckDBSink
```

Sub-packages (`sqldim.core.*`, `sqldim.sinks`, etc.) are also importable
directly but the root module is the stable, versioned surface.

---

## Core Schema

| Name | Kind | Description |
|---|---|---|
| `Field` | class | SQLModel field factory with dimensional metadata |
| `DimensionModel` | class | Base class for dimension tables |
| `FactModel` | class | Base class for fact tables |
| `BridgeModel` | class | Base class for bridge (M:N resolution) tables |
| `TransactionFact` | class | Transaction-grain fact table |
| `PeriodicSnapshotFact` | class | Periodic-snapshot fact table |
| `AccumulatingFact` | class | Accumulating-snapshot fact table |
| `CumulativeFact` | class | Cumulative metrics fact table |
| `ActivityFact` | class | Activity-stream fact table |
| `SCD2Mixin` | mixin | Adds `valid_from`/`valid_to`/`is_current` SCD2 columns |
| `SCD3Mixin` | mixin | Adds previous-value SCD3 columns |
| `DatelistMixin` | mixin | Adds a date-list column for multi-date grain |
| `CumulativeMixin` | mixin | Adds cumulative aggregation bookkeeping columns |
| `SchemaGraph` | class | Kimball schema graph (FK traversal) |
| `backfill_scd2` | function | Backfill SCD2 effective dates from snapshot data |
| `DWSchema` | class | Orchestrates dimension + fact table creation |

---

## Graph Extension

| Name | Kind | Description |
|---|---|---|
| `VertexModel` | class | Base class for vertex (node) tables |
| `EdgeModel` | class | Base class for directed-edge tables |
| `Vertex` | class | Lightweight vertex reference |
| `GraphModel` | class | Combined vertex + edge schema |
| `TraversalEngine` | class | Recursive-CTE graph traversal |
| `GraphSchemaGraph` | class | Graph-specific schema topology |
| `GraphSchema` | class | Graph schema descriptor |

---

## Narwhals / Vectorised ETL

| Name | Kind | Description |
|---|---|---|
| `NarwhalsAdapter` | class | Adapts Polars / Pandas / DuckDB frames for SCD processing |
| `NarwhalsSCDProcessor` | class | Vectorised SCD2 processor via Narwhals |
| `NarwhalsHashStrategy` | class | Row-hash change-detection strategy |
| `col` | function | Column reference for `TransformPipeline` |
| `TransformPipeline` | class | Composable column-transform pipeline |
| `backfill_scd2_narwhals` | function | Narwhals SCD2 backfill variant |

---

## Lazy Loaders

| Name | Kind | Description |
|---|---|---|
| `LazyTransactionLoader` | class | Transaction-grain incremental loader |
| `LazySnapshotLoader` | class | Periodic-snapshot incremental loader |
| `LazyAccumulatingLoader` | class | Accumulating-snapshot loader |
| `LazyCumulativeLoader` | class | Cumulative-metrics loader |
| `LazyBitmaskLoader` | class | Bitmask-encoded metrics loader |
| `LazyArrayMetricLoader` | class | Array-metric loader |
| `LazyEdgeProjectionLoader` | class | Edge-projection loader |

---

## Eager Loaders (Batch Utilities)

| Name | Kind | Description |
|---|---|---|
| `CumulativeLoader` | class | Eager cumulative load helper |
| `BitmaskerLoader` | class | Eager bitmask load helper |
| `ArrayMetricLoader` | class | Eager array-metric load helper |
| `EdgeProjectionLoader` | class | Eager edge-projection load helper |

---

## SCD Engine

| Name | Kind | Description |
|---|---|---|
| `SCDHandler` | class | Low-level SCD merge/upsert handler |
| `SCDResult` | class | Result of an SCD merge operation |
| `DimensionalLoader` | class | High-level dimension loader (wraps `SCDHandler`) |
| `SKResolver` | class | Surrogate-key resolution helper |

---

## Sinks

| Name | Kind | Description |
|---|---|---|
| `SinkAdapter` | class | Abstract base for all sinks |
| `DuckDBSink` | class | In-process DuckDB sink |
| `PostgreSQLSink` | class | PostgreSQL sink |
| `ParquetSink` | class | Parquet file sink |
| `DeltaLakeSink` | class | Delta Lake table sink |
| `MotherDuckSink` | class | MotherDuck (cloud DuckDB) sink |
| `IcebergSink` | class | Apache Iceberg sink |

---

## Sources

| Name | Kind | Description |
|---|---|---|
| `SourceAdapter` | class | Abstract base for all sources |
| `coerce_source` | function | Normalise a raw dataframe or path to a `SourceAdapter` |
| `ParquetSource` | class | Parquet file source |
| `CSVSource` | class | CSV file source |
| `DuckDBSource` | class | DuckDB table/query source |
| `PostgreSQLSource` | class | PostgreSQL table/query source |
| `DeltaSource` | class | Delta Lake table source |
| `SQLSource` | class | Arbitrary SQL string source |
| `StreamSourceAdapter` | class | Abstract streaming source |
| `StreamResult` | class | Result container for streaming reads |
| `CSVStreamSource` | class | CSV file streaming source |
| `ParquetStreamSource` | class | Parquet streaming source |

---

## Session

| Name | Kind | Description |
|---|---|---|
| `AsyncDimensionalSession` | class | Async context manager for batched dimensional writes |

---

## Config

| Name | Kind | Description |
|---|---|---|
| `SqldimConfig` | class | Runtime configuration (memory limits, spill thresholds, etc.) |

---

## Medallion

| Name | Kind | Description |
|---|---|---|
| `Layer` | enum | `BRONZE` / `SILVER` / `GOLD` medallion layers |
| `MedallionRegistry` | class | Registers models per layer and builds Silver in dependency order |
| `ModelKind` | enum | Dimension / Fact / Bridge |
| `SilverBuildOrder` | class | Resolved topological build order |

---

## DGM Query Algebra

| Name | Kind | Description |
|---|---|---|
| `DGMQuery` | class | Three-band query builder (B1 ∘ B2? ∘ B3?) |
| `PropRef` | class | Property reference for B1 filter predicates |
| `AggRef` | class | Aggregation reference for B2 HAVING predicates |
| `WinRef` | class | Window-function reference for B3 QUALIFY predicates |
| `ScalarPred` | class | Scalar equality / comparison predicate |
| `PathPred` | class | Graph-path membership predicate |
| `AND` | class | Logical conjunction of predicates |
| `OR` | class | Logical disjunction of predicates |
| `NOT` | class | Logical negation of a predicate |
| `VerbHop` | class | Hop via a named verb edge in the DGM |
| `BridgeHop` | class | Hop via a bridge table in the DGM |
| `Compose` | class | Composes two `DGMQuery` instances |

---

## Exceptions

| Name | Inherits | Raised when |
|---|---|---|
| `SqldimError` | `Exception` | Base class for all sqldim errors |
| `SchemaError` | `SqldimError` | Model definition is invalid |
| `GrainViolationError` | `SqldimError` | Fact rows violate declared grain |
| `NaturalKeyError` | `SqldimError` | Natural-key constraint violated |
| `SCDError` | `SqldimError` | SCD merge / handler failure |
| `DestructiveOperationError` | `SqldimError` | Prevented destructive DDL |
| `LoadError` | `SqldimError` | Loader failure |
| `SKResolutionError` | `SqldimError` | Surrogate-key lookup failed |
| `IdempotencyError` | `SqldimError` | Duplicate load detected |
| `MigrationError` | `SqldimError` | Schema migration failure |
| `DestructiveMigrationError` | `MigrationError` | Migration would drop data |
| `SemanticError` | `SqldimError` | Semantic-layer query construction error |
| `InvalidJoinError` | `SqldimError` | Join path is invalid |
| `GrainCompatibilityError` | `SqldimError` | Incompatible grains in join |
| `TransformTypeError` | `SqldimError` | Type mismatch in transform pipeline |

---

## Example Datasets (separate package)

`sqldim.examples.datasets` is **not** part of the core API but ships alongside
it for use in tests and tutorials.  Key public names:

| Name | Import path | Description |
|---|---|---|
| `Dataset` | `sqldim.examples.datasets` | FK-ordered multi-source dataset |
| `BaseSource` | `sqldim.examples.datasets.base` | Single-table OLTP adapter base class |
| `DatasetFactory` | `sqldim.examples.datasets.base` | Registry for sources and datasets |

See [reference/datasets.md](./datasets.md) for the full datasets reference.

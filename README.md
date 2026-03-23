# sqldim

> SQL-first dimensional modelling for Python — star schemas, graph traversals, and a full data-quality stack in one library.

`sqldim` lets you define Kimball-style dimensions and facts once, then use them as **relational tables** (for BI), **graph structures** (for network analysis), and **contract-governed pipeline layers** (for data quality) — all powered by vectorized ETL under the hood.

No row-by-row loops. No separate graph database. No third-party orchestrator required.

## Why sqldim?

| Problem | sqldim approach |
|---------|----------------|
| Slow SCD processing | Vectorized change detection via [Narwhals](https://narwhals-dev.github.io/) (Polars / Pandas / DuckDB) |
| Manual load ordering | Topological dependency resolution — dimensions always load before facts |
| Graph needs a separate DB | Dual-paradigm: dimensions → vertices, facts → edges, traversed via Recursive CTEs |
| Messy incremental loads | Purpose-built lazy loaders: Snapshot, Accumulating, Cumulative, Bitmask, Edge Projection |
| Ad-hoc data quality | Schema contracts, SLA checks, freshness rules, and quality gates at every layer boundary |
| Scattered observability | OTel-compatible tracing + metrics + P1–P4 alert routing built in |

## Installation

```bash
# Core (SQLModel, DuckDB, Narwhals included)
pip install sqldim

# Streaming sources (Kafka + Kinesis + CDC)
pip install "sqldim[streaming]"

# Individual extras
pip install "sqldim[kafka]"       # Kafka & CDC sources (confluent-kafka + Polars)
pip install "sqldim[kinesis]"     # Kinesis source (boto3 + Polars)
pip install "sqldim[cdc]"         # Change Data Capture via Debezium/Kafka
pip install "sqldim[otel]"        # OpenTelemetry exporter
pip install "sqldim[lineage]"     # OpenLineage data lineage bus
```

Requires **Python 3.11+**.

## Quick Start

```python
import asyncio
from sqlmodel import create_engine
from sqldim import DimensionModel, FactModel, SCD2Mixin, DimensionalLoader, AsyncDimensionalSession

# 1. Define a slowly-changing dimension
class User(DimensionModel, SCD2Mixin, table=True):
    __tablename__ = "dim_user"
    __natural_key__ = ["email"]
    email: str
    name: str
    plan_tier: str   # "free" → "pro" creates a new SCD2 row

# 2. Define a transaction fact
class Order(FactModel, table=True):
    __tablename__ = "fact_order"
    __natural_key__ = ["order_id"]
    order_id: str
    user_email: str   # resolved to User surrogate key automatically
    amount: float
    ordered_at: str

# 3. Load data — topological order handled automatically
engine = create_engine("duckdb:///my_dw.duckdb")

async def main():
    async with AsyncDimensionalSession(engine) as session:
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

asyncio.run(main())
```

`User` gets full SCD Type 2 tracking (`valid_from`, `valid_to`, `is_current`). `Order` has its foreign key resolved to User's surrogate key. Dimensions always load before facts.

## Feature Overview

### SCD Engine
- **Types 1, 2, 3, 6** — pick the right strategy per attribute, or combine them ([Hybrid SCD](./docs/patterns/hybrid_scd.md))
- Vectorized change detection via `NarwhalsSCDProcessor` — no Python loops over rows
- Lazy loaders (`LazyTransactionLoader`, `LazySnapshotLoader`, `LazyAccumulatingLoader`, `LazyCumulativeLoader`, `LazyBitmaskLoader`, `LazyArrayMetricLoader`, `LazyEdgeProjectionLoader`) stay in DuckDB memory until materialization
- `SKResolver` for surrogate-key resolution across dimension boundaries

### Graph Extension
- Project any star schema as a directed graph: dimensions → vertices, facts → edges
- `TraversalEngine` executes path queries via Recursive CTEs — no separate graph DB
- `DGMQuery` — typed three-band query algebra (`WHERE → GROUP BY/HAVING → QUALIFY`) with `PropRef`, `AggRef`, `WinRef`, `VerbHop`, `BridgeHop` ([DGM docs](./docs/features/dimensional_graph_model.md))

### Sources
| Source | Class |
|--------|-------|
| Parquet | `ParquetSource` / `ParquetStreamSource` |
| CSV | `CSVSource` / `CSVStreamSource` |
| DuckDB table/query | `DuckDBSource` |
| PostgreSQL | `PostgreSQLSource` |
| Delta Lake | `DeltaSource` |
| Arbitrary SQL | `SQLSource` |
| Kafka | `KafkaSource` |
| Kinesis | `KinesisSource` |
| CDC (Debezium) | `DebeziumSource` |
| dlt pipelines | `DltSource` |

### Sinks
| Sink | Class |
|------|-------|
| DuckDB (in-process) | `DuckDBSink` |
| MotherDuck (cloud) | `MotherDuckSink` |
| PostgreSQL | `PostgreSQLSink` |
| Parquet | `ParquetSink` |
| Delta Lake | `DeltaLakeSink` |
| Apache Iceberg | `IcebergSink` |

### Data Quality & Governance
- **Data contracts** — `DataContract` + `ContractEngine` enforce schema, null rates, value ranges, SCD2 invariants, freshness, and row-count deltas ([spec](./docs/features/data_contracts.md))
- **Quality gates** — `QualityGate` / `GateResult` block promotion between medallion layers on contract breach
- **SLA tracking** — `SLASpec` + `ContractVersion` for schema evolution with `ChangeKind` diff reporting

### Medallion Architecture
- `MedallionRegistry` registers models per `Layer` (Bronze / Silver / Gold)
- `SilverBuildOrder` resolves topological load order across layers
- Quality gates between Bronze→Silver and Silver→Gold with configurable pass/fail policies

### Observability & Notifications
- `OTelCollector` — OTel-compatible tracing (`start_span`) and metrics (`record_metric`) across all pipeline stages (`[otel]` extra)
- `NotificationRouter` — P1–P4 severity routing to Slack, PagerDuty, Jira, Email, or webhooks ([spec](./docs/features/notifications.md))
- Data lineage emission via `[lineage]` extra (OpenLineage-compatible)

### Other
- **Semantic time-travel** — fluent `AS OF` join resolution via the semantic layer ([guide](./docs/guides/semantic_layer.md))
- **Bitmask retention** — L7/L28/L90 activity flags packed in 32-bit integers ([guide](./docs/guides/vectorized_etl.md))
- **CLI** — `sqldim` command for schema inspection, migration, and contract validation ([reference](./docs/reference/cli.md))
- **Config** — `SqldimConfig` for memory limits, spill thresholds, and batch sizes ([reference](./docs/reference/config.md))

## Documentation

### Tutorials & Guides
| Topic | Link |
|-------|------|
| Getting started | [Tutorial](./docs/getting-started.md) |
| Lazy loaders (DuckDB-first) | [Guide](./docs/guides/lazy_loaders.md) |
| Vectorized ETL deep-dive | [Guide](./docs/guides/vectorized_etl.md) |
| Semantic layer & time-travel | [Guide](./docs/guides/semantic_layer.md) |
| Big data scaling | [Guide](./docs/guides/big_data.md) |

### Patterns
| Topic | Link |
|-------|------|
| Dual-paradigm (relational + graph) | [Pattern](./docs/patterns/dual_paradigm.md) |
| Hybrid SCD (mixed strategies) | [Pattern](./docs/patterns/hybrid_scd.md) |

### Features
| Topic | Link |
|-------|------|
| Data contracts & governance | [Spec](./docs/features/data_contracts.md) |
| Dimensional Graph Model (DGM) | [Spec](./docs/features/dimensional_graph_model.md) |
| Medallion layers | [Spec](./docs/features/medallion_layers.md) |
| Observability (OTel) | [Spec](./docs/features/observability.md) |
| Notifications & alerting | [Spec](./docs/features/notifications.md) |
| Feature dependency graph | [Diagram](./docs/features/dependency_graph.md) |

### Reference
| Topic | Link |
|-------|------|
| **Full API surface** | [Reference](./docs/reference/api-surface.md) |
| Dimensions & model base classes | [Reference](./docs/reference/dimensions.md) |
| Fact types (Kimball) | [Reference](./docs/reference/fact_types.md) |
| Dimensional loader | [Reference](./docs/reference/loader.md) |
| SCD processors | [Reference](./docs/reference/scd_processors.md) |
| Schema graph | [Reference](./docs/reference/schema_graph.md) |
| Sinks | [Reference](./docs/reference/sinks.md) |
| Sources | [Reference](./docs/reference/sources.md) |
| Session | [Reference](./docs/reference/session.md) |
| CLI | [Reference](./docs/reference/cli.md) |
| Config | [Reference](./docs/reference/config.md) |
| Lineage | [Reference](./docs/reference/lineage.md) |
| Exceptions | [Reference](./docs/reference/exceptions.md) |
| Example datasets | [Reference](./docs/reference/datasets.md) |

### Architecture
| Topic | Link |
|-------|------|
| System overview | [Overview](./docs/architecture/overview.md) |
| SCD engine design | [Design](./docs/architecture/scd-engine-design.md) |
| Schema evolution policy | [Design](./docs/architecture/schema-evolution-policy.md) |
| Graph analytics roadmap | [Roadmap](./docs/architecture/graph-analytics-roadmap.md) |
| Recursive hierarchy dimensions | [Design](./docs/architecture/recursive-hierarchy-dimensions.md) |
| Late-arriving dimensions | [Design](./docs/architecture/late-arriving-dimensions.md) |
| Physical partitioning & bucketing | [Design](./docs/architecture/physical-partitioning-and-bucketing.md) |
| Analytical indexing strategy | [Design](./docs/architecture/analytical-indexing-strategy.md) |
| Benchmark suite | [Design](./docs/architecture/benchmark-suite.md) |

## Contributing

We welcome contributions! See [CONTRIBUTING.md](./CONTRIBUTING.md) for setup instructions, coding standards, and the PR workflow.

## License

This project is developed by [Bruno L. Netto](https://github.com/brunolnetto).


# sqldim

> SQL-first dimensional modelling for Python — star schemas, graph traversals, and a full data-quality stack in one library.

`sqldim` lets you define Kimball-style dimensions and facts **once**, then use them as:

- **Relational tables** — BI-ready star schemas with surrogate keys, SCD versioning, and grain validation.
- **Graph structures** — project any star schema as a vertex/edge graph and traverse it via Recursive CTEs.
- **Contract-governed pipeline layers** — enforce schema, freshness, value ranges, and row-count SLAs at every Bronze→Silver→Gold boundary.

**No row-by-row Python loops. No separate graph database. No third-party orchestrator required.**

---

## Why sqldim?

| Pain point | How sqldim solves it |
|---|---|
| Slow SCD processing | Vectorized change detection via [Narwhals](https://narwhals-dev.github.io/) across Polars, Pandas, and DuckDB |
| Manual load ordering | Topological dependency resolution — dimensions always load before facts |
| Graph requires separate DB | Dual-paradigm: dim → vertex, fact → edge, traversed with Recursive CTEs |
| Messy incremental loads | Purpose-built lazy loaders stay in DuckDB until materialization |
| Ad-hoc data quality | Schema contracts, freshness rules, and quality gates at every layer boundary |
| Complex multi-CTE queries | Question Algebra with semiring optimisation — minimum-CTE SQL emission |
| Scattered observability | OTel-compatible tracing + P1–P4 alert routing built in |

---

## Installation

```bash
# Core (SQLModel, DuckDB, Narwhals included)
pip install sqldim

# Streaming sources (Kafka + Kinesis + CDC)
pip install "sqldim[streaming]"

# Individual extras
pip install "sqldim[kafka]"       # Kafka & CDC (confluent-kafka + Polars)
pip install "sqldim[kinesis]"     # Kinesis (boto3 + Polars)
pip install "sqldim[cdc]"         # Change Data Capture via Debezium/Kafka
pip install "sqldim[otel]"        # OpenTelemetry exporter
pip install "sqldim[lineage]"     # OpenLineage data lineage bus
```

Requires **Python 3.11+**.

---

## Quick Start

```python
import asyncio
from sqlmodel import create_engine
from sqldim import (
    DimensionModel, FactModel, SCD2Mixin,
    DimensionalLoader, AsyncDimensionalSession,
)

# 1. Define a slowly-changing dimension (SCD Type 2)
class User(DimensionModel, SCD2Mixin, table=True):
    __tablename__ = "dim_user"
    __natural_key__ = ["email"]
    email: str
    name: str
    plan_tier: str   # "free" → "pro" creates a new SCD2 row automatically

# 2. Define a transaction fact
class Order(FactModel, table=True):
    __tablename__ = "fact_order"
    __natural_key__ = ["order_id"]
    order_id: str
    user_email: str   # resolved to User surrogate key automatically
    amount: float
    ordered_at: str

# 3. Load — topological order resolved automatically
engine = create_engine("duckdb:///my_dw.duckdb")

async def main():
    async with AsyncDimensionalSession(engine) as session:
        loader = DimensionalLoader(session, models=[User, Order])

        loader.register(User, source=[
            {"email": "alice@example.com", "name": "Alice", "plan_tier": "free"},
            {"email": "bob@example.com",   "name": "Bob",   "plan_tier": "pro"},
        ])
        loader.register(Order, source=[
            {"order_id": "ORD-1", "user_email": "alice@example.com",
             "amount": 29.99, "ordered_at": "2026-01-15"},
            {"order_id": "ORD-2", "user_email": "bob@example.com",
             "amount": 49.99, "ordered_at": "2026-01-16"},
        ], key_map={"user_email": (User, "email")})

        await loader.run()

asyncio.run(main())
```

`User` gets full SCD Type 2 tracking (`valid_from`, `valid_to`, `is_current`).
`Order` has its foreign key resolved to User's surrogate key.
Dimensions always load first — no manual ordering required.

---

## Feature Overview

### SCD Engine

- **SCD Types 1, 2, 3, and 6** — pick the right strategy per attribute, or mix strategies in a single model ([Hybrid SCD](./docs/patterns/hybrid_scd.md))
- Vectorized change detection via `NarwhalsSCDProcessor` — no Python loops over rows
- `DimensionalLoader` resolves surrogate-key lookups across dimension boundaries via `SKResolver`
- Seven lazy loaders stay in DuckDB memory until you explicitly materialize:

| Loader | Use case |
|---|---|
| `LazyTransactionLoader` | Transaction-grain incremental loads |
| `LazySnapshotLoader` | Periodic snapshots (e.g. daily balance) |
| `LazyAccumulatingLoader` | Accumulating snapshots (pipeline stages) |
| `LazyCumulativeLoader` | Running totals / watermarks |
| `LazyBitmaskLoader` | L7/L28/L90 activity flags in 32-bit integers |
| `LazyArrayMetricLoader` | Array-encoded metric histories |
| `LazyEdgeProjectionLoader` | Graph edge projection into fact tables |

### Dimensional Graph Model (DGM)

A complete typed query algebra that unifies dimensional analysis and graph traversal:

#### Single-query layer — `DGMQuery`

A three-band query model maps directly to the SQL evaluation pipeline:

```
Q = B1 (context / WHERE)  ∘  B2? (aggregation / HAVING)  ∘  B3? (window / QUALIFY)
```

```python
from sqldim.core.query.dgm import DGMQuery, PropRef, AggRef, VerbHop, ScalarPred

q = (
    DGMQuery()
    .anchor("sale_fact", "s")
    .path_join(VerbHop("s", "placed", "c",
                       table="customer_dim",
                       on="c.customer_id = s.customer_id"))
    .where(ScalarPred(PropRef("s", "year"), "=", 2024))
    .group_by("c.region")
    .agg(total_rev="SUM(s.revenue)", n="COUNT(*)")
    .having(ScalarPred(AggRef("n"), ">=", 3))
)
print(q.to_sql())
```

#### Question Algebra — `QuestionAlgebra`

Compose multiple `DGMQuery` instances into a CTE chain using five operators —
`UNION`, `INTERSECT`, `EXCEPT`, `WITH`, and `JOIN`:

```python
from sqldim.core.query.dgm import QuestionAlgebra, ComposeOp, DGMPredicateBDD, BDDManager

alg = QuestionAlgebra()
alg.add("retail",  DGMQuery().anchor("sale_fact", "s").where(retail_pred))
alg.add("premium", DGMQuery().anchor("sale_fact", "s").where(premium_pred))
alg.compose("retail", ComposeOp.UNION, "premium", name="all_sales")

# Minimise: eliminate redundant CTEs via semiring laws
bdd = DGMPredicateBDD(BDDManager())
minimal = alg.minimize(bdd)
print(minimal.to_sql(final="all_sales"))
```

#### Query optimisation pipeline

| Step | Function | What it does |
|---|---|---|
| Semiring minimisation | `alg.minimize(bdd)` | Eliminates `Q ∪ Q`, `Q ∩ Q`, absorbed/subsumed CTEs |
| Common Sub-expression Elimination | `apply_cse(alg, bdd)` | Shares identical `WHERE` predicates into `__cse_*` CTEs |

The minimiser is a **tight lower bound**: the resulting CTE count equals the minimum
number of CTEs needed to represent the same query family.

#### Graph operations — `TraversalEngine`

- Project any star schema as a directed graph (dim → vertex, fact → edge)
- `TraversalEngine` executes multi-hop path queries via Recursive CTEs
- `VerbHop`, `BridgeHop`, `PathPred`, `Compose` for declarative path navigation

### Sources

| Source | Class |
|---|---|
| Parquet file | `ParquetSource` / `ParquetStreamSource` |
| CSV file | `CSVSource` / `CSVStreamSource` |
| DuckDB table / query | `DuckDBSource` |
| PostgreSQL | `PostgreSQLSource` |
| Delta Lake | `DeltaSource` |
| Arbitrary SQL | `SQLSource` |
| Kafka | `KafkaSource` |
| Kinesis | `KinesisSource` |
| CDC (Debezium) | `DebeziumSource` |
| dlt pipelines | `DltSource` |

### Sinks

| Sink | Class |
|---|---|
| DuckDB (in-process) | `DuckDBSink` |
| MotherDuck (cloud DuckDB) | `MotherDuckSink` |
| PostgreSQL | `PostgreSQLSink` |
| Parquet | `ParquetSink` |
| Delta Lake | `DeltaLakeSink` |
| Apache Iceberg | `IcebergSink` |

### Data Quality & Governance

- **Data contracts** — `DataContract` + `ContractEngine` enforce schema, null rates, value ranges, SCD2 invariants, freshness, and row-count deltas ([spec](./docs/features/data_contracts.md))
- **Quality gates** — `QualityGate` / `GateResult` block medallion-layer promotion on contract breach
- **SLA tracking** — `SLASpec` + `ContractVersion` for schema evolution with `ChangeKind` diff reporting

### Medallion Architecture

```
Bronze  →  Silver  →  Gold
              ↑           ↑
          QualityGate  QualityGate
```

- `MedallionRegistry` registers models per `Layer` (`BRONZE` / `SILVER` / `GOLD`)
- `SilverBuildOrder` resolves topological load order across all layers automatically
- Quality gates at both Bronze→Silver and Silver→Gold boundaries

### Observability & Notifications

- `OTelCollector` — OTel-compatible `start_span` + `record_metric` at every pipeline stage (`[otel]` extra)
- `NotificationRouter` — P1–P4 severity routing to Slack, PagerDuty, Jira, Email, or webhooks ([spec](./docs/features/notifications.md))
- Data lineage emission via `[lineage]` extra (OpenLineage-compatible events)

### Other Capabilities

| Feature | Details |
|---|---|
| Semantic time-travel | Fluent `AS OF` join resolution via the semantic layer ([guide](./docs/guides/semantic_layer.md)) |
| Bitmask retention | L7/L28/L90 activity flags packed in 32-bit integers ([guide](./docs/guides/vectorized_etl.md)) |
| CLI | `sqldim` command for schema inspection, migration, and contract validation ([reference](./docs/reference/cli.md)) |
| Config | `SqldimConfig` for memory limits, spill thresholds, and batch sizes ([reference](./docs/reference/config.md)) |

---

## Documentation

### Tutorials & Guides

| Topic | Link |
|---|---|
| Getting started | [Tutorial](./docs/getting-started.md) |
| Lazy loaders (DuckDB-first) | [Guide](./docs/guides/lazy_loaders.md) |
| Vectorized ETL deep-dive | [Guide](./docs/guides/vectorized_etl.md) |
| Semantic layer & time-travel | [Guide](./docs/guides/semantic_layer.md) |
| Big data scaling | [Guide](./docs/guides/big_data.md) |

### Patterns

| Topic | Link |
|---|---|
| Dual-paradigm (relational + graph) | [Pattern](./docs/patterns/dual_paradigm.md) |
| Hybrid SCD (mixed strategies) | [Pattern](./docs/patterns/hybrid_scd.md) |

### Features

| Topic | Link |
|---|---|
| Dimensional Graph Model (DGM) | [Spec](./docs/features/dimensional_graph_model.md) |
| Data contracts & governance | [Spec](./docs/features/data_contracts.md) |
| Medallion layers | [Spec](./docs/features/medallion_layers.md) |
| Observability (OTel) | [Spec](./docs/features/observability.md) |
| Notifications & alerting | [Spec](./docs/features/notifications.md) |
| Feature dependency graph | [Diagram](./docs/features/dependency_graph.md) |

### Reference

| Topic | Link |
|---|---|
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
|---|---|
| System overview | [Overview](./docs/architecture/overview.md) |
| SCD engine design | [Design](./docs/architecture/scd-engine-design.md) |
| Schema evolution policy | [Design](./docs/architecture/schema-evolution-policy.md) |
| Graph analytics roadmap | [Roadmap](./docs/architecture/graph-analytics-roadmap.md) |
| Recursive hierarchy dimensions | [Design](./docs/architecture/recursive-hierarchy-dimensions.md) |
| Late-arriving dimensions | [Design](./docs/architecture/late-arriving-dimensions.md) |
| Physical partitioning & bucketing | [Design](./docs/architecture/physical-partitioning-and-bucketing.md) |
| Analytical indexing strategy | [Design](./docs/architecture/analytical-indexing-strategy.md) |
| Benchmark suite | [Design](./docs/architecture/benchmark-suite.md) |

---

## Contributing

We welcome contributions! See [CONTRIBUTING.md](./CONTRIBUTING.md) for setup instructions, coding standards, and the PR workflow.

## License

This project is developed by [Bruno L. Netto](https://github.com/brunolnetto).


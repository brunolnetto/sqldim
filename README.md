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
| Manual load ordering | Dimension-first loading — every registered dimension loads before any fact, and fact keys resolve to surrogate keys |
| Graph requires separate DB | Dual-paradigm: dim → vertex, fact → edge, traversed with Recursive CTEs |
| Messy incremental loads | Purpose-built lazy loaders stay in DuckDB until materialization |
| Ad-hoc data quality | Schema contracts, freshness rules, and quality gates at every layer boundary |
| Complex multi-CTE queries | Question Algebra composes queries into one CTE chain and removes redundant CTEs (set laws; see the caveat below) |
| Scattered observability | OTel-compatible tracing + P1–P4 alert routing to channels you implement |

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
from typing import Optional

from sqlmodel import Session, SQLModel, create_engine, select
from sqldim import DimensionModel, DimensionalLoader, FactModel, Field, SCD2Mixin


# 1. Define a slowly-changing dimension (SCD Type 2)
class User(DimensionModel, SCD2Mixin, table=True):
    __tablename__ = "dim_user"
    __natural_key__ = ["email"]
    id: Optional[int] = Field(default=None, primary_key=True)
    email: str
    name: str
    plan_tier: str


# 2. Define a transaction fact whose foreign key points at the dimension
class Order(FactModel, table=True):
    __tablename__ = "fact_order"
    __natural_key__ = ["order_id"]
    id: Optional[int] = Field(default=None, primary_key=True)
    order_id: str
    user_id: int = Field(foreign_key="dim_user.id", dimension=User)
    amount: float


engine = create_engine("sqlite://")
SQLModel.metadata.create_all(engine, tables=[User.__table__, Order.__table__])

with Session(engine) as session:
    # 3. Load — dimensions are loaded before facts
    loader = DimensionalLoader(session, models=[User, Order])
    loader.register(User, [
        {"email": "alice@example.com", "name": "Alice", "plan_tier": "free"},
        {"email": "bob@example.com", "name": "Bob", "plan_tier": "pro"},
    ])
    loader.register(Order, [
        {"order_id": "ORD-1", "user_id": "alice@example.com", "amount": 29.99},
        {"order_id": "ORD-2", "user_id": "bob@example.com", "amount": 49.99},
    ], key_map={"user_id": (User, "email")})  # natural key → surrogate key
    asyncio.run(loader.run())

    for order in session.exec(select(Order)):
        print(order.order_id, order.user_id)
```

This prints `ORD-1 1` and `ORD-2 2`: each order's `user_id` holds the surrogate
key of the user version it was resolved to. `User` gets SCD Type 2 tracking
(`valid_from`, `valid_to`, `is_current`): loading `plan_tier="pro"` for Alice
later closes her current row and inserts a new version. `DimensionalLoader`
loads every registered dimension before any fact. The example uses SQLite
through SQLModel; [Getting started](./docs/getting-started.md) walks through
it step by step.

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

A typed query algebra that unifies dimensional analysis and graph traversal.
The specification is a draft ([v0.27](./docs/motivation/dimensional_graph_modelling.md)),
and the implementation covers part of it. What is implemented, per construct
and per backend, is on the generated [capability page](./docs/reference/capabilities.md).
Its evidence is on three generated pages:

- the [semantic conformance suite](./docs/reference/dgm-conformance.md): cases
  from the specification, run against an independent reference oracle;
- the [backend matrix](./docs/reference/dgm-backend-matrix.md): DuckDB and
  PostgreSQL through DuckDB are executed in CI; MotherDuck, Delta and Iceberg
  storage are not executed; Cypher, SPARQL and native PostgreSQL SQL have no
  emitter and fail closed;
- the [claims register](./docs/reference/dgm-claims.md): every theorem with
  the strength of its evidence.

A construct outside what is implemented raises a typed error
(`UnsupportedConstruct`, `UnsupportedCapability`). It is never approximated or
routed to another backend.

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
from sqldim.core.query.dgm import DGMQuery, QuestionAlgebra, ComposeOp, PropRef, ScalarPred
from sqldim.core.query.dgm.bdd import BDDManager, DGMPredicateBDD

retail_pred = ScalarPred(PropRef("s", "channel"), "=", "retail")
premium_pred = ScalarPred(PropRef("s", "tier"), "=", "premium")

alg = QuestionAlgebra()
alg.add("retail",  DGMQuery().anchor("sale_fact", "s").where(retail_pred))
alg.add("premium", DGMQuery().anchor("sale_fact", "s").where(premium_pred))
alg.compose("retail", ComposeOp.UNION, "premium", name="all_sales")

# Minimise: eliminate redundant CTEs by the elimination laws
bdd = DGMPredicateBDD(BDDManager())
minimal = alg.minimize(bdd)
print(minimal.to_sql(final="all_sales"))
```

#### Query optimisation pipeline

| Step | Function | What it does |
|---|---|---|
| Semiring minimisation | `alg.minimize(bdd)` | Eliminates `Q ∪ Q`, `Q ∩ Q`, absorbed/subsumed CTEs |
| Common Sub-expression Elimination | `apply_cse(alg, bdd)` | Shares identical `WHERE` predicates into `__cse_*` CTEs |

The resulting CTE count is minimal *within the canonical form*: no rewrite by
these rules gives fewer CTEs. It is not a lower bound over every equivalent
query (§8.14), and the soundness of the canonical form (`CLM-8.14-CANON`) is
proved in the specification but not independently evidenced. The laws are set
laws, and `UNION`, `INTERSECT` and `EXCEPT` emit the SQL set operations, so a
minimised algebra returns the rows of the original.

#### Natural-language interface — demo only

`sqldim nl ask`, `nl chat`, `nl showtime` and `nl evals` are demonstrations,
not a capability (D47, [ADR 0013](./docs/adr/0013-explicit-demo-mode.md)). The
§11.10 NL pipeline is not implemented yet (#143). A data question is answered
only by a heuristic path (keyword planner, template or LLM-written SQL), which
runs only with `--demo` and marks every result as demo. Without `--demo`, the
NL pipeline reports `NOT_IMPLEMENTED`. Most supervisor tools are unavailable
and fail closed; the [capability page](./docs/reference/capabilities.md) lists
which ones are available and what each missing one needs.

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

DuckDB, PostgreSQL and Parquet writes are executed against real storage in
CI. MotherDuck, Delta Lake and Iceberg writes are checked only as captured
SQL or write plans, and Delta rejects `MERGE`. See the
[backend matrix](./docs/reference/dgm-backend-matrix.md).

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
- `NotificationRouter` — P1–P4 severity routing through `NotificationRule`s to channels you implement on `NotificationChannel`; sqldim ships only the in-memory `MemoryChannel`, with no Slack, PagerDuty, Jira, email or webhook channel ([spec](./docs/features/notifications.md))
- Data lineage emission via `[lineage]` extra (OpenLineage-compatible events)

### Other Capabilities

| Feature | Details |
|---|---|
| Semantic time-travel | Fluent `AS OF` join resolution via the semantic layer ([guide](./docs/guides/semantic_layer.md)) |
| Bitmask retention | L7/L28/L90 activity flags packed in 32-bit integers ([guide](./docs/guides/vectorized_etl.md)) |
| CLI | `sqldim exercise` runs the examples and benchmarks; `sqldim nl` is a demonstration (demo only); `sqldim ops` prints migration and schema guidance only ([reference](./docs/reference/cli.md)) |
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
| Dimensional Graph Model (DGM) | [Feature](./docs/features/dimensional_graph_model.md) · [Specification (draft)](./docs/motivation/dimensional_graph_modelling.md) |
| Data contracts & governance | [Spec](./docs/features/data_contracts.md) |
| Medallion layers | [Spec](./docs/features/medallion_layers.md) |
| Observability (OTel) | [Spec](./docs/features/observability.md) |
| Notifications & alerting | [Spec](./docs/features/notifications.md) |
| Feature dependency graph | [Diagram](./docs/features/dependency_graph.md) |

### Reference

| Topic | Link |
|---|---|
| **What is implemented (generated)** | [Capabilities](./docs/reference/capabilities.md) |
| DGM semantic conformance (generated) | [Reference](./docs/reference/dgm-conformance.md) |
| DGM backend matrix (generated) | [Reference](./docs/reference/dgm-backend-matrix.md) |
| DGM claims register (generated) | [Reference](./docs/reference/dgm-claims.md) |
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


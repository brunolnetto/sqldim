# Feature: Data Contracts

Data Contracts enforce schema rules, quality checks, and SLAs at layer promotion boundaries. Every dataset crossing a layer boundary must satisfy its contract before being promoted.

## Quick Start

```python
from sqldim.contracts import DataContract, ColumnSpec, SLASpec, QualityGate
from sqldim.contracts.rules import NotNull, NoDuplicates, NullRate, Freshness
from sqldim.contracts.layer import Layer

# Define a contract
contract = DataContract(
    name="orders",
    version="2.1.0",
    owner="data-platform",
    layer=Layer.SILVER,
    columns=[
        ColumnSpec("order_id", "uuid"),
        ColumnSpec("customer_id", "uuid"),
        ColumnSpec("amount", "decimal(18,2)"),
        ColumnSpec("created_at", "timestamp"),
    ],
    sla=SLASpec(freshness="< 30m", completeness="> 99.5%"),
)

# Check for breaking changes when evolving
contract.is_breaking_change_from(previous_contract)  # → True/False
```

## Quality Gates

A `QualityGate` enforces checks at a specific layer transition (e.g., Bronze → Silver). It validates rules as DuckDB SQL queries and aggregates results.

```python
from sqldim.contracts import QualityGate
from sqldim.contracts.rules import NotNull, NoDuplicates, NullRate, Freshness
from sqldim.contracts.layer import Layer

gate = QualityGate("bronze_to_silver", Layer.BRONZE, Layer.SILVER)

# Add built-in rules
gate.add_check(NotNull("order_id"))
gate.add_check(NoDuplicates("order_id"))
gate.add_check(NullRate("customer_id", max_pct=0.1))
gate.add_check(Freshness("created_at", max_age_hours=0.5))

# Run the gate against a DuckDB view
result = gate.run(view="bronze.raw_orders")
print(result.ok)       # True if all checks pass
print(result.violations)  # list of failed check details
```

### Built-in Rules

All rules extend `Rule` and implement `.as_sql(view) -> str` to produce DuckDB-native validation queries.

| Rule | Parameters | Severity | SQL Logic |
|---|---|---|---|
| `NotNull(column)` | Column name | ERROR | `WHERE column IS NULL` |
| `NoDuplicates(column)` | Column name | ERROR | `GROUP BY HAVING COUNT(*) > 1` |
| `NullRate(column, max_pct)` | Column, max null percentage | WARNING | Null rate exceeds threshold |
| `Freshness(ts_col, max_age_hours)` | Timestamp column, max age | WARNING | `NOW() - ts_col > interval` |
| `RowCountDelta(baseline, max_pct)` | Expected row count, max deviation | WARNING | Row count drift from baseline |

### Custom Rules

Implement the `Rule` protocol to add domain-specific checks:

```python
from sqldim.contracts import Rule

class ReferentialIntegrity(Rule):
    def __init__(self, fk_col: str, ref_table: str, ref_col: str):
        self.fk_col = fk_col
        self.ref_table = ref_table
        self.ref_col = ref_col

    def as_sql(self, view: str) -> str:
        return f"""
            SELECT '{self.fk_col}' as violation
            FROM {view} f
            LEFT JOIN {self.ref_table} r ON f.{self.fk_col} = r.{self.ref_col}
            WHERE r.{self.ref_col} IS NULL
        """
```

## Layer Promotion Flow

```
Source → [Bronze] --gate: bronze_to_silver-→ [Silver] --gate: silver_to_gold-→ [Gold]
            ↓                                  ↓                                    ↓
      Contract registered                Strictest checks              Business-logic checks
      (permissive)                      (structural + SLA)            (drift + integrity)
```

- **Bronze**: `schema_on_read` is permissive. The contract is registered but enforcement is deferred.
- **Silver**: All structural rules run — null rates, duplicates, schema match, SLA freshness.
- **Gold**: Business-logic rules — referential integrity, statistical drift, completeness.

## Versioning

| Change type | Example | Action |
|---|---|---|
| **Additive** | New nullable column | Minor bump (`2.1.0 → 2.2.0`) |
| **Compatible rename** | Column alias | Minor bump + deprecation notice |
| **Breaking** | Remove column / type change | Major bump (`2.x.x → 3.0.0`) + fork |
| **SLA relaxation** | Raise freshness to 60 min | Minor bump + owner approval |
| **SLA tightening** | Lower latency SLA | Major bump (consumers must re-validate) |

```python
# Detect breaking changes programmatically
new_contract = DataContract(name="orders", version="3.0.0", ...)
if new_contract.is_breaking_change_from(old_contract):
    # Alert consumers, fork dataset, require migration plan
    ...
```

## Lineage

sqldim emits structured lineage events at contract enforcement points (quality gates) and pipeline runs. Lineage is opt-in — install the extra and wire an emitter.

### Event model

```python
from sqldim.lineage import LineageEvent, RunState, DatasetRef

event = LineageEvent(
    run_id="run-abc-123",
    job_name="bronze_to_silver",
    inputs=[DatasetRef(name="bronze.raw_orders", namespace="sqldim")],
    outputs=[DatasetRef(name="silver.orders", namespace="sqldim")],
    state=RunState.COMPLETE,
)
```

| Class | Description |
|---|---|
| `LineageEvent` | Run lifecycle event with `run_id`, `job_name`, `inputs`, `outputs`, `state`, `facets` |
| `RunState` | Enum: `START`, `RUNNING`, `COMPLETE`, `FAIL`, `ABORT` |
| `DatasetRef` | Logical reference to a table/topic with optional namespace and facets |

### Emitters

Two emitters are provided — a zero-dependency console logger and an OpenLineage transmitter:

```python
from sqldim.lineage import ConsoleLineageEmitter, OpenLineageEmitter

# Zero-dep: JSON lines to stderr (default for development)
console = ConsoleLineageEmitter()
console.emit(event)

# OpenLineage: requires pip install sqldim[lineage]
ol = OpenLineageEmitter(url="http://marquez:5000", namespace="sqldim")
ol.emit(event)  # translates to official OpenLineage RunEvent
```

### Quality Gate integration

`QualityGate.run()` automatically emits lineage events when an emitter is provided:

```python
from sqldim.lineage import ConsoleLineageEmitter

gate = QualityGate("bronze_to_silver", Layer.BRONZE, Layer.SILVER)
gate.add_check(NotNull("order_id"))

result = gate.run(
    view="bronze.raw_orders",
    lineage_emitter=ConsoleLineageEmitter(),  # opt-in
    job_name="bronze_to_silver",
    inputs=[DatasetRef(name="bronze.raw_orders")],
    outputs=[DatasetRef(name="silver.orders")],
)
# Emits START → COMPLETE (or FAIL) events automatically
```

### DimensionalLoader integration

`DimensionalLoader.run()` also accepts a `lineage_emitter` to track per-model pipeline runs with input/output dataset references.

### Static metadata

For schema-level documentation (ER diagrams, data dictionaries), use [`SchemaGraph.to_dict()`](../reference/schema_graph.md) and [`SchemaGraph.to_mermaid()`](../reference/schema_graph.md).

## Dependencies

- **[Medallion Layers](medallion_layers.md)** — contracts are anchored to a specific layer (`bronze`, `silver`, `gold`)
- **[Observability](observability.md)** — SLA breach events feed into the OTel metrics pipeline
- **[Notifications](notifications.md)** — contract violations trigger P1 (Gold) or P2 (Silver) alerts
- **[Lineage](../reference/lineage.md)** — structured event emission and OpenLineage integration

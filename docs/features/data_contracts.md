# Feature: Data Contracts

sqldim has three contract pieces. They are separate, and nothing wires them
together for you:

- **Schema contracts** (`DataContract`, `ColumnSpec`, `SLASpec`,
  `ContractVersion`, `ContractRegistry`) describe a dataset and detect
  breaking schema changes. They are metadata: nothing enforces an `SLASpec`
  against data.
- **SQL rules** (`NotNull`, `NoDuplicates`, `NullRate`, `Freshness`, …) are
  run against a DuckDB view by `ContractEngine.validate`, in one round trip.
- **Quality gates** (`QualityGate`) run callables that return a
  `CheckResult` before a promotion between two adjacent layers, and can emit
  lineage events.

## Schema contracts

```python
from sqldim.contracts import ColumnSpec, ContractVersion, DataContract, SLASpec
from sqldim.medallion import Layer

columns = [
    ColumnSpec("order_id", "UUID", nullable=False, primary_key=True),
    ColumnSpec("customer_id", "UUID"),
    ColumnSpec("amount", "DECIMAL(18,2)"),
    ColumnSpec("created_at", "TIMESTAMP"),
]
contract = DataContract(
    name="orders",
    version=ContractVersion.parse("2.1.0"),
    owner="data-platform",
    layer=Layer.SILVER,
    columns=columns,
    sla=SLASpec(freshness_minutes=30, completeness_pct=99.5),
)

# Breaking: a column removed, a dtype changed, or a nullable column made required.
v3 = DataContract(
    name="orders",
    version=ContractVersion.parse("3.0.0"),
    owner="data-platform",
    layer=Layer.SILVER,
    columns=columns[:-1],  # drops created_at
)
print(v3.is_breaking_change_from(contract))              # True
print(contract.version.classify_change(v3.version).value)  # major
print(contract.sla.is_freshness_met(actual_minutes=12))    # True
```

## SQL rules

Each rule implements `.as_sql(view)`, a DuckDB `SELECT` that returns no row
when the rule passes. `ContractEngine.validate` runs every rule of a
contract (`SourceContract`, `StateContract` or `OutputContract`) as one
`UNION ALL`.

```python
import duckdb

from sqldim.contracts import ContractEngine, NoDuplicates, NotNull, NullRate, SourceContract

con = duckdb.connect()
con.execute(
    "CREATE TABLE raw_orders AS SELECT * FROM (VALUES "
    "(1, 10), (2, NULL), (2, 11)) t(order_id, customer_id)"
)
rules = SourceContract(
    rules=[NotNull("order_id"), NoDuplicates("order_id"), NullRate("customer_id", max_pct=0.1)]
)
report = ContractEngine().validate(con, "raw_orders", rules)
print(sorted(v.rule for v in report.violations))  # ['NO_DUPLICATES', 'NULL_RATE']
print(report.has_errors())                          # True
```

| Rule | Parameters | Severity | Fails when |
|---|---|---|---|
| `NotNull(column)` | column | ERROR | a value is NULL |
| `NoDuplicates(column)` | column | ERROR | a value occurs more than once |
| `NullRate(column, max_pct)` | column, maximum NULL fraction | WARNING | the NULL fraction exceeds `max_pct` |
| `Freshness(ts_col, max_age_hours)` | timestamp column, maximum age | WARNING | the newest value is older than `max_age_hours` |
| `RowCountDelta(baseline, max_pct)` | expected row count, maximum deviation | WARNING | the row count drifts from `baseline` by more than `max_pct` |

`TypeMatch`, `ColumnExists`, `RowCount`, `ValueRange`, `RegexMatch` and the
SCD2 rules (`SCD2Invariants`, `NoOrphanVersions`, `MonotonicValidFrom`,
`NoGapPeriods`, `HashConsistency`) follow the same protocol.

### Custom rules

Subclass `Rule`. The `SELECT` must return the columns `rule`, `severity`,
`violations` and `detail`, and no row when the rule passes:

```python
from sqldim.contracts import Rule, Severity


class ReferentialIntegrity(Rule):
    severity = Severity.ERROR

    def __init__(self, fk_col: str, ref_table: str, ref_col: str) -> None:
        self.fk_col, self.ref_table, self.ref_col = fk_col, ref_table, ref_col
        self.name = "REFERENTIAL_INTEGRITY"

    def as_sql(self, view: str) -> str:
        return (
            f"SELECT '{self.name}' AS rule, '{self.severity.value}' AS severity,"
            f" COUNT(*) AS violations, 'column={self.fk_col}' AS detail"
            f" FROM {view} f LEFT JOIN {self.ref_table} r"
            f" ON f.{self.fk_col} = r.{self.ref_col}"
            f" WHERE f.{self.fk_col} IS NOT NULL AND r.{self.ref_col} IS NULL"
            f" HAVING COUNT(*) > 0"
        )
```

## Quality gates

A `QualityGate` runs between two adjacent layers (`BRONZE → SILVER`,
`SILVER → GOLD`, `GOLD → SEMANTIC`); any other pair raises `ValueError`. A
check is a callable that returns a `CheckResult`. `gate.run(**kwargs)` passes
each check the keyword arguments its signature accepts, and records an
exception as a failing check.

```python
from sqldim.contracts import CheckResult, QualityGate
from sqldim.medallion import Layer


def orders_not_empty(con) -> CheckResult:
    n = con.execute("SELECT COUNT(*) FROM raw_orders").fetchone()[0]
    return CheckResult("orders_not_empty", n > 0, f"{n} rows")


def no_rule_errors(con) -> CheckResult:
    found = ContractEngine().validate(con, "raw_orders", rules)
    return CheckResult("no_rule_errors", not found.has_errors())


gate = QualityGate("bronze_to_silver", Layer.BRONZE, Layer.SILVER)
gate.add_check(orders_not_empty).add_check(no_rule_errors)
result = gate.run(con=con)
print(result.ok)                          # False
print([r.name for r in result.failing])   # ['no_rule_errors']
```

Layer policies — what each layer should check, and when to bump a contract's
major or minor version — are conventions for you to apply. sqldim does not
enforce them: `ContractVersion.classify_change` reports the kind of a version
change, and `is_breaking_change_from` reports a breaking schema change.

## Lineage

sqldim emits lineage events from quality gates and from `DimensionalLoader`
when you give them an emitter. Lineage is opt-in.

### Event model

```python
from sqldim.lineage import LineageEvent, RunState
from sqldim.lineage.events import DatasetRef

event = LineageEvent(
    run_id="run-abc-123",
    job_name="bronze_to_silver",
    inputs=[DatasetRef(namespace="sqldim", name="bronze.raw_orders")],
    outputs=[DatasetRef(namespace="sqldim", name="silver.orders")],
    state=RunState.COMPLETE,
)
print(event.to_dict()["eventType"])  # COMPLETE
```

| Class | Description |
|---|---|
| `LineageEvent` | Run lifecycle event with `run_id`, `job_name`, `namespace`, `state`, `inputs`, `outputs`, `facets` |
| `RunState` | Enum: `START`, `RUNNING`, `COMPLETE`, `FAIL`, `ABORT` |
| `DatasetRef` (`sqldim.lineage.events`) | A dataset: `namespace`, `name` and optional `facets` |

### Emitters

`ConsoleLineageEmitter` writes JSON lines to a stream (stderr by default) and
has no dependency. `OpenLineageEmitter(url=..., namespace=...)` sends events
through the OpenLineage client; it needs `pip install sqldim[lineage]` and
raises `ImportError` without it.

```python
import io

from sqldim.lineage import ConsoleLineageEmitter

stream = io.StringIO()
ConsoleLineageEmitter(stream=stream).emit(event)
print('"COMPLETE"' in stream.getvalue())  # True
```

### Quality Gate integration

Pass the emitter to the gate's constructor. `run()` then emits `START`
before the checks and `COMPLETE` or `FAIL` after them, with check counts in
the facets:

```python
stream = io.StringIO()
gate = QualityGate(
    "bronze_to_silver",
    Layer.BRONZE,
    Layer.SILVER,
    lineage_emitter=ConsoleLineageEmitter(stream=stream),
)
gate.add_check(orders_not_empty)
gate.run(con=con)
print(len(stream.getvalue().splitlines()))  # 2: START, then COMPLETE
```

### DimensionalLoader integration

`DimensionalLoader(..., lineage_emitter=...)` emits an event per loaded model.

### Static metadata

For schema-level documentation (ER diagrams, data dictionaries), use [`SchemaGraph.to_dict()`](../reference/schema_graph.md) and [`SchemaGraph.to_mermaid()`](../reference/schema_graph.md).

## Dependencies

- **[Medallion Layers](medallion_layers.md)** — a `DataContract` names its `Layer`; a `QualityGate` runs between two adjacent layers
- **[Observability](observability.md)** and **[Notifications](notifications.md)** — not wired to contracts: nothing emits a metric or a notification for an SLA breach or a violation. Route a `ContractReport` or `GateResult` yourself.
- **[Lineage](../reference/lineage.md)** — structured event emission and OpenLineage integration

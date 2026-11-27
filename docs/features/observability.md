# Feature: Observability

`sqldim.observability` records spans and metrics in memory with an
OTel-compatible model, and can hand each one to an exporter. Nothing in
sqldim instruments itself: you open the spans and record the metrics.

## Quick Start

```python
from sqldim.observability import MetricKind, MetricSample, OTelCollector

collector = OTelCollector()

# Trace a pipeline step: duration and status are set when the block exits.
with collector.start_span("scd_process", table="dim_customer") as span:
    rows = sum(range(1_000))

collector.record_metric(
    MetricSample(
        name="sqldim.rows_inserted",
        value=1_000,
        kind=MetricKind.COUNTER,
        labels={"table": "dim_customer"},
    )
)
print(span.status.value, span.duration_s >= 0)  # ok True
print([m.name for m in collector.metrics()])     # ['sqldim.rows_inserted']
```

## OTelCollector

`OTelCollector(span_exporter=None, metric_exporter=None)` keeps every span
and metric in memory and, when an exporter is given, passes each one to it as
it is recorded.

| Method | Description |
|---|---|
| `start_span(name, **attributes)` | Context manager yielding a `PipelineSpan`. On an exception the span's status is `ERROR`, `error` holds the message, and the exception propagates. |
| `record_metric(sample)` | Store a `MetricSample` and export it. |
| `spans()` / `metrics()` | Everything recorded, in order. |
| `clear()` | Drop everything recorded. |

```python
try:
    with collector.start_span("bronze_to_silver", layer="silver"):
        raise RuntimeError("boom")
except RuntimeError:
    pass
failed = collector.spans()[-1]
print(failed.status.value, failed.error)  # error boom
```

## MetricSample and PipelineSpan

| `MetricSample` field | Type | Description |
|---|---|---|
| `name` | `str` | Metric name |
| `value` | `float` | Numeric value |
| `kind` | `MetricKind` | `GAUGE`, `COUNTER` or `HISTOGRAM` |
| `labels` | `dict[str, str]` | Dimensions for grouping and filtering |
| `timestamp` | `datetime` | When it was recorded (UTC) |

| `PipelineSpan` field | Type | Description |
|---|---|---|
| `name`, `attributes` | `str`, `dict` | What `start_span()` was given |
| `status` | `SpanStatus` | `OK` or `ERROR` |
| `duration_s` | `float` | Elapsed seconds |
| `start_time`, `end_time` | `datetime` | UTC |
| `error` | `str \| None` | The exception message, if any |

Metric names are yours to choose; sqldim defines no standard set and emits
none by itself.

## Exporters

`ConsoleExporter(stream=None)` writes each span or metric as a JSON line (to
stderr by default) and has no dependency. `OTLPSpanExporter(endpoint=...)`
and `OTLPMetricExporter(endpoint=...)` send to an OTLP collector over gRPC;
they need `pip install sqldim[otel]` and raise `ImportError` without it.

```python
import io

from sqldim.observability import ConsoleExporter

stream = io.StringIO()
console = ConsoleExporter(stream=stream)
exported = OTelCollector(span_exporter=console, metric_exporter=console)
with exported.start_span("scd_process"):
    pass
print(len(stream.getvalue().splitlines()))  # 1
```

A custom exporter is any object with `export(item)`; the `SpanExporter` and
`MetricExporter` protocols describe it:

```python
class ListExporter:
    def __init__(self) -> None:
        self.items: list = []

    def export(self, item) -> None:
        self.items.append(item)


sink = ListExporter()
with OTelCollector(span_exporter=sink).start_span("noop"):
    pass
print(len(sink.items))  # 1
```

## Decorators

`@traced(collector, name=None)` wraps a sync or async function in a span
named after the function. `@metered(collector, name=None, kind=COUNTER,
labels=None, value=1.0)` records a sample after each call; `value` may be a
callable that receives the return value.

```python
from sqldim.observability import metered, traced


@traced(collector)
@metered(collector, name="sqldim.rows_inserted", value=lambda n: n)
def load(rows: list) -> int:
    return len(rows)


load([1, 2, 3])
print(collector.metrics()[-1].value)  # 3.0
```

## Instruments

Wrappers that record a sample of a fixed name, kind and labels:

```python
from sqldim.observability import Counter, Gauge, Histogram

Counter(collector, "sqldim.rows_inserted", labels={"table": "orders"}).increment(50)
Histogram(collector, "sqldim.duration", labels={"step": "scd"}).record(0.342)
Gauge(collector, "sqldim.backlog", labels={"topic": "events"}).set(1_200)
print([m.kind.value for m in collector.metrics()[-3:]])  # ['counter', 'histogram', 'gauge']
```

| Instrument | Method | Kind |
|---|---|---|
| `Counter` | `.increment(amount=1.0)` | `COUNTER` |
| `Gauge` | `.set(value)` | `GAUGE` |
| `Histogram` | `.record(value)` | `HISTOGRAM` |

## With Notifications

The two are not connected. To alert on a slow span, read the spans and route
an event yourself — see [Notifications](notifications.md) for the router.

---

## DriftObservatory

`DriftObservatory` stores schema evolution and data-quality results as a
Kimball star schema in DuckDB. The schema is declared with sqldim's own
`DimensionModel` and `TransactionFact` classes.

### Star Schema

| Table | Kind | Purpose |
|---|---|---|
| `obs_dataset_dim` | Dimension | Every tracked table/view |
| `obs_evolution_type_dim` | Dimension | Taxonomy of schema-change kinds (seeded on init) |
| `obs_rule_dim` | Dimension | Contract rule catalog |
| `obs_pipeline_run_dim` | Dimension | One row per pipeline execution |
| `obs_schema_evolution_fact` | Fact | One row per column-level schema change |
| `obs_quality_drift_fact` | Fact | One row per contract-rule check result |

### Quick Start

The evolution report comes from `ContractEngine.check_evolution_safety` and
the quality report from `ContractEngine.validate` (see
[Data Contracts](data_contracts.md)).

```python
import duckdb

from sqldim.contracts import ColumnSpec, ContractEngine, NotNull, SourceContract
from sqldim.observability.drift import DriftObservatory

evo_report = ContractEngine.check_evolution_safety(
    [ColumnSpec("id", "INTEGER"), ColumnSpec("email", "VARCHAR")],
    [ColumnSpec("id", "BIGINT")],
)
con = duckdb.connect()
con.execute("CREATE TABLE orders AS SELECT * FROM (VALUES (1), (NULL)) t(id)")
quality_report = ContractEngine().validate(
    con, "orders", SourceContract(rules=[NotNull("id")])
)

obs = DriftObservatory.in_memory()  # or DriftObservatory.from_path("./drift.duckdb")
obs.ingest_evolution(evo_report, dataset="orders_dim", run_id="run-001", layer="silver")
obs.ingest_quality(quality_report, dataset="orders_dim", run_id="run-001", layer="silver")

print(obs.breaking_change_rate().fetchall())
print(obs.worst_quality_datasets(top_n=5).fetchall())
```

### Bulk ingest with `transaction()`

Each ingest call commits on its own. Wrap many calls in one transaction:

```python
with obs.transaction():
    for i, report in enumerate([evo_report, evo_report]):
        obs.ingest_evolution(report, dataset="orders_dim", run_id=f"run-{i + 2:03}")
```

### `ingest_evolution(report, *, dataset, run_id, layer="", pipeline_name="", domain="", detected_at=None)`

Converts an `EvolutionReport` into `obs_schema_evolution_fact` rows and
returns how many it inserted. `ingest_quality(report, ...)` does the same for
a `ContractReport` (with `checked_at` in place of `detected_at`); a report
with no violation inserts nothing and returns `0`.

### Analytical queries

| Method | Description |
|---|---|
| `breaking_change_rate()` | Breaking vs. total schema changes, grouped by dataset |
| `worst_quality_datasets(top_n=10)` | Datasets with the highest cumulative violation counts |
| `drift_velocity(bucket="day")` | Schema-change events bucketed by day, week or month |
| `quality_score_trend(dataset, bucket="day")` | A 0–1 quality score for a dataset over time |
| `migration_backlog()` | Columns requiring migration or backfill |
| `rule_failure_heatmap()` | Dataset × rule violation matrix |

Each returns a DuckDB relation: `.fetchall()` for tuples, `.fetchdf()` for a
pandas DataFrame.

---

## Dependencies

- **[Data Contracts](data_contracts.md)** — the source of the `EvolutionReport` and `ContractReport` that `DriftObservatory` ingests
- **[Notifications](notifications.md)** — not wired to observability; route events yourself

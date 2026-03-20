# Feature: Observability

The observability plane provides full-stack visibility into pipeline runs using an OTel-compatible model for distributed tracing and metrics.

## Quick Start

```python
from sqldim.observability import OTelCollector, MetricSample
from sqldim.observability.metrics import MetricKind

collector = OTelCollector()

# Trace a pipeline step
with collector.start_span("scd_process", table="dim_customer", processor="LazySCDProcessor") as span:
    result = processor.process(source, "dim_customer")
    # span.duration_s and span.status are set automatically on exit

# Record metrics
collector.record_metric(MetricSample(
    name="sqldim.throughput",
    value=1240.0,
    kind=MetricKind.GAUGE,
    labels={"table": "dim_customer", "processor": "LazySCDProcessor"},
))
```

## OTelCollector

The central collector for traces and metrics.

```python
from sqldim.observability import OTelCollector

collector = OTelCollector()
```

### start_span()

Create a traced pipeline span. Returns a context manager that records duration and status.

```python
with collector.start_span(name, **attributes) as span:
    # do work
    pass

# span.duration_s  — elapsed time in seconds
# span.status      — "OK" or "ERROR" (ERROR if exception raised)
```

| Parameter | Type | Description |
|---|---|---|
| `name` | `str` | Span name (e.g., `"scd_process"`, `"bronze_to_silver"`) |
| `**attributes` | `str -> Any` | Key-value pairs attached to the span (table, processor, layer, etc.) |

### record_metric()

Record a metric sample.

```python
collector.record_metric(MetricSample(
    name="sqldim.throughput",
    value=1240.0,
    kind=MetricKind.GAUGE,
    labels={"table": "dim_customer"},
))
```

## MetricSample

```python
from sqldim.observability import MetricSample
from sqldim.observability.metrics import MetricKind

MetricSample(
    name="sqldim.throughput",    # metric name
    value=1240.0,                # numeric value
    kind=MetricKind.GAUGE,       # GAUGE, COUNTER, or HISTOGRAM
    labels={"table": "dim_customer", "processor": "scd2"},
)
```

| Field | Type | Description |
|---|---|---|
| `name` | `str` | Metric name |
| `value` | `float` | Numeric value |
| `kind` | `MetricKind` | `GAUGE` (point-in-time), `COUNTER` (cumulative), `HISTOGRAM` (distribution) |
| `labels` | `dict` | Key-value dimensions for grouping and filtering |

## PipelineSpan

Returned by `start_span()`. A dataclass with timing and status metadata.

| Field | Type | Description |
|---|---|---|
| `duration_s` | `float` | Elapsed time in seconds |
| `status` | `str` | `"OK"` or `"ERROR"` |
| `attributes` | `dict` | Attributes passed to `start_span()` |

## Instrumenting a Pipeline

Wrap every processor call with a span and record throughput metrics:

```python
from sqldim.observability import OTelCollector, MetricSample
from sqldim.observability.metrics import MetricKind

collector = OTelCollector()

def load_dimension(processor, source, table_name):
    with collector.start_span("scd_process", table=table_name, processor=type(processor).__name__) as span:
        result = processor.process(source, table_name)

    collector.record_metric(MetricSample(
        name="sqldim.rows_inserted",
        value=result.inserted,
        kind=MetricKind.COUNTER,
        labels={"table": table_name},
    ))
    collector.record_metric(MetricSample(
        name="sqldim.throughput",
        value=result.inserted / span.duration_s if span.duration_s > 0 else 0,
        kind=MetricKind.GAUGE,
        labels={"table": table_name},
    ))
    return result
```

## Standard Metric Names

| Metric | Kind | Unit | Description |
|---|---|---|---|
| `sqldim.throughput` | GAUGE | rows/s | Processing rate per table |
| `sqldim.duration` | HISTOGRAM | s | Pipeline step wall time |
| `sqldim.memory.rss` | GAUGE | GiBy | Peak resident set size |
| `sqldim.spill` | COUNTER | GiBy | Total disk spill |
| `sqldim.rows_inserted` | COUNTER | rows | Total rows written |
| `sqldim.rows_versioned` | COUNTER | rows | SCD version rows created |

## Exporters

By default `OTelCollector` stores spans and metrics in memory. To ship data to external backends, attach one or more exporters. All exporters are **opt-in** — zero dependencies unless you choose an OTLP backend.

### ConsoleExporter (zero-dep)

Writes spans and metrics as JSON lines to stderr. Ideal for local development and CI.

```python
from sqldim.observability import OTelCollector
from sqldim.observability.exporters import ConsoleExporter

collector = OTelCollector(exporters=[ConsoleExporter()])
```

### OTLP Exporters (requires `sqldim[otel]`)

Ship to any OTLP-compatible collector (Jaeger, Tempo, Prometheus, Grafana Cloud, etc.).

```python
from sqldim.observability import OTelCollector
from sqldim.observability.exporters import OTLPSpanExporter, OTLPMetricExporter

collector = OTelCollector(exporters=[
    OTLPSpanExporter(endpoint="http://localhost:4317"),       # gRPC (default)
    OTLPMetricExporter(endpoint="http://localhost:4318", protocol="http"),  # HTTP
])
```

| Exporter | Dependency | Protocol | Use case |
|---|---|---|---|
| `ConsoleExporter` | None | stderr JSON | Development, CI |
| `OTLPSpanExporter` | `sqldim[otel]` | gRPC / HTTP | Distributed tracing backends |
| `OTLPMetricExporter` | `sqldim[otel]` | gRPC / HTTP | Prometheus, Grafana |

### Custom exporters

Implement the `SpanExporter` or `MetricExporter` protocol:

```python
from sqldim.observability.exporters import SpanExporter, MetricExporter

class MySpanExporter(SpanExporter):
    def export(self, spans: list) -> None:
        ...  # send to your backend

    def flush(self) -> None:
        ...
```

## Decorators

For opt-in auto-instrumentation without manual span/metric calls:

### @traced

Wraps a sync or async function in a `PipelineSpan`:

```python
from sqldim.observability import OTelCollector
from sqldim.observability.decorators import traced

collector = OTelCollector(exporters=[ConsoleExporter()])

@traced(collector)
def process(source, table_name):
    ...
    # span is recorded automatically with duration and status
```

### @metered

Records a `MetricSample` after each call:

```python
from sqldim.observability.decorators import metered
from sqldim.observability.metrics import MetricKind

@metered(collector, name="sqldim.rows_inserted", kind=MetricKind.COUNTER)
def process(source, table_name):
    ...
    return result  # result.inserted (or result itself if numeric) is recorded
```

## Instruments

Ergonomic wrappers for common metric patterns — each holds a reference to a named metric and provides a domain-specific API:

```python
from sqldim.observability.instruments import Counter, Gauge, Histogram

rows_counter = Counter(collector, "sqldim.rows_inserted", labels={"table": "orders"})
rows_counter.increment(50)

latency = Histogram(collector, "sqldim.duration", labels={"step": "scd"})
latency.record(0.342)

backlog = Gauge(collector, "sqldim.backlog", labels={"topic": "events"})
backlog.set(1_200)
```

| Instrument | Method | Description |
|---|---|---|
| `Counter` | `.increment(value=1)` | Cumulative count |
| `Gauge` | `.set(value)` | Point-in-time value |
| `Histogram` | `.record(value)` | Distribution observation |

## Integration with Notifications

Feed observability data into the [Notifications](notifications.md) system for anomaly-triggered alerts:

```python
from sqldim.notifications import NotificationRouter, Severity, NotificationEvent

def check_latency_anomaly(collector, threshold_s=5.0):
    for span in collector.recent_spans:
        if span.duration_s > threshold_s:
            router.route(NotificationEvent(
                title=f"Latency anomaly: {span.attributes.get('table')}",
                severity=Severity.P3,
                event_type="otel_latency_drift",
                attributes={"duration_s": span.duration_s, **span.attributes},
            ))
```

---

## DriftObservatory

`DriftObservatory` is a higher-level observability component that models **schema evolution and data-quality drift as first-class Kimball facts**. The library dog-foods itself: the star schema is declared using `DimensionModel` / `TransactionFact` base classes and materialised into DuckDB.

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

```python
from sqldim.observability.drift import DriftObservatory

# In-memory (testing / one-shot analysis)
obs = DriftObservatory.in_memory()

# File-backed (persistent across runs)
obs = DriftObservatory.from_path("./drift.duckdb")

# Ingest a schema-evolution report (silver layer)
obs.ingest_evolution(evo_report, dataset="orders_dim", run_id="run-001", layer="silver")

# Ingest a contract-check report (silver layer)
obs.ingest_quality(quality_report, dataset="orders_dim", run_id="run-001", layer="silver")

# Gold-layer queries
obs.breaking_change_rate().fetchdf()
obs.worst_quality_datasets(top_n=5).fetchdf()
obs.drift_velocity(bucket="week").fetchdf()
obs.quality_score_trend("orders_dim").fetchdf()
obs.migration_backlog().fetchdf()
obs.rule_failure_heatmap().fetchdf()
```

### Bulk Ingest with `transaction()`

For high-volume scenarios, wrap all ingest calls in a single explicit transaction.
Without this wrapper each call auto-commits, making bulk loads significantly slower.

```python
with obs.transaction():
    for report in evolution_reports:
        obs.ingest_evolution(report, dataset="orders_dim", run_id="run-001")
    for report in quality_reports:
        obs.ingest_quality(report, dataset="orders_dim", run_id="run-001")
```

### `ingest_evolution(report, *, dataset, run_id, layer, pipeline_name, domain, detected_at)`

Convert an `EvolutionReport` into `ObsSchemaEvolutionFact` rows. Returns the number of rows inserted.

| Parameter | Type | Description |
|---|---|---|
| `report` | `EvolutionReport` | Schema diff report from `SchemaGraph.diff()` |
| `dataset` | `str` | Logical table/view name |
| `run_id` | `str` | Pipeline run identifier |
| `layer` | `str` | Medallion layer (`"bronze"`, `"silver"`, `"gold"`) |
| `pipeline_name` | `str` | Human-readable pipeline label |
| `domain` | `str` | Business domain (optional) |
| `detected_at` | `datetime \| None` | Override detection timestamp |

### `ingest_quality(report, *, dataset, run_id, layer, pipeline_name, domain, checked_at)`

Convert a `ContractReport` into `ObsQualityDriftFact` rows. Returns the number of rows inserted.
A clean run (no violations) returns `0`.

### Gold-Layer Analytical Queries

| Method | Returns | Description |
|---|---|---|
| `breaking_change_rate()` | relation | Breaking vs. total schema changes, grouped by dataset |
| `worst_quality_datasets(top_n=10)` | relation | Datasets with highest cumulative violation counts |
| `drift_velocity(bucket="day")` | relation | Schema-change events bucketed by day/week/month |
| `quality_score_trend(dataset, bucket="day")` | relation | Rolling 0–1 quality score for a dataset over time |
| `migration_backlog()` | relation | Columns requiring migration or backfill |
| `rule_failure_heatmap()` | relation | Dataset × rule violation matrix |

All methods return a DuckDB relation — call `.fetchdf()` for a Pandas DataFrame or `.fetchall()` for a list of tuples.

### Medallion Mapping

| Medallion Layer | Activity |
|---|---|
| **Bronze** | Raw `EvolutionReport` / `ContractReport` objects arrive as events |
| **Silver** | `ingest_evolution()` / `ingest_quality()` explode them into the star schema |
| **Gold** | Analytical queries (`breaking_change_rate()`, `drift_velocity()`, etc.) |

---

## Dependencies

- **[Data Contracts](data_contracts.md)** — observability contracts enforced at OTel Bronze/Silver/Gold promotion gates
- **[Notifications](notifications.md)** — OTel anomalies trigger P3 notifications; ingestion lag triggers P4
- **[Medallion Layers](medallion_layers.md)** — OTel signals stored in their own medallion structure

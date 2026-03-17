# Feature: Observability

## Summary

The Observability plane gives the platform full-stack visibility into every
pipeline run.  It uses **OpenTelemetry** (OTel) as the collection standard
and stores signals in its **own medallion structure** — OTel Bronze, OTel
Silver, OTel Gold — so that pipeline health is subject to the same
data-contract and quality-gate regime as business data.

---

## Signal types

| Signal | Key metric | Retention | Storage |
|---|---|---|---|
| **Traces** | p99 end-to-end latency | 7 days (sampled 10%) | OTel Bronze |
| **Metrics** | Row throughput, lag, error rate | 90 days | OTel Gold (Prometheus-compatible) |
| **Logs** | Structured JSON, correlated via `trace_id` | 30 days | OTel Bronze |

Baseline values from the current benchmark suite:

```
p99 pipeline latency  : 4.2 s
Row throughput        : 1 240 rows/s  (benchmark peak: 1.2 M/s)
Log delivery success  : 99.98 %
```

---

## OTel Collector pipeline

```
SDK (auto-instrumented)
        │  OTLP/gRPC
        ▼
┌──────────────────┐
│  OTel Collector  │   OTLP receiver; processor chain: batch → memory_limiter
└────────┬─────────┘
         │
   ┌─────┴──────────────────────────────────────────────┐
   │                                                     │
   ▼                                                     ▼
Traces / Logs                                         Metrics
   │                                                     │
   ▼                                                     ▼
otlp/bronze  →  ingest.bronze.otel:4317          prometheus/gold  →  :8889
```

### Collector configuration (excerpt)

```yaml
exporters:
  otlp/bronze:
    endpoint:    "ingest.bronze.otel:4317"
    compression: gzip
  prometheus/gold:
    endpoint: "0.0.0.0:8889"

pipelines:
  traces:  { exporters: [otlp/bronze] }
  metrics: { exporters: [prometheus/gold] }
  logs:    { exporters: [otlp/bronze] }
```

---

## OTel medallion layers

The Observability plane mirrors the business data medallion, giving teams a
single mental model for both domains.

### OTel Bronze — raw signals

```yaml
contract: otel_traces_raw
  schema:
    trace_id:   string   PK
    span_id:    string
    timestamp:  nanoseconds
    attributes: map<str, any>
  sla:
    latency: "< 2s"          # from SDK emission to Bronze landing
```

Purpose: lossless, high-fidelity raw signal capture.  No sampling at this
stage — all spans land here before any filtering.

### OTel Silver — enriched and indexed

- Service name resolved from resource attributes
- Span hierarchy reconstructed (parent/child links)
- Error spans tagged with normalised error codes
- PII scrubbed from `http.url` and `db.statement` attributes
- Sampling: only spans from p95+ latency roots or errored traces retained

### OTel Gold — aggregated SLOs

```yaml
contract: slo_aggregates
  schema:
    service:         string
    window:          interval
    error_rate:      float64
    p99_latency_ms:  float64
  sla:
    freshness: "< 1m"
```

Prometheus scrapes OTel Gold directly; Grafana dashboards read from here.

---

## Instrumentation strategy for sqldim

Every `process()` call on any processor should emit a trace span:

```python
from opentelemetry import trace

tracer = trace.get_tracer("sqldim")

with tracer.start_as_current_span("scd_process") as span:
    span.set_attribute("sqldim.table",     table_name)
    span.set_attribute("sqldim.processor", processor_name)
    span.set_attribute("sqldim.tier",      tier)
    result = processor.process(source, table_name)
    span.set_attribute("sqldim.rows.inserted",  result.inserted)
    span.set_attribute("sqldim.rows.versioned", result.versioned)
    span.set_attribute("sqldim.rows.unchanged", result.unchanged)
```

The benchmark suite already captures `rows_per_sec`, `peak_rss_gb`, and
`total_spill_gb` — these map directly to OTel metric instruments:

| Benchmark field | OTel instrument | Unit |
|---|---|---|
| `rows_per_sec` | `sqldim.throughput` (gauge) | `{rows}/s` |
| `peak_rss_gb` | `sqldim.memory.rss` (gauge) | `GiBy` |
| `total_spill_gb` | `sqldim.spill` (counter) | `GiBy` |
| `wall_s` | `sqldim.duration` (histogram) | `s` |

---

## Anomaly detection

OTel Gold feeds the Notification system with a P3 rule:

> *p99 latency exceeds 3σ from the rolling 7-day baseline →
> create Jira ticket + post to #data-platform in Slack.*

The 3σ threshold is computed over the `slo_aggregates` table's
`p99_latency_ms` column with a 7-day rolling window.

---

## Dependencies

- **Medallion Layers** — the OTel Bronze/Silver/Gold structure inherits the
  same promotion gates and storage contracts as the business medallion.
- **Data Contracts** — `otel_traces_raw` and `slo_aggregates` are full
  first-class contracts enforced at their respective promotion gates.
- **Notifications** — OTel Gold anomalies trigger P3 notifications; OTel
  Bronze ingestion lag triggers P4 freshness warnings.

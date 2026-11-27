# Feature: Medallion Layers

## Summary

The Medallion architecture divides the data platform into four progressively
refined zones — **Bronze**, **Silver**, **Gold**, and **Semantic** — each with
its own data policy, contract regime, and quality guarantee.  Data flows in
one direction only: dimensional models → pre-aggregates → aggregates →
business views.

---

## Layer overview

```
Source events / APIs  (staging / raw ingestion)
        │
        ▼
┌───────────────────────────────────────────────────────────┐
│  BRONZE  (dimensional models: dim, fact, bridge)          │
│  SCD-versioned · Type-safe · Surrogate keys               │
│  Format: Delta Lake / Iceberg / DuckDB tables             │
└────────────────────────────┬──────────────────────────────┘
                             │  Quality gate: Bronze → Silver
                             ▼
┌───────────────────────────────────────────────────────────┐
│  SILVER  (pre-aggregated / array-like data)               │
│  Snapshot views · Nested structures · PII masked          │
│  Format: materialised tables / views                      │
└────────────────────────────┬──────────────────────────────┘
                             │  Quality gate: Silver → Gold
                             ▼
┌───────────────────────────────────────────────────────────┐
│  GOLD  (aggregated metrics and KPIs)                      │
│  Pre-computed aggregates · SLA ≤ 5 min                    │
│  Format: materialised tables / views                      │
└────────────────────────────┬──────────────────────────────┘
                             │  Quality gate: Gold → Semantic
                             ▼
┌───────────────────────────────────────────────────────────┐
│  SEMANTIC  (business-level consumption layer)             │
│  BI-ready · NL-queryable · Domain-owned                   │
│  Format: views / virtual datasets                         │
└───────────────────────────────────────────────────────────┘
              ↓
  BI tools / ML pipelines / NL agents / APIs
```

---

## Bronze — dimensional models

**Principle:** *structure over raw fidelity*.  Source data is modelled into
dimensions, facts, and bridges following Kimball methodology.

| Property | Value |
|---|---|
| Data types | Dimensions (SCD2/SCD1), Facts, Bridges |
| Schema | Enforced by Data Contract |
| Versioning | SCD2 with `valid_from`, `valid_to`, `is_current` |
| Build order | DIMENSION → FACT → BRIDGE → GRAPH |
| Format | Delta Lake / Iceberg / DuckDB tables |

sqldim's `LazySCDProcessor` and `LazySCDMetadataProcessor` operate at this
layer, producing fully versioned dimension tables.

---

## Silver — pre-aggregated / array-like data

**Principle:** *readiness over completeness*.  Bronze models are snapshotted,
flattened, or nested into consumption-optimised structures.

| Property | Value |
|---|---|
| Data types | Current-snapshot views, nested arrays, denormalised tables |
| Deduplication | Natural-key dedup, current-version filtering |
| PII | Masked or tokenised at write time |
| Format | Materialised tables or views |

---

## Gold — aggregated metrics

**Principle:** *performance over flexibility*.  Pre-computed aggregates
eliminate fan-out at query time.

| Property | Value |
|---|---|
| Data types | Aggregated KPIs, materialised metrics, cohort tables |
| Aggregation | Pre-computed (no fan-out at query time) |
| SLA | Freshness ≤ 5 min after Silver promotion |
| Format | Materialised tables in DuckDB / MotherDuck |

---

## Semantic — business consumption

**Principle:** *meaning over mechanics*.  Gold aggregates are wrapped in
business-friendly names, descriptions, and access policies for end-user
consumption via BI tools, NL agents, or APIs.

| Property | Value |
|---|---|
| Data types | Business views, domain-owned datasets |
| Consumers | NL agents, BI dashboards, ML pipelines, APIs |
| Governance | Domain-owned, access-controlled |
| Format | Views / virtual datasets |

---

## In code

`sqldim.medallion` has the `Layer` enum, a `MedallionRegistry` of which layer
each dataset is in, and `SilverBuildOrder`. The layer policies above — what
each layer holds, its format and its SLA — are conventions, not something
sqldim enforces.

```python
from sqldim.medallion import Layer, MedallionRegistry

registry = MedallionRegistry()
registry.register("dim_customer", Layer.BRONZE)
registry.register("customer_snapshot", Layer.SILVER)
print(registry.datasets_in(Layer.SILVER))           # ['customer_snapshot']
print(Layer.BRONZE.can_promote_to(Layer.SILVER))    # True
print(Layer.BRONZE.can_promote_to(Layer.GOLD))      # False
```

## Quality gates

A promotion between two adjacent layers is guarded by a `QualityGate`, whose
checks you write (see [Data Contracts](data_contracts.md#quality-gates)). The
policies below are examples of what such checks could cover; sqldim does not
ship them. There is no YAML gate format, no quarantine partition, no
statistical-drift baseline and no automatic alert on a failed gate.

| Promotion | Example checks |
|---|---|
| Bronze → Silver | row count > 0, NULL rate of the key, schema match with the contract, duplicate rate |
| Silver → Gold | completeness, referential integrity, freshness, drift against a baseline |
| Gold → Semantic | business-rule coverage, naming conventions, access policy defined |

---

## Storage format decision guide

| Use case | Format | sqldim sink today |
|---|---|---|
| High-volume append, long retention | Parquet | `ParquetSink` — append supported; SCD2 close and merge partial |
| SCD2 / MERGE workloads | DuckDB, PostgreSQL | `DuckDBSink`, `PostgreSQLSink` — append, SCD2 close and merge supported |
| Multi-engine access | Delta Lake, Apache Iceberg | `DeltaLakeSink` — append only (SCD2 close and merge refused); `IcebergSink` — partial, storage not executed in CI |
| Interactive BI | DuckDB / MotherDuck | `DuckDBSink`; `MotherDuckSink` not executed in CI |

The support levels are those of the generated
[capability page](../reference/capabilities.md#write-sinks-62-rule-8) and the
[backend matrix](../reference/dgm-backend-matrix.md).

---

## Dependencies

- **Data Contracts** — a `DataContract` names its layer, and a `QualityGate`
  runs between two adjacent layers; neither is required by the other.
- **Observability** and **Notifications** — not wired to layer promotion; a
  failed gate emits nothing unless you route its `GateResult`.

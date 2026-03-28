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

## Quality gates

### Bronze → Silver

```yaml
checks:
  - row_count              > 0
  - null_rate(order_id)    < 0.001
  - schema_match(contract: orders_v2)
  - duplicate_rate         < 0.005
on_fail: quarantine + alert(P2)
```

Failed rows land in a `quarantine` partition, tagged with the failing check
name and the pipeline run ID for triage.

### Silver → Gold

```yaml
checks:
  - completeness            > 99.5%
  - referential_integrity(FK)
  - freshness               < "30m"
  - statistical_drift       < 3σ
on_fail: hold + alert(P1)
```

`statistical_drift` is computed against a rolling 7-day baseline; a 3σ
deviation in row count or key metric distribution halts the promotion.

### Gold → Semantic

```yaml
checks:
  - business_rule_coverage  > 95%
  - naming_convention       PASS
  - access_policy           DEFINED
on_fail: hold + alert(P1)
```

Semantic promotion verifies that business naming conventions are followed
and access policies are attached before datasets are exposed to consumers.

---

## Storage format decision guide

| Use case | Recommended format |
|---|---|
| High-volume append, long retention | Parquet (Bronze) |
| SCD2 / MERGE workloads | Delta Lake (Silver) |
| Multi-engine access (Spark + DuckDB) | Apache Iceberg (Silver) |
| Interactive BI, sub-second queries | DuckDB / MotherDuck (Gold) |

---

## Dependencies

- **Data Contracts** — each layer transition requires a contract to exist for
  the target dataset; gate checks are defined inside the contract.
- **Observability** — the Observability plane mirrors this three-tier
  structure with OTel Bronze / Silver / Gold layers.
- **Notifications** — gate failures route to the notification system
  (P2 for Silver failures, P1 for Gold failures).

# Feature: Medallion Layers

## Summary

The Medallion architecture divides the data platform into three progressively
refined zones — **Bronze**, **Silver**, and **Gold** — each with its own
storage format, contract regime, and quality guarantee.  Data flows in one
direction only: raw → validated → aggregated.

---

## Layer overview

```
Source events / APIs
        │
        ▼
┌───────────────────────────────────────────────────────────┐
│  BRONZE  (raw ingestion)                                  │
│  Append-only · Schema-on-read · Immutable once written    │
│  Format: Parquet / Avro                                   │
└────────────────────────────┬──────────────────────────────┘
                             │  Quality gate: Bronze → Silver
                             ▼
┌───────────────────────────────────────────────────────────┐
│  SILVER  (validated / enriched)                           │
│  Cleansed · Deduplicated · Type-safe · PII masked         │
│  Format: Delta Lake / Iceberg                             │
└────────────────────────────┬──────────────────────────────┘
                             │  Quality gate: Silver → Gold
                             ▼
┌───────────────────────────────────────────────────────────┐
│  GOLD  (aggregated / consumption-ready)                   │
│  Star schema · Pre-computed aggregates · SLA ≤ 5 min      │
│  Format: materialised tables / views                      │
└────────────────────────────┬──────────────────────────────┘
                             │
                             ▼
              BI tools / ML pipelines / APIs
```

---

## Bronze — raw ingestion

**Principle:** *fidelity over quality*.  Every source event lands here
unchanged so that any downstream bug can be reprocessed from the source of
truth.

| Property | Value |
|---|---|
| Write mode | Append-only |
| Schema | Schema-on-read; no enforcement |
| Retention | Configurable (default: indefinite) |
| Transformation | None |
| Format | Parquet (columnar) or Avro (schema-evolution-friendly) |

sqldim produces Bronze-layer Parquet files today via `ParquetSink`.

---

## Silver — validated / enriched

**Principle:** *quality over completeness*.  A row that fails validation is
quarantined, not silently dropped.

| Property | Value |
|---|---|
| Write mode | Insert + SCD2 versioning (via sqldim processors) |
| Schema | Enforced by Data Contract |
| Deduplication | Natural-key dedup before insert |
| PII | Masked or tokenised at write time |
| Format | Delta Lake (`DeltaLakeSink`) or Apache Iceberg (`IcebergSink`) |

sqldim's `LazySCDProcessor` and `LazySCDMetadataProcessor` operate at this
layer, producing a fully versioned dimension table with `is_current`,
`valid_from`, and `valid_to` columns.

---

## Gold — aggregated / consumption-ready

**Principle:** *performance over flexibility*.  Structure is fixed, latency
is guaranteed, consumers never touch raw data.

| Property | Value |
|---|---|
| Structure | Star schema (fact + conformed dimensions) |
| Aggregation | Pre-computed (no fan-out at query time) |
| SLA | Freshness ≤ 5 min after Silver promotion |
| Format | Materialised tables in DuckDB / MotherDuck |

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

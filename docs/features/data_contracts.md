# Feature: Data Contracts

## Summary

Data Contracts are the trust boundary of the platform. Every dataset that
crosses a layer boundary — or is consumed by an external team — must be
backed by a contract that declares its schema, ownership, quality SLA, and
lineage events.

---

## Core responsibilities

| Concern | Mechanism |
|---|---|
| **Schema** | Column types, nullability, PK/FK constraints validated at ingestion and at every layer promotion |
| **Ownership** | `owner` field ties every dataset to a responsible team; routes alerts on breach |
| **SLA** | Freshness threshold, completeness percentage, and p99 latency declared and monitored continuously |
| **Versioning** | Backward-compatible changes → minor bump; breaking changes → major bump + dataset fork |
| **Lineage** | Every schema event emits an OpenLineage `RunEvent` to the metadata bus |

---

## Contract schema

```yaml
contract: orders_v2
  version: "2.1.0"
  owner:   "data-platform"
  layer:   "silver"

  schema:
    order_id:    uuid       PK
    customer_id: uuid       FK → customers_v1.customer_id
    amount:      decimal(18,2)
    created_at:  timestamp

  sla:
    freshness:     "< 30m"
    completeness:  "> 99.5%"
    latency_p99:   "< 5m"
```

A contract lives alongside the dataset definition in version control.
Schema changes are reviewed like code — a diff that removes a column or
changes a type is automatically flagged as a breaking change.

---

## Enforcement points

### 1 — Ingestion gate (Source → Bronze)
`schema_on_read` is permissive by design; the Bronze layer accepts raw data
without transformation.  The contract is *registered* here but enforcement
is deferred to the Bronze → Silver gate.

### 2 — Promotion gate (Bronze → Silver)
The strictest checkpoint.  All four contract dimensions are verified:

```yaml
checks:
  - row_count         > 0
  - null_rate(order_id) < 0.001
  - schema_match(contract=orders_v2)
  - duplicate_rate    < 0.005
on_fail: quarantine + alert(P2)
```

### 3 — Promotion gate (Silver → Gold)
Business-logic checks replace structural ones:

```yaml
checks:
  - completeness       > 99.5%
  - referential_integrity(FK)
  - freshness          < "30m"
  - statistical_drift  < 3σ
on_fail: hold + alert(P1)
```

### 4 — Consumer registration
Any BI tool, ML model, or downstream pipeline that reads a Gold dataset
registers as a *consumer* of that contract version.  When a contract is
deprecated, all registered consumers receive a migration notice.

---

## OpenLineage integration

Every contract enforcement event emits a structured `RunEvent`:

```json
{
  "eventType": "COMPLETE",
  "run":  { "runId": "<uuid>" },
  "job":  { "namespace": "bronze→silver", "name": "orders_transform" },
  "inputs":  [{ "namespace": "bronze", "name": "raw_orders" }],
  "outputs": [{
    "namespace": "silver",
    "name":      "orders_v2",
    "facets": {
      "schema":      "<contract_schema>",
      "dataQuality": "<gate_results>"
    }
  }]
}
```

The lineage graph is therefore built automatically from contract enforcement
rather than from hand-crafted metadata.

---

## Versioning strategy

| Change type | Example | Action |
|---|---|---|
| **Additive** | New nullable column | Minor bump (`2.1.0 → 2.2.0`) |
| **Compatible rename** | Column alias | Minor bump + deprecation notice |
| **Breaking** | Remove column / type change | Major bump (`2.x.x → 3.0.0`) + fork |
| **SLA relaxation** | Raise freshness to 60 min | Minor bump + owner approval |
| **SLA tightening** | Lower latency SLA | Major bump (consumers must re-validate) |

---

## Dependencies

- **Medallion Layers** — contracts are anchored to a specific layer (`bronze`, `silver`, `gold`).
- **Observability** — SLA breach events feed directly into the OTel metrics pipeline.
- **Notifications** — contract violations trigger P1 (Gold) or P2 (Silver) alerts.

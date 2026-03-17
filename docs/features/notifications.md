# Feature: Notifications

## Summary

The Notification system is the operational spine of the platform.  It routes
alerts from contract violations, quality gate failures, and observability
anomalies to the right team, at the right severity, through the right channel
— with a defined escalation path for every rule.

---

## Severity model

| Level | Name | Response target | Trigger examples |
|---|---|---|---|
| **P1** | Critical | Immediate (pager) | Gold contract breach, Gold SLA miss |
| **P2** | High | 2 minutes | Silver gate failure, Bronze schema violation |
| **P3** | Medium | 15 minutes | OTel latency 3σ drift, Silver freshness warning |
| **P4** | Low | Daily digest | Bronze source lag > 10% of SLA |

---

## Notification rules

### P1 — Contract breach on Gold layer

**Trigger:** Schema violation *or* SLA miss on any Gold dataset.

**Actions:**
1. Page on-call engineer via PagerDuty (immediate).
2. Notify dataset owner via Slack DM.
3. Auto-halt all registered downstream consumers of the affected contract
   version until the breach is resolved or the contract is rolled back.

```yaml
rule: gold_contract_breach
  severity:   P1
  trigger:
    event:    contract_violation
    layer:    gold
  actions:
    - pagerduty: { policy: data-platform-oncall }
    - slack:     { channel: "@owner", template: gold_breach }
    - pipeline:  { action: halt_consumers, contract: "{{ contract_id }}" }
```

---

### P2 — Quality gate failure on Silver promotion

**Trigger:** Bronze → Silver gate fails `completeness` or `duplicate_rate`
check.

**Actions:**
1. Quarantine the failing batch (move to `quarantine/` partition, tag with
   `run_id` and failing check name).
2. Post to `#data-platform` Slack channel within 2 minutes.
3. Other pipelines sharing the Bronze source continue unaffected.

```yaml
rule: silver_gate_failure
  severity:   P2
  trigger:
    event:    quality_gate_fail
    gate:     bronze_to_silver
  actions:
    - slack:     { channel: "#data-platform", template: gate_failure }
    - pipeline:  { action: quarantine, partition: "failed/{{ date }}" }
```

---

### P3 — OTel anomaly: latency drift

**Trigger:** p99 latency in `slo_aggregates` exceeds 3σ from its 7-day
rolling baseline for ≥ 3 consecutive evaluation windows (15 min cadence).

**Actions:**
1. Create a Jira ticket in `DATA` project (auto-assigned to on-call).
2. Post a summary to `#data-platform` Slack channel.
3. Suppressed if a P1 or P2 is already active for the same service.

```yaml
rule: otel_latency_drift
  severity:   P3
  trigger:
    metric:   slo_aggregates.p99_latency_ms
    condition: rolling_zscore(7d) > 3
    for:      3 windows
  actions:
    - jira:  { project: DATA, assignee: oncall }
    - slack: { channel: "#data-platform", template: latency_anomaly }
  suppress_if: [P1, P2]
```

---

### P4 — Freshness warning on Bronze ingestion

**Trigger:** Source lag exceeds the Bronze contract's freshness SLA by > 10%.

**Actions:**
1. Batch into a daily digest email to the pipeline owner.
2. Escalate to P2 if the lag exceeds 2× the SLA threshold.

```yaml
rule: bronze_freshness_warning
  severity:   P4
  trigger:
    event:    freshness_sla_exceeded
    layer:    bronze
    threshold: 1.10          # 10% over SLA
  actions:
    - email: { to: owner, digest: daily }
  escalate:
    when:      threshold > 2.0
    to:        P2
```

---

## Notification channels

| Channel | Used by | Notes |
|---|---|---|
| **PagerDuty** | P1 | Integrates with on-call rotation; auto-acks on resolution |
| **Slack** | P1, P2, P3 | Per-rule channel targeting; templated messages |
| **Jira** | P3 | Auto-creates `DATA` project ticket; links to OTel trace |
| **Email** | P4 | Daily digest only; no real-time delivery |
| **Webhook** | All | Generic egress for custom ITSM or chat systems |

---

## Escalation policy

```
P4 ──(lag > 2×)──► P2 ──(no ack in 10 min)──► P1
                         │
                    (batch quarantined 3× in 24h)
                         │
                         ▼
                        P1 + auto-freeze Bronze source
```

Every escalation creates a new notification with a fresh `incident_id` that
links back to the original P4 event for full audit trail.

---

## Contract-triggered events

Contract events (emitted by the enforcement points — see
[data_contracts.md](./data_contracts.md)) map to severity levels directly:

| Contract event | Severity | Layer |
|---|---|---|
| `schema_violation` | P1 (Gold), P2 (Silver/Bronze) | Any |
| `sla_miss.freshness` | P1 (Gold), P4 (Bronze) | Any |
| `sla_miss.completeness` | P1 (Gold), P2 (Silver) | Gold / Silver |
| `version_breaking_change` | P2 | Any |
| `consumer_on_deprecated_version` | P3 | Any |

---

## Dependencies

- **Data Contracts** — all P1/P2 rules are triggered by contract enforcement
  events; every breach payload includes the `contract_id` and `version`.
- **Observability** — P3 is sourced from OTel Gold aggregates; P4 is sourced
  from Bronze ingestion lag metrics.
- **Medallion Layers** — severity is partly a function of which layer the
  event originates in (Gold breach = always P1).

# Feature Dependency Graph

> Describes the static and runtime dependencies between the four platform
> features extracted from the architecture discussion.  An arrow `A → B`
> means *A depends on B* (B must exist and be operational for A to function).

---

## Dependency map (Mermaid)

```mermaid
graph TD
    %% ── nodes ──────────────────────────────────────────────────────────
    ML["🥉🥈🥇 Medallion Layers<br/><sub>Bronze · Silver · Gold</sub>"]
    DC["📄 Data Contracts<br/><sub>Schema · SLA · Versioning · Lineage</sub>"]
    OB["🔭 Observability<br/><sub>OTel Collector · OTel Medallion · SLO Aggregates</sub>"]
    NO["🔔 Notifications<br/><sub>P1–P4 rules · Multi-channel · Escalation</sub>"]

    QG_BS["⛩ Quality Gate<br/>Bronze → Silver"]
    QG_SG["⛩ Quality Gate<br/>Silver → Gold"]

    OL["🔗 OpenLineage Bus"]

    CHL_PD["PagerDuty"]
    CHL_SL["Slack"]
    CHL_JR["Jira"]
    CHL_EM["Email"]
    CHL_WH["Webhook"]

    %% ── feature-level dependencies ──────────────────────────────────────
    DC  --> ML         %% contracts are anchored to a layer
    QG_BS --> DC       %% gate checks are defined inside the contract
    QG_SG --> DC
    ML  --> QG_BS      %% layers host the gates
    ML  --> QG_SG

    OB  --> ML         %% OTel has its own Bronze/Silver/Gold
    OB  --> DC         %% OTel traces_raw and slo_aggregates are contracts

    NO  --> DC         %% P1/P2 are contract violation events
    NO  --> OB         %% P3 is sourced from OTel Gold anomalies
    NO  --> ML         %% severity is partly layer-dependent

    DC  --> OL         %% each enforcement point emits a RunEvent
    OB  --> OL         %% OTel spans carry lineage facets

    %% ── channel bindings ────────────────────────────────────────────────
    NO  --> CHL_PD
    NO  --> CHL_SL
    NO  --> CHL_JR
    NO  --> CHL_EM
    NO  --> CHL_WH

    %% ── styles ──────────────────────────────────────────────────────────
    classDef feature  fill:#1e2230,stroke:#4a90d9,color:#e8eaf6,rx:8
    classDef gate     fill:#2a1f0e,stroke:#B8772A,color:#ffe0b2,rx:4
    classDef bus      fill:#1a2420,stroke:#2E8B6E,color:#b2dfdb,rx:4
    classDef channel  fill:#1a1a2e,stroke:#6B5EA8,color:#d1c4e9,rx:4

    class ML,DC,OB,NO feature
    class QG_BS,QG_SG gate
    class OL bus
    class CHL_PD,CHL_SL,CHL_JR,CHL_EM,CHL_WH channel
```

---

## Dependency table

| Feature | Hard dependencies | Soft / runtime dependencies |
|---|---|---|
| **Medallion Layers** | *(none — foundation layer)* | Observability (monitoring layer health) |
| **Data Contracts** | Medallion Layers (layer anchoring) | OpenLineage bus (lineage emission) |
| **Quality Gates** | Data Contracts (check definitions), Medallion Layers (layer transitions) | Notifications (breach alerting) |
| **Observability** | Medallion Layers (own OTel medallion), Data Contracts (OTel contracts) | Notifications (P3/P4 anomaly alerts) |
| **Notifications** | Data Contracts (breach events), Observability (OTel Gold metrics), Medallion Layers (severity routing) | PagerDuty · Slack · Jira · Email · Webhook |

---

## Topological build order

The correct initialisation order — each step can only start after its line's
predecessors are ready:

```
1.  Medallion Layers        (storage, no upstream dependencies)
2.  Data Contracts          (requires layer catalogue)
3.  Quality Gates           (requires contracts + layers)
4.  Observability           (requires layers + contracts for OTel medallion)
5.  Notifications           (requires contracts + observability)
6.  OpenLineage bus         (passive — receives events from contracts + observability)
```

---

## Critical path

The longest chain that must be operational before any consumer can safely
read Gold data:

```
Source
  → Bronze (Medallion Layers)
  → Bronze→Silver Gate (Quality Gates ← Data Contracts)
  → Silver (Medallion Layers)
  → Silver→Gold Gate (Quality Gates ← Data Contracts)
  → Gold (Medallion Layers)
  → SLA check (Data Contracts)
  → OpenLineage event (OpenLineage bus)
  → OTel metric emitted (Observability)
  → Anomaly detection (Observability Gold)
  → Alert routing (Notifications)
  → Consumer unblocked
```

Any outage in **Data Contracts** or **Medallion Layers** halts the entire
pipeline.  **Observability** and **Notifications** are resilient — their
failure degrades visibility and alerting but does not block data promotion.

---

## Inter-feature event flows

```
Contract enforcement event
    ├── schema_violation  ─────────────────────────► Notifications (P1/P2)
    ├── sla_miss          ─────────────────────────► Notifications (P1/P4)
    └── RunEvent          ─────────────────────────► OpenLineage bus

Quality gate failure
    └── quarantine write  ─────────────────────────► Notifications (P2)

Observability anomaly (OTel Gold)
    └── 3σ latency drift  ─────────────────────────► Notifications (P3)

Source ingestion lag
    └── freshness > 110%  ─────────────────────────► Notifications (P4)
         freshness > 200%  ────(escalate)──────────► Notifications (P2)
```

---

## Feature documents

| Feature | Document |
|---|---|
| Medallion Layers | [medallion_layers.md](./medallion_layers.md) |
| Data Contracts | [data_contracts.md](./data_contracts.md) |
| Observability | [observability.md](./observability.md) |
| Notifications | [notifications.md](./notifications.md) |

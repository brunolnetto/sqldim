# Feature: Notifications

The notification system routes alerts from contract violations, quality gate failures, and observability anomalies to the right team at the right severity.

## Quick Start

```python
from sqldim.medallion import Layer
from sqldim.notifications import (
    MemoryChannel, NotificationEvent, NotificationRouter, NotificationRule, Severity,
)

pager, chat = MemoryChannel(), MemoryChannel()  # stand-ins for your channels
router = (
    NotificationRouter()
    .add_channel("pagerduty", pager)
    .add_channel("slack", chat)
    .add_rule(NotificationRule("p1_gold", ["*"], [Layer.GOLD], Severity.P1, ["pagerduty"]))
    .add_rule(NotificationRule("silver_gates", ["quality_gate_fail"], [Layer.SILVER],
                               Severity.P2, ["slack"]))
)

results = router.route(NotificationEvent(
    event_type="schema_violation",
    severity=Severity.P1,
    layer=Layer.GOLD,
    contract_id="orders_v2",
))
assert results == [("pagerduty", True)]
assert len(pager.received()) == 1
```

sqldim ships one channel, the in-memory `MemoryChannel`. It has no Slack,
PagerDuty, Jira, email or webhook channel: you implement those on
`NotificationChannel` (see [Channels](#channels)).

## Severity Model

```python
from sqldim.notifications import Severity

Severity.P1  # Critical — immediate pager
Severity.P2  # High — 2 minute response
Severity.P3  # Medium — 15 minute response
Severity.P4  # Low — daily digest
```

| Level | Name | Response target | Trigger examples |
|---|---|---|---|
| **P1** | Critical | Immediate (pager) | Gold contract breach, Gold SLA miss |
| **P2** | High | 2 minutes | Silver gate failure, Bronze schema violation |
| **P3** | Medium | 15 minutes | OTel latency 3σ drift, Silver freshness warning |
| **P4** | Low | Daily digest | Bronze source lag > 10% of SLA |

## NotificationRouter

`NotificationRouter.route(event)` evaluates every `NotificationRule` in order
and dispatches the event to each channel a matching rule names. It returns
one `(channel_name, success)` pair per dispatch attempted. A rule that names
an unregistered channel raises `KeyError`. `escalate(event)` returns a copy one
level closer to P1, or `None` at P1.

A `NotificationRule(name, event_types, layers, min_severity, channels)`
matches when the event type is in `event_types` (`["*"]` matches any), its
layer is in `layers` (`None` matches any), and its severity is at least as
critical as `min_severity` (`None` matches any).

## Channels

sqldim ships `MemoryChannel`, which stores events in-process for tests and
audits (`received()`, `clear()`). For any other destination, implement
`NotificationChannel.dispatch(event) -> bool`:

```python
from sqldim.notifications import NotificationChannel, NotificationEvent


class SlackChannel(NotificationChannel):  # yours, not part of sqldim
    def __init__(self, webhook_url: str) -> None:
        self.webhook_url = webhook_url

    def dispatch(self, event: NotificationEvent) -> bool:
        ...  # POST to the webhook; return False on failure
        return True
```

The severities, response targets and escalation paths on this page are a
convention for the rules you write. sqldim does not enforce response targets
and does not page anyone.

## Contract-Triggered Events

Events from the [Data Contracts](data_contracts.md) enforcement points map to severity by layer:

Illustrative (not runnable as written):

```python
# After a quality gate failure
if not gate_result.ok:
    router.route(NotificationEvent(
        title=f"Gate failed: {gate.name}",
        severity=Severity.P2 if gate.layer_to == Layer.SILVER else Severity.P1,
        layer=gate.layer_to.value,
        event_type="quality_gate_fail",
        violations=gate_result.violations,
    ))
```

| Contract event | Severity | Layer |
|---|---|---|
| `schema_violation` | P1 (Gold), P2 (Silver/Bronze) | Any |
| `sla_miss.freshness` | P1 (Gold), P4 (Bronze) | Any |
| `sla_miss.completeness` | P1 (Gold), P2 (Silver) | Gold / Silver |
| `version_breaking_change` | P2 | Any |

## Escalation

```
P4 ──(lag > 2×)──► P2 ──(no ack in 10 min)──► P1
                         │
                    (batch quarantined 3× in 24h)
                         │
                         ▼
                        P1 + auto-freeze Bronze source
```

Implement escalation by re-routing events with increased severity:

```python
def escalate_if_repeated(event, history):
    recent_failures = [e for e in history if e.contract_id == event.contract_id]
    if len(recent_failures) >= 3:
        event.severity = Severity.P1
    router.route(event)
```

## Dependencies

- **[Data Contracts](data_contracts.md)** — P1/P2 rules triggered by contract enforcement events
- **[Observability](observability.md)** — P3 sourced from OTel metrics; P4 from ingestion lag
- **[Medallion Layers](medallion_layers.md)** — severity is a function of the originating layer

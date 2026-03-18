# Feature: Notifications

The notification system routes alerts from contract violations, quality gate failures, and observability anomalies to the right team at the right severity.

## Quick Start

```python
from sqldim.notifications import NotificationRouter, Severity
from sqldim.notifications.channels import MemoryChannel

# Create a router and register channels
router = NotificationRouter()
router.add_channel("slack", MemoryChannel())       # swap for SlackChannel in production
router.add_channel("pagerduty", MemoryChannel())   # swap for PagerDutyChannel in production

# Define routing rules
router.add_rule(lambda event: event.severity >= Severity.P1)
router.add_rule(lambda event: event.severity >= Severity.P2 and event.layer == "silver")

# Route an event
router.route(NotificationEvent(
    title="Gold contract breach: orders_v2",
    severity=Severity.P1,
    layer="gold",
    contract_id="orders_v2",
))
```

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

```python
router = NotificationRouter()

# Register delivery channels
router.add_channel("slack", SlackChannel(webhook_url="https://hooks.slack.com/..."))
router.add_channel("pagerduty", PagerDutyChannel(routing_key="..."))
router.add_channel("email", EmailChannel(smtp_config=...))

# Add routing rules (callables that return True to match)
router.add_rule(lambda e: e.severity == Severity.P1 and e.layer == "gold")
router.add_rule(lambda e: e.severity >= Severity.P2 and e.event_type == "quality_gate_fail")

# Route events
router.route(event)
```

## Channels

Implement the `NotificationChannel` protocol to add delivery backends:

```python
from sqldim.notifications import NotificationChannel

class SlackChannel(NotificationChannel):
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url

    def send(self, event: NotificationEvent) -> None:
        # POST to Slack webhook
        ...
```

| Channel | Severity | Notes |
|---|---|---|
| `PagerDutyChannel` | P1 | Integrates with on-call rotation |
| `SlackChannel` | P1, P2, P3 | Per-rule channel targeting |
| `MemoryChannel` | All | In-process buffer for testing |
| `WebhookChannel` | All | Generic egress for custom ITSM |

Use `MemoryChannel` in tests to capture notifications without external dependencies:

```python
router = NotificationRouter()
memory = MemoryChannel()
router.add_channel("test", memory)
router.route(event)
assert len(memory.events) == 1
```

## Contract-Triggered Events

Events from the [Data Contracts](data_contracts.md) enforcement points map to severity by layer:

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

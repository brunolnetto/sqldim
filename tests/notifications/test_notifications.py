"""Tests for Notifications — Severity, NotificationEvent, channels, router."""

import pytest

from sqldim.medallion import Layer
from sqldim.notifications import (
    Severity,
    NotificationEvent,
    NotificationChannel,
    MemoryChannel,
    NotificationRule,
    NotificationRouter,
)


# ---------------------------------------------------------------------------
# Severity
# ---------------------------------------------------------------------------


class TestSeverity:
    def test_p1_is_highest(self):
        assert Severity.P1 < Severity.P2 < Severity.P3 < Severity.P4

    def test_integer_values(self):
        assert int(Severity.P1) == 1
        assert int(Severity.P4) == 4

    def test_all_members_exist(self):
        for name in ("P1", "P2", "P3", "P4"):
            assert hasattr(Severity, name)


# ---------------------------------------------------------------------------
# NotificationEvent
# ---------------------------------------------------------------------------


class TestNotificationEvent:
    def test_required_fields(self):
        ev = NotificationEvent(
            event_type="contract_violation",
            severity=Severity.P1,
            layer=Layer.SILVER,
        )
        assert ev.event_type == "contract_violation"
        assert ev.severity is Severity.P1
        assert ev.layer is Layer.SILVER
        assert ev.contract_id is None
        assert ev.details == {}
        assert ev.timestamp is not None

    def test_optional_contract_id(self):
        ev = NotificationEvent(
            event_type="sla_miss",
            severity=Severity.P2,
            layer=Layer.GOLD,
            contract_id="orders_v2",
            details={"threshold": 30, "actual": 45},
        )
        assert ev.contract_id == "orders_v2"
        assert ev.details["threshold"] == 30

    def test_timestamp_is_recent(self):
        from datetime import datetime, timezone

        before = datetime.now(timezone.utc)
        ev = NotificationEvent("x", Severity.P4, Layer.BRONZE)
        after = datetime.now(timezone.utc)
        assert before <= ev.timestamp <= after


# ---------------------------------------------------------------------------
# NotificationChannel (abstract base)
# ---------------------------------------------------------------------------


class TestNotificationChannelABC:
    def test_cannot_instantiate_abstract(self):
        with pytest.raises(TypeError):
            NotificationChannel()  # type: ignore

    def test_subclass_must_implement_dispatch(self):
        class Incomplete(NotificationChannel):
            pass

        with pytest.raises(TypeError):
            Incomplete()  # type: ignore


# ---------------------------------------------------------------------------
# MemoryChannel
# ---------------------------------------------------------------------------


class TestMemoryChannel:
    def _ev(self, event_type="test", severity=Severity.P3, layer=Layer.BRONZE):
        return NotificationEvent(event_type, severity, layer)

    def test_dispatch_returns_true(self):
        ch = MemoryChannel()
        result = ch.dispatch(self._ev())
        assert result is True

    def test_received_empty_initially(self):
        ch = MemoryChannel()
        assert ch.received() == []

    def test_dispatch_stores_event(self):
        ch = MemoryChannel()
        ev = self._ev("gate_failure", Severity.P2)
        ch.dispatch(ev)
        assert len(ch.received()) == 1
        assert ch.received()[0] is ev

    def test_multiple_events(self):
        ch = MemoryChannel()
        ch.dispatch(self._ev("a"))
        ch.dispatch(self._ev("b"))
        assert len(ch.received()) == 2

    def test_clear_resets(self):
        ch = MemoryChannel()
        ch.dispatch(self._ev())
        ch.clear()
        assert ch.received() == []

    def test_received_returns_copy(self):
        ch = MemoryChannel()
        ch.dispatch(self._ev())
        r1 = ch.received()
        r1.append(self._ev("extra"))
        assert len(ch.received()) == 1  # original is unchanged


# ---------------------------------------------------------------------------
# NotificationRule
# ---------------------------------------------------------------------------


class TestNotificationRule:
    def _ev(self, event_type="gate_failure", severity=Severity.P2, layer=Layer.SILVER):
        return NotificationEvent(event_type, severity, layer)

    def test_match_event_type(self):
        rule = NotificationRule(
            name="r",
            event_types=["gate_failure"],
            layers=None,
            min_severity=None,
            channels=["ops"],
        )
        assert rule.matches(self._ev("gate_failure")) is True
        assert rule.matches(self._ev("sla_miss")) is False

    def test_match_all_event_types_wildcard(self):
        rule = NotificationRule(
            name="r",
            event_types=["*"],
            layers=None,
            min_severity=None,
            channels=["ops"],
        )
        assert rule.matches(self._ev("anything")) is True

    def test_match_specific_layer(self):
        rule = NotificationRule(
            name="r",
            event_types=["*"],
            layers=[Layer.SILVER],
            min_severity=None,
            channels=["ops"],
        )
        assert rule.matches(self._ev(layer=Layer.SILVER)) is True
        assert rule.matches(self._ev(layer=Layer.GOLD)) is False

    def test_match_min_severity(self):
        rule = NotificationRule(
            name="r",
            event_types=["*"],
            layers=None,
            min_severity=Severity.P2,
            channels=["ops"],
        )
        assert rule.matches(self._ev(severity=Severity.P1)) is True
        assert rule.matches(self._ev(severity=Severity.P2)) is True
        assert rule.matches(self._ev(severity=Severity.P3)) is False

    def test_no_match_wrong_type_and_layer(self):
        rule = NotificationRule(
            name="r",
            event_types=["sla_miss"],
            layers=[Layer.GOLD],
            min_severity=None,
            channels=["ops"],
        )
        ev = self._ev("gate_failure", layer=Layer.SILVER)
        assert rule.matches(ev) is False


# ---------------------------------------------------------------------------
# NotificationRouter
# ---------------------------------------------------------------------------


class TestNotificationRouter:
    def _make_router(self):
        router = NotificationRouter()
        ch = MemoryChannel()
        router.add_channel("mem", ch)
        return router, ch

    def _ev(self, event_type="gate_failure", severity=Severity.P2, layer=Layer.SILVER):
        return NotificationEvent(event_type, severity, layer)

    def test_add_channel_returns_self(self):
        router = NotificationRouter()
        returned = router.add_channel("x", MemoryChannel())
        assert returned is router

    def test_add_rule_returns_self(self):
        router = NotificationRouter()
        rule = NotificationRule("r", ["*"], None, None, ["x"])
        returned = router.add_rule(rule)
        assert returned is router

    def test_route_no_rules_returns_empty(self):
        router, _ = self._make_router()
        results = router.route(self._ev())
        assert results == []

    def test_route_matching_rule_dispatches(self):
        router, ch = self._make_router()
        rule = NotificationRule("r", ["gate_failure"], None, None, ["mem"])
        router.add_rule(rule)
        ev = self._ev("gate_failure")
        results = router.route(ev)
        assert len(results) == 1
        assert results[0] == ("mem", True)
        assert len(ch.received()) == 1

    def test_route_non_matching_rule_skips(self):
        router, ch = self._make_router()
        rule = NotificationRule("r", ["sla_miss"], None, None, ["mem"])
        router.add_rule(rule)
        ev = self._ev("gate_failure")
        results = router.route(ev)
        assert results == []
        assert ch.received() == []

    def test_route_multiple_matching_rules(self):
        ch1, ch2 = MemoryChannel(), MemoryChannel()
        router = (
            NotificationRouter()
            .add_channel("ch1", ch1)
            .add_channel("ch2", ch2)
            .add_rule(NotificationRule("r1", ["*"], None, None, ["ch1"]))
            .add_rule(NotificationRule("r2", ["*"], None, None, ["ch2"]))
        )
        ev = self._ev()
        results = router.route(ev)
        assert len(results) == 2
        assert len(ch1.received()) == 1
        assert len(ch2.received()) == 1

    def test_route_unknown_channel_raises(self):
        router = NotificationRouter()
        rule = NotificationRule("r", ["*"], None, None, ["nonexistent"])
        router.add_rule(rule)
        with pytest.raises(KeyError):
            router.route(self._ev())

    def test_escalate_returns_none_when_already_p1(self):
        router = NotificationRouter()
        ev = self._ev(severity=Severity.P1)
        assert router.escalate(ev) is None

    def test_escalate_bumps_severity(self):
        router = NotificationRouter()
        ev = self._ev(severity=Severity.P3)
        escalated = router.escalate(ev)
        assert escalated is not None
        assert escalated.severity == Severity.P2
        assert escalated.event_type == ev.event_type

    def test_escalate_p2_to_p1(self):
        router = NotificationRouter()
        ev = self._ev(severity=Severity.P2)
        escalated = router.escalate(ev)
        assert escalated.severity == Severity.P1

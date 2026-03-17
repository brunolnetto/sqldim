"""Notification router — applies rules and dispatches to channels."""
from __future__ import annotations

import dataclasses

from sqldim.notifications.channel import NotificationChannel
from sqldim.notifications.event import NotificationEvent
from sqldim.notifications.rule import NotificationRule
from sqldim.notifications.severity import Severity


class NotificationRouter:
    """Routes :class:`NotificationEvent` objects through matching rules and channels.

    Usage::

        router = (
            NotificationRouter()
            .add_channel("pagerduty", PagerDutyChannel(...))
            .add_channel("slack",     SlackChannel(...))
            .add_rule(NotificationRule("p1_all", ["*"], None, Severity.P1, ["pagerduty"]))
            .add_rule(NotificationRule("all",    ["*"], None, None,         ["slack"]))
        )
        router.route(event)
    """

    def __init__(self) -> None:
        self._rules: list[NotificationRule] = []
        self._channels: dict[str, NotificationChannel] = {}

    def add_rule(self, rule: NotificationRule) -> "NotificationRouter":
        """Register a routing rule; returns *self* for method chaining."""
        self._rules.append(rule)
        return self

    def add_channel(self, name: str, channel: NotificationChannel) -> "NotificationRouter":
        """Register a named channel; returns *self* for method chaining."""
        self._channels[name] = channel
        return self

    def route(self, event: NotificationEvent) -> list[tuple[str, bool]]:
        """Evaluate all rules against *event* and dispatch to matching channels.

        Returns a list of ``(channel_name, success)`` tuples for every
        dispatch that was attempted.

        Raises :class:`KeyError` if a matching rule references an unknown
        channel name.
        """
        results: list[tuple[str, bool]] = []
        for rule in self._rules:
            if rule.matches(event):
                for ch_name in rule.channels:
                    channel = self._channels[ch_name]  # KeyError if unknown
                    ok = channel.dispatch(event)
                    results.append((ch_name, ok))
        return results

    def escalate(self, event: NotificationEvent) -> NotificationEvent | None:
        """Return a copy of *event* with severity bumped one level towards P1.

        Returns ``None`` if the event is already at :attr:`Severity.P1`.
        """
        if event.severity == Severity.P1:
            return None
        new_severity = Severity(int(event.severity) - 1)
        return dataclasses.replace(event, severity=new_severity)

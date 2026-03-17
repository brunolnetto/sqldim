"""Notification routing rule."""
from __future__ import annotations

from dataclasses import dataclass

from sqldim.medallion import Layer
from sqldim.notifications.event import NotificationEvent
from sqldim.notifications.severity import Severity


@dataclass
class NotificationRule:
    """Declarative rule that maps events to channel names.

    Parameters
    ----------
    name:
        Human-readable rule identifier.
    event_types:
        List of event type strings this rule matches.  Use ``["*"]`` to
        match any type.
    layers:
        List of :class:`~sqldim.medallion.Layer` values this rule applies
        to.  ``None`` means all layers.
    min_severity:
        Minimum (most critical) severity required for a match.  ``None``
        matches any severity.  Because :class:`Severity` is an
        :class:`~enum.IntEnum` with P1=1, *lower* integer values are *more*
        critical — an event at P1 satisfies ``min_severity=P2``.
    channels:
        Channel names (keys registered in :class:`NotificationRouter`) that
        should receive the event when the rule matches.
    """

    name: str
    event_types: list[str]
    layers: list[Layer] | None
    min_severity: Severity | None
    channels: list[str]

    def matches(self, event: NotificationEvent) -> bool:
        """Return ``True`` if *event* satisfies every filter in this rule."""
        # Event-type filter — "*" acts as wildcard
        if "*" not in self.event_types and event.event_type not in self.event_types:
            return False
        # Layer filter
        if self.layers is not None and event.layer not in self.layers:
            return False
        # Severity filter — event must be at least as critical as threshold
        if self.min_severity is not None and event.severity > self.min_severity:
            return False
        return True

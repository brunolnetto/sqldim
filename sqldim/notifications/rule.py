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

    def _type_matches(self, event: NotificationEvent) -> bool:
        return "*" in self.event_types or event.event_type in self.event_types

    def _layer_matches(self, event: NotificationEvent) -> bool:
        return self.layers is None or event.layer in self.layers

    def _severity_matches(self, event: NotificationEvent) -> bool:
        return self.min_severity is None or event.severity <= self.min_severity

    def matches(self, event: NotificationEvent) -> bool:
        """Return ``True`` if *event* satisfies every filter in this rule."""
        return (
            self._type_matches(event)
            and self._layer_matches(event)
            and self._severity_matches(event)
        )

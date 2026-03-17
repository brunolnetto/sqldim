"""sqldim.notifications — event-driven alert routing for pipeline failures."""
from sqldim.notifications.severity import Severity
from sqldim.notifications.event import NotificationEvent
from sqldim.notifications.channel import NotificationChannel, MemoryChannel
from sqldim.notifications.rule import NotificationRule
from sqldim.notifications.router import NotificationRouter

__all__ = [
    "Severity",
    "NotificationEvent",
    "NotificationChannel",
    "MemoryChannel",
    "NotificationRule",
    "NotificationRouter",
]

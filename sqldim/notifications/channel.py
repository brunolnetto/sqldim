"""Notification channel abstractions."""

from __future__ import annotations

from abc import ABC, abstractmethod

from sqldim.notifications.event import NotificationEvent


class NotificationChannel(ABC):
    """Abstract base for all notification channels."""

    @abstractmethod
    def dispatch(self, event: NotificationEvent) -> bool:
        """Send *event*; return ``True`` on success, ``False`` on failure."""


class MemoryChannel(NotificationChannel):
    """Stores dispatched events in-memory — useful for testing and audit."""

    def __init__(self) -> None:
        self._events: list[NotificationEvent] = []

    def dispatch(self, event: NotificationEvent) -> bool:
        self._events.append(event)
        return True

    def received(self) -> list[NotificationEvent]:
        """Return a copy of all received events in dispatch order."""
        return list(self._events)

    def clear(self) -> None:
        """Remove all stored events."""
        self._events.clear()

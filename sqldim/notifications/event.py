"""Notification event model."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from sqldim.medallion import Layer
from sqldim.notifications.severity import Severity


@dataclass
class NotificationEvent:
    """An event that may trigger one or more notification dispatches."""

    event_type: str
    severity: Severity
    layer: Layer
    contract_id: str | None = None
    details: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

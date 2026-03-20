"""Pipeline span model."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class SpanStatus(str, Enum):
    OK = "ok"
    ERROR = "error"


@dataclass
class PipelineSpan:
    """Record of a single pipeline execution span."""

    name: str
    attributes: dict[str, Any] = field(default_factory=dict)
    status: SpanStatus = SpanStatus.OK
    duration_s: float = 0.0
    start_time: datetime | None = None
    end_time: datetime | None = None
    error: str | None = None

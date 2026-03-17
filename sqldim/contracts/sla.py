"""sqldim/contracts/sla.py — SLA specification for data contracts."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class SLASpec:
    """Declares freshness, completeness, and latency expectations."""

    freshness_minutes:   float | None = None
    completeness_pct:    float | None = None
    latency_p99_minutes: float | None = None

    def is_freshness_met(self, actual_minutes: float) -> bool:
        if self.freshness_minutes is None:
            return True
        return actual_minutes <= self.freshness_minutes

    def is_completeness_met(self, actual_pct: float) -> bool:
        if self.completeness_pct is None:
            return True
        return actual_pct >= self.completeness_pct

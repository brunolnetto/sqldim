"""DGM temporal mode types — TemporalMode, singleton subclasses, UntilMode, SinceMode
(DGM v0.16 §4.1)."""

from __future__ import annotations

__all__ = [
    "TemporalMode",
    "EVENTUALLY",
    "GLOBALLY",
    "NEXT",
    "ONCE",
    "PREVIOUSLY",
    "UntilMode",
    "SinceMode",
]


# ---------------------------------------------------------------------------
# TemporalMode  (DGM §4.1 — used inside PathPred.temporal_mode)
# ---------------------------------------------------------------------------


class TemporalMode:
    """Abstract base for all CTL temporal modes used in PathPred."""


class _SingletonMode(TemporalMode):
    """Base for modes without a payload — subclasses are used as the class itself."""

    sql_operator: str = ""


class EVENTUALLY(_SingletonMode):
    """EF / AF — the property holds at some future hop."""

    sql_operator = "EVENTUALLY"


class GLOBALLY(_SingletonMode):
    """EG / AG — the property holds at every hop."""

    sql_operator = "GLOBALLY"


class NEXT(_SingletonMode):
    """EX / AX — the property holds at the immediately next hop."""

    sql_operator = "NEXT"


class ONCE(_SingletonMode):
    """Past dual of NEXT — holds at the immediately previous hop."""

    sql_operator = "ONCE"


class PREVIOUSLY(_SingletonMode):
    """Past dual of GLOBALLY — holds at every prior hop."""

    sql_operator = "PREVIOUSLY"


class UntilMode(TemporalMode):
    """E(φ U ψ) / A(φ U ψ) — φ holds until ψ becomes true (CTL operator)."""

    def __init__(self, pred: object) -> None:
        self.pred = pred


class SinceMode(TemporalMode):
    """Past dual of UNTIL — φ has held since ψ (LTL past operator)."""

    def __init__(self, pred: object) -> None:
        self.pred = pred

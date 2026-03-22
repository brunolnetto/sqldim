"""DGM temporal-property sugar and signature predicates (DGM §4.1).

SAFETY, LIVENESS, RESPONSE, PERSISTENCE, RECURRENCE, SequenceMatch, SignaturePred.
"""

from __future__ import annotations

import enum


# ---------------------------------------------------------------------------
# TemporalProperty sugar  (DGM §4.1)
# ---------------------------------------------------------------------------


class SAFETY:
    """AG(¬bad) — the bad state is globally unreachable on all paths."""

    def __init__(self, bad: object) -> None:
        self.bad = bad

    def to_sql(self) -> str:
        return f"AG(NOT ({self.bad.to_sql()}))"


class LIVENESS:
    """AF(good) — the good state is eventually reached on all paths."""

    def __init__(self, good: object) -> None:
        self.good = good

    def to_sql(self) -> str:
        return f"AF({self.good.to_sql()})"


class RESPONSE:
    """AG(trigger → AF(response)) — every trigger is eventually followed by a response."""

    def __init__(self, trigger: object, response: object) -> None:
        self.trigger = trigger
        self.response = response

    def to_sql(self) -> str:
        return f"AG({self.trigger.to_sql()} -> AF({self.response.to_sql()}))"


class PERSISTENCE:
    """AF(AG(good)) — the good state is eventually and permanently reached."""

    def __init__(self, good: object) -> None:
        self.good = good

    def to_sql(self) -> str:
        return f"AF(AG({self.good.to_sql()}))"


class RECURRENCE:
    """good occurs in every window period (liveness within a bounded window)."""

    def __init__(self, good: object, window: object) -> None:
        self.good = good
        self.window = window

    def to_sql(self) -> str:
        win_sql = self.window.to_sql() if hasattr(self.window, "to_sql") else str(self.window)
        return f"G_win({self.good.to_sql()}, {win_sql})"


# ---------------------------------------------------------------------------
# SignaturePred  (DGM §4.1)
# ---------------------------------------------------------------------------


class SequenceMatch(enum.Enum):
    """Match mode for :class:`SignaturePred`."""

    EXACT = "EXACT"
    PREFIX = "PREFIX"
    CONTAINS = "CONTAINS"
    REGEX = "REGEX"


class SignaturePred:
    """Filter on the label-sequence signature of a named path instance.

    Parameters
    ----------
    path_alias:
        Alias of the BoundPath instance whose signature is matched.
    sequence:
        Ordered list of edge labels (or ``None`` for a wildcard position).
    match:
        Matching strategy — EXACT, PREFIX, CONTAINS, or REGEX.
    """

    def __init__(
        self,
        path_alias: str,
        sequence: list,
        match: SequenceMatch,
    ) -> None:
        self.path_alias = path_alias
        self.sequence = sequence
        self.match = match

    def to_sql(self) -> str:
        seq_repr = repr(self.sequence)
        return (
            f"signature_match({self.path_alias}, {seq_repr}, {self.match.value!r})"
        )

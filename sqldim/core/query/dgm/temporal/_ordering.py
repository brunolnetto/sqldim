"""DGM temporal ordering types — TemporalOrdering, Allen's 13 interval relations,
UntilOrdering, SinceOrdering (DGM v0.16 §3.2)."""

from __future__ import annotations

__all__ = [
    "TemporalOrdering",
    "BEFORE",
    "AFTER",
    "CONCURRENT",
    "MEETS",
    "MET_BY",
    "OVERLAPS",
    "OVERLAPPED_BY",
    "STARTS",
    "STARTED_BY",
    "DURING",
    "CONTAINS",
    "FINISHES",
    "FINISHED_BY",
    "EQUALS",
    "UntilOrdering",
    "SinceOrdering",
]


# ---------------------------------------------------------------------------
# TemporalOrdering  (DGM §3.2 — used in Compose.temporal)
# ---------------------------------------------------------------------------


class TemporalOrdering:
    """Abstract base for all ordering constraints on composed path hops."""

    def to_sql(self) -> str:  # pragma: no cover
        raise NotImplementedError(f"{type(self).__name__}.to_sql()")


# ── Point-based orderings ────────────────────────────────────────────────────


class BEFORE(TemporalOrdering):
    """left_ts < right_ts (strict temporal precedence)."""

    def __init__(self, left_ts: str, right_ts: str) -> None:
        self.left_ts = left_ts
        self.right_ts = right_ts

    def to_sql(self) -> str:
        return f"{self.left_ts} < {self.right_ts}"


class AFTER(TemporalOrdering):
    """left_ts > right_ts (strict temporal succession)."""

    def __init__(self, left_ts: str, right_ts: str) -> None:
        self.left_ts = left_ts
        self.right_ts = right_ts

    def to_sql(self) -> str:
        return f"{self.left_ts} > {self.right_ts}"


class CONCURRENT(TemporalOrdering):
    """left_ts ≈ right_ts — equal or within optional tolerance."""

    def __init__(
        self,
        left_ts: str,
        right_ts: str,
        tolerance: "object | None" = None,
    ) -> None:
        self.left_ts = left_ts
        self.right_ts = right_ts
        self.tolerance = tolerance

    def to_sql(self) -> str:
        if self.tolerance is None:
            return f"{self.left_ts} = {self.right_ts}"
        return f"ABS({self.left_ts} - {self.right_ts}) <= {self.tolerance}"


# ── Allen's 13 interval relations ────────────────────────────────────────────
# Each takes l=(l_start, l_end) and r=(r_start, r_end) tuples.


class _AllenBase(TemporalOrdering):
    def __init__(self, left: tuple, right: tuple) -> None:
        self.left = left
        self.right = right

    def to_sql(self) -> str:  # pragma: no cover
        raise NotImplementedError(f"{type(self).__name__}.to_sql()")


class MEETS(_AllenBase):
    """l ends exactly when r starts: l_end = r_start."""

    def to_sql(self) -> str:
        return f"{self.left[1]} = {self.right[0]}"


class MET_BY(_AllenBase):
    """r ends exactly when l starts: r_end = l_start."""

    def to_sql(self) -> str:
        return f"{self.right[1]} = {self.left[0]}"


class OVERLAPS(_AllenBase):
    """l starts before r, ends after r starts but before r ends."""

    def to_sql(self) -> str:
        ls, le = self.left
        rs, re = self.right
        return f"{ls} < {rs} AND {le} > {rs} AND {le} < {re}"


class OVERLAPPED_BY(_AllenBase):
    """r starts before l, ends after l starts but before l ends."""

    def to_sql(self) -> str:
        ls, le = self.left
        rs, re = self.right
        return f"{rs} < {ls} AND {re} > {ls} AND {re} < {le}"


class STARTS(_AllenBase):
    """l and r share a start; l ends before r."""

    def to_sql(self) -> str:
        ls, le = self.left
        rs, re = self.right
        return f"{ls} = {rs} AND {le} < {re}"


class STARTED_BY(_AllenBase):
    """l and r share a start; l ends after r."""

    def to_sql(self) -> str:
        ls, le = self.left
        rs, re = self.right
        return f"{ls} = {rs} AND {le} > {re}"


class DURING(_AllenBase):
    """l is entirely within r."""

    def to_sql(self) -> str:
        ls, le = self.left
        rs, re = self.right
        return f"{ls} > {rs} AND {le} < {re}"


class CONTAINS(_AllenBase):
    """r is entirely within l."""

    def to_sql(self) -> str:
        ls, le = self.left
        rs, re = self.right
        return f"{ls} < {rs} AND {le} > {re}"


class FINISHES(_AllenBase):
    """l and r share an end; l starts after r."""

    def to_sql(self) -> str:
        ls, le = self.left
        rs, re = self.right
        return f"{le} = {re} AND {ls} > {rs}"


class FINISHED_BY(_AllenBase):
    """l and r share an end; l starts before r."""

    def to_sql(self) -> str:
        ls, le = self.left
        rs, re = self.right
        return f"{le} = {re} AND {ls} < {rs}"


class EQUALS(_AllenBase):
    """l and r have the same start and end."""

    def to_sql(self) -> str:
        ls, le = self.left
        rs, re = self.right
        return f"{ls} = {rs} AND {le} = {re}"


# ── LTL path-level operators ─────────────────────────────────────────────────


class UntilOrdering(TemporalOrdering):
    """φ holds at every intermediate hop until ψ first becomes true."""

    def __init__(self, hold_pred: object, trigger_pred: object) -> None:
        self.hold_pred = hold_pred
        self.trigger_pred = trigger_pred

    def to_sql(self) -> str:
        return (
            f"path_until({self.hold_pred.to_sql()}, {self.trigger_pred.to_sql()})"
        )


class SinceOrdering(TemporalOrdering):
    """Past dual of UNTIL: φ has held at every hop since ψ last became true."""

    def __init__(self, hold_pred: object, trigger_pred: object) -> None:
        self.hold_pred = hold_pred
        self.trigger_pred = trigger_pred

    def to_sql(self) -> str:
        return (
            f"path_since({self.hold_pred.to_sql()}, {self.trigger_pred.to_sql()})"
        )

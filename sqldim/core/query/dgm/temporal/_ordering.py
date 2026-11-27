"""DGM temporal ordering types — TemporalOrdering, Allen's 13 interval relations,
UntilOrdering, SinceOrdering (DGM §3.2).

The constructors check TO1–TO3 (TO4 needs typed arguments, which plain SQL
text does not carry).  Allen relations read a NULL bound as unbounded on its
side and hold on no degenerate interval; their SQL is two-valued.
"""

from __future__ import annotations

from sqldim.core.query.dgm.refs import _SQLExpr
from sqldim.exceptions import SemanticError

__all__ = [
    "TemporalOrdering",
    "BEFORE",
    "AFTER",
    "CONCURRENT",
    "PRECEDES",
    "PRECEDED_BY",
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


# ── Typing (TO1–TO3, checked at construction) ─────────────────────────────────


def _is_interval(arg: object) -> bool:
    return isinstance(arg, (tuple, list)) and len(arg) == 2


def _point_arg(arg: object, rel: str) -> object:
    if isinstance(arg, (tuple, list)):
        raise SemanticError(
            f"{rel}: an Interval where a TimePoint is required is rejected (TO1)"
        )
    return arg


def _interval_arg(arg: object, rel: str) -> tuple:
    if not _is_interval(arg):
        raise SemanticError(
            f"{rel} takes two Intervals (start, end); a TimePoint is not "
            "widened to an interval (TO2)"
        )
    return tuple(arg)  # type: ignore[arg-type]


def _check_tolerance(tolerance: object) -> None:
    number = isinstance(tolerance, (int, float)) and not isinstance(tolerance, bool)
    if number and tolerance < 0:  # type: ignore[operator]
        raise SemanticError(f"CONCURRENT: tolerance {tolerance!r} is negative (TO3)")


# ── Point-based orderings ────────────────────────────────────────────────────
# A NULL TimePoint makes the comparison UNKNOWN, which does not hold (§3.2).


class _PointBase(TemporalOrdering):
    def __init__(self, left_ts: str, right_ts: str) -> None:
        self.left_ts = _point_arg(left_ts, type(self).__name__)
        self.right_ts = _point_arg(right_ts, type(self).__name__)


class BEFORE(_PointBase):
    """left_ts < right_ts (strict temporal precedence)."""

    def to_sql(self) -> str:
        return f"{self.left_ts} < {self.right_ts}"


class AFTER(_PointBase):
    """left_ts > right_ts (strict temporal succession)."""

    def to_sql(self) -> str:
        return f"{self.left_ts} > {self.right_ts}"


class CONCURRENT(_PointBase):
    """left_ts ≈ right_ts — equal or within optional tolerance."""

    def __init__(
        self,
        left_ts: str,
        right_ts: str,
        tolerance: "object | None" = None,
    ) -> None:
        super().__init__(left_ts, right_ts)
        _check_tolerance(tolerance)
        self.tolerance = tolerance

    def to_sql(self) -> str:
        if self.tolerance is None:
            return f"{self.left_ts} = {self.right_ts}"
        return f"ABS({self.left_ts} - {self.right_ts}) <= {self.tolerance}"


# ── Allen's 13 interval relations ────────────────────────────────────────────
# Each takes l = (l_start, l_end) and r = (r_start, r_end), half-open.  A NULL
# start is earlier than every instant and a NULL end later than every instant;
# two NULL ends are equal.  An interval with start ≥ end is degenerate and
# satisfies no relation (§3.2).

_START, _END = -1, 1  # the infinity a NULL bound stands for on each side

_SQL_BOOL = {True: "TRUE", False: "FALSE"}


def _bound_cmp(x: str, x_side: int, op: str, y: str, y_side: int) -> str:
    """``x op y`` (``<`` or ``=``), two-valued, a NULL bound read as ±∞."""
    if op == "<":
        both, x_null, y_null = x_side < y_side, x_side == _START, y_side == _END
    else:
        both, x_null, y_null = x_side == y_side, False, False
    return (
        f"CASE WHEN {x} IS NULL AND {y} IS NULL THEN {_SQL_BOOL[both]}"
        f" WHEN {x} IS NULL THEN {_SQL_BOOL[x_null]}"
        f" WHEN {y} IS NULL THEN {_SQL_BOOL[y_null]}"
        f" ELSE {x} {op} {y} END"
    )


def _proper(iv: tuple) -> str:
    """``start < end`` with NULL bounds open: the interval is not degenerate."""
    s, e = iv
    return f"({s} IS NULL OR {e} IS NULL OR {s} < {e})"


class _AllenBase(TemporalOrdering):
    #: The defining conditions of §3.2 as ``(x, op, y)`` over the bounds
    #: a = l.start, b = l.end, c = r.start, d = r.end.
    conditions: tuple[tuple[str, str, str], ...] = ()

    def __init__(self, left: tuple, right: tuple) -> None:
        self.left = _interval_arg(left, type(self).__name__)
        self.right = _interval_arg(right, type(self).__name__)

    def to_sql(self) -> str:
        (a, b), (c, d) = self.left, self.right
        bounds = {"a": (a, _START), "b": (b, _END), "c": (c, _START), "d": (d, _END)}
        parts = [_proper(self.left), _proper(self.right)]
        for x, op, y in self.conditions:
            (xs, xside), (ys, yside) = bounds[x], bounds[y]
            parts.append(_bound_cmp(xs, xside, op, ys, yside))
        return "(" + " AND ".join(parts) + ")"


class PRECEDES(_AllenBase):
    """l ends before r starts, with a gap: l.end < r.start."""

    conditions = (("b", "<", "c"),)


class PRECEDED_BY(_AllenBase):
    """r ends before l starts, with a gap: r.end < l.start."""

    conditions = (("d", "<", "a"),)


class MEETS(_AllenBase):
    """l ends exactly when r starts: l.end = r.start."""

    conditions = (("b", "=", "c"),)


class MET_BY(_AllenBase):
    """r ends exactly when l starts: r.end = l.start."""

    conditions = (("d", "=", "a"),)


class OVERLAPS(_AllenBase):
    """l.start < r.start < l.end < r.end."""

    conditions = (("a", "<", "c"), ("c", "<", "b"), ("b", "<", "d"))


class OVERLAPPED_BY(_AllenBase):
    """r.start < l.start < r.end < l.end."""

    conditions = (("c", "<", "a"), ("a", "<", "d"), ("d", "<", "b"))


class STARTS(_AllenBase):
    """l and r share a start; l ends before r."""

    conditions = (("a", "=", "c"), ("b", "<", "d"))


class STARTED_BY(_AllenBase):
    """l and r share a start; l ends after r."""

    conditions = (("a", "=", "c"), ("d", "<", "b"))


class DURING(_AllenBase):
    """l is entirely within r."""

    conditions = (("c", "<", "a"), ("b", "<", "d"))


class CONTAINS(_AllenBase):
    """r is entirely within l."""

    conditions = (("a", "<", "c"), ("d", "<", "b"))


class FINISHES(_AllenBase):
    """l and r share an end; l starts after r."""

    conditions = (("b", "=", "d"), ("c", "<", "a"))


class FINISHED_BY(_AllenBase):
    """l and r share an end; l starts before r."""

    conditions = (("b", "=", "d"), ("a", "<", "c"))


class EQUALS(_AllenBase):
    """l and r have the same start and end."""

    conditions = (("a", "=", "c"), ("b", "=", "d"))


# ── LTL path-level operators ─────────────────────────────────────────────────


class UntilOrdering(TemporalOrdering):
    """φ holds at every intermediate hop until ψ first becomes true."""

    def __init__(self, hold_pred: _SQLExpr, trigger_pred: _SQLExpr) -> None:
        self.hold_pred = hold_pred
        self.trigger_pred = trigger_pred

    def to_sql(self) -> str:
        return f"path_until({self.hold_pred.to_sql()}, {self.trigger_pred.to_sql()})"


class SinceOrdering(TemporalOrdering):
    """Past dual of UNTIL: φ has held at every hop since ψ last became true."""

    def __init__(self, hold_pred: _SQLExpr, trigger_pred: _SQLExpr) -> None:
        self.hold_pred = hold_pred
        self.trigger_pred = trigger_pred

    def to_sql(self) -> str:
        return f"path_since({self.hold_pred.to_sql()}, {self.trigger_pred.to_sql()})"

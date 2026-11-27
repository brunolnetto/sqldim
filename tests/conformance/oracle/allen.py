"""TemporalOrdering: point relations and Allen's 13 interval relations (§3.2, §8.8).

Intervals are half-open ``[start, end)``.  A NULL ``start`` is earlier than
every instant and a NULL ``end`` later than every instant; two NULL ends are
equal.  An interval with ``start ≥ end`` is degenerate and satisfies no Allen
relation.  A point relation with a NULL TimePoint is UNKNOWN and does not hold.
"""

from __future__ import annotations

from collections.abc import Callable

_NEG, _POS = float("-inf"), float("inf")

Interval = tuple[object, object]

ALLEN: tuple[str, ...] = (
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
)


def _bounds(i: Interval) -> tuple[float, float]:
    s, e = i
    return (_NEG if s is None else float(s)), (_POS if e is None else float(e))  # type: ignore[arg-type]


def degenerate(i: Interval) -> bool:
    s, e = _bounds(i)
    return s >= e


def open_bound(i: Interval) -> bool:
    return i[0] is None or i[1] is None


_DEFS: dict[str, Callable[[float, float, float, float], bool]] = {
    # the §3.2 table, l = [a, b), r = [c, d)
    "PRECEDES": lambda a, b, c, d: b < c,
    "PRECEDED_BY": lambda a, b, c, d: d < a,
    "MEETS": lambda a, b, c, d: b == c,
    "MET_BY": lambda a, b, c, d: d == a,
    "OVERLAPS": lambda a, b, c, d: a < c < b < d,
    "OVERLAPPED_BY": lambda a, b, c, d: c < a < d < b,
    "STARTS": lambda a, b, c, d: a == c and b < d,
    "STARTED_BY": lambda a, b, c, d: a == c and d < b,
    "DURING": lambda a, b, c, d: c < a and b < d,
    "CONTAINS": lambda a, b, c, d: a < c and d < b,
    "FINISHES": lambda a, b, c, d: b == d and c < a,
    "FINISHED_BY": lambda a, b, c, d: b == d and a < c,
    "EQUALS": lambda a, b, c, d: a == c and b == d,
}


def allen(rel: str, left: Interval, right: Interval) -> bool:
    """Does Allen relation *rel* hold between *left* and *right*?"""
    if degenerate(left) or degenerate(right):
        return False
    a, b = _bounds(left)
    c, d = _bounds(right)
    return _DEFS[rel](a, b, c, d)


def relations(left: Interval, right: Interval) -> list[str]:
    return [r for r in ALLEN if allen(r, left, right)]


def domain_of(left: Interval, right: Interval) -> str:
    """``degenerate`` if either is degenerate, else ``open`` if a bound is NULL."""
    if degenerate(left) or degenerate(right):
        return "degenerate"
    return "open" if open_bound(left) or open_bound(right) else "proper"


POINT_RELATIONS = ("BEFORE", "AFTER", "CONCURRENT")


def typing(case_input: dict) -> str:
    """TO1–TO4 of §3.2, checked at construction."""
    rel, args = case_input["relation"], case_input["args"]
    if rel in ALLEN and "point" in args:
        return "REJECTED"  # TO2
    if rel in POINT_RELATIONS and "interval" in args:
        return "REJECTED"  # TO1
    tol = case_input.get("tolerance")
    if tol is not None and tol < 0:
        return "REJECTED"  # TO3
    types = case_input.get("types")
    if types is not None and len(set(types)) > 1:
        return "REJECTED"  # TO4
    return "ACCEPTED"


def point(rel: str, a: object, b: object, tolerance: float = 0) -> bool:
    """BEFORE / AFTER / CONCURRENT; NULL → UNKNOWN → does not hold."""
    if a is None or b is None:
        return False
    x, y = float(a), float(b)  # type: ignore[arg-type]
    if rel == "BEFORE":
        return x < y
    if rel == "AFTER":
        return x > y
    if rel == "CONCURRENT":
        return abs(x - y) <= tolerance
    raise ValueError(rel)

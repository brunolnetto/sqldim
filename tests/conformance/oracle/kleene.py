"""Three-valued logic and the NULL rules of §4.1 / §6.1.

``True``/``False`` are TRUE/FALSE; ``None`` is UNKNOWN.  A filter keeps a
tuple only when its predicate is TRUE (§4.1 *NULL rules*).
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Optional

K = Optional[bool]

_OPS = {
    "=": lambda a, b: a == b,
    "!=": lambda a, b: a != b,
    "<": lambda a, b: a < b,
    "<=": lambda a, b: a <= b,
    ">": lambda a, b: a > b,
    ">=": lambda a, b: a >= b,
}


def compare(left: object, op: str, right: object) -> K:
    """``Compare``: UNKNOWN when an operand is NULL (§4.1)."""
    if left is None or right is None:
        return None
    return bool(_OPS[op](left, right))


def not_(p: K) -> K:
    return None if p is None else not p


def and_(ps: Iterable[K]) -> K:
    vals = list(ps)
    if any(v is False for v in vals):
        return False
    if any(v is None for v in vals):
        return None
    return True


def or_(ps: Iterable[K]) -> K:
    vals = list(ps)
    if any(v is True for v in vals):
        return True
    if any(v is None for v in vals):
        return None
    return False


def is_true(p: K) -> bool:
    """``(p) IS TRUE``; ``NotTrue(p)`` of §6.1 is its negation."""
    return p is True

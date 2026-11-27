"""Repeat — the recursion construct of BoundPath (DGM §3.2).

``Repeat(path, min, max)`` matches *path* traversed k times in sequence for
some k with ``min ≤ k ≤ max``.  Well-formedness R2–R4 is checked here, at
construction.  R1 (the body ends at the node type it starts from) needs the
schema graph, which a hop does not carry; the planner checks it at the
relation level where it relies on it (:mod:`sqldim.core.query.dgm.planner`).
"""

from __future__ import annotations

import enum

from sqldim.exceptions import SemanticError

__all__ = ["RepeatMode", "UNBOUNDED", "Repeat"]


class RepeatMode(enum.Enum):
    """What an instance may revisit within the repeated segment (§3.2)."""

    SIMPLE = "SIMPLE"  # no node instance and no edge instance repeated
    TRAIL = "TRAIL"  # node instances may repeat, edge instances may not
    WALK = "WALK"  # both may repeat; requires a finite max (R4)


class _Unbounded:
    """The ``UNBOUNDED`` upper bound of a Repeat (admitted only as max)."""

    _instance: "_Unbounded | None" = None

    def __new__(cls) -> "_Unbounded":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "UNBOUNDED"


UNBOUNDED = _Unbounded()


def _is_count(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_hop(item: object) -> bool:
    from sqldim.core.query.dgm.preds._core import _HopBase

    return isinstance(item, _HopBase)


def _body_items(path: object) -> list:
    from sqldim.core.query.dgm.preds._core import _flatten_path

    return _flatten_path(path)


def _check_bounds(lo: object, hi: object) -> None:
    if not _is_count(lo) or lo < 0:  # type: ignore[operator]
        raise SemanticError(f"Repeat: min must be a natural number, got {lo!r}")
    if hi is UNBOUNDED:
        return
    if not _is_count(hi) or hi < 1 or hi < lo:  # type: ignore[operator]
        raise SemanticError(
            "Repeat: max must be UNBOUNDED or a natural number with max ≥ 1 "
            f"and max ≥ min (R2), got min={lo!r}, max={hi!r}"
        )


def _check_body(path: object) -> None:
    items = _body_items(path)
    if any(isinstance(h, Repeat) for h in items):
        raise SemanticError("Repeat: the body contains no Repeat (R3)")
    if not items or not all(_is_hop(h) for h in items):
        raise SemanticError("Repeat: the body must be a hop or a Compose of hops")


class Repeat:
    """``Repeat(path, min, max, mode=SIMPLE, depth_alias=None)`` (§3.2).

    Parameters
    ----------
    path:
        The body: a hop or a ``Compose`` of hops.  It contains no Repeat (R3).
    min, max:
        Iteration bounds, ``0 ≤ min ≤ max`` and ``max ≥ 1``; ``max`` may be
        :data:`UNBOUNDED` (R2).
    mode:
        :class:`RepeatMode`; ``WALK`` requires a finite ``max`` (R4).
    depth_alias:
        Optional alias bound to the iteration count k of the instance.
    """

    def __init__(
        self,
        path: object,
        min: int,  # noqa: A002 — the spec's parameter name
        max: "int | _Unbounded",  # noqa: A002
        *,
        mode: RepeatMode = RepeatMode.SIMPLE,
        depth_alias: str | None = None,
    ) -> None:
        _check_bounds(min, max)
        if not isinstance(mode, RepeatMode):
            raise SemanticError(f"Repeat: mode must be a RepeatMode, got {mode!r}")
        if mode is RepeatMode.WALK and max is UNBOUNDED:
            raise SemanticError("Repeat: mode=WALK requires a finite max (R4)")
        _check_body(path)
        if depth_alias is not None and not isinstance(depth_alias, str):
            raise SemanticError("Repeat: depth_alias must be a string")
        self.path = path
        self.min = min
        self.max = max
        self.mode = mode
        self.depth_alias = depth_alias

    @property
    def bounded(self) -> bool:
        """True when ``max`` is finite."""
        return self.max is not UNBOUNDED

    @property
    def from_alias(self) -> str:
        """The alias the first iteration starts from (first body hop)."""
        return _body_items(self.path)[0].from_alias

    @property
    def to_alias(self) -> str:
        """The body's end alias: outside the path, the node the instance ends at."""
        return _body_items(self.path)[-1].to_alias

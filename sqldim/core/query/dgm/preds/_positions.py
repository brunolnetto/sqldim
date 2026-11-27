"""Temporal modes of a PathPred over one evaluation position (§4.1, ADR 0001).

An evaluation position of a path instance is a node whose alias
``sub_filter`` reads (ADR 0001 *Definitions*).  A pattern without a Repeat,
whose ``sub_filter`` (and UNTIL target) read exactly one alias of the path,
has one position per instance, so each maximal trace has one position and
the CTL forms reduce to quantification over instances (strategy ALL):

=============================  =============================================
EF φ, EG φ                     some instance has φ TRUE at its position
EX φ (one-hop pattern)         some successor has φ TRUE
E(φ U ψ)                       some instance has ψ TRUE at its position
                               (strong UNTIL: ψ at position 0 needs no φ)
AF φ, AG φ, AX φ, A(φ U ψ)     FORALL of the same sub_filter: no position
                               where it is not TRUE, and at least one
                               instance (REQUIRE_WITNESS, ADR 0001)
=============================  =============================================

:func:`without_mode` rewrites such a PathPred into the PathPred without a
mode that has the same truth value at every start node.  ``to_sql()`` and the
plan IR both render that one.  Everything else raises
``UnsupportedConstruct`` with its reason:

* a Repeat in the pattern: a trace has several positions, and SIMPLE or
  TRAIL repetition excludes revisits by node identity, which a hop does not
  declare;
* a ``sub_filter`` that reads no alias of the path, or several, or an UNTIL
  target that reads another: the positions are not one node per instance;
* NEXT over more than one hop: ``Succ(s)`` is the first hop's targets, which
  ``sub_filter`` would then have to read alone;
* the past-time modes (ONCE, PREVIOUSLY, SINCE): they traverse ``G^T``
  backwards along a time-ordered path, which no template emits;
* a strategy other than ALL (checked by the caller): the order ≺ of §3.3
  breaks ties by node keys, which a hop does not declare.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqldim.core.query.dgm.preds._core import PathPred

__all__ = ["without_mode"]


def _unsupported(msg: str) -> Exception:
    from sqldim.core.query.dgm.planner._ir import UnsupportedConstruct

    return UnsupportedConstruct(msg)


def _read(pred: Any, aliases: list[str]) -> set[str]:
    text = re.sub(r"'(?:[^']|'')*'", "''", pred.to_sql())
    return {a for a in aliases if re.search(rf"(?<![\w.]){re.escape(a)}\.\w", text)}


def _hops(pp: "PathPred") -> list:
    from sqldim.core.query.dgm.preds._core import _flatten_path, _HopBase

    hops = _flatten_path(pp.path)
    if any(not isinstance(h, _HopBase) for h in hops):
        raise _unsupported(
            "a temporal mode over a Repeat needs several evaluation positions per "
            "trace and, under SIMPLE or TRAIL, node identities a hop does not "
            "declare (§4.1, ADR 0001)"
        )
    return hops


def _one_position(hops: list, sub: Any, target: Any = None) -> None:
    """``sub`` reads exactly one path alias, and ``target`` (an UNTIL ψ) no
    other: one evaluation position per instance."""
    aliases = [h.to_alias for h in hops]
    read = _read(sub, aliases)
    if len(read) != 1:
        raise _unsupported(
            f"a temporal mode whose sub_filter reads {sorted(read) or 'no'} path "
            "aliases has no single evaluation position per instance; only one is "
            "rendered (§4.1, ADR 0001)"
        )
    if target is not None and not _read(target, aliases) <= read:
        raise _unsupported(
            "the UNTIL target reads a path alias the sub_filter does not: its "
            "positions are not the sub_filter's (§4.1)"
        )


def without_mode(pp: "PathPred") -> "PathPred":
    """The mode-free PathPred with the truth value of *pp* (see module doc)."""
    from sqldim.core.query.dgm.preds._core import PathPred, _is_past_mode
    from sqldim.core.query.dgm.temporal import EVENTUALLY, GLOBALLY, NEXT, UntilMode

    mode = pp.temporal_mode
    if mode is None:
        return pp
    if _is_past_mode(mode):
        raise _unsupported(
            "past-time modes (ONCE, PREVIOUSLY, SINCE) traverse G^T backwards "
            "along a time-ordered path; no SQL template emits them (§4.1)"
        )
    hops = _hops(pp)
    sub: Any = pp.sub_filter
    if isinstance(mode, UntilMode):
        _one_position(hops, sub, mode.pred)
        sub = mode.pred  # strong UNTIL on a one-position trace: ψ at position 0
    elif mode is NEXT:
        if len(hops) != 1:
            raise _unsupported("NEXT is rendered over a one-hop pattern only (§4.1)")
        _one_position(hops, sub)
    elif mode in (EVENTUALLY, GLOBALLY):
        _one_position(hops, sub)
    else:
        raise _unsupported(f"temporal mode {mode!r} has no SQL template")
    return PathPred(
        pp.anchor,
        pp.path,
        sub,
        quantifier=pp.quantifier,
        strategy=pp.strategy,
        promote=pp.promote,
    )

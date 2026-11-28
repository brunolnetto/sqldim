"""Temporal modes of a PathPred and their evaluation positions (§4.1, ADR 0001).

An evaluation position of a path instance is a node whose alias
``sub_filter`` reads (ADR 0001 *Definitions*).  A pattern without a Repeat,
whose ``sub_filter`` (and UNTIL target) read exactly one alias of the path,
has one position per instance, so each maximal trace has one position and
the CTL forms reduce to quantification over instances (strategy ALL):

=============================  =============================================
EF φ, EG φ                     some instance has φ TRUE at its position
E(φ U ψ)                       some instance has ψ TRUE at its position
                               (strong UNTIL: ψ at position 0 needs no φ)
AF φ, AG φ, A(φ U ψ)           FORALL of the same sub_filter: no position
                               where it is not TRUE, and at least one
                               instance (REQUIRE_WITNESS, ADR 0001)
=============================  =============================================

NEXT reads φ at ``Succ(s)``, the nodes one hop of the path away from the
start: the targets of the first hop, or of the first body hop of a leading
Repeat.  EX φ and AX φ are therefore the PathPred over that one hop,
whatever follows it, when φ reads no path alias other than that hop's.

:func:`without_mode` rewrites such a PathPred into the PathPred without a
mode that has the same truth value at every start node.  ``to_sql()`` and the
plan IR both render that one.  :func:`trace_mode` names the mode of a
PathPred over one Repeat, whose traces have one position per iteration
(§3.2); only the plan IR evaluates it, with node identities from a declared
model.  Everything else raises ``UnsupportedConstruct`` with its reason:

* a ``sub_filter`` that reads no alias of a Repeat-free path, or several, or
  an UNTIL target that reads another: ADR 0001 defines a position as a node
  at which every alias the sub_filter reads is bound, and leaves to #31/#35
  whether the nodes after the last of them are positions too;
* a mode over a Repeat in any other pattern than one Repeat over one hop;
* NEXT whose sub_filter reads a path alias beyond the first hop, or after a
  leading Repeat with ``min = 0`` followed by more hops (one hop of the path
  is then its first iteration or the hop after it);
* the past-time modes (ONCE, PREVIOUSLY, SINCE): §4.1 and ADR 0001 state
  truth conditions over ``T(s)`` and ``Succ(s)`` for the future forms only;
  no trace domain on ``G^T`` is defined for the past ones;
* a strategy other than ALL with a temporal mode (checked by the caller).
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqldim.core.query.dgm.preds._core import PathPred

__all__ = ["trace_mode", "without_mode"]


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
            "a temporal mode over a Repeat has one evaluation position per "
            "iteration (§3.2); it is evaluated by the plan IR with node "
            "identities from a declared model (DGMPlanner(model=…)), and has "
            "no to_sql() rendering"
        )
    return hops


def _first_hop(pp: "PathPred") -> Any:
    """The hop whose targets are ``Succ(s)``."""
    from sqldim.core.query.dgm.preds._core import _flatten_path
    from sqldim.core.query.dgm.preds._repeat import Repeat

    items = _flatten_path(pp.path)
    first = items[0]
    if not isinstance(first, Repeat):
        return first
    if first.min == 0 and len(items) > 1:
        raise _unsupported(
            "NEXT after a Repeat with min = 0 followed by more hops: one hop "
            "of the path is its first iteration or the hop after it, so "
            "Succ(s) is not one hop's targets (§4.1)"
        )
    return _flatten_path(first.path)[0]


def _path_aliases(pp: "PathPred") -> list[str]:
    from sqldim.core.query.dgm.preds._core import _path_hops

    return [h.to_alias for h in _path_hops(pp.path)]


def _next_form(pp: "PathPred") -> "PathPred":
    from sqldim.core.query.dgm.preds._core import PathPred

    first = _first_hop(pp)
    read = _read(pp.sub_filter, _path_aliases(pp))
    if not read <= {first.to_alias}:
        raise _unsupported(
            f"NEXT evaluates sub_filter at Succ(s), the targets of the first "
            f"hop ({first.to_alias!r}); a sub_filter reading {sorted(read)} is "
            "not evaluated there (§4.1)"
        )
    return PathPred(
        pp.anchor,
        first,
        pp.sub_filter,
        quantifier=pp.quantifier,
        strategy=pp.strategy,
        promote=pp.promote,
    )


def trace_mode(pp: "PathPred") -> str | None:
    """The mode of *pp* when its traces are those of one Repeat (§3.2).

    ``EVENTUALLY``, ``GLOBALLY`` or ``UNTIL`` for a pattern that is one
    Repeat over one hop; ``None`` when *pp* has no mode, a past-time mode,
    NEXT, or no Repeat (:func:`without_mode` handles those).
    """
    from sqldim.core.query.dgm.preds._core import _flatten_path, _is_past_mode
    from sqldim.core.query.dgm.preds._repeat import Repeat
    from sqldim.core.query.dgm.temporal import EVENTUALLY, GLOBALLY, NEXT, UntilMode

    mode = pp.temporal_mode
    if mode is None or mode is NEXT or _is_past_mode(mode):
        return None
    items = _flatten_path(pp.path)
    if not any(isinstance(h, Repeat) for h in items):
        return None
    if len(items) != 1 or len(_flatten_path(items[0].path)) != 1:
        raise _unsupported(
            "a temporal mode over a Repeat is evaluated for a pattern that is "
            "one Repeat over a one-hop body, whose positions are the iterations' "
            "end nodes (§3.2); other patterns mix positions of hops and "
            "iterations, which this IR version does not emit"
        )
    if isinstance(mode, UntilMode):
        return "UNTIL"
    if mode in (EVENTUALLY, GLOBALLY):
        return mode.__name__  # type: ignore[attr-defined]
    raise _unsupported(f"temporal mode {mode!r} has no SQL template")


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
            "past-time modes (ONCE, PREVIOUSLY, SINCE) have no truth condition: "
            "§4.1 and ADR 0001 define T(s) and Succ(s) for the future forms "
            "only, and no trace domain on G^T, so no SQL is emitted for them"
        )
    if mode is NEXT:
        return _next_form(pp)
    hops = _hops(pp)
    sub: Any = pp.sub_filter
    if isinstance(mode, UntilMode):
        _one_position(hops, sub, mode.pred)
        sub = mode.pred  # strong UNTIL on a one-position trace: ψ at position 0
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

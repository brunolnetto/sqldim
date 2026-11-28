"""Temporal modes of a PathPred over the plan IR (§4.1, ADR 0001, #183).

A PathPred with a temporal mode is lowered with its hops and ``sub_filter``
(``planner/_ir_build.py``); :func:`lower_mode` then states the mode over
the evaluation positions of the lowered path:

* **past modes** (ONCE, PREVIOUSLY, SINCE) are EVENTUALLY, NEXT and UNTIL on
  ``G^T`` (§2.2), built only over time-ordered edge types (§2.3).
  :func:`past_hops` checks every edge type of the path against the
  ``BridgeSemantics`` of the declared model and reverses each hop: the hop's
  ``on`` with its two aliases exchanged reads the edge relation by the key of
  its target (§2.6), so the node it reaches is the source of an edge into the
  node before it;
* **NEXT** reads φ at ``Succ(s)``, the targets of the first hop (of the
  first body hop of a leading Repeat).  Under strategy ALL it is the
  PathPred over that hop; under another strategy the strategy selects
  instances of the whole path first and φ is read at the first hop of each;
* **EVENTUALLY, GLOBALLY, UNTIL on a path without a Repeat**: every node
  after the anchor is a position, and ``sub_filter`` is read at each one
  with every path alias it reads denoting that node.  The trace of an
  instance is therefore one condition over the instance — ``φ₁ ∨ … ∨ φₙ``,
  ``φ₁ ∧ … ∧ φₙ`` or ``∨ⱼ (ψⱼ ∧ φ₁ ∧ … ∧ φⱼ₋₁)`` — and the PathPred is the
  mode-free PathPred over that condition: FORALL negates it with NotTrue,
  and a strategy selects the instances it is read on (§3.3);
* **EVENTUALLY, GLOBALLY, UNTIL over one Repeat** keep their mode: the
  emitter evaluates the maximal traces of the recursion
  (``planner/_ir_emit.py``), after the strategy's selection.

Everything else raises ``UnsupportedConstruct`` with its reason.
``DGMQuery.to_sql()`` renders a temporal mode through the same lowering
(:func:`render_sql`), without a model, so it refuses what needs one.
"""

from __future__ import annotations

import re
from dataclasses import replace
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqldim.core.query.dgm.planner._ir import PathExists, PathHop, Pred
    from sqldim.core.query.dgm.preds._core import PathPred

__all__ = ["MIRROR", "PAST", "lower_mode", "mode_name", "past_hops", "render_sql"]

#: The future mode each past mode mirrors on ``G^T`` (§4.1).
MIRROR = {"ONCE": "EVENTUALLY", "PREVIOUSLY": "NEXT", "SINCE": "UNTIL"}
PAST = frozenset(MIRROR)


def _unsupported(msg: str) -> Exception:
    from sqldim.core.query.dgm.planner._ir import UnsupportedConstruct

    return UnsupportedConstruct(msg)


def mode_name(mode: object) -> str | None:
    """The name of a TemporalMode: ``EVENTUALLY`` … ``SINCE``; ``None``."""
    from sqldim.core.query.dgm.temporal import (
        EVENTUALLY,
        GLOBALLY,
        NEXT,
        ONCE,
        PREVIOUSLY,
        SinceMode,
        UntilMode,
    )

    if mode is None:
        return None
    if isinstance(mode, UntilMode):
        return "UNTIL"
    if isinstance(mode, SinceMode):
        return "SINCE"
    names = {EVENTUALLY: "EVENTUALLY", GLOBALLY: "GLOBALLY", NEXT: "NEXT",
             ONCE: "ONCE", PREVIOUSLY: "PREVIOUSLY"}  # fmt: skip
    if mode in names:
        return names[mode]  # type: ignore[index]
    raise _unsupported(f"temporal mode {mode!r} is not one of §4.1")


# ---------------------------------------------------------------------------
# Past modes: G^T over time-ordered edge types
# ---------------------------------------------------------------------------


def _check_time_ordered(pp: "PathPred", ids: Any) -> None:
    """Every edge type of the path is time-ordered (§2.3), or raise."""
    from sqldim.core.query.dgm.planner._ir import NotTimeOrdered
    from sqldim.core.query.dgm.preds._core import _path_hops

    if ids is None:
        raise _unsupported(
            "a past-time mode is evaluated on G^T, built only over time-ordered "
            "edge types (§2.3, §4.1); whether a bridge is time-ordered is its "
            "BridgeSemantics, which only a declared model states: plan the "
            "query with DGMPlanner(model=…)"
        )
    for h in _path_hops(pp.path):
        ordered = ids.time_ordered(getattr(h, "kind", "hop"), h.label)
        if ordered is None:
            raise _unsupported(
                f"the declared model does not state whether {h.label!r} is "
                "time-ordered: it declares no BridgeSemantics (sigma_kinds) or "
                "no bridge edge type with that label (§2.3, §2.6)"
            )
        if not ordered:
            raise NotTimeOrdered(
                f"{h.label!r} is not a time-ordered edge type (BridgeSemantics "
                "CAUSAL, TEMPORAL or SUPERSESSION, §2.3): ONCE, PREVIOUSLY and "
                "SINCE are evaluated on G^T over time-ordered edge types only "
                "(§4.1)"
            )


def _reverse(h: "PathHop", source: str, source_relation: str | None) -> "PathHop":
    """*h* traversed against its direction (§2.2 G^T)."""
    from sqldim.core.query.dgm.planner._ir import rename_aliases

    if source_relation != h.scan.relation:
        raise _unsupported(
            f"hop {h.label!r} traversed in G^T reaches a node of its source "
            f"type, read from {source_relation!r}, but the hop scans "
            f"{h.scan.relation!r}; only a hop whose source and target are read "
            "from the same relation is reversed"
        )
    swap = {source: h.scan.alias, h.scan.alias: source}
    return replace(h, on=rename_aliases(h.on, swap))


def past_hops(pp: "PathPred", hops: tuple, ids: Any, relations: dict) -> tuple:
    """The lowered *hops* of *pp* on ``G^T``, after the time-ordered check.

    *relations* maps the aliases in scope to the relations they read.
    """
    from sqldim.core.query.dgm.planner._ir import PathRepeat
    from sqldim.core.query.dgm.preds._core import _flatten_path

    _check_time_ordered(pp, ids)
    known = dict(relations)
    out: list = []
    for item, src in zip(hops, _flatten_path(pp.path)):
        if isinstance(item, PathRepeat):
            if len(item.body) != 1:
                raise _unsupported("only a one-hop Repeat body is reversed on G^T")
            body = _reverse(item.body[0], item.start, item.start_relation)
            out.append(replace(item, body=(body,)))
        else:
            out.append(_reverse(item, src.from_alias, known.get(src.from_alias)))
        last = out[-1].body[-1] if isinstance(out[-1], PathRepeat) else out[-1]
        known[last.scan.alias] = last.scan.relation
    return tuple(out)


# ---------------------------------------------------------------------------
# Reading sub_filter at a position
# ---------------------------------------------------------------------------


def _text(p: "Pred | None") -> str:
    from sqldim.core.query.dgm.planner._ir_emit import emit_pred

    if p is None:
        return ""
    return re.sub(r"'(?:[^']|'')*'", "''", emit_pred(p))


def _reads(p: "Pred | None", aliases: list[str]) -> set[str]:
    text = _text(p)
    return {a for a in aliases if re.search(rf"(?<![\w.]){re.escape(a)}\.\w", text)}


def _rename(p: "Pred", mapping: dict[str, str]) -> "Pred":
    """*p* with the aliases of *mapping* requalified (a position's node)."""
    from sqldim.core.query.dgm.planner._ir import (
        Compare,
        Conj,
        Disj,
        Neg,
        RawCondition,
        rename_aliases,
    )

    if all(k == v for k, v in mapping.items()):
        return p
    if isinstance(p, Compare):
        return replace(p, lhs=rename_aliases(p.lhs, mapping))
    if isinstance(p, RawCondition):
        return RawCondition(rename_aliases(p.fragment, mapping))
    if isinstance(p, (Conj, Disj)):
        return type(p)(tuple(_rename(s, mapping) for s in p.preds))
    if isinstance(p, Neg):
        return Neg(_rename(p.pred, mapping))
    raise _unsupported(
        "a PathPred inside the sub_filter of a temporal mode is not read at "
        "the other positions of the path (§4.1)"
    )


def _one(cls: type, preds: list) -> "Pred":
    return preds[0] if len(preds) == 1 else cls(tuple(preds))


def _trace_condition(p: "PathExists", mode: str, until: "Pred | None") -> "Pred":
    """The per-instance condition of a mode on a path without a Repeat."""
    from sqldim.core.query.dgm.planner._ir import Conj, Disj

    aliases = [h.scan.alias for h in p.hops]  # type: ignore[union-attr]
    read = _reads(p.sub_filter, aliases) | _reads(until, aliases)
    if read and len({h.scan.relation for h in p.hops}) > 1:  # type: ignore[union-attr]
        raise _unsupported(
            "every node after the anchor is an evaluation position (§4.1), and "
            f"sub_filter, which reads {sorted(read)}, is read at each one; the "
            "hops of this path scan different relations, so a property of one "
            "position need not exist at another"
        )

    def at(q: "Pred", alias: str) -> "Pred":
        return _rename(q, {a: alias for a in read})

    phis = [at(p.sub_filter, a) for a in aliases]
    if mode == "EVENTUALLY":
        return _one(Disj, phis)
    if mode == "GLOBALLY":
        return _one(Conj, phis)
    assert until is not None
    # strong UNTIL: ψ at some position j, φ at every position before j
    terms = [_one(Conj, [at(until, a)] + phis[:j]) for j, a in enumerate(aliases)]
    return _one(Disj, terms)


def _next(p: "PathExists") -> "PathExists":
    from sqldim.core.query.dgm.planner._ir import PathRepeat

    first = p.hops[0]
    if isinstance(first, PathRepeat):
        if first.min == 0 and len(p.hops) > 1:
            raise _unsupported(
                "NEXT after a Repeat with min = 0 followed by more hops: one hop "
                "of the path is its first iteration or the hop after it, so "
                "Succ(s) is not one hop's targets (§4.1)"
            )
        if p.strategy != "ALL":
            raise _unsupported(
                f"NEXT with strategy {p.strategy} over a Repeat: the strategy "
                "selects instances of the Repeat (§3.3, §4.1), whose first hops "
                "this IR version does not read"
            )
        hop = first.body[0]
    else:
        hop = first
    aliases = [h.scan.alias for h in _flat(p)]
    read = _reads(p.sub_filter, aliases)
    if not read <= {hop.scan.alias}:
        raise _unsupported(
            f"NEXT evaluates sub_filter at Succ(s), the targets of the first "
            f"hop ({hop.scan.alias!r}); a sub_filter reading {sorted(read)} is "
            "not evaluated there (§4.1)"
        )
    if p.strategy == "ALL":
        return replace(p, hops=(hop,))
    return p  # φ at the first hop of each selected instance


def _flat(p: "PathExists") -> list:
    from sqldim.core.query.dgm.planner._ir import PathRepeat

    out: list = []
    for h in p.hops:
        out += list(h.body) if isinstance(h, PathRepeat) else [h]
    return out


def lower_mode(p: "PathExists", mode: str | None, until: "Pred | None") -> "PathExists":
    """*p* (lowered without a mode) with the future *mode* stated over it."""
    from sqldim.core.query.dgm.planner._ir import PathRepeat

    if mode is None:
        return p
    if mode == "NEXT":
        return _next(p)
    if mode not in ("EVENTUALLY", "GLOBALLY", "UNTIL"):
        raise _unsupported(f"temporal mode {mode} has no SQL form")
    if not any(isinstance(h, PathRepeat) for h in p.hops):
        return replace(p, sub_filter=_trace_condition(p, mode, until))
    if len(p.hops) != 1 or len(p.hops[0].body) != 1:  # type: ignore[union-attr]
        raise _unsupported(
            "a temporal mode over a Repeat is evaluated for a pattern that is "
            "one Repeat over a one-hop body, whose positions are the iterations' "
            "end nodes (§3.2); other patterns mix positions of hops and "
            "iterations, which this IR version does not emit"
        )
    return replace(p, mode=mode, until=until)


# ---------------------------------------------------------------------------
# DGMQuery.to_sql(): the same lowering, without a model
# ---------------------------------------------------------------------------


def _outer_scope(pp: "PathPred") -> frozenset[str]:
    """The aliases outside the path that *pp*'s fragments qualify names with."""
    from sqldim.core.query.dgm.preds._core import _path_hops

    hops = _path_hops(pp.path)
    texts = [pp.sub_filter.to_sql()] + [str(h.on or "") for h in hops]
    psi = getattr(pp.temporal_mode, "pred", None)
    if psi is not None:
        texts.append(psi.to_sql())
    heads: set[str] = {pp.anchor}
    for t in texts:
        t = re.sub(r"'(?:[^']|'')*'", "''", t)
        heads |= set(re.findall(r"(?<![\w.])([A-Za-z_]\w*)\.\w", t))
    return frozenset(heads - {h.to_alias for h in hops})


def render_sql(pp: "PathPred") -> str:
    """``to_sql()`` of a PathPred with a temporal mode: the plan IR's SQL."""
    from sqldim.core.query.dgm.planner._ir import UnsupportedConstruct
    from sqldim.core.query.dgm.planner._ir_build import lower_pred
    from sqldim.core.query.dgm.planner._ir_emit import emit_pred

    try:
        return emit_pred(lower_pred(pp, _outer_scope(pp)))
    except UnsupportedConstruct as exc:
        raise UnsupportedConstruct(
            "DGMQuery.to_sql() renders a temporal mode through the plan IR "
            f"without a model, which refuses it: {exc}"
        ) from exc

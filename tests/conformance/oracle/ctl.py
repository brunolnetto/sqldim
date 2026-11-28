"""CTL over maximal finite traces, the empty-domain policy and monitors.

Definitions: ADR 0001 (*Definitions*, *Truth conditions*, *Past-time modes*,
*Temporal mode with a strategy*, *Empty-domain policy*) and §4.1 (*Path
semantics*, *Temporal mode with a strategy*, *Path-order modes and temporal
modes*, *FORALL elimination*, *Recorded traces and monitoring verdicts*).

The three decisions of #183 are read from those definitions:

* on a pattern without a Repeat, every node after the start is an
  evaluation position of EVENTUALLY, GLOBALLY and UNTIL (the ``read`` flag
  of a hop is ignored there);
* ONCE, PREVIOUSLY and SINCE are EVENTUALLY, NEXT and UNTIL evaluated on the
  transposed graph ``G^T``; a case declares with ``time_ordered`` that the
  edge types of its path are time-ordered (§2.3);
* a strategy other than ALL selects the instances first; ``T(s)`` is formed
  from the selected instances, and ``Succ(s)`` is the set of nodes they reach
  by their first hop.

A predicate is data: ``("prop", key, op, value)``, ``("true",)``,
``("not", p)``, ``("and", p, q, ...)``, ``("or", p, q, ...)``.  It is read at
one node with the NULL rules of §4.1.  A path form holds at a position only
where ``sub_filter`` is TRUE: under EXISTS an UNKNOWN position is no witness,
and under FORALL it counts against the universal condition, because FORALL
elimination negates with ``NotTrue`` (§4.1, §6.1).

Not modelled: infinite traces.  Every pattern the cases use has finite
instances (SIMPLE/TRAIL repetition or WALK with a finite max), so the lasso
reading of ADR 0001 is never needed; a case that would need it is not written.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from typing import Any

from tests.conformance.oracle import REJECTED, Undefined
from tests.conformance.oracle.kleene import K, and_, compare, is_true, not_, or_
from tests.conformance.oracle.paths import instances, select

REQUIRE_WITNESS = "REQUIRE_WITNESS"
VACUOUS = "VACUOUS"
PAST_MODES = ("SINCE", "ONCE", "PREVIOUSLY")


def pred(p: Sequence, props: Mapping[str, Any], outer: Mapping | None = None) -> K:
    """Evaluate predicate data *p* on one property bag (Kleene).

    ``("outer", key, op, outer_key)`` compares a property of the path node
    with one of the start node — a Ref resolved in C ∪ L (§3.5).
    """
    tag = p[0]
    if tag == "true":
        return True
    if tag == "prop":
        _, key, op, value = p
        return compare(props.get(key), op, value)
    if tag == "outer":
        _, key, op, okey = p
        return compare(props.get(key), op, (outer or {}).get(okey))
    if tag == "not":
        return not_(pred(p[1], props, outer))
    if tag == "and":
        return and_(pred(q, props, outer) for q in p[1:])
    if tag == "or":
        return or_(pred(q, props, outer) for q in p[1:])
    raise Undefined(f"unknown predicate form {tag!r}")


def _has_repeat(pattern: Sequence) -> bool:
    return any(elem[0] == "repeat" for elem in pattern)


def traces(
    graph: Mapping[str, Any],
    s: str,
    pattern: Sequence,
    insts: Sequence | None = None,
) -> set[tuple[str, ...]]:
    """``T(s)``: maximal traces — not a proper prefix of another trace from s.

    *insts* are the instances the traces are formed from: every instance
    (strategy ALL) when omitted, else those a strategy selected.  On a
    pattern without a Repeat every node after *s* is a position (#183).
    """
    every_node = not _has_repeat(pattern)
    all_traces = set()
    if insts is None:
        insts = instances(graph, s, pattern)
    for nodes, _edges, positions in insts:
        trace = tuple(nodes[1:]) if every_node else tuple(nodes[i] for i in positions)
        if trace:
            all_traces.add(trace)
    return {
        t
        for t in all_traces
        if not any(len(u) > len(t) and u[: len(t)] == t for u in all_traces)
    }


def succ(graph: Mapping[str, Any], s: str, pattern: Sequence) -> set[str]:
    """``Succ(s)``: nodes reached from *s* by exactly one hop of the path."""
    elem = pattern[0]
    label, direction = elem[1], elem[2]
    out = set()
    for e in graph["edges"]:
        src, dst, lab = e[0], e[1], e[2]
        if lab != label:
            continue
        if direction == "fwd" and src == s:
            out.add(dst)
        elif direction == "rev" and dst == s:
            out.add(src)
    return out


def transpose(graph: Mapping[str, Any]) -> dict[str, Any]:
    """``G^T`` (§2.2): every edge instance ``(a, b)`` read as ``(b, a)``."""
    edges = [(e[1], e[0], *e[2:]) for e in graph["edges"]]
    return {"nodes": graph["nodes"], "edges": edges}


def first_hop_nodes(insts: Sequence) -> set[str]:
    """The nodes the instances reach by their first hop (§4.1, strategy)."""
    return {nodes[1] for nodes, _e, _p in insts if len(nodes) > 1}


def construction(case_input: Mapping[str, Any]) -> str | None:
    """Rejections at construction (§4.1, §3.5); ``None`` if well-formed."""
    quant, mode = case_input["quantifier"], case_input.get("mode")
    mode_name = mode[0] if isinstance(mode, (tuple, list)) else mode
    if quant == "FORALL" and mode_name in ("ONCE", "SINCE"):
        return REJECTED  # §4.1: their duals need a past-time GLOBALLY
    if mode_name in PAST_MODES and not case_input.get("time_ordered", False):
        return REJECTED  # §4.1: past-time modes need a time-ordered path
    if case_input.get("promote") and case_input.get("strategy", ("ALL",))[0] in (
        "ALL",
        "K_SHORTEST",
    ):
        return REJECTED  # §3.5
    aliases = case_input.get("aliases")
    if aliases and set(aliases["outer"]) & set(aliases["path"]):
        return REJECTED  # §3.2: C ∩ L ≠ ∅ rejected at construction
    return None


def _holder(graph: Mapping[str, Any], s: str):
    """``ok(n, p)``: *p* is TRUE at node *n*, with outer Refs read at *s*."""
    nodes = graph["nodes"]
    return lambda n, p: is_true(pred(p, nodes[n], nodes[s]))


def _until(ok, trace, phi, psi) -> bool:
    for j, n in enumerate(trace):
        if ok(n, psi):
            return all(ok(m, phi) for m in trace[:j])
    return False  # strong UNTIL: psi must occur before the trace ends


def _no_mode(graph, s, case_input) -> bool:
    """FORALL/EXISTS without a temporal mode: quantification over instances."""
    ok = _holder(graph, s)
    insts = select(graph, instances(graph, s, case_input["pattern"]),
                   case_input.get("strategy", ("ALL",)))  # fmt: skip
    values = []
    for nodes, _e, positions in insts:
        if len(positions) != 1:
            raise Undefined("no-mode sub_filter over several positions is not modelled")
        values.append(ok(nodes[positions[0]], case_input["phi"]))
    if case_input["quantifier"] == "EXISTS":
        return any(values)
    if not values:
        return case_input.get("on_empty", REQUIRE_WITNESS) == VACUOUS
    return all(values)


def _per_trace(ok, trace, mode, phi) -> bool:
    if mode == "EVENTUALLY":
        return any(ok(n, phi) for n in trace)
    if mode == "GLOBALLY":
        return all(ok(n, phi) for n in trace)
    if isinstance(mode, (tuple, list)) and mode[0] == "UNTIL":
        return _until(ok, trace, phi, mode[1])
    raise Undefined(f"mode {mode!r} is not a future trace mode")


def evaluate(graph: Mapping[str, Any], s: str, case_input: Mapping[str, Any]) -> Any:
    """Truth of a PathPred at start node *s*, or :data:`REJECTED`.

    ``negate`` wraps the PathPred in a tuple-level NOT (the constructions of
    the FORALL elimination table read ``NOT PathPred(EXISTS, …)``).
    """
    rejected = construction(case_input)
    if rejected:
        return rejected
    value = _evaluate(graph, s, case_input)
    return (not value) if case_input.get("negate") else value


_MIRROR = {"ONCE": "EVENTUALLY", "PREVIOUSLY": "NEXT"}


def _future(graph: Mapping[str, Any], mode: Any) -> tuple[Mapping[str, Any], Any]:
    """A past mode as its future mirror on ``G^T`` (§4.1, ADR 0001)."""
    if isinstance(mode, (tuple, list)) and mode[0] == "SINCE":
        return transpose(graph), ("UNTIL", mode[1])
    if mode in _MIRROR:
        return transpose(graph), _MIRROR[mode]
    return graph, mode


def _evaluate(graph: Mapping[str, Any], s: str, case_input: Mapping[str, Any]) -> bool:
    mode = case_input.get("mode")
    if mode is None:
        return _no_mode(graph, s, case_input)
    ok = _holder(graph, s)  # node properties are the same in G^T
    graph, mode = _future(graph, mode)
    phi, quant = case_input["phi"], case_input["quantifier"]
    on_empty = case_input.get("on_empty", REQUIRE_WITNESS)
    pattern, strategy = case_input["pattern"], case_input.get("strategy", ("ALL",))
    selected = None
    if strategy[0] != "ALL":  # the strategy selects first (§3.3, §4.1)
        selected = select(graph, instances(graph, s, pattern), strategy)
    if mode == "NEXT":
        nexts = (
            succ(graph, s, pattern) if selected is None else first_hop_nodes(selected)
        )
        domain: list = sorted(nexts)
        values = [ok(m, phi) for m in domain]
    else:
        domain = sorted(traces(graph, s, pattern, selected))
        values = [_per_trace(ok, t, mode, phi) for t in domain]
    if quant == "EXISTS":
        return any(values)
    if not domain:
        return on_empty == VACUOUS
    return all(values)


_DUAL = {
    "GLOBALLY": "EVENTUALLY",
    "EVENTUALLY": "GLOBALLY",
    "NEXT": "NEXT",
    "PREVIOUSLY": "PREVIOUSLY",  # self-dual (§4.1 table)
}


def forall_elimination(mode: str, on_empty: str = REQUIRE_WITNESS) -> list[tuple]:
    """The construction of the §4.1 FORALL elimination table, as literals.

    Each literal is ``(sign, quantifier, mode, sub_filter)``: ``"-"`` for a
    negated EXISTS atom, ``"+"`` for the non-emptiness guard.
    """
    if mode not in _DUAL:
        raise Undefined(f"elimination of FORALL with {mode} is not tabulated here")
    lits = [("-", "EXISTS", _DUAL[mode], "NotTrue(φ)")]
    if on_empty == REQUIRE_WITNESS:
        # one hop for NEXT and PREVIOUSLY (§4.1 non-emptiness guard)
        guard_mode = mode if mode in ("NEXT", "PREVIOUSLY") else "EVENTUALLY"
        lits.append(("+", "EXISTS", guard_mode, "TRUE"))
    return sorted(lits)


def select_starts(graph: Mapping[str, Any], starts: Sequence[str], case_input) -> Any:
    """The start nodes where the PathPred holds (a Where filter keeps TRUE)."""
    rejected = construction(case_input)
    if rejected:
        return rejected
    return sorted(s for s in starts if evaluate(graph, s, case_input))


# ---------------------------------------------------------------------------
# Monitors (§4.1, *Recorded traces and monitoring verdicts*)
# ---------------------------------------------------------------------------

SATISFIED, VIOLATED, UNDETERMINED = "SATISFIED", "VIOLATED", "UNDETERMINED"


def _finite_truth(prop: Sequence, trace: Sequence[Mapping[str, Any]]) -> bool:
    """Finite-trace truth of a TemporalProperty on one closed trace."""
    kind = prop[0]
    t = [p for p in trace]
    if kind == "LIVENESS":  # AF good
        return any(is_true(pred(prop[1], x)) for x in t)
    if kind == "SAFETY":  # AG ¬bad, with NotTrue: ¬bad must be TRUE everywhere
        return all(is_true(not_(pred(prop[1], x))) for x in t)
    if kind == "RESPONSE":  # AG(trig → AF resp)
        return all(
            not is_true(pred(prop[1], x))
            or any(is_true(pred(prop[2], y)) for y in t[i:])
            for i, x in enumerate(t)
        )
    if kind == "PERSISTENCE":  # AF AG good
        return any(all(is_true(pred(prop[1], y)) for y in t[i:]) for i in range(len(t)))
    raise Undefined(f"no finite-trace reading of {kind} is modelled")


def _open_verdict(prop: Sequence, trace: Sequence[Mapping[str, Any]]) -> str:
    kind = prop[0]
    if kind == "LIVENESS":
        return (
            SATISFIED if any(is_true(pred(prop[1], x)) for x in trace) else UNDETERMINED
        )
    if kind == "SAFETY":
        return (
            VIOLATED if any(is_true(pred(prop[1], x)) for x in trace) else UNDETERMINED
        )
    if kind in ("RESPONSE", "PERSISTENCE", "RECURRENCE"):
        return UNDETERMINED
    raise Undefined(f"unknown TemporalProperty {kind!r}")


def _closed(case_input: Mapping[str, Any], trace) -> bool:
    if case_input.get("log", "OPEN") == "CLOSED":
        return True
    final = case_input.get("final")
    if final is not None and trace and is_true(pred(final, trace[-1])):
        return True
    deadline = case_input.get("deadline")
    if deadline is not None:
        clock, within_days = deadline
        now = datetime.fromisoformat(case_input["now"])
        return now > datetime.fromisoformat(clock) + timedelta(days=within_days)
    return False


def monitor(case_input: Mapping[str, Any]) -> str:
    """Verdict of ``Monitor(property, path, log, final, deadline)``."""
    prop, recorded = case_input["property"], case_input["traces"]
    if not recorded:
        if case_input.get("log", "OPEN") != "CLOSED":
            return UNDETERMINED
        vacuous = case_input.get("on_empty", REQUIRE_WITNESS) == VACUOUS
        return SATISFIED if vacuous else VIOLATED
    verdicts = []
    for trace in recorded:
        if _closed(case_input, trace):
            verdicts.append(SATISFIED if _finite_truth(prop, trace) else VIOLATED)
        else:
            verdicts.append(_open_verdict(prop, trace))
    if VIOLATED in verdicts:
        return VIOLATED
    if all(v == SATISFIED for v in verdicts):
        return SATISFIED
    return UNDETERMINED

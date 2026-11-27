"""Path instances, evaluation positions and strategies (§3.1–§3.3, §3.5).

A graph is a mapping ``{"nodes": {id: props}, "edges": [(src, dst, label[, weight])]}``.
A pattern is a tuple of elements:

* ``("hop", label, direction, read)`` — one hop, ``direction`` ``"fwd"`` or
  ``"rev"``; ``read`` says whether ``sub_filter`` reads the hop's node alias,
  which makes the node it reaches an evaluation position (ADR 0001).
* ``("repeat", label, direction, min, max, mode)`` — ``Repeat`` over a one-hop
  body (§3.2); ``max`` ``None`` is UNBOUNDED.  Each iteration's end node is an
  evaluation position; with ``min = 0`` the start node is the first one.

A path instance is ``(nodes, edges, positions)``: the node ids in path order,
the edge instances ``(label, source, target)`` and the indices of ``nodes``
that are evaluation positions.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from typing import Any

from tests.conformance.oracle import REJECTED, Undefined

Instance = tuple[tuple[str, ...], tuple[tuple[str, str, str], ...], tuple[int, ...]]


def _edges(graph: Mapping[str, Any]) -> list[tuple[str, str, str, float]]:
    out = []
    for e in graph["edges"]:
        src, dst, label = e[0], e[1], e[2]
        out.append((src, dst, label, float(e[3]) if len(e) > 3 else 1.0))
    return out


def _step(graph: Mapping[str, Any], node: str, label: str, direction: str):
    for src, dst, lab, _w in _edges(graph):
        if lab != label:
            continue
        if direction == "fwd" and src == node:
            yield dst, (lab, src, dst)
        elif direction == "rev" and dst == node:
            yield src, (lab, src, dst)


def _admits(mode: str, seg_nodes: list[str], seg_edges: list, nxt: str, edge) -> bool:
    """§3.2 RepeatMode: what an instance may revisit within the repeated segment."""
    if mode == "SIMPLE":
        return nxt not in seg_nodes and edge not in seg_edges
    if mode == "TRAIL":
        return edge not in seg_edges
    if mode == "WALK":
        return True
    raise Undefined(f"unknown RepeatMode {mode!r}")


def _repeat(graph, elem, nodes, edges, positions) -> Iterator[Instance]:
    _, label, direction, lo, hi, mode = elem
    if mode == "WALK" and hi is None:
        raise Undefined("R4: WALK requires a finite max; no instance set exists")
    start = len(nodes) - 1
    pos0 = positions + ((start,) if lo == 0 and start not in positions else ())

    def grow(ns, es, ps, k) -> Iterator[Instance]:
        if k >= lo:
            yield tuple(ns), tuple(es), tuple(ps)
        if hi is not None and k >= hi:
            return
        seg_nodes, seg_edges = ns[start:], es[start:]
        for nxt, edge in _step(graph, ns[-1], label, direction):
            if _admits(mode, seg_nodes, seg_edges, nxt, edge):
                yield from grow(ns + [nxt], es + [edge], ps + [len(ns)], k + 1)

    yield from grow(list(nodes), list(edges), list(pos0), 0)


def instances(
    graph: Mapping[str, Any], start: str, pattern: Sequence
) -> list[Instance]:
    """Every instance of *pattern* from *start* (strategy ALL)."""
    if start not in graph["nodes"]:
        raise Undefined(f"{start!r} is not a node of the graph")

    def rec(i: int, inst: Instance) -> Iterator[Instance]:
        if i == len(pattern):
            yield inst
            return
        nodes, edges, positions = inst
        elem = pattern[i]
        if elem[0] == "hop":
            _, label, direction, read = elem
            for nxt, edge in _step(graph, nodes[-1], label, direction):
                pos = positions + ((len(nodes),) if read else ())
                yield from rec(i + 1, (nodes + (nxt,), edges + (edge,), pos))
        elif elem[0] == "repeat":
            for sub in _repeat(graph, elem, nodes, edges, positions):
                yield from rec(i + 1, sub)
        else:
            raise Undefined(f"unknown pattern element {elem[0]!r}")

    return list(rec(0, ((start,), (), ())))


def _weight(graph: Mapping[str, Any], inst: Instance) -> float:
    """§3.3 weight(π): bridge weights; an unweighted edge counts 1."""
    w = {(lab, s, d): wt for s, d, lab, wt in _edges(graph)}
    return sum(w[e] for e in inst[1])


def _order_key(inst: Instance) -> tuple:
    """§3.3 ≺: (len(π), key(π)) with key the sequence of (label, src, dst)."""
    return (len(inst[1]), inst[1])


def select(graph: Mapping[str, Any], insts: list[Instance], strategy: Sequence) -> list:
    """Apply a Strategy (§3.3) to the instance set of one binding."""
    kind = strategy[0]
    if kind == "ALL":
        return list(insts)
    ordered = sorted(insts, key=_order_key)
    if kind == "SHORTEST":
        return ordered[:1]
    if kind == "K_SHORTEST":
        k = strategy[1]
        if k < 1:
            raise Undefined("K_SHORTEST needs k ≥ 1")
        return ordered[:k]
    if kind == "MIN_WEIGHT":
        return sorted(insts, key=lambda i: (_weight(graph, i),) + _order_key(i))[:1]
    raise Undefined(f"unknown strategy {kind!r}")


def promote_check(strategy: Sequence, promote: bool) -> str | None:
    """§3.5: promote is valid only for SHORTEST and MIN_WEIGHT."""
    if promote and strategy[0] in ("ALL", "K_SHORTEST"):
        return REJECTED
    return None


def end_nodes(graph: Mapping[str, Any], start: str, pattern: Sequence) -> list[str]:
    """Multiset (sorted list) of instance end nodes: a PathJoin under ALL."""
    return sorted(inst[0][-1] for inst in instances(graph, start, pattern))

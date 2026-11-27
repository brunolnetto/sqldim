"""RelationshipSubgraph endpoint cases by reachability (§3.4.2, §8.7).

``x →* y`` holds when a walk of zero or more edges leads from x to y, so
``x →* x`` always.  A graph is a mapping as in
:mod:`tests.conformance.oracle.paths`; edges are ``(src, dst, label)``.

* ``N_A* = {n | A →* n}``,        ``E_A* = {(u, v, l) | A →* u}``
* ``N_*B = {n | n →* B}``,        ``E_*B = {(u, v, l) | v →* B}``
* ``N_AB = {n | A →* n →* B}``,   ``E_AB = {(u, v, l) | A →* u and v →* B}``

The definitions are those of the specification after #139; the simple-path
reading of earlier versions is not modelled here.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from tests.conformance.oracle import Undefined

Edge = tuple[str, str, str]


def _edges(graph: Mapping[str, Any]) -> list[Edge]:
    return [(e[0], e[1], e[2]) for e in graph["edges"]]


def reach(graph: Mapping[str, Any], start: str, *, backward: bool = False) -> set[str]:
    """Nodes reachable from *start* (``backward``: nodes that reach it)."""
    if start not in graph["nodes"]:
        raise Undefined(f"{start!r} is not a node of the graph")
    seen, frontier = {start}, [start]
    while frontier:
        cur = frontier.pop()
        for src, dst, _l in _edges(graph):
            a, b = (dst, src) if backward else (src, dst)
            if a == cur and b not in seen:
                seen.add(b)
                frontier.append(b)
    return seen


def forward(graph: Mapping[str, Any], a: str) -> tuple[set[str], set[Edge]]:
    """``(N_A*, E_A*)``."""
    nodes = reach(graph, a)
    return nodes, {e for e in _edges(graph) if e[0] in nodes}


def backward(graph: Mapping[str, Any], b: str) -> tuple[set[str], set[Edge]]:
    """``(N_*B, E_*B)``."""
    nodes = reach(graph, b, backward=True)
    return nodes, {e for e in _edges(graph) if e[1] in nodes}


def between(graph: Mapping[str, Any], a: str, b: str) -> tuple[set[str], set[Edge]]:
    """``(N_AB, E_AB)``, from the definition (not from the intersection)."""
    from_a = reach(graph, a)
    to_b = reach(graph, b, backward=True)
    nodes = {n for n in graph["nodes"] if n in from_a and n in to_b}
    edges = {e for e in _edges(graph) if e[0] in from_a and e[1] in to_b}
    return nodes, edges


def subgraph(graph: Mapping[str, Any], case_input: Mapping[str, Any]) -> dict:
    """``{"nodes": sorted, "edges": sorted}`` of one endpoint case."""
    kind = case_input["case"]
    if kind == "between":
        nodes, edges = between(graph, case_input["source"], case_input["target"])
    elif kind == "forward":
        nodes, edges = forward(graph, case_input["source"])
    elif kind == "backward":
        nodes, edges = backward(graph, case_input["target"])
    elif kind == "intersection":
        n1, e1 = forward(graph, case_input["source"])
        n2, e2 = backward(graph, case_input["target"])
        nodes, edges = n1 & n2, e1 & e2
    else:
        raise Undefined(f"unknown endpoint case {kind!r}")
    return {"nodes": sorted(nodes), "edges": sorted(edges)}

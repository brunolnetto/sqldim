"""Anchor sets and the anchor-set subgraph ``G_𝒜`` (§3.4.6).

Computed by enumerating walks in ``U(G_S)`` explicitly — every edge type
traversable in both directions — rather than by distances, so the oracle does
not share the implementation's shortcut.

A schema is data::

    {"nodes": {"Customer": "dim", "Sale": "fact", ...},
     "edges": [("Customer", "Sale", "placed", "verb"), ...],
     "roles": {"Date": ["order", "ship"]},                 # RolePlaying
     "sigma": [("SCDType", ("Customer",), ()), ...]}       # kind, node types, edge labels

An anchor is ``"Type"`` or ``"Type@role"``.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from itertools import combinations
from typing import Any

from tests.conformance.oracle import Undefined

Edge = tuple[str, str, str]  # (source, label, target)


def _parse(anchor: str) -> tuple[str, str | None]:
    t, sep, role = anchor.partition("@")
    return t, (role if sep else None)


def _steps(schema: Mapping[str, Any], node: str):
    for s, t, label, _kind in schema["edges"]:
        if s == node:
            yield t, (s, label, t)
        if t == node:
            yield s, (s, label, t)


def walks(schema: Mapping[str, Any], start: str, h: int):
    """Every walk of 1…h hops from *start* in U(G_S): (nodes, edges)."""
    stack = [((start,), ())]
    while stack:
        nodes, edges = stack.pop()
        if edges:
            yield nodes, edges
        if len(edges) < h:
            for nxt, e in _steps(schema, nodes[-1]):
                stack.append((nodes + (nxt,), edges + (e,)))


def _joining(schema, types: Sequence[str], h: int):
    """Walks of ≤ h hops between two distinct anchors (by index)."""
    for i, a in enumerate(types):
        for nodes, edges in walks(schema, a, h):
            if any(j != i and nodes[-1] == b for j, b in enumerate(types)):
                yield i, nodes, edges


def _role_problems(schema: Mapping[str, Any], parsed) -> list[str]:
    by_type: dict[str, list[str | None]] = {}
    for t, role in parsed:
        by_type.setdefault(t, []).append(role)
    problems = []
    for t, roles in sorted(by_type.items()):
        if len(roles) < 2:
            continue
        if None in roles or len(set(roles)) != len(roles):
            problems.append("AS2")
            continue
        declared = set(schema.get("roles", {}).get(t, ()))
        incident = {lab for s, d, lab, _k in schema["edges"] if t in (s, d)}
        for r in roles:
            if r in declared:
                continue
            if r in incident:  # "a distinct edge label reaching that type"
                raise Undefined("which labels 'reach' a type is not modelled")
            problems.append("AS2")
            break
    return problems


def violations(schema: Mapping[str, Any], case_input: Mapping[str, Any]) -> list[str]:
    """Sorted rule tags violated by an anchor set: K, DUP, AS1, AS2, AS3."""
    anchors, h, k = case_input["anchors"], case_input["h"], case_input["max_anchors"]
    parsed = [_parse(a) for a in anchors]
    if any(t not in schema["nodes"] for t, _ in parsed):
        return ["UNKNOWN"]
    problems: list[str] = []
    if len(parsed) < 2:
        problems.append("K")
    if len(set(parsed)) != len(parsed):
        problems.append("DUP")
    if len(parsed) > k:
        problems.append("AS3")
    problems += _role_problems(schema, parsed)
    types = [t for t, _ in parsed]
    joined = {(i, j) for i, j in combinations(range(len(types)), 2)}
    found = set()
    for i, nodes, _edges in _joining(schema, types, h):
        for j, b in enumerate(types):
            if j != i and nodes[-1] == b:
                found.add((min(i, j), max(i, j)))
    if joined - found:
        problems.append("AS1")
    return sorted(set(problems))


def subgraph(schema: Mapping[str, Any], case_input: Mapping[str, Any]) -> dict:
    """``N_𝒜``, ``E_𝒜`` and ``Σ|_𝒜`` of a valid anchor set."""
    if violations(schema, case_input):
        raise Undefined("an anchor set that is not valid has no question space")
    types = [_parse(a)[0] for a in case_input["anchors"]]
    nodes: set[str] = set()
    edges: set[Edge] = set()
    for _i, ns, es in _joining(schema, types, case_input["h"]):
        nodes.update(ns)
        edges.update(es)
    labels = {lab for _s, lab, _t in edges}
    sigma = sorted(
        kind
        for kind, named_nodes, named_labels in schema.get("sigma", ())
        if set(named_nodes) <= nodes and set(named_labels) <= labels
    )
    return {"nodes": sorted(nodes), "edges": sorted(edges), "sigma": sigma}


def partition_problems(schema: Mapping[str, Any], edge: Sequence[str]) -> bool:
    """§2.1: does a verb edge type break ``V_S ⊆ D_S × F_S``?"""
    s, t, _label, kind = edge
    if kind != "verb":
        return False
    return schema["nodes"][s] != "dim" or schema["nodes"][t] != "fact"

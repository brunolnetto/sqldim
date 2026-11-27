"""Dispatch a case to the oracle function of its area.

This is glue between the case data and :mod:`tests.conformance.oracle`; it
adds no semantics of its own.
"""

from __future__ import annotations

from collections.abc import Callable
from itertools import product
from typing import Any

from tests.conformance.cases import Case
from tests.conformance.cases.anchors import SCHEMAS
from tests.conformance.cases.fixtures import GRAPHS, GRID_VALUES, INPUTS, POINT_VALUES
from tests.conformance.oracle import aggregation, allen, anchors, ctl, logic, model, scd
from tests.conformance.oracle import paths, structure


def interval_pairs() -> list[tuple[tuple, tuple]]:
    ivs = list(product(GRID_VALUES, repeat=2))
    return [(lv, rv) for lv in ivs for rv in ivs]


def point_pairs() -> list[tuple]:
    return list(product(POINT_VALUES, repeat=2))


def _allen(i: dict) -> list:
    return [
        (lv, rv)
        for lv, rv in interval_pairs()
        if allen.domain_of(lv, rv) == i["domain"] and allen.allen(i["relation"], lv, rv)
    ]


def _point(i: dict) -> list:
    tol = i.get("tolerance") or 0
    return [(a, b) for a, b in point_pairs() if allen.point(i["relation"], a, b, tol)]


def _allen_theorem(i: dict) -> bool:
    want = 0 if i.get("none") else 1
    return all(
        len(allen.relations(lv, rv)) == want
        for lv, rv in interval_pairs()
        if allen.domain_of(lv, rv) in i["domains"]
    )


def _anchor(part: str) -> Callable[[Case], Any]:
    def run(c: Case) -> Any:
        schema = SCHEMAS[c.fixture]
        if part == "valid":
            return anchors.violations(schema, c.input)
        return anchors.subgraph(schema, c.input)[part]

    return run


def completeness_tree(bits: tuple[bool, ...]) -> tuple:
    """The filter tree of the §8.2 proof for the function with truth table *bits*."""
    names = ("p", "q")
    if not any(bits):
        return ("and", ("atom", "p"), ("not", ("atom", "p")))
    minterms = []
    for assignment, bit in zip(product([False, True], repeat=2), bits):
        if bit:
            lits = [
                ("atom", n) if v else ("not", ("atom", n))
                for n, v in zip(names, assignment)
            ]
            minterms.append(("and",) + tuple(lits))
    return ("or",) + tuple(minterms)


def truth_set(formula: tuple) -> list[tuple[bool, ...]]:
    names = ("p", "q", "r")
    table = logic.truth_table(formula, names)
    return [a for a, t in zip(product([False, True], repeat=3), table) if t]


_AREAS: dict[str, Callable[[Case], Any]] = {
    "ctl": lambda c: ctl.evaluate(GRAPHS[c.fixture], c.input["start"], c.input),
    "pathpred": lambda c: ctl.evaluate(GRAPHS[c.fixture], c.input["start"], c.input),
    "repeat_join": lambda c: paths.end_nodes(
        GRAPHS[c.fixture], c.input["start"], c.input["pattern"]
    ),
    "construction": lambda c: getattr(structure, c.input["kind"])(c.input),
    "fanout": lambda c: aggregation.fan_out({**INPUTS[c.fixture], **c.input}),
    "allen": lambda c: _allen(dict(c.input)),
    "point": lambda c: _point(dict(c.input)),
    "ordering_typing": lambda c: allen.typing(dict(c.input)),
    "allen_theorem": lambda c: _allen_theorem(dict(c.input)),
    "monitor": lambda c: ctl.monitor(c.input),
    "scd_measure": lambda c: scd.measure_by(c.input),
    "edge_validity": lambda c: scd.valid_neighbours(c.input),
    "additivity": lambda c: aggregation.additivity(c.input),
    "temporal_agg": lambda c: aggregation.temporal_agg(c.input),
    "drill_across": lambda c: aggregation.drill_across(
        {**INPUTS[c.fixture], **c.input}
    ),
    "constellation_check": lambda c: aggregation.constellation_check(c.input),
    "anchor_valid": _anchor("valid"),
    "anchor_nodes": _anchor("nodes"),
    "anchor_edges": _anchor("edges"),
    "anchor_sigma": _anchor("sigma"),
    "declaration": lambda c: model.declaration(c.input),
    "declaration_partition": lambda c: model.declaration(c.input),
    "partition": lambda c: anchors.partition_problems(
        {"nodes": c.input["nodes"]}, c.input["edge"]
    ),
    "bdd": lambda c: logic.decide(c.input),
    "forall_elim": lambda c: ctl.forall_elimination(c.input["mode"]),
    "completeness": lambda c: (
        logic.truth_table(completeness_tree(c.input["bits"]), ("p", "q"))
        == tuple(c.input["bits"])
    ),
    "cubes": lambda c: truth_set(c.input["formula"]),
}

AREAS = frozenset(_AREAS)


def oracle_result(case: Case) -> Any:
    return _AREAS[case.area](case)

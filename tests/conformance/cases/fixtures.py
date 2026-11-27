"""Tiny instance graphs and schemas the cases read, as data.

Graphs follow :mod:`tests.conformance.oracle.paths`: ``nodes`` maps an id to
its property bag (``type`` is the node type), ``edges`` lists
``(source, target, label)``.
"""

from __future__ import annotations

from typing import Any

# ADR 0001, Graph A — terminal facts.  c placed s1 (600) and s2 (300); c0 none.
GRAPH_A: dict[str, Any] = {
    "nodes": {
        "c": {"type": "Customer"},
        "c0": {"type": "Customer"},
        "s1": {"type": "Sale", "revenue": 600},
        "s2": {"type": "Sale", "revenue": 300},
    },
    "edges": [("c", "s1", "placed"), ("c", "s2", "placed")],
}

# NULL measures, outer Refs and strategy ties (derived cases).
GRAPH_A2: dict[str, Any] = {
    "nodes": {
        "c2": {"type": "Customer", "threshold": 650},
        "c3": {"type": "Customer", "threshold": 0},
        "c4": {"type": "Customer", "threshold": 500},
        "s3": {"type": "Sale", "revenue": 700},
        "s4": {"type": "Sale", "revenue": None},
        "s5": {"type": "Sale", "revenue": None},
        "s6": {"type": "Sale", "revenue": 300},
        "s7": {"type": "Sale", "revenue": 600},
    },
    "edges": [
        ("c2", "s3", "placed"),
        ("c2", "s4", "placed"),
        ("c3", "s5", "placed"),
        ("c4", "s6", "placed"),
        ("c4", "s7", "placed"),
    ],
}

# ADR 0001, Graph B — a chain ending in a terminal (transition bridges).
GRAPH_B: dict[str, Any] = {
    "nodes": {
        "a0": {"type": "Artifact", "state": "Missing"},
        "a1": {"type": "Artifact", "state": "In-flight"},
        "a2": {"type": "Artifact", "state": "Complete"},
    },
    "edges": [("a0", "a1", "next"), ("a1", "a2", "next")],
}

# §4.1 "AF and AG are distinct": G1 = s0 → s1; G2 = s0 → s1, s0 → s2.
GRAPH_G1: dict[str, Any] = {
    "nodes": {"s0": {"phi": False}, "s1": {"phi": True}},
    "edges": [("s0", "s1", "next")],
}
GRAPH_G2: dict[str, Any] = {
    "nodes": {"s0": {"phi": False}, "s1": {"phi": True}, "s2": {"phi": False}},
    "edges": [("s0", "s1", "next"), ("s0", "s2", "next")],
}

# A cycle x1 → x2 → x3 → x1 with a tail x3 → x4 (Repeat modes, §3.2).
GRAPH_CYCLE: dict[str, Any] = {
    "nodes": {f"x{i}": {"type": "X"} for i in range(1, 5)},
    "edges": [("x1", "x2", "next"), ("x2", "x3", "next"), ("x3", "x1", "next"),
              ("x3", "x4", "next")],
}  # fmt: skip

# §3.4.2 / §8.7: the cycle on which the simple-path reading of the cones broke
# the intersection identity — A → X → B → n → X.
GRAPH_CONE_CYCLE: dict[str, Any] = {
    "nodes": {n: {"type": "T"} for n in ("A", "X", "B", "n")},
    "edges": [("A", "X", "e"), ("X", "B", "e"), ("B", "n", "e"), ("n", "X", "e")],
}

GRAPHS: dict[str, dict[str, Any]] = {
    "graph_a": GRAPH_A,
    "graph_a2": GRAPH_A2,
    "graph_b": GRAPH_B,
    "graph_g1": GRAPH_G1,
    "graph_g2": GRAPH_G2,
    "graph_cycle": GRAPH_CYCLE,
    "graph_cone_cycle": GRAPH_CONE_CYCLE,
}

ONE_HOP = (("hop", "placed", "fwd", True),)
"""``VerbHop(Customer, 'placed', Sale, node_alias=fs)``; sub_filter reads fs."""

CHAIN = (("repeat", "next", "fwd", 0, None, "SIMPLE"),)
"""``Repeat(BridgeHop(next), 0, UNBOUNDED)``: the start node is a position."""

REVENUE_GT_500 = ("prop", "revenue", ">", 500)
COMPLETE = ("prop", "state", "=", "Complete")
PHI = ("prop", "phi", "=", True)

# §4.2 example: sale S1 (revenue 100) reaches store T by two campaign paths.
TWO_CAMPAIGN_PATHS: dict[str, Any] = {
    "tuples": [
        {"s": "S1", "p": "A", "st": "T"},
        {"s": "S1", "p": "B", "st": "T"},
    ],
    "props": {"S1": {"revenue": 100}},
}

# Every pair of intervals over {0, 1, 2, 3} with NULL bounds (§3.2, §8.8).
GRID_VALUES: tuple = (0, 1, 2, 3, None)
POINT_VALUES: tuple = (0, 1, 2, None)

# §2.5 example: a customer changes region (SCDType(Customer, SCD2)).
DIM_CUSTOMER_REGION: list[dict[str, Any]] = [
    {"version_key": 101, "entity_key": "C42", "region": "South",
     "effective_from": "2023-01-01", "effective_to": "2024-03-01"},
    {"version_key": 102, "entity_key": "C42", "region": "North",
     "effective_from": "2024-03-01", "effective_to": None},
]  # fmt: skip
FACT_SALE_REGION: list[dict[str, Any]] = [
    {"id": "S1", "dim_ref": "C42", "event_time": "2023-06-10", "revenue": 600},
    {"id": "S2", "dim_ref": "C42", "event_time": "2024-05-02", "revenue": 400},
]

# §2.6 "SCD2 at two instants": segment of C42, sale S1.
DIM_CUSTOMER_SEGMENT: list[dict[str, Any]] = [
    {"version_key": 101, "entity_key": "C42", "segment": "retail",
     "effective_from": "2023-01-01", "effective_to": "2024-03-01"},
    {"version_key": 102, "entity_key": "C42", "segment": "wholesale",
     "effective_from": "2024-03-01", "effective_to": None},
]  # fmt: skip
FACT_SALE_SEGMENT: list[dict[str, Any]] = [
    {"id": "S1", "dim_ref": "C42", "event_time": "2023-06-10", "revenue": 600},
]

# A version boundary inside a day (derived from the half-open window of §2.6).
DIM_CUSTOMER_INTRADAY: list[dict[str, Any]] = [
    {"version_key": 201, "entity_key": "C7", "segment": "retail",
     "effective_from": "2023-01-01 00:00:00", "effective_to": "2024-03-01 18:00:00"},
    {"version_key": 202, "entity_key": "C7", "segment": "wholesale",
     "effective_from": "2024-03-01 18:00:00", "effective_to": None},
]  # fmt: skip
FACT_SALE_INTRADAY: list[dict[str, Any]] = [
    {"id": "S9", "dim_ref": "C7", "event_time": "2024-01-10 09:00:00", "revenue": 50},
]

# §2.6 "Reverse traversal with edge validity" (+ E9 with an open valid_from).
BRIDGE_REPORTS_TO: list[dict[str, Any]] = [
    {"source_key": "E7", "target_key": "M1", "valid_from": "2023-01-01",
     "valid_to": "2024-01-01"},
    {"source_key": "E7", "target_key": "M2", "valid_from": "2024-01-01",
     "valid_to": None},
]  # fmt: skip
BRIDGE_REPORTS_TO_OPEN = BRIDGE_REPORTS_TO + [
    {"source_key": "E9", "target_key": "M1", "valid_from": None,
     "valid_to": "2024-01-01"},
]  # fmt: skip

# §5.2.2 drill-across example.
CONSTELLATION: dict[str, Any] = {
    "key": "customer",
    "branches": {
        "sale": [
            {"id": "S1", "customer": "C1", "revenue": 100},
            {"id": "S2", "customer": "C1", "revenue": 200},
            {"id": "S3", "customer": "C2", "revenue": 50},
            {"id": "S4", "customer": "C2", "revenue": 70},
        ],
        "return_ev": [
            {"id": "R1", "customer": "C1", "amount": 10},
            {"id": "R2", "customer": "C1", "amount": 20},
            {"id": "R3", "customer": "C1", "amount": 30},
            {"id": "R4", "customer": "C3", "amount": 5},
        ],
    },
}

# §2.4 AD2 example: Grain(AccountBalance, PERIOD), one row per account per day.
ACCOUNT_BALANCE: list[dict[str, Any]] = [
    {"account": "A1", "branch": "B1", "day": "2024-01-01", "balance": 100},
    {"account": "A1", "branch": "B1", "day": "2024-01-02", "balance": 120},
    {"account": "A2", "branch": "B1", "day": "2024-01-01", "balance": 50},
    {"account": "A2", "branch": "B1", "day": "2024-01-02", "balance": 40},
    {"account": "A3", "branch": "B2", "day": "2024-01-01", "balance": 10},
]

INPUTS: dict[str, dict[str, Any]] = {
    "two_campaign_paths": TWO_CAMPAIGN_PATHS,
    "constellation": CONSTELLATION,
}
"""Fixtures that are merged into a case's input as they are."""

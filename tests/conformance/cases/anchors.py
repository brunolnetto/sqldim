"""Anchor sets ``G_𝒜`` (§3.4.6) and the declared model (§2.1, §2.6)."""

from __future__ import annotations

import random
from typing import Any

from tests.conformance.cases import DERIVED, Case
from tests.conformance.oracle import REJECTED

ACCEPTED = "ACCEPTED"
AS = "§3.4.6 Anchor sets (AS1–AS3, N_𝒜, E_𝒜, Σ|_𝒜)"

RETAIL: dict[str, Any] = {
    "nodes": {
        "Customer": "dim",
        "Product": "dim",
        "Date": "dim",
        "Region": "dim",
        "Sale": "fact",
        "Return": "fact",
    },
    "edges": [
        ("Customer", "Sale", "placed", "verb"),
        ("Product", "Sale", "included in", "verb"),
        ("Date", "Sale", "occurred on", "verb"),
        ("Customer", "Return", "returned", "verb"),
    ],
    "roles": {"Date": ["order", "ship"]},
    "sigma": [
        ("Conformed(Customer, {Sale, Return})", ("Customer", "Sale", "Return"), ()),
        ("Grain(Return, EVENT)", ("Return",), ()),
        ("Grain(Sale, EVENT)", ("Sale",), ()),
        ("MedallionLayer(Product, GOLD)", ("Product",), ()),
        ("SCDType(Customer, SCD2)", ("Customer",), ()),
    ],
}
"""§3.4.6 example schema, with Return (constellation) and an isolated Region."""


def random_schema(seed: int) -> dict[str, Any]:
    """A small schema drawn deterministically from *seed* (no annotations)."""
    rng = random.Random(seed)
    n = rng.randint(3, 6)
    kinds = {f"T{i}": rng.choice(("dim", "fact")) for i in range(n)}
    names = sorted(kinds)
    pairs = [(names[i], rng.choice(names[:i])) for i in range(1, n)]  # connected
    pairs += [(rng.choice(names), rng.choice(names)) for _ in range(rng.randint(0, 2))]
    edges = []
    for j, (a, b) in enumerate(pairs):
        s, t = (a, b) if rng.random() < 0.5 else (b, a)
        kind = "verb" if kinds[s] == "dim" and kinds[t] == "fact" else "bridge"
        edges.append((s, t, f"e{j}", kind))
    return {"nodes": kinds, "edges": edges, "roles": {}, "sigma": []}


def _random_anchors(seed: int, part: str) -> dict[str, Any]:
    """Anchors of distinct types; ``h`` ≥ |N_S| − 1 for the subgraph parts (AS1)."""
    rng = random.Random(seed * 7919)
    names = sorted(random_schema(seed)["nodes"])
    k = rng.randint(2, min(3, len(names)))
    h = rng.randint(1, 3) if part == "valid" else min(len(names) - 1 + seed % 2, 5)
    return {"anchors": sorted(rng.sample(names, k)), "h": h, "max_anchors": 3}


SCHEMAS: dict[str, dict[str, Any]] = {"retail": RETAIL}
SCHEMAS.update({f"random_{s}": random_schema(s) for s in range(12)})


def _a(cid, area, rule, exp, fixture="retail", **inp):
    base = {"max_anchors": 4, "h": 2}
    base.update(inp)
    return Case(cid, area, AS, rule, fixture, base, exp)


CPD = ["Customer", "Date", "Product"]
SR = ["Return", "Sale"]

ANCHOR_CASES = [
    _a("AS-01", "anchor_valid", "customer, product, date: AS1 holds at h = 2", [],
       anchors=CPD),
    _a("AS-02", "anchor_nodes", "N_𝒜 of the customer/product/date example",
       ["Customer", "Date", "Product", "Sale"], anchors=CPD),
    _a("AS-03", "anchor_edges", "E_𝒜 of the customer/product/date example",
       [("Customer", "placed", "Sale"), ("Date", "occurred on", "Sale"),
        ("Product", "included in", "Sale")], anchors=CPD),
    _a("AS-04", "anchor_nodes", "the constellation Sale ← Customer → Return is in G_𝒜",
       DERIVED, anchors=SR),
    _a("AS-05", "anchor_edges", "E_𝒜 of {Sale, Return} uses reverse hops", DERIVED,
       anchors=SR),
    _a("AS-06", "anchor_sigma", "Σ|_𝒜 keeps every annotation whose types lie in G_𝒜",
       DERIVED, anchors=SR),
    _a("AS-07", "anchor_valid", "an unconnected anchor is not dropped: AS1", DERIVED,
       anchors=["Customer", "Region"]),
    _a("AS-08", "anchor_valid", "h = 1 does not join customer and product", DERIVED,
       anchors=["Customer", "Product"], h=1),
    _a("AS-09", "anchor_valid", "two anchors of one type without roles: AS2", DERIVED,
       anchors=["Date", "Date"]),
    _a("AS-10", "anchor_valid", "two roles declared by RolePlaying", DERIVED,
       anchors=["Date@order", "Date@ship"]),
    _a("AS-11", "anchor_valid", "the same anchor twice", DERIVED,
       anchors=["Date@order", "Date@order"]),
    _a("AS-12", "anchor_valid", "k > MAX_ANCHORS: AS3", DERIVED, anchors=CPD,
       max_anchors=2),
    _a("AS-13", "anchor_valid", "k ≥ 2", DERIVED, anchors=["Customer"]),
    _a("AS-14", "anchor_valid", "an anchor that is not a node type", DERIVED,
       anchors=["Customer", "Nope"]),
]  # fmt: skip

GENERATED = [
    _a(f"AS-GEN-{s:02d}-{part}", f"anchor_{part}",
       f"seeded schema {s}: {part} by walk enumeration", DERIVED,
       fixture=f"random_{s}", **_random_anchors(s, part))
    for s in range(12)
    for part in ("valid", "nodes", "edges")
]  # fmt: skip

DECL = "§2.6 Declared model (well-formedness at construction); §2.4 argument kinds"
PART = "§2.1 Structural invariants (V_S ⊆ D_S × F_S)"


def _decl(cid, rule, exp, **inp):
    base: dict[str, Any] = {
        "nodes": {"Customer": "dim", "Sale": "fact"},
        "edges": [("Customer", "Sale", "placed", "verb")],
        "sigma": [],
    }
    base.update(inp)
    return Case(cid, "declaration", DECL, rule, "none", base, exp)


MODEL_CASES = [
    _decl("MOD-01", "a well-formed declaration", ACCEPTED,
          sigma=[("Conformed", ("Customer", ["Sale"])), ("Grain", ("Sale",)),
                 ("SCDType", ("Customer",))]),
    _decl("MOD-02", "Conformed names a fact where a dimension is required", REJECTED,
          sigma=[("Conformed", ("Sale", ["Sale"]))]),
    _decl("MOD-03", "Grain names a dimension", REJECTED, sigma=[("Grain", ("Customer",))]),
    _decl("MOD-04", "SCDType names a fact", REJECTED, sigma=[("SCDType", ("Sale",))]),
    _decl("MOD-05", "an edge type declared twice", REJECTED,
          edges=[("Customer", "Sale", "placed", "verb")] * 2),
    _decl("MOD-06", "an edge type naming no node type", REJECTED,
          edges=[("Customer", "Nope", "placed", "verb")]),
    _decl("MOD-07", "a node type declared twice", REJECTED,
          node_list=["Customer", "Customer", "Sale"]),
    Case("MOD-08", "partition", PART, "a verb edge type fact → dim breaks V_S ⊆ D_S × F_S",
         "none", {"nodes": {"Customer": "dim", "Sale": "fact"},
                  "edge": ("Sale", "Customer", "placed", "verb")}, True),
    Case("MOD-09", "partition", PART, "a verb edge type dim → dim", "none",
         {"nodes": {"Customer": "dim", "Region": "dim"},
          "edge": ("Customer", "Region", "in", "verb")}, True),
    Case("MOD-10", "partition", PART, "a verb edge type dim → fact", "none",
         {"nodes": {"Customer": "dim", "Sale": "fact"},
          "edge": ("Customer", "Sale", "placed", "verb")}, False),
    Case("MOD-11", "partition", PART, "a bridge fact → dim is admitted", "none",
         {"nodes": {"Customer": "dim", "Sale": "fact"},
          "edge": ("Sale", "Customer", "billed_to", "bridge")}, False),
    Case("MOD-12", "declaration_partition", "§2.6 Declared model: §2.1 invariants are "
         "not construction checks", "a verb edge type fact → dim is not rejected at "
         "construction", "none",
         {"nodes": {"Customer": "dim", "Sale": "fact"},
          "edges": [("Sale", "Customer", "placed", "verb")], "sigma": []}, ACCEPTED),
]  # fmt: skip

CASES = ANCHOR_CASES + GENERATED + MODEL_CASES

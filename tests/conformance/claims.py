"""Register of the DGM's theoretical claims and of the evidence for each (#139).

Each :class:`Claim` states one theorem, proposition or worked bound of
``docs/motivation/dimensional_graph_modelling.md`` or an ADR, the kind of
evidence that backs it, the artifact that reproduces the evidence and its
status.  The register is rendered into ``docs/reference/dgm-claims.md`` between
two markers; ``test_claims.py`` fails when the committed text differs, when an
artifact names a check or a case that does not exist, or when a check in
``test_claims.py`` is not referenced by any claim.  Regenerate with::

    PYTHONPATH=. python -m tests.conformance.claims --write

A check is evidence, not a proof: it decides the statement on the bounded
domain it names and on nothing larger.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

DOC = Path(__file__).resolve().parents[2] / "docs" / "reference" / "dgm-claims.md"
BEGIN = "<!-- BEGIN GENERATED: tests/conformance/claims.py -->"
END = "<!-- END GENERATED -->"
CHECKS = "tests/conformance/test_claims.py"


class Kind(Enum):
    EXHAUSTIVE = "exhaustive check on a bounded domain"
    ORACLE = "values stated by the spec, reproduced by the oracle"
    WITNESS = "executable witness (a single fixture)"
    CITED = "cited proof from the literature"
    SPEC_PROOF = "proof in the specification, not checked"
    NONE = "not independently evidenced"


class Status(Enum):
    HOLDS = "holds on the checked domain"
    COUNTEREXAMPLE = "**counterexample**"
    CITED = "rests on the citation"
    UNCHECKED = "not evidenced"


@dataclass(frozen=True)
class Claim:
    id: str
    clause: str
    statement: str
    kinds: tuple[Kind, ...]
    status: Status
    artifacts: tuple[str, ...] = ()
    """``check:<test function in test_claims.py>`` or ``cases:<id glob>``."""
    domain: str = ""
    note: str = ""
    corrected: str = ""
    """The earlier statement and the counterexample that made the spec change it."""


E, R, W, C, P, N = (
    Kind.EXHAUSTIVE,
    Kind.ORACLE,
    Kind.WITNESS,
    Kind.CITED,
    Kind.SPEC_PROOF,
    Kind.NONE,
)

CLAIMS: tuple[Claim, ...] = (
    Claim(
        "CLM-3.3-ORDER",
        "§3.3 Refinement order",
        "SHORTEST = K_SHORTEST(1) ⊑ K_SHORTEST(k) ⊑ K_SHORTEST(k') ⊑ ALL for "
        "1 ≤ k ≤ k', and MIN_WEIGHT ⊑ ALL, where s ⊑ s' iff s(I) ⊆ s'(I); "
        "MIN_WEIGHT is incomparable with every K_SHORTEST(k).",
        (E, W),
        Status.HOLDS,
        (
            "check:test_claim_strategy_refinement_order",
            "check:test_claim_min_weight_incomparable",
        ),
        "instance sets of every one-label graph on 3 nodes (self-loops "
        "included) under SIMPLE Repeat, k ≤ 4; incomparability by one "
        "weighted witness per k ≤ 3",
    ),
    Claim(
        "CLM-3.4.2-CONE-SUBGRAPH",
        "§3.4.2 RelationshipSubgraph by reachability",
        "G_AB, G_A* and G_*B are subgraphs (every edge has both ends in the "
        "node set), and G_AB ⊆ G_A* ⊆ G_**, G_AB ⊆ G_*B ⊆ G_**.",
        (E, R),
        Status.HOLDS,
        ("check:test_claim_cone_intersection_identity", "cases:CONE-*"),
        "every directed graph on ≤ 3 nodes, self-loops included, every (A, B)",
    ),
    Claim(
        "CLM-3.4.5-PROJ",
        "§3.4.5 Projection lemma",
        "h maps G_ab^I into G_AB^S: h_N(N_ab^I) ⊆ N_AB^S and h_E(E_ab^I) ⊆ E_AB^S.",
        (P,),
        Status.UNCHECKED,
        (),
        note="the proof was restated on reachability with the cones (#139); no "
        "check enumerates typed instance graphs",
    ),
    Claim(
        "CLM-4.1-DUAL",
        "§4.1 FORALL elimination; ADR 0001 Duality and construction",
        "Under on_empty = VACUOUS, AG φ = ¬EF ¬φ, AF φ = ¬EG ¬φ, AX φ = ¬EX ¬φ and "
        "A(φ U ψ) = ¬E(¬ψ U (¬φ ∧ ¬ψ)) ∧ ¬EG ¬ψ over maximal traces; under "
        "REQUIRE_WITNESS each FORALL form is its construction conjoined with "
        "EF TRUE (EX TRUE for NEXT). The inner ¬ must be NotTrue.",
        (E, R),
        Status.HOLDS,
        (
            "check:test_claim_ctl_dualities",
            "check:test_claim_dualities_need_nottrue",
            "cases:BDD-F-*",
        ),
        "every one-label graph on 3 nodes without self-loops, every two-valued "
        "labelling of φ and ψ, start node fixed by symmetry, patterns Repeat(0|1, UNBOUNDED, "
        "SIMPLE) and one hop; finite traces only (the lasso of ADR 0001 is not "
        "modelled)",
        "with Kleene ¬ instead of NotTrue the AG duality fails at a node where φ "
        "is UNKNOWN — the reason §4.1 reads the inner NOT as NotTrue",
    ),
    Claim(
        "CLM-ADR1-VALUES",
        "ADR 0001 Worked values (Graphs A and B)",
        "The truth tables of ADR 0001 for EF, EG, EX, AF, AG, AX under both "
        "empty-domain policies.",
        (R,),
        Status.HOLDS,
        ("cases:CTL-A-*", "cases:CTL-B-*"),
        "the two graphs of the ADR",
    ),
    Claim(
        "CLM-6.3-HOEFF",
        "§6.3 Strategy 3 — sampling with error bounds",
        "For n anchors drawn uniformly without replacement, P(|X̄ − μ| ≥ t) ≤ "
        "2·exp(−2nt²/(hi − lo)²); n* = ⌈log(2k/δ)/(2ε²)⌉ gives error_bound ≤ ε, "
        "and k = 1, δ = 0.05, ε = 0.05 gives n* = 738.",
        (C, E),
        Status.HOLDS,
        ("check:test_claim_hoeffding_sample_size", "check:test_claim_hoeffding_exact"),
        "the inequality by Hoeffding [15] (§6 for sampling without replacement); "
        "checked exactly (hypergeometric, rational arithmetic) for every "
        "population of N ≤ 16 values in {0, 1}, every n ≤ N and t on a grid; "
        "n* minimal for k ≤ 5, δ ∈ {0.01, 0.05, 0.1}, ε ∈ {0.01, 0.02, 0.05, 0.1}",
        "values in a general [lo, hi] rest on the citation; the union bound over "
        "k aggregates is elementary and not checked",
    ),
    Claim(
        "CLM-8.1-KRIPKE",
        "§8.1 Derived Kripke structure",
        "Paths of K_π from (start, 0) correspond one to one to path instances of π.",
        (N,),
        Status.UNCHECKED,
        (),
        note="no product automaton is built by the oracle",
    ),
    Claim(
        "CLM-8.2-PC",
        "§8.2 Propositional completeness",
        "Every Boolean function of a finite atom set A is expressed by a filter tree "
        "over A with AND, OR, NOT; a condition that is not a function of the atoms "
        "(x > 3 over {x > 5}) is not.",
        (E, R, P),
        Status.HOLDS,
        ("check:test_claim_propositional_completeness", "cases:THM-PC-*"),
        "all 2^(2^m) functions for m ≤ 3 atoms, by the proof's minterm "
        "construction; the scope counterexample at x = 4 and x = 0",
    ),
    Claim(
        "CLM-8.3-CUBE",
        "§8.3 Minterms, maxterms, canonical form",
        "A cube with j literals over |V| = m variables covers 2^(m−j) minterms; a "
        "BDD's paths to TRUE are pairwise disjoint cubes whose union is the "
        "formula's minterm set.",
        (E,),
        Status.HOLDS,
        ("check:test_claim_cubes_and_bdd_paths",),
        "every cube over 3 variables; every function of 3 variables, with a "
        "reduced decision diagram built in the check (not sqldim's BDD)",
    ),
    Claim(
        "CLM-8.4-DIAMOND",
        "§8.4 PathPred as compact quantification",
        "The diamond chain D_n has |N| = 3n + 1, |E| = 4n and 2^n simple paths from "
        "s to v_n; in a tree there is one path between two nodes, so no gain.",
        (E,),
        Status.HOLDS,
        ("check:test_claim_diamond_chain_and_tree",),
        "D_n for n ≤ 6 (path instances by the oracle); a binary tree of depth 3",
    ),
    Claim(
        "CLM-8.5-COLLAPSE",
        "§8.5 EXISTS/FORALL duality",
        "SHORTEST and MIN_WEIGHT select at most one instance, so EXISTS and FORALL "
        "collapse to a single term.",
        (E,),
        Status.HOLDS,
        ("check:test_claim_strategy_refinement_order",),
        "as CLM-3.3-ORDER",
    ),
    Claim(
        "CLM-8.6-SIMPLE-BOUND",
        "§8.6 Finite traversal space",
        "The number of simple path instances in G_I is at most "
        "Σ_k |N_I|!/(|N_I|−k)! · m^k, where m ≥ 1 is the maximum number of "
        "parallel edges between an ordered pair of nodes (1 without edges).",
        (E, W),
        Status.HOLDS,
        (
            "check:test_claim_simple_path_bound_one_label",
            "check:test_claim_simple_path_bound_parallel_edges",
            "check:test_claim_simple_path_bound_counterexample",
        ),
        "k read as the number of nodes, 1 ≤ k ≤ |N_I|; every one-label graph on "
        "≤ 4 nodes (m = 1); every graph on ≤ 3 nodes with up to 2 parallel "
        "edges of distinct labels per ordered pair, and on 2 nodes with up to 3",
        "simple path instances are counted by §3.3 key, so parallel edges are "
        "distinct instances",
        "the bound was Σ_k |N_I|!/(|N_I|−k)!, which counts node sequences: nodes "
        "a, b and edges (a,b,x), (a,b,y), (a,b,z) give 5 simple path instances "
        "(2 of length 0, 3 of length 1) against 4; the corrected bound is 24",
    ),
    Claim(
        "CLM-8.6-PATTERNS",
        "§8.6 Finite traversal space",
        "There are at most Σ_{j=0}^{k} (2·|E_S|)^j path patterns of at most k hops.",
        (E,),
        Status.HOLDS,
        ("check:test_claim_pattern_count",),
        "|E_S| ≤ 3, k ≤ 4: the enumeration meets the bound with equality",
    ),
    Claim(
        "CLM-8.7-CONE",
        "§8.7 Cone intersection theorem; §6.2 Rule 9",
        "G_A* ∩ G_*B = G_AB, for node and edge sets; Rule 9 is therefore exact.",
        (E, R),
        Status.HOLDS,
        (
            "check:test_claim_cone_intersection_identity",
            "check:test_claim_cone_old_counterexample_now_holds",
            "cases:CONE-*",
        ),
        "every directed graph on ≤ 3 nodes, self-loops included, every (A, B)",
        "false under the simple-path reading of §3.4.2 before #139 (A→X, X→B, B→n, "
        "n→X); the spec now defines the cones by reachability",
    ),
    Claim(
        "CLM-8.8-ALLEN",
        "§8.8 Allen exhaustiveness",
        "For two intervals of the domain (s < e, NULL bounds as ±∞) exactly one of "
        "the 13 relations holds, and the three-comparison tree of the proof picks "
        "it; a degenerate interval satisfies none.",
        (E, R, P),
        Status.HOLDS,
        ("check:test_claim_allen_exhaustive", "cases:TO-THM-*"),
        "every pair of intervals with bounds in {0, …, 5, NULL}",
    ),
    Claim(
        "CLM-8.9-HIER",
        "§8.9 Temporal property class hierarchy",
        "SAFETY, LIVENESS ∈ Π₁/Σ₁; RESPONSE ∈ Π₂; PERSISTENCE ∈ Σ₂; RECURRENCE ∈ GF.",
        (N,),
        Status.UNCHECKED,
        (),
        note="no proof and no citation in the spec (the classes are those of "
        "Manna and Pnueli's safety-progress hierarchy, which is not cited)",
    ),
    Claim(
        "CLM-8.10-ENTROPY",
        "§8.10 SIGNATURE_ENTROPY",
        "H = 0 iff one signature type occurs; H = log₂ k for k equally frequent "
        "types, the maximum over k types.",
        (E,),
        Status.HOLDS,
        ("check:test_claim_entropy",),
        "every count vector of ≤ 4 types with counts ≤ 4",
    ),
    Claim(
        "CLM-8.11-QC",
        "§8.11 Question completeness theorem",
        "Under QH1–QH6 Q_valid(A, B) is finite and enumerable from the schema, "
        "membership is decidable, and every concrete query over G_AB is q(π).",
        (P,),
        Status.UNCHECKED,
        (),
        note="the theorem as a whole is not checked: CLM-8.11-ENUM checks that "
        "the §12.3 construction lists Q_valid on a bounded domain; membership "
        "(part 2) and coverage (part 3) have no independent check",
    ),
    Claim(
        "CLM-8.11-ENUM",
        "§8.11 Question completeness, part 1; §12.3 `dgm_what_can_i_answer` "
        "enumeration",
        "The construction (a)–(e) of §12.3, with its Σ rules, lists each skeleton "
        "of Q_valid(𝒜) exactly once and in non-decreasing grade.",
        (E,),
        Status.HOLDS,
        ("check:test_claim_question_enumeration",),
        "an enumeration written from §8.11, §12.3, §4.1, §4.2 and §5.2.3 in "
        "`tests/conformance/qvalid.py` (no `sqldim`), compared as a set and a "
        "count with the lattice's generator: every skeleton of grade ≤ 2 over "
        "two anchors and one edge type with h = 1 (one Join), for six schemas "
        "— no properties; a numeric dimension attribute; a String property on "
        "a BRONZE dimension and a Boolean; an ADDITIVE measure; a NON_ADDITIVE "
        "measure; a Degenerate attribute — 5,920 skeletons, none missing, "
        "none extra, none twice",
        "evidence that sqldim's enumeration meets the definition on this "
        "domain. Grade ≤ 2 keeps every filter to two atoms, where the reduced "
        "BDD size does not depend on the variable order that §8.14 leaves to "
        "the atoms' encoding. Outside the domain: properties of a "
        "FactlessFact, constellations (DA1–DA3), time-ordered bridges and "
        "Joins of more than one hop",
    ),
    Claim(
        "CLM-8.13-WALK",
        "§8.13 Theorem 1 (key step) and Theorem 2",
        "For a one-hop body under Repeat(·, 0, UNBOUNDED), SIMPLE, TRAIL and walk "
        "semantics (the Datalog rules of Theorem 1, as a least fixpoint) agree on "
        "EF φ, EX φ and E(φ U ψ).",
        (E,),
        Status.HOLDS,
        ("check:test_claim_walk_equals_simple_one_hop",),
        "every one-label graph on 3 nodes (self-loops included for EF and EX, "
        "excluded for E(φ U ψ)), every two-valued labelling of φ, ψ, start node "
        "fixed by symmetry",
    ),
    Claim(
        "CLM-8.13-D4",
        "§8.13 Why D4 is needed",
        "For a body of two or more hops, walk and simple-path existence differ.",
        (W, C),
        Status.HOLDS,
        ("check:test_claim_two_hop_body_breaks_walk_reading",),
        "one witness: (a·a)* from s to t on s→t, t→u, u→v, v→t; NP-completeness "
        "of the simple case is cited [19]",
    ),
    Claim(
        "CLM-8.13-T1",
        "§8.13 Theorem 1 (F_D ⊆ Datalog¬,≤)",
        "Every query in F_D has a stratified Datalog program computing it.",
        (P,),
        Status.UNCHECKED,
        (),
        note="the translation is executed on a bounded sub-fragment "
        "(CLM-8.13-TRANSL) and its walk/simple step on its own (CLM-8.13-WALK); "
        "F_D as a whole — Joins over several anchors and PathJoins, IN, nested "
        "sub_filters, outer Refs, several edge labels, non-recursive WITH — "
        "is not",
    ),
    Claim(
        "CLM-8.13-TRANSL",
        "§8.13 Theorem 1, the translation of its proof",
        "The proof's translation — conditions in the T⁺ form with NULL as an "
        "absent fact, a fixed-length path or finite-max Repeat as a union of "
        "edge conjunctions with ≠ literals for SIMPLE and TRAIL, the recursion "
        "for an UNBOUNDED one-hop Repeat, a negated PathPred and EXCEPT as a "
        "negated IDB literal — is a safe, stratified Datalog¬,≤ program whose "
        "perfect model is the query's result.",
        (E,),
        Status.HOLDS,
        ("check:test_claim_datalog_translation",),
        "unary queries anchored on one node type, against the reference "
        "semantics: 392 conditions (comparison with a constant and of two "
        "properties under all six operators, IsNull, NOT, AND, OR to depth 2) "
        "on a node with x, y ∈ {NULL, 0, 1, 2}; 35 queries — EXISTS with "
        "EVENTUALLY, UNTIL and NEXT over one hop, two hops, Repeat(e, 0|1, "
        "UNBOUNDED, SIMPLE|TRAIL) and Repeat(e, 0|1, 2, SIMPLE|TRAIL|WALK), with "
        "a sub_filter that is UNKNOWN on NULL, their negations, two mixed "
        "conditions, UNION, INTERSECT and EXCEPT — on every graph with one edge "
        "label and x ∈ {NULL, 0, 1}, on 3 nodes without self-loops and on 2 "
        "with them, up to isomorphism (406 graphs). The stratified evaluator "
        "is `tests/conformance/datalog.py`, which refuses an unsafe or "
        "unstratified program",
        "TRAIL distinctness of two edges is a disjunction (source or target "
        "differs), so it gives one conjunction of ≠ literals per choice — a "
        "union of conjunctions, still Datalog, where the proof writes 'a "
        "conjunction'",
    ),
    Claim(
        "CLM-8.13-PTIME",
        "§8.13 Corollary (data complexity of F_D) and table",
        "F_D has polynomial data complexity; simple regular paths are NP-complete; "
        "counting simple paths is #P-complete; Immerman–Vardi does not make "
        "Q_algebra capture PTIME.",
        (C,),
        Status.CITED,
        (),
        note="Abiteboul, Hull, Vianu [20]; Mendelzon and Wood [19]; Valiant [18]; "
        "Immerman [16], Vardi [17]",
    ),
    Claim(
        "CLM-8.14-CANON",
        "§8.14 Canonical form theorem (soundness)",
        "Two composed queries that reduce to the same query DAG node are equivalent.",
        (P,),
        Status.UNCHECKED,
        (),
    ),
    Claim(
        "CLM-8.15-PARAM",
        "§8.15 Parameter completeness; finite parametric skeleton",
        "Π_valid(q) is constructively characterisable; its discrete part is finite.",
        (P,),
        Status.UNCHECKED,
        (),
        note="proof sketch only",
    ),
    Claim(
        "CLM-8.16.6-NBHD",
        "§8.16.6 Neighbourhood size under local moves",
        "The neighbourhood has at most m + g·(|V_P| − g) + 2L·|E_AB| + a·|AggFn| "
        "+ 2|P_ord| + Σ(|D_p| − 1) elements, one line per move kind, where g is "
        "the number of GroupBy keys of q.",
        (E, W),
        Status.HOLDS,
        (
            "check:test_claim_neighbourhood_bound",
            "check:test_claim_neighbourhood_bound_counterexample",
        ),
        "the GroupBy line exactly, for every key set of every size g over "
        "|V_P| ≤ 6 properties; the other lines summed on a toy skeleton space; "
        "the §8.16.8 arithmetic (56)",
        "",
        "the line read 'replace one GroupBy key ≤ |V_P|', which fails when q "
        "groups by g ≥ 2 keys: replacing one of them gives g·(|V_P| − g) "
        "neighbours, 6 > 5 for g = 2, |V_P| = 5. Polynomiality is unaffected",
    ),
    Claim(
        "CLM-ADR7-METRIC",
        "ADR 0007 d_S",
        "d_S, the unit-weight shortest-path distance in the undirected move graph "
        "M, is a metric on each connected component of M; BFS to depth k visits at "
        "most Σ_{j≤k} N^j skeletons; the ADR's example has d_S = 3.",
        (E,),
        Status.HOLDS,
        ("check:test_claim_shortest_path_metric", "check:test_claim_move_graph"),
        "every undirected graph on ≤ 5 nodes; a toy move graph of 3 atoms × 3 "
        "keys × 2 hops × 3 AggFns",
        "the example reads 'order count' as COUNT over the same argument; a "
        "change of measure argument is not one of the four moves",
    ),
)


def _artifact(a: str) -> str:
    kind, _, ref = a.partition(":")
    if kind == "check":
        return f"`{CHECKS}::{ref}`"
    return f"cases `{ref}`"


def render() -> str:
    n = {s: sum(c.status is s for c in CLAIMS) for s in Status}
    lines = [
        BEGIN,
        "",
        f"{len(CLAIMS)} claims: {n[Status.HOLDS]} hold on their checked domain, "
        f"{n[Status.COUNTEREXAMPLE]} have a counterexample, {n[Status.CITED]} "
        f"only cited, {n[Status.UNCHECKED]} are not independently evidenced.",
        "",
        "| Claim | Clause | Evidence | Status | Artifacts |",
        "|---|---|---|---|---|",
    ]
    for c in CLAIMS:
        kinds = "; ".join(k.value for k in c.kinds)
        arts = "<br>".join(_artifact(a) for a in c.artifacts) or "—"
        lines.append(f"| `{c.id}` | {c.clause} | {kinds} | {c.status.value} | {arts} |")
    lines += ["", "### Statements and checked domains", ""]
    for c in CLAIMS:
        lines.append(f"- **`{c.id}`** — {c.statement}")
        if c.domain:
            lines.append(f"  *Domain:* {c.domain}.")
        if c.note:
            lines.append(f"  *Note:* {c.note}.")
    lines += ["", "### Counterexamples", ""]
    open_ = [c for c in CLAIMS if c.status is Status.COUNTEREXAMPLE]
    for c in open_:
        lines.append(f"- **`{c.id}`** ({c.clause}): {c.note}.")
    if not open_:
        lines.append("None open.")
    lines += ["", "### Corrected after a counterexample", ""]
    lines += [
        f"- **`{c.id}`** ({c.clause}): {c.corrected}." for c in CLAIMS if c.corrected
    ]
    lines += ["", END]
    return "\n".join(lines) + "\n"


def committed() -> str:
    text = DOC.read_text(encoding="utf-8")
    start, end = text.index(BEGIN), text.index(END) + len(END)
    return text[start:end] + "\n"


def write() -> None:
    text = DOC.read_text(encoding="utf-8")
    start, end = text.index(BEGIN), text.index(END) + len(END)
    DOC.write_text(text[:start] + render().rstrip("\n") + text[end:], encoding="utf-8")


if __name__ == "__main__":
    if "--write" in sys.argv:
        write()
    else:
        sys.stdout.write(render())

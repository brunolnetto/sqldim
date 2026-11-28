"""CTL on finite traces and the empty-domain policy (ADR 0001, §4.1).

Expected values are the worked tables of ADR 0001 (Graph A, Graph B) and of
§4.1 (*AF and AG are distinct*, G1/G2); the remaining cases are derived from
the truth conditions and marked ``DERIVED``.
"""

from __future__ import annotations

from tests.conformance.cases import DERIVED, Case
from tests.conformance.oracle import REJECTED
from tests.conformance.cases.fixtures import (
    CHAIN,
    COMPLETE,
    NEXT_HOP,
    ONE_HOP,
    TWO_HOPS,
    PHI,
    REVENUE_GT_500,
)

FORMS = {
    "EF": ("EXISTS", "EVENTUALLY"),
    "EG": ("EXISTS", "GLOBALLY"),
    "EX": ("EXISTS", "NEXT"),
    "AF": ("FORALL", "EVENTUALLY"),
    "AG": ("FORALL", "GLOBALLY"),
    "AX": ("FORALL", "NEXT"),
}
_ORDER = ("EF", "EG", "EX", "AF", "AG", "AX")

ADR = "ADR 0001 Truth conditions; §4.1 Path semantics"
EMPTY = "ADR 0001 Empty-domain policy; §4.1 Empty domain"


def _table(prefix, fixture, pattern, phi, start_type, rows, note):
    out = []
    for start, policy, values in rows:
        for form, exp in zip(_ORDER, values):
            quant, mode = FORMS[form]
            empty = policy is not None
            out.append(
                Case(
                    id=f"{prefix}-{start}-{form}" + (f"-{policy[:3]}" if empty else ""),
                    area="ctl",
                    clause=EMPTY if empty and quant == "FORALL" else ADR,
                    rule=f"{form} at {start} ({note})",
                    fixture=fixture,
                    input={
                        "start": start,
                        "start_type": start_type,
                        "pattern": pattern,
                        "quantifier": quant,
                        "mode": mode,
                        "phi": phi,
                        "on_empty": policy or "REQUIRE_WITNESS",
                    },
                    expected=exp,
                )
            )
    return out


T, F = True, False

# ADR 0001 Graph A table.
GRAPH_A_CASES = _table(
    "CTL-A", "graph_a", ONE_HOP, REVENUE_GT_500, "Customer",
    [
        ("c", None, (T, T, T, F, F, F)),
        ("c0", "REQUIRE_WITNESS", (F, F, F, F, F, F)),
        ("c0", "VACUOUS", (F, F, F, T, T, T)),
    ],
    "terminal facts, φ = revenue > 500",
)  # fmt: skip

# ADR 0001 Graph B table.
GRAPH_B_CASES = _table(
    "CTL-B", "graph_b", CHAIN, COMPLETE, "Artifact",
    [
        ("a0", None, (T, F, F, T, F, F)),
        ("a1", None, (T, F, T, T, F, T)),
        ("a2", "REQUIRE_WITNESS", (T, T, F, T, T, F)),
        ("a2", "VACUOUS", (T, T, F, T, T, T)),
    ],
    "chain ending in a terminal, φ = state = Complete",
)  # fmt: skip


def _g(gid, fixture, form, exp, negate=False, rule=""):
    quant, mode = FORMS[form]
    return Case(
        id=f"CTL-{gid}-{'NOT-' if negate else ''}{form}",
        area="ctl",
        clause="§4.1 AF and AG are distinct; FORALL elimination",
        rule=rule,
        fixture=fixture,
        input={
            "start": "s0",
            "start_type": None,
            "pattern": CHAIN,
            "quantifier": quant,
            "mode": mode,
            "phi": ("not", PHI) if negate else PHI,
            "negate": negate,
            "on_empty": "VACUOUS",
        },
        expected=exp,
    )


# §4.1 G1/G2: AF φ, ¬EG ¬φ, AG φ, ¬EF ¬φ at s0.
AF_AG_CASES = [
    _g("G1", "graph_g1", "AF", T, rule="AF φ: the only path reaches φ"),
    _g("G1", "graph_g1", "EG", T, True, "¬EG ¬φ: no path stays in ¬φ"),
    _g("G1", "graph_g1", "AG", F, rule="AG φ: s0 violates φ"),
    _g("G1", "graph_g1", "EF", F, True, "¬EF ¬φ = AG φ, not AF φ"),
    _g("G2", "graph_g2", "AF", F, rule="AF φ: (s0 s2) never reaches φ"),
    _g("G2", "graph_g2", "EG", F, True, "¬EG ¬φ: (s0 s2) stays in ¬φ"),
    _g("G2", "graph_g2", "AG", F, rule="AG φ false"),
    _g("G2", "graph_g2", "EF", F, True, "¬EF ¬φ false"),
]


def _until(cid, start, quant, phi, psi, exp, rule):
    return Case(
        id=cid,
        area="ctl",
        clause="ADR 0001 Truth conditions (UNTIL is strong); §4.1",
        rule=rule,
        fixture="graph_b",
        input={
            "start": start,
            "start_type": "Artifact",
            "pattern": CHAIN,
            "quantifier": quant,
            "mode": ("UNTIL", psi),
            "phi": phi,
        },
        expected=exp,
    )


NOT_COMPLETE = ("not", COMPLETE)
MISSING = ("prop", "state", "=", "Missing")
FAILED = ("prop", "state", "=", "Failed")

UNTIL_CASES = [
    _until("CTL-U-01", "a0", "EXISTS", NOT_COMPLETE, COMPLETE, DERIVED,
           "E(¬Complete U Complete) on (a0 a1 a2)"),
    _until("CTL-U-02", "a0", "FORALL", NOT_COMPLETE, COMPLETE, DERIVED,
           "A(¬Complete U Complete) on the single maximal trace"),
    _until("CTL-U-03", "a0", "EXISTS", MISSING, COMPLETE, DERIVED,
           "E(Missing U Complete) fails: a1 is neither"),
    _until("CTL-U-04", "a0", "EXISTS", NOT_COMPLETE, FAILED, DERIVED,
           "strong UNTIL: ψ never occurs before the terminal"),
    _until("CTL-U-05", "a2", "EXISTS", MISSING, COMPLETE, DERIVED,
           "ψ at position 0 needs no φ before it"),
]  # fmt: skip

CASES = GRAPH_A_CASES + GRAPH_B_CASES + AF_AG_CASES + UNTIL_CASES


# ---------------------------------------------------------------------------
# The three decisions of #183 (§3.3, §4.1, ADR 0001).  Each expected value is
# worked out by hand from the definitions; the oracle must agree with it.
# ---------------------------------------------------------------------------

POS = "§4.1 evaluation position; ADR 0001 Definitions (path without a Repeat)"
STRAT = "§4.1 Temporal mode with a strategy; §3.3; ADR 0001"
PAST = "§4.1 Path-order modes and temporal modes (G^T); ADR 0001 Past-time modes"
IN_FLIGHT = ("prop", "state", "=", "In-flight")


def _d(cid, clause, rule, fixture, start, quant, mode, phi, exp, pattern=CHAIN,
       **extra):  # fmt: skip
    inp = {
        "start": start,
        "start_type": None,
        "pattern": pattern,
        "quantifier": quant,
        "mode": mode,
        "phi": phi,
    }
    inp.update(extra)
    return Case(cid, "ctl", clause, rule, fixture, inp, exp)


# Decision 2: on graph_b, TWO_HOPS from a0 has the one instance (a0 a1 a2);
# its trace is (a1 a2) although sub_filter reads only the alias of a2.
POSITION_CASES = [
    _d("CTL-POS-01", POS, "EF Complete: a2 is a position", "graph_b", "a0",
       "EXISTS", "EVENTUALLY", COMPLETE, T, TWO_HOPS),
    _d("CTL-POS-02", POS, "EG Complete fails at a1, a position with no alias read",
       "graph_b", "a0", "EXISTS", "GLOBALLY", COMPLETE, F, TWO_HOPS),
    _d("CTL-POS-03", POS, "EG ¬Missing over (a1 a2)", "graph_b", "a0",
       "EXISTS", "GLOBALLY", ("not", MISSING), T, TWO_HOPS),
    _d("CTL-POS-04", POS, "E(¬Complete U Complete) over (a1 a2)", "graph_b", "a0",
       "EXISTS", ("UNTIL", COMPLETE), NOT_COMPLETE, T, TWO_HOPS),
    _d("CTL-POS-05", POS, "E(Missing U Complete): a1 is neither", "graph_b", "a0",
       "EXISTS", ("UNTIL", COMPLETE), MISSING, F, TWO_HOPS),
    _d("CTL-POS-06", POS, "AG Complete fails at a1", "graph_b", "a0",
       "FORALL", "GLOBALLY", COMPLETE, F, TWO_HOPS),
    _d("CTL-POS-07", POS, "AF Complete on the one trace", "graph_b", "a0",
       "FORALL", "EVENTUALLY", COMPLETE, T, TWO_HOPS),
    _d("CTL-POS-08", POS, "EF In-flight: a1 is read though no alias of it is",
       "graph_b", "a0", "EXISTS", "EVENTUALLY", IN_FLIGHT, T, TWO_HOPS),
    _d("CTL-POS-09", POS, "no two-hop instance from a1: EF is false", "graph_b",
       "a1", "EXISTS", "EVENTUALLY", COMPLETE, F, TWO_HOPS),
    _d("CTL-POS-10", POS, "no two-hop instance from a1: AF false (REQUIRE_WITNESS)",
       "graph_b", "a1", "FORALL", "EVENTUALLY", COMPLETE, F, TWO_HOPS),
]  # fmt: skip

# Decision 3: on graph_g2, CHAIN from s0 has the instances (s0), (s0 s1) and
# (s0 s2) in ≺ order; φ holds at s1 only.  ONE hop from s0 has (s0 s1), (s0 s2).
STRATEGY_CASES = [
    _d("CTL-STR-01", STRAT, "SHORTEST selects (s0): EF φ is false", "graph_g2",
       "s0", "EXISTS", "EVENTUALLY", PHI, F, strategy=("SHORTEST",)),
    _d("CTL-STR-02", STRAT, "K_SHORTEST(2): the maximal selected trace is (s0 s1)",
       "graph_g2", "s0", "EXISTS", "EVENTUALLY", PHI, T, strategy=("K_SHORTEST", 2)),
    _d("CTL-STR-03", STRAT, "K_SHORTEST(2): AF φ holds (ALL: false)", "graph_g2",
       "s0", "FORALL", "EVENTUALLY", PHI, T, strategy=("K_SHORTEST", 2)),
    _d("CTL-STR-04", STRAT, "K_SHORTEST(3) selects all: AF φ fails on (s0 s2)",
       "graph_g2", "s0", "FORALL", "EVENTUALLY", PHI, F, strategy=("K_SHORTEST", 3)),
    _d("CTL-STR-05", STRAT, "SHORTEST: EG ¬φ on (s0)", "graph_g2", "s0",
       "EXISTS", "GLOBALLY", ("not", PHI), T, strategy=("SHORTEST",)),
    _d("CTL-STR-06", STRAT, "MIN_WEIGHT (unweighted) = SHORTEST: AG ¬φ on (s0)",
       "graph_g2", "s0", "FORALL", "GLOBALLY", ("not", PHI), T,
       strategy=("MIN_WEIGHT",)),
    _d("CTL-STR-07", STRAT, "K_SHORTEST(2): E(¬φ U φ) on (s0 s1)", "graph_g2",
       "s0", "EXISTS", ("UNTIL", PHI), ("not", PHI), T, strategy=("K_SHORTEST", 2)),
    _d("CTL-STR-08", STRAT, "SHORTEST over one hop selects (s0 s1): AX φ holds",
       "graph_g2", "s0", "FORALL", "NEXT", PHI, T, NEXT_HOP, strategy=("SHORTEST",)),
    _d("CTL-STR-09", STRAT, "K_SHORTEST(2) over one hop: AX φ fails at s2",
       "graph_g2", "s0", "FORALL", "NEXT", PHI, F, NEXT_HOP,
       strategy=("K_SHORTEST", 2)),
    _d("CTL-STR-10", STRAT, "SHORTEST over one hop: EF φ at s1", "graph_g2", "s0",
       "EXISTS", "EVENTUALLY", PHI, T, NEXT_HOP, strategy=("SHORTEST",)),
    _d("CTL-STR-11", STRAT, "SHORTEST picks s6 (300): EF revenue > 500 is false",
       "graph_a2", "c4", "EXISTS", "EVENTUALLY", REVENUE_GT_500, F, ONE_HOP,
       strategy=("SHORTEST",)),
]  # fmt: skip

# Decision 1: graph_b in G^T is a2 → a1 → a0; CHAIN from a2 has the maximal
# backward trace (a2 a1 a0).  ``time_ordered`` declares `next` TEMPORAL.
PAST_CASES = [
    _d("CTL-PAST-ONCE-01", PAST, "ONCE Missing: a0 on (a2 a1 a0)", "graph_b", "a2",
       "EXISTS", "ONCE", MISSING, T, time_ordered=True),
    _d("CTL-PAST-ONCE-02", PAST, "ONCE Complete at a0: T⁻(a0) = {(a0)}", "graph_b",
       "a0", "EXISTS", "ONCE", COMPLETE, F, time_ordered=True),
    _d("CTL-PAST-ONCE-03", PAST, "ONCE Complete at a2: the start is position 0",
       "graph_b", "a2", "EXISTS", "ONCE", COMPLETE, T, time_ordered=True),
    _d("CTL-PAST-ONCE-04", PAST, "ONCE over two hops: (a1 a0), the anchor is not "
       "a position", "graph_b", "a2", "EXISTS", "ONCE", COMPLETE, F, TWO_HOPS,
       time_ordered=True),
    _d("CTL-PAST-ONCE-05", PAST, "ONCE over two hops reaches a0", "graph_b", "a2",
       "EXISTS", "ONCE", MISSING, T, TWO_HOPS, time_ordered=True),
    _d("CTL-PAST-ONCE-06", PAST, "SHORTEST in G^T selects (a2): ONCE Missing fails",
       "graph_b", "a2", "EXISTS", "ONCE", MISSING, F, time_ordered=True,
       strategy=("SHORTEST",)),
    _d("CTL-PAST-ONCE-07", PAST, "K_SHORTEST(3) in G^T reaches a0", "graph_b", "a2",
       "EXISTS", "ONCE", MISSING, T, time_ordered=True, strategy=("K_SHORTEST", 3)),
    _d("CTL-PAST-PREV-01", PAST, "PREVIOUSLY In-flight: Succ⁻(a2) = {a1}",
       "graph_b", "a2", "EXISTS", "PREVIOUSLY", IN_FLIGHT, T, NEXT_HOP,
       time_ordered=True),
    _d("CTL-PAST-PREV-02", PAST, "PREVIOUSLY Missing at a2: a0 is two hops back",
       "graph_b", "a2", "EXISTS", "PREVIOUSLY", MISSING, F, NEXT_HOP,
       time_ordered=True),
    _d("CTL-PAST-PREV-03", PAST, "FORALL PREVIOUSLY In-flight at a2", "graph_b",
       "a2", "FORALL", "PREVIOUSLY", IN_FLIGHT, T, NEXT_HOP, time_ordered=True),
    _d("CTL-PAST-PREV-04", PAST, "FORALL PREVIOUSLY at a0: Succ⁻ = ∅ "
       "(REQUIRE_WITNESS)", "graph_b", "a0", "FORALL", "PREVIOUSLY", MISSING, F,
       NEXT_HOP, time_ordered=True),
    _d("CTL-PAST-PREV-05-VAC", PAST, "FORALL PREVIOUSLY at a0: Succ⁻ = ∅ (VACUOUS)",
       "graph_b", "a0", "FORALL", "PREVIOUSLY", MISSING, T, NEXT_HOP,
       time_ordered=True, on_empty="VACUOUS"),
    _d("CTL-PAST-PREV-06", PAST, "PREVIOUSLY over a leading Repeat reads its first "
       "body hop", "graph_b", "a1", "EXISTS", "PREVIOUSLY", MISSING, T,
       time_ordered=True),
    _d("CTL-PAST-SINCE-01", PAST, "E(¬Missing S Missing) on (a2 a1 a0)", "graph_b",
       "a2", "EXISTS", ("SINCE", MISSING), ("not", MISSING), T, time_ordered=True),
    _d("CTL-PAST-SINCE-02", PAST, "strong SINCE: no Complete on (a1 a0)", "graph_b",
       "a1", "EXISTS", ("SINCE", COMPLETE), ("not", MISSING), F, time_ordered=True),
    _d("CTL-PAST-SINCE-03", PAST, "E(Complete S Missing) fails at a1", "graph_b",
       "a2", "EXISTS", ("SINCE", MISSING), COMPLETE, F, time_ordered=True),
    _d("CTL-PAST-REJ-01", PAST, "ONCE over a bridge that is not time-ordered",
       "graph_b", "a2", "EXISTS", "ONCE", MISSING, REJECTED, time_ordered=False),
    _d("CTL-PAST-REJ-02", PAST, "FORALL ONCE stays rejected", "graph_b", "a2",
       "FORALL", "ONCE", MISSING, REJECTED, time_ordered=True),
]  # fmt: skip

CASES += POSITION_CASES + STRATEGY_CASES + PAST_CASES

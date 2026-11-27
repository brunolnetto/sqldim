"""CTL on finite traces and the empty-domain policy (ADR 0001, §4.1).

Expected values are the worked tables of ADR 0001 (Graph A, Graph B) and of
§4.1 (*AF and AG are distinct*, G1/G2); the remaining cases are derived from
the truth conditions and marked ``DERIVED``.
"""

from __future__ import annotations

from tests.conformance.cases import DERIVED, Case
from tests.conformance.cases.fixtures import (
    CHAIN,
    COMPLETE,
    ONE_HOP,
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

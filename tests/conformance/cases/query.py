"""PathPred, Repeat, strategies, bands and aggregation fan-out (§3.2–§4.2, §5.2)."""

from __future__ import annotations

from tests.conformance.cases import DERIVED, Case
from tests.conformance.cases.fixtures import ONE_HOP, REVENUE_GT_500
from tests.conformance.oracle import REJECTED

ACCEPTED = "ACCEPTED"
T, F = True, False

PP = "§4.1 PathPred; FORALL elimination with NotTrue; ADR 0001"


def _pp(cid, fixture, start, rule, exp=DERIVED, clause=PP, **inp):
    base = {
        "start": start,
        "start_type": "Customer",
        "pattern": ONE_HOP,
        "quantifier": "EXISTS",
        "mode": None,
        "phi": REVENUE_GT_500,
    }
    base.update(inp)
    return Case(cid, "pathpred", clause, rule, fixture, base, exp)


EMPTY = "§4.1 Empty domain; ADR 0001 Empty-domain policy"
STRAT = "§3.3 Strategy (deterministic ≺ order)"
ALIAS = "§3.5 Evaluation contexts (Refs in C ∪ L)"
PAST = "§4.1 FORALL with ONCE/SINCE; past-time modes need a time-ordered path"

PATHPRED = [
    # EXISTS / FORALL without a temporal mode, ADR 0001 Graph A.
    _pp("PP-EX-01", "graph_a", "c", "EXISTS: s1 is a witness", T),
    _pp("PP-EX-02", "graph_a", "c0", "EXISTS is false on an empty domain", F, EMPTY),
    _pp("PP-FA-01", "graph_a", "c", "FORALL: s2 = 300 violates", F, quantifier="FORALL"),
    _pp("PP-FA-02", "graph_a", "c0",
        "'all sales exceed 500' is false for a customer with no sales (REQUIRE_WITNESS)",
        F, EMPTY, quantifier="FORALL"),
    _pp("PP-FA-03", "graph_a", "c0", "VACUOUS makes FORALL true on an empty domain",
        T, EMPTY, quantifier="FORALL", on_empty="VACUOUS"),
    # Tuple-level NOT over a PathPred (two-valued atom).
    _pp("PP-NOT-01", "graph_a", "c0", "NOT EXISTS on an empty domain", T, negate=True),
    _pp("PP-NOT-02", "graph_a", "c", "NOT FORALL where a witness violates", T,
        quantifier="FORALL", negate=True),
    _pp("PP-NOT-03", "graph_a", "c0", "NOT FORALL on an empty domain (REQUIRE_WITNESS)",
        T, EMPTY, quantifier="FORALL", negate=True),
    # NULL measures: UNKNOWN is no witness, and counts against FORALL (NotTrue).
    _pp("PP-NULL-01", "graph_a2", "c2", "EXISTS ignores an UNKNOWN position"),
    _pp("PP-NULL-02", "graph_a2", "c2", "FORALL: an UNKNOWN position counts against",
        quantifier="FORALL"),
    _pp("PP-NULL-03", "graph_a2", "c3", "FORALL over only UNKNOWN positions",
        quantifier="FORALL"),
    _pp("PP-NULL-04", "graph_a2", "c3", "EXISTS over only UNKNOWN positions"),
    # Strategies select among the instances of one binding (§3.3).
    _pp("PP-STR-01", "graph_a2", "c4", "SHORTEST: the ≺-least instance (s6) only",
        clause=STRAT, strategy=("SHORTEST",)),
    _pp("PP-STR-02", "graph_a2", "c4", "ALL: any instance", clause=STRAT),
    _pp("PP-STR-03", "graph_a2", "c4", "K_SHORTEST(2) covers both instances",
        clause=STRAT, strategy=("K_SHORTEST", 2)),
    _pp("PP-STR-04", "graph_a2", "c4", "MIN_WEIGHT: unweighted, ties broken by key",
        clause=STRAT, strategy=("MIN_WEIGHT",)),
    # Refs of C inside sub_filter (alias binding, §3.5).
    _pp("PP-ALIAS-01", "graph_a2", "c2", "fs.revenue > c.threshold, c from C",
        clause=ALIAS, phi=("outer", "revenue", ">", "threshold")),
    _pp("PP-ALIAS-02", "graph_a2", "c3", "outer Ref compared with a NULL measure",
        clause=ALIAS, phi=("outer", "revenue", ">", "threshold")),
    _pp("PP-ALIAS-03", "graph_a2", "c4", "FORALL with an outer Ref",
        clause=ALIAS, quantifier="FORALL", phi=("outer", "revenue", ">", "threshold")),
    _pp("PP-ALIAS-04", "graph_a", "c", "path alias equal to an alias of C is rejected",
        REJECTED, "§3.2 Alias uniqueness (C ∩ L ≠ ∅ rejected)",
        aliases={"outer": ["c"], "path": ["c"]}),
    # Rejections at construction.
    _pp("PP-PROM-01", "graph_a", "c", "promote: true with ALL is rejected", REJECTED,
        "§3.5 L-promotion", promote=True),
    _pp("PP-PAST-01", "graph_a", "c", "ONCE over a verb hop (not time-ordered)",
        REJECTED, PAST, mode="ONCE"),
    _pp("PP-PAST-02", "graph_a", "c", "PREVIOUSLY over a verb hop", REJECTED, PAST,
        mode="PREVIOUSLY"),
    _pp("PP-PAST-03", "graph_a", "c", "FORALL with SINCE is rejected", REJECTED, PAST,
        quantifier="FORALL", mode=("SINCE", ("true",)), time_ordered=True),
    # UNTIL over a one-position trace: ψ must occur (strong UNTIL).
    _pp("PP-UNTIL-01", "graph_a", "c", "E(φ U ψ) with ψ = revenue > 1000 never holding",
        F, "ADR 0001 Truth conditions (UNTIL is strong)",
        mode=("UNTIL", ("prop", "revenue", ">", 1000))),
    _pp("PP-UNTIL-02", "graph_a", "c", "E(φ U ψ) with ψ = revenue < 400 at s2",
        T, "ADR 0001 Truth conditions (UNTIL is strong)",
        mode=("UNTIL", ("prop", "revenue", "<", 400))),
]  # fmt: skip


def _rep(cid, rule, lo, hi, mode, exp=DERIVED):
    return Case(
        cid,
        "repeat_join",
        "§3.2 Repetition and RepeatMode; PathJoin under ALL",
        rule,
        "graph_cycle",
        {"start": "x1", "pattern": (("repeat", "next", "fwd", lo, hi, mode),)},
        exp,
    )


REPEAT_JOIN = [
    _rep("RJ-01", "WALK 1..3 revisits the cycle: one tuple per instance", 1, 3, "WALK"),
    _rep("RJ-02", "WALK exactly 2 iterations", 2, 2, "WALK"),
    _rep("RJ-03", "WALK 1..1 is a single hop", 1, 1, "WALK"),
    _rep("RJ-04", "SIMPLE 1..UNBOUNDED stops before revisiting a node", 1, None, "SIMPLE"),
    _rep("RJ-05", "TRAIL 1..UNBOUNDED may revisit a node, not an edge", 1, None, "TRAIL"),
    _rep("RJ-06", "min = 0 includes the start node alone", 0, 2, "WALK"),
]  # fmt: skip


def _con(cid, kind, clause, rule, exp, **inp):
    return Case(cid, "construction", clause, rule, "none", {"kind": kind, **inp}, exp)


R = "§3.2 Repeat well-formedness"
B = "§4.1 Band table; §5.2 cross-band Ref rules"
N = "§4.1 NULL rules (NULL is not a literal)"

CONSTRUCTION = [
    _con("CON-R2-01", "repeat", R, "R2: min > max", REJECTED, min=2, max=1),
    _con("CON-R2-02", "repeat", R, "R2: max ≥ 1", REJECTED, min=0, max=0),
    _con("CON-R2-03", "repeat", R, "R2: min is a natural number", REJECTED, min=-1, max=2),
    _con("CON-R3-01", "repeat", R, "R3: no Repeat in the body", REJECTED, min=1, max=2,
         nested=True),
    _con("CON-R4-01", "repeat", R, "R4: WALK needs a finite max", REJECTED, min=1,
         max=None, mode="WALK"),
    _con("CON-R-01", "repeat", R, "0..UNBOUNDED SIMPLE is well-formed", ACCEPTED, min=0,
         max=None),
    _con("CON-R-02", "repeat", R, "exactly 2, WALK, is well-formed", ACCEPTED, min=2,
         max=2, mode="WALK"),
    _con("CON-B-01", "band", B, "AggRef in Where", REJECTED, band="WHERE", uses=["AggRef"]),
    _con("CON-B-02", "band", B, "WinRef in Where", REJECTED, band="WHERE", uses=["WinRef"]),
    _con("CON-B-03", "band", B, "PathPred in Having", REJECTED, band="HAVING",
         uses=["PathPred"]),
    _con("CON-B-04", "band", B, "PropRef in Having", REJECTED, band="HAVING",
         uses=["PropRef"]),
    _con("CON-B-05", "band", B, "WinRef in Having", REJECTED, band="HAVING", uses=["WinRef"]),
    _con("CON-B-06", "band", B, "AggRef in Qualify", REJECTED, band="QUALIFY",
         uses=["AggRef"]),
    _con("CON-B-07", "band", B, "PathPred in Qualify", REJECTED, band="QUALIFY",
         uses=["PathPred"]),
    _con("CON-B-08", "band", B, "PropRef and PathPred in Where", ACCEPTED, band="WHERE",
         uses=["PropRef", "PathPred"]),
    _con("CON-B-09", "band", B, "AggRef in Having", ACCEPTED, band="HAVING",
         uses=["AggRef"]),
    _con("CON-B-10", "band", B, "WinRef in Qualify", ACCEPTED, band="QUALIFY",
         uses=["WinRef"]),
    _con("CON-N-01", "null_literal", N, "Compare(e, =, NULL) is rejected", REJECTED,
         form="compare", value=None),
    _con("CON-N-02", "null_literal", N, "IsNull(e) is the NULL test", ACCEPTED,
         form="isnull", value=None),
]  # fmt: skip

FAN = "§4.2 What an aggregate counts (ENTITIES / PATHS)"


def _fan(cid, rule, fn, over, exp):
    return Case(
        cid,
        "fanout",
        FAN,
        rule,
        "two_campaign_paths",
        {"fn": fn, "alias": "s", "prop": "revenue", "group": "st", "over": over},
        exp,
    )


FANOUT = [
    _fan("AGG-OVER-01", "SUM(s.revenue) = 100: ENTITIES is the default", "SUM",
         "ENTITIES", [("T", 100)]),
    _fan("AGG-OVER-02", "SUM(s.revenue, over=PATHS) = 200", "SUM", "PATHS", [("T", 200)]),
    _fan("AGG-OVER-03", "COUNT(s) = 1 distinct sale", "COUNT", "ENTITIES", [("T", 1)]),
    _fan("AGG-OVER-04", "COUNT(s, over=PATHS) = 2", "COUNT", "PATHS", [("T", 2)]),
]  # fmt: skip

CASES = PATHPRED + REPEAT_JOIN + CONSTRUCTION + FANOUT

"""Declared status of each (case, backend) pair that does not simply pass.

Every entry is checked by ``test_backends.py``: an XFAIL is a strict xfail
(it fails the moment the implementation conforms), a REFUSED case must be
rejected by the implementation, and a NOT_EXPRESSIBLE case must raise
``NotExpressible`` in the runner.  ``test_matrix.py`` checks that every entry
still names a case in the backend's scope.  Keys are ``(case id glob, backend)``.

The oracle is never changed to match the implementation: a disagreement is
recorded here and listed in ``docs/reference/dgm-conformance.md``.
"""

from __future__ import annotations

from enum import Enum
from fnmatch import fnmatch

from tests.conformance.cases import Case
from tests.conformance.runners import BACKENDS


class Status(Enum):
    PASS = 1
    XFAIL = 2
    REFUSED = 3
    NOT_EXPRESSIBLE = 4


X, R, N = Status.XFAIL, Status.REFUSED, Status.NOT_EXPRESSIBLE
LEG, IR = "duckdb-dgmquery", "duckdb-plan-ir"
ORD, BDD = "duckdb-ordering", "dgm-bdd"

_NO_ON_EMPTY = "PathPred has no `on_empty` field, so VACUOUS cannot be requested"
_IR_MODES = (
    "the plan IR raises UnsupportedConstruct for every temporal mode "
    "(fail-closed; no SQL template)"
)
_IR_REPEAT = "a Repeat inside a PathPred is not emitted (UnsupportedConstruct)"
_LEG_REPEAT = "DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner)"
_IR_STRATEGY = "the plan IR executes only strategy ALL (UnsupportedConstruct)"
_LEG_MODE_FORALL = (
    "to_sql() ignores the quantifier when a temporal mode is set: FORALL renders as "
    "the EXISTS template, so AF/AG/AX at c read 'some sale > 500' (true); "
    "expected false (s2 = 300)"
)
_AS_WAS = (
    "no AS_WAS: join_dim(...).as_of(τ) reads version(T, key, τ) for every fact; §2.5 "
    "makes AS_WAS the default (South 600, North 400 expected; got North 1 000)"
)
_DRILL = (
    "a two-branch Join is evaluated row to row, not by drill-across: C1 gives 6 sales, "
    "900 revenue, 6 returns (expected 2, 300, 3); 1/0 yields inf, not NULL (§4.1)"
)
_OVER = (
    "aggregates count path fan-out: no `over`, so the ENTITIES default is not "
    "implemented (got the PATHS value)"
)
_ALLEN_OPEN = (
    "a NULL bound is UNKNOWN in the emitted comparison instead of −∞/+∞ (§3.2), so "
    "the relation never holds on an open interval"
)
_ALLEN_DEGENERATE = (
    "start ≥ end is not checked: the relation holds on degenerate intervals, which "
    "satisfy no Allen relation (§3.2)"
)
_NO_PRECEDES = "sqldim has no PRECEDES / PRECEDED_BY TemporalOrdering (§3.2 has 13)"
_BDD_ID = (
    "atoms are BDD variables by Python object identity, not by canonical form "
    "(§6.1 Variables), and comparisons are not normalised"
)
_BDD_ORDER = "no order theory Δ_ord: atoms over one expression are unrelated"
_BDD_NULL = (
    "the BDD is two-valued over every atom: no p⁺/p⁻ encoding for nullable atoms, "
    "no Δ_null"
)
_BDD_ELIM = (
    "FORALL is eliminated as NOT EXISTS(mode, NOT φ): the temporal mode is not "
    "dualised, NOT is Kleene NOT instead of NotTrue, and the REQUIRE_WITNESS "
    "guard is missing"
)

EXPECTATIONS: dict[tuple[str, str], tuple[Status, str]] = {}


def _add(status: Status, why: str, backends: tuple[str, ...], *patterns: str) -> None:
    for b in backends:
        for p in patterns:
            EXPECTATIONS[(p, b)] = (status, why)


BOTH = (LEG, IR)

# --- CTL and PathPred -------------------------------------------------------
_add(N, _NO_ON_EMPTY, BOTH, "CTL-*-VAC", "CTL-G1-*", "CTL-G2-*", "PP-FA-03")
_add(X, _LEG_MODE_FORALL, (LEG,), "CTL-A-c-AF", "CTL-A-c-AG", "CTL-A-c-AX")
_add(R, _IR_MODES, (IR,), "CTL-A-c-*", "CTL-A-c0-*-REQ", "PP-UNTIL-*")
_add(R, _IR_REPEAT, (IR,), "CTL-B-a?-??", "CTL-B-a2-*-REQ", "CTL-U-*")
_add(R, _LEG_REPEAT, (LEG,), "CTL-B-a?-??", "CTL-B-a2-*-REQ", "CTL-U-*", "RJ-*")
_add(R, _IR_STRATEGY, (IR,), "PP-STR-01", "PP-STR-03", "PP-STR-04")
_add(X, "to_sql() ignores the strategy: SHORTEST/MIN_WEIGHT read every instance "
        "(s7 = 600 found; the ≺-least instance s6 = 300 expected)",
     (LEG,), "PP-STR-01", "PP-STR-04")  # fmt: skip
_add(X, "UNTIL(ψ) renders as EXISTS φ: ψ is never evaluated (strong UNTIL expected "
        "false when ψ never holds)", (LEG,), "PP-UNTIL-01")  # fmt: skip
_add(X, "a path alias equal to an alias of C is accepted and shadows it "
        "(rejected at construction by §3.2)", (LEG,), "PP-ALIAS-04")  # fmt: skip
_add(X, "promote: true with ALL is accepted (§3.5 rejects it)", (LEG,), "PP-PROM-01")
_add(X, "past-time modes are accepted on a non-time-ordered path, and FORALL with "
        "SINCE is accepted (§4.1 rejects both)", (LEG,), "PP-PAST-*")  # fmt: skip
_add(R, "rejected only because the plan IR refuses every temporal mode, not by the "
        "§4.1 rule", (IR,), "PP-PAST-*")  # fmt: skip
_add(R, "rejected only because the plan IR refuses promote=True for every "
        "strategy, not by the §3.5 rule", (IR,), "PP-PROM-01")  # fmt: skip
_add(R, "the plan IR emits only WALK with 1 ≤ min (SIMPLE/TRAIL need node "
        "identities; min = 0 not emitted)", (IR,), "RJ-04", "RJ-05", "RJ-06")  # fmt: skip

# --- Construction rules -----------------------------------------------------
_add(X, "Compare(e, =, NULL) is accepted and emitted as `= NULL` (§4.1: NULL is not "
        "a literal)", BOTH, "CON-N-01")  # fmt: skip

# --- Aggregation ------------------------------------------------------------
_add(X, _OVER, BOTH, "AGG-OVER-01", "AGG-OVER-03")
_add(X, _DRILL, BOTH, "DA-01", "DA-02")

# --- SCD and validity -------------------------------------------------------
_add(X, _AS_WAS, BOTH, "SCD-01", "SCD-05")
_add(X, "VERSION_KEY joined under as_of reads only the stored version valid at τ: "
        "S1 (version 101) loses its region under AS_WAS and AS_IS", BOTH,
     "SCD-03", "SCD-04")  # fmt: skip
_add(X, "a missing version leaves the tuple with NULL attributes (LEFT JOIN): LAX "
        "should drop it; TemporalContext is ignored by to_sql()", (LEG,), "SCD-V-01")  # fmt: skip
_add(X, "STRICT does not reject a missing version: TemporalContext is ignored by "
        "to_sql() (got the tuple with a NULL segment)", (LEG,), "SCD-V-02")  # fmt: skip
_add(R, "the plan IR refuses TemporalContext (node_resolution cannot be stated)",
     (IR,), "SCD-V-01", "SCD-TAU-01")  # fmt: skip
_add(R, "rejected only because the plan IR refuses TemporalContext, not because the "
        "version is missing", (IR,), "SCD-V-02")  # fmt: skip
_add(X, "join_dim compares valid_from/valid_to after ::DATE casts: a version that "
        "starts later on the same day is read at τ (wholesale instead of retail)",
     BOTH, "SCD-V-06")  # fmt: skip
_add(X, "a NULL valid_from is treated as UNKNOWN (`valid_from <= τ`), not as open "
        "(§2.6 valid(e, t)): E9 is missing", BOTH, "SCD-E-06")  # fmt: skip
_add(X, "TemporalContext.default_as_of is ignored by to_sql(): validity is not "
        "resolved at τ, so the expired edge E7 → M1 is followed", (LEG,),
     "SCD-TAU-01")  # fmt: skip

# --- TemporalOrdering -------------------------------------------------------
_add(N, _NO_PRECEDES, (ORD,), "TO-PRECEDES-*", "TO-PRECEDED_BY-*", "TO-THM-*")
_OPEN_HELD = ("OVERLAPS", "OVERLAPPED_BY", "STARTS", "STARTED_BY", "DURING",
              "CONTAINS", "FINISHES", "FINISHED_BY", "EQUALS")  # fmt: skip
_add(X, _ALLEN_OPEN, (ORD,), *(f"TO-{r}-open" for r in _OPEN_HELD))
_DEGENERATE_HELD = ("MEETS", "MET_BY", "STARTS", "STARTED_BY", "DURING", "CONTAINS",
                    "FINISHES", "FINISHED_BY", "EQUALS")  # fmt: skip
_add(X, _ALLEN_DEGENERATE, (ORD,), *(f"TO-{r}-degenerate" for r in _DEGENERATE_HELD))
_add(X, "TemporalOrdering constructors take untyped SQL text and check nothing "
        "(TO1–TO3 are construction-time rejections)", (ORD,),
     "TO-TYPE-01", "TO-TYPE-02", "TO-TYPE-03")  # fmt: skip
_add(N, "arguments are untyped SQL text: Date and Timestamp cannot be told apart",
     (ORD,), "TO-TYPE-04")  # fmt: skip

# --- BDD --------------------------------------------------------------------
_add(X, _BDD_NULL, (BDD,), "BDD-K-01", "BDD-K-06", "BDD-K-08")
_add(N, "sqldim has no NotTrue filter node", (BDD,), "BDD-K-05")
_add(X, _BDD_ORDER, (BDD,), "BDD-O-01", "BDD-O-02", "BDD-O-06", "BDD-O-07", "BDD-O-08")
_add(X, _BDD_ID, (BDD,), "BDD-C-*")
_add(X, _BDD_ELIM, (BDD,), "BDD-F-*")

# --- Model, anchor sets, annotations ----------------------------------------
_add(X, "build_space keeps in Σ|_𝒜 only the kinds the enumeration reads "
        "(Conformed, Grain, FactlessFact, MedallionLayer, BridgeSemantics); "
        "SCDType(Customer) is dropped although Customer ∈ N_𝒜", ("dgm-model",),
     "AS-06")  # fmt: skip
_add(X, "Grain.sum_invalid is true for every SUM over a PERIOD fact, without the "
        "AD2 exceptions (GroupBy period, pinned period, LATEST/EARLIEST)",
     ("dgm-annotations",), "ADD-02", "ADD-03", "ADD-11", "ADD-12")  # fmt: skip
_add(N, "sqldim has no Additivity annotation; Grain decides SUM only",
     ("dgm-annotations",), "ADD-04", "ADD-06", "ADD-07", "ADD-08", "ADD-09", "ADD-10")  # fmt: skip

NOT_COVERED: list[tuple[str, str]] = [
    (
        "§4.1 Monitor verdicts",
        "sqldim has no Monitor construct; the cases `MON-*` "
        "run against the oracle only",
    ),
    (
        "§2.4 Additivity annotation, AD3/AD4",
        "sqldim has no Additivity annotation; "
        "only the grain default for SUM is checked (`dgm-annotations`)",
    ),
    (
        "§4.2 LATEST / EARLIEST",
        "sqldim has no LATEST or EARLIEST TemporalWindow; "
        "`TAGG-*` run against the oracle only",
    ),
    (
        "§5.2.2 DA1–DA3",
        "constellation well-formedness is enforced only inside the "
        "question-lattice enumeration (`lattice/_context.py`), which has no query to "
        "submit; `DA-WF-*` run against the oracle only",
    ),
    (
        "ADR 0001 infinite traces (lasso)",
        "the oracle does not model infinite traces "
        "and no backend executes a cyclic temporal PathPred; no case is written",
    ),
    (
        "§4.1 SINCE / ONCE / PREVIOUSLY evaluation",
        "only their construction-time "
        "rejections are covered; no backend evaluates a past-time mode",
    ),
    (
        "§4.1 typing rules of ScalarPred (Int/String/Enum mixes)",
        "PropRef and "
        "ScalarPred carry no property types; a typing error cannot be posed",
    ),
    (
        "§5.1 Q_delta, §2.5 SCD3/SCD6, TRAILING/ROLLING/YTD at τ",
        "DeltaQuery and the "
        "windows render placeholders only; nothing is executable to compare",
    ),
    (
        "§3.3 K_SHORTEST / MIN_WEIGHT over weighted bridges",
        "covered by the oracle on "
        "unweighted hops only; no backend executes a non-ALL strategy",
    ),
    (
        "§8.7 cone intersection theorem",
        "deferred to #139; under the simple-path "
        "reading of §3.4.2 the identity needs a proof of its own (see the doc)",
    ),
]


def _lookup(case: Case, backend_name: str) -> tuple[Status, str] | None:
    hits = [
        v
        for (pattern, b), v in EXPECTATIONS.items()
        if b == backend_name and fnmatch(case.id, pattern)
    ]
    return hits[0] if hits else None  # the first declared entry wins


def status(case: Case, backend_name: str) -> Status | None:
    """The declared status; ``None`` when the backend does not cover the area."""
    b = next(b for b in BACKENDS if b.name == backend_name)
    if case.area not in b.areas:
        return None
    hit = _lookup(case, backend_name)
    return hit[0] if hit else Status.PASS


def reason(case: Case, backend_name: str) -> str | None:
    hit = _lookup(case, backend_name)
    return hit[1] if hit else None

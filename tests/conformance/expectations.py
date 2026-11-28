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
ORD = "duckdb-ordering"

_NO_ON_EMPTY = "PathPred has no `on_empty` field, so VACUOUS cannot be requested"
_LEG_REPEAT = (
    "DGMQuery.to_sql() reads no model: a Repeat PathJoin and a temporal mode "
    "over a Repeat's iterations need node identities, so it raises (plan with "
    "DGMPlanner(model=…))"
)
_LEG_STRATEGY = (
    "DGMQuery.to_sql() renders only strategy ALL (UnsupportedConstruct): ≺ "
    "(§3.3) breaks ties by entity keys, and to_sql() reads no model"
)
_LEG_CONTEXT = (
    "DGMQuery.to_sql() raises UnsupportedConstruct for a TemporalContext "
    "(fail-closed, as the plan IR)"
)
_LEG_PAST = (
    "DGMQuery.to_sql() reads no model: whether a bridge is time-ordered (§2.3) "
    "is its BridgeSemantics, so a past-time mode on G^T is refused (plan with "
    "DGMPlanner(model=…))"
)
EXPECTATIONS: dict[tuple[str, str], tuple[Status, str]] = {}


def _add(status: Status, why: str, backends: tuple[str, ...], *patterns: str) -> None:
    for b in backends:
        for p in patterns:
            EXPECTATIONS[(p, b)] = (status, why)


BOTH = (LEG, IR)

# --- CTL and PathPred -------------------------------------------------------
_add(N, _NO_ON_EMPTY, BOTH, "CTL-*-VAC", "CTL-G1-*", "CTL-G2-*", "PP-FA-03")
# NEXT reads Succ(s), the first hop's targets: to_sql() renders EX and AX over
# a Repeat as the PathPred over its first body hop, so only F, G and U remain
_add(R, _LEG_REPEAT, (LEG,), "CTL-B-a?-[EA][FG]", "CTL-B-a2-[EA][FG]-REQ",
     "CTL-U-*", "RJ-*")  # fmt: skip
_add(R, _LEG_STRATEGY, (LEG,), "PP-STR-01", "PP-STR-03", "PP-STR-04", "CTL-STR-*")
_add(R, _LEG_PAST, (LEG,), "CTL-PAST-ONCE-*", "CTL-PAST-PREV-*", "CTL-PAST-SINCE-*")

# --- SCD and validity -------------------------------------------------------
_add(R, _LEG_CONTEXT, (LEG,), "SCD-V-01", "SCD-TAU-01")
_add(R, "rejected only because DGMQuery.to_sql() refuses TemporalContext, not "
        "because the version is missing", (LEG,), "SCD-V-02")  # fmt: skip
_add(R, "the plan IR refuses TemporalContext (node_resolution cannot be stated)",
     (IR,), "SCD-V-01", "SCD-TAU-01")  # fmt: skip
_add(R, "rejected only because the plan IR refuses TemporalContext, not because the "
        "version is missing", (IR,), "SCD-V-02")  # fmt: skip

# --- TemporalOrdering -------------------------------------------------------
_add(N, "arguments are untyped SQL text: Date and Timestamp cannot be told apart",
     (ORD,), "TO-TYPE-04")  # fmt: skip

# --- Model, anchor sets, annotations ----------------------------------------
_SEMI = (
    "the question lattice does not enumerate SUM over a SEMI_ADDITIVE measure: "
    "AD2 decides it by the period of the fact or the entity of a dimension, "
    "which the declaration does not identify (Grain names no period property; "
    "§12.3)"
)
_add(R, _SEMI, ("dgm-annotations",), "ADD-10")
_add(R, _SEMI + "; rejected for that reason, not by AD2", ("dgm-annotations",),
     "ADD-09")  # fmt: skip
_add(R, "the question lattice does not offer AVG over a NON_ADDITIVE measure: AD3 "
        "accepts it only with a warning, and a skeleton of the enumeration has no "
        "field to carry one (§12.3)", ("dgm-annotations",), "ADD-07")  # fmt: skip

# --- Per-backend matrix (#138) -----------------------------------------------
PG, REF, WP = "pg-duckdb-plan-ir", "planner-refusals", "dgm-write-plan"

# PG(DD) executes the SQL of the plan IR: the IR's declarations carry over
for (_pattern, _b), _v in list(EXPECTATIONS.items()):
    if _b == IR:
        EXPECTATIONS[(_pattern, PG)] = _v
_add(R, "POSTGRESQL_VIA_DUCKDB declares window_aggregate unsupported: DuckDB "
        "1.5.0's postgres extension raises NotImplementedException "
        "(PostgresScanSerialize) for a SUM window over a PostgreSQL table, so "
        "DGMPlanner(execution_profile=POSTGRESQL_VIA_DUCKDB) raises "
        "UnsupportedCapability before execution", (PG,), "WIN-02", "WIN-03")  # fmt: skip

_add(N, _NO_ON_EMPTY, (REF,), "CTL-*-VAC", "CTL-G1-*", "CTL-G2-*", "PP-FA-03")
_add(N, "the query is rejected while it is built (§3.5, §4.1), before planning: "
        "it says nothing about the targets", (REF,), "PP-PROM-01", "PP-PAST-*",
     "CTL-PAST-REJ-02")  # fmt: skip
_add(R, "every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, "
        "DGM_JSON, DGM_YAML) raises UnsupportedCapability", (REF,), "*")  # fmt: skip

_DELTA_APPEND_ONLY = (
    "§6.2 Delta writes: DuckDB's delta extension rejects UPDATE, DELETE and "
    "MERGE INTO, so DeltaLakeSink.close_versions() raises SinkUnsupported and "
    "sends nothing"
)
_add(R, _DELTA_APPEND_ONLY, ("sink-delta",), "SINK-CL-*", "SINK-SQL-C")

NOT_COVERED: list[tuple[str, str]] = [
    (
        "§4.1 Monitor verdicts",
        "sqldim has no Monitor construct; the cases `MON-*` "
        "run against the oracle only",
    ),
    (
        "§2.4 AD2 over a declared SEMI_ADDITIVE axis",
        "no implementation decides it: `Grain.rejects_sum` covers the grain "
        "default only, and the question lattice enumerates COUNT, MIN, MAX and "
        "AVG over a SEMI_ADDITIVE measure but never its SUM (`ADD-09`, `ADD-10` "
        "are refusals)",
    ),
    (
        "§4.2 LATEST / EARLIEST",
        "sqldim has no LATEST or EARLIEST TemporalWindow; "
        "`TAGG-*` run against the oracle only",
    ),
    (
        "ADR 0001 infinite traces (lasso)",
        "the oracle does not model infinite traces "
        "and no backend executes a cyclic temporal PathPred; no case is written",
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
        "§3.4.2 RelationshipSubgraph cones; §8.7 cone intersection",
        "sqldim represents REACHABLE_FROM / REACHABLE_TO / REACHABLE_BETWEEN "
        "symbolically and computes no cone; `CONE-*` run against the oracle only, "
        "and `docs/reference/dgm-claims.md` checks the identity exhaustively",
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

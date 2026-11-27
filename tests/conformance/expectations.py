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
_IR_REPEAT = (
    "a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace "
    "has several evaluation positions, and SIMPLE needs node identities a hop "
    "does not declare"
)
_LEG_REPEAT = "DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner)"
_IR_STRATEGY = (
    "the plan IR executes only strategy ALL (UnsupportedConstruct): ≺ (§3.3) "
    "breaks ties by node keys a hop does not declare"
)
_LEG_STRATEGY = (
    "DGMQuery.to_sql() renders only strategy ALL (UnsupportedConstruct): ≺ "
    "(§3.3) breaks ties by node keys a hop does not declare"
)
_LEG_CONTEXT = (
    "DGMQuery.to_sql() raises UnsupportedConstruct for a TemporalContext "
    "(fail-closed, as the plan IR)"
)
_DRILL_IR = (
    "the plan IR raises UnsupportedConstruct for a declared constellation: it has "
    "no node for per-branch aggregation aligned by FULL OUTER JOIN (fail-closed)"
)
EXPECTATIONS: dict[tuple[str, str], tuple[Status, str]] = {}


def _add(status: Status, why: str, backends: tuple[str, ...], *patterns: str) -> None:
    for b in backends:
        for p in patterns:
            EXPECTATIONS[(p, b)] = (status, why)


BOTH = (LEG, IR)

# --- CTL and PathPred -------------------------------------------------------
_add(N, _NO_ON_EMPTY, BOTH, "CTL-*-VAC", "CTL-G1-*", "CTL-G2-*", "PP-FA-03")
_add(R, _IR_REPEAT, (IR,), "CTL-B-a?-??", "CTL-B-a2-*-REQ", "CTL-U-*")
_add(R, _LEG_REPEAT, (LEG,), "CTL-B-a?-??", "CTL-B-a2-*-REQ", "CTL-U-*", "RJ-*")
_add(R, _IR_STRATEGY, (IR,), "PP-STR-01", "PP-STR-03", "PP-STR-04")
_add(R, _LEG_STRATEGY, (LEG,), "PP-STR-01", "PP-STR-03", "PP-STR-04")
_add(R, "the plan IR emits only WALK with 1 ≤ min (SIMPLE/TRAIL need node "
        "identities; min = 0 not emitted)", (IR,), "RJ-04", "RJ-05", "RJ-06")  # fmt: skip

# --- Aggregation ------------------------------------------------------------
_add(R, _DRILL_IR, (IR,), "DA-01", "DA-02")

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
    "the question lattice raises LatticeUnsupported for a SEMI_ADDITIVE measure: "
    "AD2 decides its SUM by the period or entity of each axis, which the "
    "enumeration does not decide (fail-closed)"
)
_add(R, _SEMI, ("dgm-annotations",), "ADD-04", "ADD-10")
_add(R, _SEMI + "; rejected for that reason, not by AD2", ("dgm-annotations",),
     "ADD-09")  # fmt: skip
_add(R, "the question lattice does not offer AVG over a NON_ADDITIVE measure: AD3 "
        "accepts it only with a warning, and the enumeration offers no aggregate "
        "that needs one", ("dgm-annotations",), "ADD-07")  # fmt: skip

# --- Per-backend matrix (#138) -----------------------------------------------
PG, REF, WP = "pg-duckdb-plan-ir", "planner-refusals", "dgm-write-plan"

# PG(DD) executes the SQL of the plan IR: the IR's declarations carry over
for (_pattern, _b), _v in list(EXPECTATIONS.items()):
    if _b == IR:
        EXPECTATIONS[(_pattern, PG)] = _v
# construction only: not in the scope of PG(DD), which executes queries
_add(R, _DRILL_IR, (IR,), "DA-WF-04")
_add(X, "DuckDB 1.5.0's postgres extension raises NotImplementedException "
        "(PostgresScanSerialize is not serializable) for a SUM window over a "
        "PostgreSQL table; the same SQL passes on DuckDB storage", (PG,),
     "WIN-02", "WIN-03")  # fmt: skip

_add(N, _NO_ON_EMPTY, (REF,), "CTL-*-VAC", "CTL-G1-*", "CTL-G2-*", "PP-FA-03")
_add(N, "the query is rejected while it is built (§3.5, §4.1), before planning: "
        "it says nothing about the targets", (REF,), "PP-PROM-01", "PP-PAST-*")  # fmt: skip
_add(R, "every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, "
        "DGM_JSON, DGM_YAML) raises UnsupportedCapability", (REF,), "*")  # fmt: skip

_add(X, "Rule 8 plans `INSERT INTO delta_scan`; §6.2 Delta writes: delta_scan is "
        "read-only, the write goes to a Delta table attached with TYPE delta",
     (WP,), "WP-DELTA-target")  # fmt: skip
_add(X, "Rule 8 plans `INSERT INTO iceberg_scan`; §6.2 Iceberg writes: "
        "iceberg_scan never appears as a write target", (WP,), "WP-ICEBERG-target")  # fmt: skip
_add(X, "SinkTarget.ICEBERG carries no catalog, so Rule 8 cannot check that a "
        "REST catalog is attached and plans the write anyway (§6.2 Iceberg "
        "writes: rejected at planning time)", (WP,), "WP-ICEBERG-catalog")  # fmt: skip
_add(X, "Rule 10 plans a MERGE into ICEBERG without the engine version or the "
        "table's merge-on-read mode; §6.2 rejects every Iceberg plan but APPEND "
        "on an engine version the capability table does not cover", (WP,),
     "WP-ICEBERG-merge")  # fmt: skip

_PQ_BOOL = (
    "is_current, ParquetSink's default hive partition column, is read back as "
    "VARCHAR ('true'/'false'), not as the BOOLEAN written"
)
_add(X, _PQ_BOOL + "; and a second write() replaces the files of every partition "
        "it writes (COPY … OVERWRITE_OR_IGNORE), so the first write's rows there "
        "are lost", ("sink-parquet",), "SINK-RT-02")  # fmt: skip
_add(X, _PQ_BOOL + "; and close_versions() rewrites only the current rows into "
        "the partitions, overwriting the historical versions, which are lost",
     ("sink-parquet",), "SINK-CL-01", "SINK-CL-02")  # fmt: skip
_add(X, _PQ_BOOL + "; and when every current row is closed nothing is written to "
        "is_current=true, so the old files stay and each key is read back both "
        "open and closed", ("sink-parquet",), "SINK-CL-03")  # fmt: skip
_add(X, _PQ_BOOL, ("sink-parquet",), "SINK-RT-01", "SINK-RT-03", "SINK-CL-04")
_add(X, "DeltaLakeSink.write() sends `MERGE INTO delta.`<path>``, Spark SQL that "
        "DuckDB's parser rejects (and §6.2 allows a Delta write to APPEND only)",
     ("sink-delta",), "SINK-SQL-W")  # fmt: skip
_add(X, "DeltaLakeSink.close_versions() is a no-op: it sends no statement and "
        "returns 0, so no version is closed", ("sink-delta",), "SINK-SQL-C")  # fmt: skip

NOT_COVERED: list[tuple[str, str]] = [
    (
        "§4.1 Monitor verdicts",
        "sqldim has no Monitor construct; the cases `MON-*` "
        "run against the oracle only",
    ),
    (
        "§2.4 AD2 over a declared SEMI_ADDITIVE axis",
        "no implementation decides it: `Grain.rejects_sum` covers the grain "
        "default only, and the question lattice refuses every SEMI_ADDITIVE "
        "measure (`ADD-04`, `ADD-09`, `ADD-10` are refusals)",
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

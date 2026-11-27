"""Per-backend capability matrix (#138): backend × capability × status, with evidence.

Each row is one way sqldim runs a query or writes a result: a query target,
an execution path, or a sink.  Its declaration comes from
``sqldim.core.query.dgm.planner._capabilities`` and its evidence from the
conformance backends that exercise it.  A cell counts the cases of that
capability by their status (``tests/conformance/expectations.py``); a
capability with no case that ran says so and is never counted as passing.

The matrix is kept in ``docs/reference/dgm-backend-matrix.md`` between two
markers; ``test_backend_matrix.py`` fails when it is out of date::

    PYTHONPATH=. python -m tests.conformance.backend_matrix --write
"""

from __future__ import annotations

import sys
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from fnmatch import fnmatch
from pathlib import Path

from sqldim.core.query.dgm.planner import (
    DGMQUERY_TO_SQL,
    POSTGRESQL_VIA_DUCKDB,
    QUERY_PROFILES,
    SINK_PROFILES,
    Capability,
    Profile,
    QueryTarget,
    SinkTarget,
    Support,
)
from tests.conformance.cases import Case, all_cases
from tests.conformance.expectations import Status, status
from tests.conformance.runners import BACKENDS

DOC = (
    Path(__file__).resolve().parents[2] / "docs" / "reference" / "dgm-backend-matrix.md"
)
BEGIN = "<!-- BEGIN GENERATED: tests/conformance/backend_matrix.py -->"
END = "<!-- END GENERATED -->"

PG_ONLY = (
    "yes, against the `postgres:19` service container (`SQLDIM_PG_HOST`); "
    "skipped locally without a server"
)


@dataclass(frozen=True)
class Row:
    label: str
    profile: Profile
    evidence: tuple[str, ...]
    executed: str
    sink: str | None = None  # the SinkTarget name the write-plan cases ask about


def _q(target: QueryTarget) -> Profile:
    return QUERY_PROFILES[target]


def _s(sink: SinkTarget) -> Profile:
    return SINK_PROFILES[sink]


_NO_EMITTER = "yes — the refusal of every query is executed"

QUERY_ROWS: tuple[Row, ...] = (
    Row("DuckDB, plan IR", _q(QueryTarget.SQL_DUCKDB), ("duckdb-plan-ir",), "yes"),
    Row("DuckDB, `DGMQuery.to_sql()`", DGMQUERY_TO_SQL, ("duckdb-dgmquery",), "yes"),
    Row("PostgreSQL via DuckDB (PG(DD))", POSTGRESQL_VIA_DUCKDB,
        ("pg-duckdb-plan-ir",), PG_ONLY),
    Row("MotherDuck", _q(QueryTarget.SQL_MOTHERDUCK), (),
        "**not executed** — needs a MotherDuck account and `MOTHERDUCK_TOKEN`; "
        "running its SQL on local DuckDB would be DuckDB evidence, not MotherDuck"),
    Row("PostgreSQL, native SQL", _q(QueryTarget.SQL_POSTGRESQL),
        ("planner-refusals",), _NO_EMITTER),
    Row("Cypher", _q(QueryTarget.CYPHER), ("planner-refusals",), _NO_EMITTER),
    Row("SPARQL", _q(QueryTarget.SPARQL), ("planner-refusals",), _NO_EMITTER),
    Row("DGM_JSON", _q(QueryTarget.DGM_JSON), ("planner-refusals",), _NO_EMITTER),
    Row("DGM_YAML", _q(QueryTarget.DGM_YAML), ("planner-refusals",), _NO_EMITTER),
)  # fmt: skip

SINK_ROWS: tuple[Row, ...] = (
    Row("DuckDB", _s(SinkTarget.DUCKDB), ("sink-duckdb", "dgm-write-plan"),
        "yes — temporary file", "DUCKDB"),
    Row("PostgreSQL", _s(SinkTarget.POSTGRESQL),
        ("sink-postgresql", "dgm-write-plan"), PG_ONLY, "POSTGRESQL"),
    Row("Parquet", _s(SinkTarget.PARQUET), ("sink-parquet", "dgm-write-plan"),
        "yes — temporary directory", "PARQUET"),
    Row("MotherDuck", _s(SinkTarget.MOTHERDUCK),
        ("sink-motherduck", "dgm-write-plan"),
        "captured SQL and write plan only; storage **not executed** (needs "
        "`MOTHERDUCK_TOKEN`)", "MOTHERDUCK"),
    Row("Delta", _s(SinkTarget.DELTA), ("sink-delta", "dgm-write-plan"),
        "yes — an empty Delta table built in a temporary directory (DuckDB "
        "1.5.0 cannot create one), appended to through the `delta` "
        "extension", "DELTA"),
    Row("Iceberg", _s(SinkTarget.ICEBERG), ("dgm-write-plan",),
        "write plan only; storage **not executed**: `IcebergSink` needs "
        "`pyiceberg` and an Iceberg REST catalog, and §6.2 writes need DuckDB "
        "2.0; CI has neither (see *Reproducing* below)", "ICEBERG"),
)  # fmt: skip

ROWS = QUERY_ROWS + SINK_ROWS


def _ids(*patterns: str) -> Callable[[Case], bool]:
    return lambda c: any(fnmatch(c.id, p) for p in patterns)


def _areas(*areas: str) -> Callable[[Case], bool]:
    return lambda c: c.area in areas


#: Which cases evidence a capability.  A case may evidence several.
SELECTORS: dict[Capability, Callable[[Case], bool]] = {
    Capability.JOIN: _areas("fanout", "drill_across"),
    Capability.NULL: _ids("PP-NULL-*", "PP-NOT-*", "CON-N-*", "SCD-E-06"),
    Capability.TEMPORAL: _areas("scd_measure", "edge_validity"),
    Capability.AGGREGATION: _areas("fanout", "drill_across"),
    Capability.CTE: _areas("repeat_join"),
    Capability.WINDOW: lambda c: c.area == "window" or fnmatch(c.id, "CON-B-*"),
    Capability.WINDOW_AGGREGATE: lambda c: (
        c.area == "window" and c.input["window"][0] != "ROW_NUMBER"
    ),
    Capability.PATHPRED: _areas("ctl", "pathpred"),
    Capability.APPEND: _ids("SINK-RT-*", "SINK-SQL-W", "WP-*-target",
                            "WP-*-partition", "WP-*-append", "WP-*-catalog"),
    Capability.SCD2_CLOSE: _ids("SINK-CL-*", "SINK-SQL-C"),
    Capability.MERGE: _ids("WP-*-merge"),
}  # fmt: skip

QUERY_CAPS = (
    Capability.JOIN,
    Capability.NULL,
    Capability.TEMPORAL,
    Capability.AGGREGATION,
    Capability.CTE,
    Capability.WINDOW,
    Capability.WINDOW_AGGREGATE,
    Capability.PATHPRED,
)
SINK_CAPS = (Capability.APPEND, Capability.SCD2_CLOSE, Capability.MERGE)


def evidence(row: Row, cap: Capability) -> list[tuple[str, Case, Status]]:
    """The (backend, case, status) triples that evidence *cap* for *row*."""
    out = []
    for name in row.evidence:
        for c in all_cases():
            st = status(c, name)
            if st is None or not SELECTORS[cap](c):
                continue
            if row.sink and c.area == "write_plan" and c.input["sink"] != row.sink:
                continue
            out.append((name, c, st))
    return out


_WORD = {
    Status.PASS: "pass",
    Status.XFAIL: "**non-conf.**",
    Status.REFUSED: "refused",
    Status.NOT_EXPRESSIBLE: "n/e",
}


def cell(row: Row, cap: Capability) -> str:
    declared = row.profile.declared(cap).support.value
    ev = evidence(row, cap)
    if not ev:
        why = "not executed" if "not executed" in row.executed else "no case"
        return f"{declared} · *{why}*"
    counts = Counter(st for _n, _c, st in ev)
    parts = [f"{counts[s]} {_WORD[s]}" for s in Status if counts[s]]
    return f"{declared} · {', '.join(parts)}"


def _table(rows: tuple[Row, ...], caps: tuple[Capability, ...]) -> list[str]:
    lines = [
        "| Backend | " + " | ".join(c.value for c in caps) + " |",
        "|---|" + "---|" * len(caps),
    ]
    for r in rows:
        lines.append(f"| {r.label} | " + " | ".join(cell(r, c) for c in caps) + " |")
    return lines


def _backends_table() -> list[str]:
    lines = [
        "| Backend | Declared by | Emitter | Version | Evidence | Executed in CI |",
        "|---|---|---|---|---|---|",
    ]
    for r in ROWS:
        ev = ", ".join(f"`{n}`" for n in r.evidence) or "—"
        lines.append(
            f"| {r.label} | `{r.profile.name}` | {r.profile.emitter or '**none**'} "
            f"| {r.profile.min_version} | {ev} | {r.executed} |"
        )
    return lines


def _notes() -> list[str]:
    """One line per distinct (support, note), naming every profile and capability."""
    groups: dict[tuple[str, str], dict[str, list[str]]] = {}
    for r in ROWS:
        for c in QUERY_CAPS if r in QUERY_ROWS else SINK_CAPS:
            d = r.profile.declared(c)
            if d.support is Support.SUPPORTED:
                continue
            by_profile = groups.setdefault((d.support.value, d.note), {})
            caps = by_profile.setdefault(r.profile.name, [])
            if c.value not in caps:
                caps.append(c.value)
    lines = ["| Declared | Where | Note |", "|---|---|---|"]
    for (support, note), by_profile in groups.items():
        where = "; ".join(
            f"`{name}`: {', '.join(caps)}" for name, caps in by_profile.items()
        )
        lines.append(f"| {support} | {where} | {note} |")
    return lines


def _divergences() -> list[str]:
    lines = ["| Case | Backend | Observed vs. specified |", "|---|---|---|"]
    from tests.conformance.expectations import reason

    names = {n for r in ROWS for n in r.evidence}
    for b in BACKENDS:
        if b.name not in names or b.name in ("duckdb-plan-ir", "duckdb-dgmquery"):
            continue
        for c in all_cases():
            if status(c, b.name) is Status.XFAIL:
                lines.append(f"| `{c.id}` | `{b.name}` | {reason(c, b.name)} |")
    return lines


def render() -> str:
    lines = [
        BEGIN,
        "",
        "### Backends",
        "",
        *_backends_table(),
        "",
        "### Query capabilities",
        "",
        "Cells: declared support · cases by status (*pass*, **non-conf.** = "
        "strict xfail, *refused* = fail-closed rejection, *n/e* = no construct).",
        "",
        *_table(QUERY_ROWS, QUERY_CAPS),
        "",
        "### Export contracts (sinks)",
        "",
        *_table(SINK_ROWS, SINK_CAPS),
        "",
        "### Partial and unsupported declarations",
        "",
        *_notes(),
        "",
        "### Divergences found by the matrix (strict xfail)",
        "",
        "Those of the DuckDB paths are listed in `dgm-conformance.md`.",
        "",
        *_divergences(),
        "",
        END,
    ]
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

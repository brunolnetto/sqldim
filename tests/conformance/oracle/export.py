"""Export contracts: writes to a sink and write planning (§6.2 Rules 8 and 10, §6.4).

* ``appended`` — APPEND (§2.4 ``write_mode``, §6.2 Rule 8 ``INSERT INTO``):
  every write adds its rows; nothing already written is replaced.  A row is
  read back with the values it was written with, NULLs included.
* ``closed`` — closing SCD2 versions (§2.5, §2.6 M2): the *current* version of
  each listed natural key gets ``is_current = FALSE`` and ``valid_to`` set;
  historical versions and other keys are untouched.
* ``emits_duckdb_sql`` — the statements a sink sends are DuckDB SQL.
* ``write_plan`` — properties of the write plan that §6.2 states per sink.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from tests.conformance.oracle import REJECTED, Undefined

FILE_SINKS = frozenset({"PARQUET", "DELTA", "ICEBERG"})
#: Engine versions the §6.2 Iceberg capability table was read for.
ICEBERG_DOCUMENTED = frozenset({"2.0", "2.0.1"})


def _key(row: Sequence[Any]) -> tuple:
    return tuple((v is None, v) for v in row)


def rows_of(table: Sequence[Mapping[str, Any]], cols: Sequence[str]) -> list[tuple]:
    return sorted((tuple(r[c] for c in cols) for r in table), key=_key)


def appended(i: Mapping[str, Any]) -> list[tuple]:
    """The rows read back after every write of ``i["writes"]``, sorted."""
    return rows_of([r for w in i["writes"] for r in w], i["columns"])


def closed(i: Mapping[str, Any]) -> list[tuple]:
    out = []
    for r in i["rows"]:
        r = dict(r)
        if r["id"] in i["close"] and r["is_current"] is True:
            r["is_current"], r["valid_to"] = False, i["valid_to"]
        out.append(r)
    return rows_of(out, i["columns"])


def emits_duckdb_sql(i: Mapping[str, Any]) -> bool:
    """§6.2 Rule 8 / §6.4: a sink's statements are DuckDB statements."""
    return True


def _target(i: Mapping[str, Any]) -> str:
    # §6.2: delta_scan / iceberg_scan are read-only; a write targets a table
    return "WRITABLE"


def _partition(i: Mapping[str, Any]) -> bool:
    # Rule 8: if TemporalAgg and sink ∈ {PARQUET, DELTA, ICEBERG}
    return bool(i["temporal_agg"]) and i["sink"] in FILE_SINKS


def _append(i: Mapping[str, Any]) -> bool:
    # Rule 8: Q_delta and sink = DELTA → APPEND; Delta writes are APPEND only
    if i["sink"] != "DELTA":
        raise Undefined("APPEND is stated for DELTA only")
    return True


def _iceberg_catalog(i: Mapping[str, Any]) -> str:
    # §6.2 Iceberg writes: no attached catalog → rejected at planning time
    return "ACCEPTED" if i.get("catalog") else REJECTED


def _merge(i: Mapping[str, Any]) -> str:
    if i["sink"] == "DELTA":
        return REJECTED  # §6.2 Delta writes: a MERGE WritePlan is rejected
    if i["sink"] == "ICEBERG":
        # §6.2 Iceberg writes: an engine version the table does not cover
        # rejects everything but APPEND; a MERGE needs merge-on-read
        if i["engine"] not in ICEBERG_DOCUMENTED or not i["merge_on_read"]:
            return REJECTED
        return "MERGE"
    raise Undefined(f"§6.2 states no MERGE rule for {i['sink']}")


_QUESTIONS = {
    "target": _target,
    "partition": _partition,
    "append": _append,
    "iceberg_catalog": _iceberg_catalog,
    "merge": _merge,
}


def write_plan(i: Mapping[str, Any]) -> Any:
    return _QUESTIONS[i["question"]](i)

"""Sinks: the export contract of each ``SinkAdapter`` (§6.2 Rule 8, §6.4).

Three areas, one backend per sink:

* ``sink_roundtrip`` — the rows are written with ``write()`` (one call per
  batch) and read back through ``current_state_sql()``;
* ``sink_close`` — the rows are written, ``close_versions()`` closes the
  listed keys, and the rows are read back;
* ``sink_sql`` — every statement ``write()`` / ``close_versions()`` sends is
  captured by a recording connection and parsed by DuckDB's parser.  Nothing
  is executed, so this runs for every sink that speaks SQL, MotherDuck
  included; it validates the statements, not their effect.

A sink that refuses the operation with :class:`SinkUnsupported` (#178) is
:data:`REJECTED`.

Executed against storage: DuckDB (a temporary file), Parquet (a temporary
directory), Delta (an empty Delta table created by DuckDB in a temporary
directory, DuckDB 2.0.1) and PostgreSQL (a fresh schema of the server
of :mod:`tests.conformance.runners.postgres`).  MotherDuck and Iceberg are
not executed against storage; ``docs/reference/dgm-backend-matrix.md`` gives
the reason for each.
"""

from __future__ import annotations

import tempfile
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager
from typing import Any

import duckdb

from sqldim.sinks import (
    DeltaLakeSink,
    DuckDBSink,
    MotherDuckSink,
    ParquetSink,
    PostgreSQLSink,
    SinkUnsupported,
)
from tests.conformance.cases import Case
from tests.conformance.cases.export import TYPES
from tests.conformance.oracle.export import rows_of
from tests.conformance.runners import REJECTED, Backend
from tests.conformance.runners import postgres
from tests.conformance.runners.duckdb_sql import insert_rows

TABLE = "dim"
#: (sink, the connection its statements run on, the table's qualified name)
Opened = tuple[Any, duckdb.DuckDBPyConnection, str]
Harness = Callable[[Sequence[str]], AbstractContextManager[Opened]]


def _ddl(columns: Sequence[str]) -> str:
    return ", ".join(f"{c} {TYPES[c]}" for c in columns)


def _source(con: duckdb.DuckDBPyConnection, name: str, columns, rows) -> None:
    """A local DuckDB table *name* holding *rows* (possibly none)."""
    con.execute(f"CREATE TABLE memory.main.{name} ({_ddl(columns)})")
    insert_rows(con, f"memory.main.{name}", [[r[c] for c in columns] for r in rows])


# ---------------------------------------------------------------------------
# Harnesses: open a sink on real storage with an empty table
# ---------------------------------------------------------------------------


@contextmanager
def _duckdb(columns: Sequence[str]) -> Iterator[Opened]:
    with tempfile.TemporaryDirectory() as d, DuckDBSink(f"{d}/sink.duckdb") as sink:
        con = sink._con
        assert con is not None
        con.execute(f"CREATE TABLE sqldim_ddb.main.{TABLE} ({_ddl(columns)})")
        yield sink, con, TABLE


@contextmanager
def _parquet(columns: Sequence[str]) -> Iterator[Opened]:
    # COPY TO creates the files: there is no table to create first
    with tempfile.TemporaryDirectory() as d:
        con = duckdb.connect()
        try:
            yield ParquetSink(d), con, TABLE
        finally:
            con.close()


@contextmanager
def _delta(columns: Sequence[str]) -> Iterator[Opened]:
    # DuckDB 2.0.1 creates the empty Delta table: it is named after the alias
    with tempfile.TemporaryDirectory() as d:
        con = duckdb.connect()
        try:
            con.execute("INSTALL delta; LOAD delta")
            con.execute(f"ATTACH '{d}/{TABLE}' AS {TABLE} (TYPE delta)")
            con.execute(f"CREATE TABLE {TABLE}.{TABLE} ({_ddl(columns)})")
            con.execute(f"DETACH {TABLE}")
            yield DeltaLakeSink(d), con, TABLE
        finally:
            con.close()


@contextmanager
def _postgresql(columns: Sequence[str]) -> Iterator[Opened]:
    schema = postgres.new_schema()
    dsn = postgres.dsn()
    assert dsn is not None
    with PostgreSQLSink(dsn, schema=schema) as sink:
        con = sink._con
        assert con is not None
        con.execute(f"CREATE SCHEMA sqldim_pg.{schema}")
        try:
            con.execute(f"CREATE TABLE sqldim_pg.{schema}.{TABLE} ({_ddl(columns)})")
            yield sink, con, TABLE
        finally:
            postgres.drop_schema(con, "sqldim_pg", schema)


def _read(sink: Any, con: duckdb.DuckDBPyConnection, table: str, cols) -> list:
    src = sink.current_state_sql(table)
    src = f"({src})" if src.strip().upper().startswith("SELECT") else src
    rows = con.execute(f"SELECT {', '.join(cols)} FROM {src}").fetchall()
    return rows_of([dict(zip(cols, r)) for r in rows], cols)


def _roundtrip(harness: Harness) -> Callable[[Case], Any]:
    def run(case: Case) -> Any:
        cols = case.input["columns"]
        with harness(cols) as (sink, con, table):
            try:
                for n, batch in enumerate(case.input["writes"]):
                    _source(con, f"batch_{n}", cols, batch)
                    sink.write(con, f"memory.main.batch_{n}", table)
            except SinkUnsupported:
                return REJECTED
            return _read(sink, con, table, cols)

    return run


def _close(harness: Harness) -> Callable[[Case], Any]:
    def run(case: Case) -> Any:
        i, cols = case.input, case.input["columns"]
        with harness(cols) as (sink, con, table):
            _source(con, "batch", cols, i["rows"])
            sink.write(con, "memory.main.batch", table)
            _source(con, "nk", ("id",), [{"id": k} for k in i["close"]])
            try:
                sink.close_versions(
                    con, table, "id", "memory.main.nk", str(i["valid_to"])
                )
            except SinkUnsupported:
                return REJECTED
            return _read(sink, con, table, cols)

    return run


# ---------------------------------------------------------------------------
# Captured SQL
# ---------------------------------------------------------------------------


class Recorder:
    """A stand-in connection that records statements and executes none."""

    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, sql: str, *_args: Any, **_kwargs: Any) -> "Recorder":
        self.statements.append(sql)
        return self

    def fetchone(self) -> tuple[int]:
        return (1,)  # a non-empty source, so write() does not return early

    def fetchall(self) -> list[tuple[int]]:
        return [(1,)]


def parses(sql: str) -> bool:
    try:
        return bool(duckdb.connect().extract_statements(sql))
    except duckdb.ParserException:
        return False


def _sql(make: Callable[[str], Any]) -> Callable[[Case], Any]:
    def run(case: Case) -> Any:
        rec = Recorder()
        with tempfile.TemporaryDirectory() as d:
            sink = make(d)
            try:
                if case.input["operation"] == "write":
                    sink.write(rec, "batch", TABLE)
                else:
                    sink.close_versions(rec, TABLE, "id", "nk", "2024-12-31")
            except SinkUnsupported:
                return REJECTED
        return bool(rec.statements) and all(parses(s) for s in rec.statements)

    return run


_SQL_SINKS: Mapping[str, Callable[[str], Any]] = {
    "DUCKDB": lambda d: DuckDBSink(f"{d}/sink.duckdb"),
    "PARQUET": lambda d: ParquetSink(d),
    "POSTGRESQL": lambda d: PostgreSQLSink("host=unused dbname=unused"),
    "MOTHERDUCK": lambda d: MotherDuckSink("my_db", token="unused"),
    "DELTA": lambda d: DeltaLakeSink(d),
}


def _sink_backend(
    sink: str,
    harness: Harness | None,
    *,
    requires: Callable[[], str | None] | None = None,
) -> Backend:
    runners: dict[str, Callable[[Case], Any]] = {"sink_sql": _sql(_SQL_SINKS[sink])}
    if harness is not None:
        runners["sink_roundtrip"] = _roundtrip(harness)
        runners["sink_close"] = _close(harness)
    stored = "write and read back on real storage; " if harness else ""
    return Backend(
        f"sink-{sink.lower()}",
        f"{sink} SinkAdapter: {stored}captured SQL parsed by DuckDB",
        frozenset(runners),
        lambda case: runners[case.area](case),
        requires=requires,
    )


DUCKDB = _sink_backend("DUCKDB", _duckdb)
PARQUET = _sink_backend("PARQUET", _parquet)
POSTGRESQL = _sink_backend("POSTGRESQL", _postgresql, requires=postgres.unavailable)
MOTHERDUCK = _sink_backend("MOTHERDUCK", None)
DELTA = _sink_backend("DELTA", _delta)

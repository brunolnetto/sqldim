"""PostgreSQL through DuckDB (``PG(DD)`` of §6.4).

``pg-duckdb-plan-ir`` loads each case's fixture into a fresh schema of a real
PostgreSQL server, attached with DuckDB's postgres extension, and executes the
SQL emitted by ``DGMPlanner.plan_query()`` there, planned with
``execution_profile=POSTGRESQL_VIA_DUCKDB``: the tables live in
PostgreSQL, the scans go through the postgres scanner.  This is how sqldim
reads PostgreSQL; ``QueryTarget.SQL_POSTGRESQL`` itself has no emitter (see
:mod:`tests.conformance.runners.refusals`).

The server is the one of ``tests/sinks/test_postgresql_sink.py``:
``SQLDIM_PG_HOST`` (CI runs a ``postgres`` service container),
``SQLDIM_PG_PORT``, ``SQLDIM_PG_USER``, ``SQLDIM_PG_PASSWORD`` and
``SQLDIM_PG_DBNAME`` (default ``postgres``).  Without ``SQLDIM_PG_HOST`` the
cases are skipped locally and fail under CI.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from uuid import uuid4

import duckdb

from sqldim.core.query.dgm import DGMPlanner, DGMQuery
from sqldim.core.query.dgm.model import DGMModel
from sqldim.core.query.dgm.planner import POSTGRESQL_VIA_DUCKDB, QueryTarget
from tests.conformance.runners.duckdb_sql import EXECUTED_AREAS, Engine, backend


def dsn() -> str | None:
    host = os.environ.get("SQLDIM_PG_HOST")
    if not host:
        return None
    parts = {
        "host": host,
        "port": os.environ.get("SQLDIM_PG_PORT", "5432"),
        "user": os.environ.get("SQLDIM_PG_USER", "postgres"),
        "dbname": os.environ.get("SQLDIM_PG_DBNAME", "postgres"),
        "password": os.environ.get("SQLDIM_PG_PASSWORD"),
    }
    return " ".join(f"{k}={v}" for k, v in parts.items() if v)


def unavailable() -> str | None:
    if dsn() is None:
        return "no PostgreSQL server: SQLDIM_PG_HOST is not set"
    return None


def attach(con: duckdb.DuckDBPyConnection, alias: str) -> None:
    con.execute("INSTALL postgres; LOAD postgres")
    con.execute(f"ATTACH '{dsn()}' AS {alias} (TYPE postgres)")


def new_schema() -> str:
    return f"conf_{uuid4().hex[:16]}"


def drop_schema(con: duckdb.DuckDBPyConnection, alias: str, schema: str) -> None:
    con.execute(f"CALL postgres_execute('{alias}', 'DROP SCHEMA {schema} CASCADE')")


@contextmanager
def session() -> Iterator[duckdb.DuckDBPyConnection]:
    """A DuckDB connection whose default schema is a fresh PostgreSQL schema."""
    con = duckdb.connect()
    attach(con, "pg")
    schema = new_schema()
    con.execute(f"CREATE SCHEMA pg.{schema}")
    try:
        con.execute(f"USE pg.{schema}")
        yield con
    finally:
        con.execute("USE memory")
        drop_schema(con, "pg", schema)
        con.close()


def _pg_sql(q: DGMQuery, model: DGMModel | None) -> str:
    """The plan IR's SQL, planned for the engine that runs it: PG(DD) (#178).

    A construct ``POSTGRESQL_VIA_DUCKDB`` declares unsupported raises
    ``UnsupportedCapability`` here, before the postgres scanner sees it.
    """
    planner = DGMPlanner(None, None, None, None, QueryTarget.SQL_DUCKDB,  # type: ignore[arg-type]
                         execution_profile=POSTGRESQL_VIA_DUCKDB,
                         model=model)  # fmt: skip
    return planner.plan_query(q).query_text


PG_DUCKDB = backend(
    "pg-duckdb-plan-ir",
    "DGMPlanner.plan_query() SQL executed by DuckDB over tables stored in "
    "PostgreSQL (postgres extension; PG(DD) of §6.4)",
    Engine(_pg_sql, session),
    areas=EXECUTED_AREAS,
    requires=unavailable,
)

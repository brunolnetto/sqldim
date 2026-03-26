"""SQL/OLAP sinks: DuckDB, MotherDuck, PostgreSQL."""

from sqldim.sinks.sql.duckdb import DuckDBSink  # noqa: F401
from sqldim.sinks.sql.motherduck import MotherDuckSink  # noqa: F401
from sqldim.sinks.sql.postgresql import PostgreSQLSink  # noqa: F401

__all__ = ["DuckDBSink", "MotherDuckSink", "PostgreSQLSink"]

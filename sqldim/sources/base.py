"""
sqldim/sources/base.py

SourceAdapter protocol — the read boundary between any storage system
and the DuckDB execution engine.

One method. Returns a SQL fragment DuckDB can use directly in FROM.
The processor registers it as a VIEW — no data ever enters Python.
"""

from typing import Protocol, runtime_checkable
import duckdb


@runtime_checkable
class SourceAdapter(Protocol):
    """
    Abstracts the read boundary between any storage system and the
    DuckDB execution engine.

    Rules:
      - as_sql() must be idempotent and side-effect free.
      - Processors never import pyarrow, boto3, psycopg2, or delta-rs.
      - If the source needs a DuckDB extension (postgres, delta),
        as_sql() may call con.execute("LOAD ...") as a one-time setup.
    """

    def as_sql(self, con: duckdb.DuckDBPyConnection) -> str:
        """
        Return a SQL fragment usable directly inside a DuckDB FROM clause.

            CREATE VIEW incoming AS
            SELECT * FROM ({source.as_sql(con)})
        """
        ...

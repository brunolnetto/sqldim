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


_QUERY_STARTS = ("SELECT", "WITH", "FROM", "VALUES", "(")


def from_item(sql: str) -> str:
    """*sql* as an item of a ``FROM`` clause (#192).

    A source or a sink's current state is either a query (``SELECT …``) or a
    table reference: a table function such as ``read_parquet('x.parquet')``,
    or a table name.  A query is parenthesised as a subquery; a reference is
    returned as it is, since ``FROM (read_parquet(…))`` is a parser error.
    """
    text = sql.strip()
    if text.upper().startswith(_QUERY_STARTS):
        return f"({text})"
    return text

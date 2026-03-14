"""sqldim/sources/postgresql.py"""


class PostgreSQLSource:
    """
    Read directly from a PostgreSQL table via DuckDB's postgres extension.
    Zero Python memory — DuckDB streams rows from PG on demand.

        PostgreSQLSource(
            dsn="host=localhost dbname=rfb",
            table="estabelecimento",
            schema="public",
        )
    """

    def __init__(self, dsn: str, table: str, schema: str = "public"):
        self._dsn    = dsn
        self._table  = table
        self._schema = schema

    def as_sql(self, con) -> str:
        con.execute("INSTALL postgres; LOAD postgres;")
        return f"postgres_scan('{self._dsn}', '{self._schema}', '{self._table}')"

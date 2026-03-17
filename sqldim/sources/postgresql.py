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

    Parameters
    ----------
    dsn:
        libpq connection string or PostgreSQL URI.
    table:
        Table name.
    schema:
        Schema name.  Default ``'public'``.
    filter_pushdown:
        Enable experimental filter pushdown to PostgreSQL.  Reduces data
        transferred for filtered queries but is disabled by default due to
        correctness edge cases in older pg extension versions.
    pages_per_task:
        Controls parallel scan granularity.  Lower values create more,
        smaller scan tasks.  Default 1000.
    """

    def __init__(
        self,
        dsn: str,
        table: str,
        schema: str = "public",
        filter_pushdown: bool = False,
        pages_per_task: int = 1000,
    ):
        self._dsn             = dsn
        self._table           = table
        self._schema          = schema
        self._filter_pushdown = filter_pushdown
        self._pages_per_task  = pages_per_task

    def as_sql(self, con) -> str:
        con.execute("INSTALL postgres; LOAD postgres;")
        if self._filter_pushdown:
            con.execute("SET pg_experimental_filter_pushdown = true")
        if self._pages_per_task != 1000:
            con.execute(f"SET pg_pages_per_task = {self._pages_per_task}")
        return (
            f"postgres_scan('{self._dsn}', '{self._schema}', '{self._table}')"
        )

"""sqldim/sources/duckdb_source.py"""


class DuckDBSource:
    """
    Read from a table or view already registered in the DuckDB connection.
    Zero overhead — no translation layer.

    Use this to chain processors: the output VIEW of one processor
    becomes the input of the next.

        DuckDBSource("classified")           # view in the current connection
        DuckDBSource("dim_product", schema="main")
        DuckDBSource("staging.empresa")      # already-qualified name
    """

    def __init__(self, table_or_view: str, schema: str | None = None):
        already_qualified = "." in table_or_view
        if schema and already_qualified:
            raise ValueError(
                f"DuckDBSource: 'schema' was given ('{schema}') but '{table_or_view}' "
                "is already schema-qualified.  Pass one or the other, not both."
            )
        if schema:
            self._ref = f"{schema}.{table_or_view}"
        else:
            self._ref = table_or_view

    def as_sql(self, con) -> str:
        return f"SELECT * FROM {self._ref}"

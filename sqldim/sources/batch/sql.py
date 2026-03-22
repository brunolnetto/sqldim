"""sqldim/sources/sql.py"""


class SQLSource:
    """
    Arbitrary SQL fragment as a source — the escape hatch for anything
    not covered by the typed sources.

        SQLSource("SELECT * FROM raw.empresa WHERE uf = 'SP'")
        SQLSource("SELECT a.*, b.name FROM a JOIN b ON a.id = b.id")

    The fragment is wrapped in a subquery by the processor, so it can be
    any valid SELECT expression.
    """

    def __init__(self, sql: str):
        self._sql = sql.strip().rstrip(";")

    def as_sql(self, con) -> str:
        return self._sql

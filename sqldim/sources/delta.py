"""sqldim/sources/delta.py"""


class DeltaSource:
    """
    Read from a Delta Lake table via DuckDB's delta extension.

        DeltaSource("/data/delta/empresa")
        DeltaSource("s3://my-bucket/delta/empresa")
    """

    def __init__(self, path: str):
        self._path = path

    def as_sql(self, con) -> str:
        con.execute("INSTALL delta; LOAD delta;")
        return f"delta_scan('{self._path}')"

"""sqldim/sources/parquet.py"""


class ParquetSource:
    """
    Read one or more Parquet files.

        ParquetSource("data/empresa.parquet")
        ParquetSource("data/empresa/*.parquet")
        ParquetSource(["part1.parquet", "part2.parquet"])
    """

    def __init__(self, path: str | list[str], hive_partitioning: bool = False):
        if isinstance(path, list):
            quoted = ", ".join(f"'{p}'" for p in path)
            self._expr = f"[{quoted}]"
        else:
            self._expr = f"'{path}'"
        self._hive = hive_partitioning

    def as_sql(self, con) -> str:
        hive = ", hive_partitioning=true" if self._hive else ""
        return f"read_parquet({self._expr}{hive})"

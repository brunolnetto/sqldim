"""sqldim/sources/csv.py"""


class CSVSource:
    """
    Read one or more CSV / TSV files.

        CSVSource("data/empresa.csv")
        CSVSource("data/empresa/*.csv", delimiter=";", header=True)
    """

    def __init__(
        self,
        path: str | list[str],
        delimiter: str = ",",
        header: bool = True,
        encoding: str = "utf-8",
    ):
        if isinstance(path, list):
            quoted = ", ".join(f"'{p}'" for p in path)
            self._expr = f"[{quoted}]"
        else:
            self._expr = f"'{path}'"
        self._delimiter = delimiter
        self._header    = str(header).upper()
        self._encoding  = encoding

    def as_sql(self, con) -> str:
        return (
            f"read_csv({self._expr}, "
            f"delim='{self._delimiter}', "
            f"header={self._header}, "
            f"encoding='{self._encoding}')"
        )

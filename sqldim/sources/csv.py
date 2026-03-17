"""sqldim/sources/csv.py"""


class CSVSource:
    """
    Read one or more CSV / TSV files.

        CSVSource("data/empresa.csv")
        CSVSource("data/empresa/*.csv", delimiter=";", header=True)

    Note on *encoding*: DuckDB's ``read_csv`` only supports encodings that
    the underlying ICU library recognises (e.g. ``'utf-8'``, ``'latin-1'``,
    ``'cp1252'``).  Exotic or Python-only codec names (e.g. ``'utf_8_sig'``)
    will raise a DuckDB error at query time.  Normalise the encoding string
    before constructing this object if necessary.

    Parameters
    ----------
    path:
        File path, glob pattern, or list of paths.
    delimiter:
        Field separator.  Default ``','``.
    header:
        Whether the first row is a header.  Default ``True``.
    encoding:
        File encoding (ICU-compatible names only).
    columns:
        Dict of ``{column_name: sql_type}``.  When provided, disables
        auto-detection and avoids the double file scan DuckDB performs for
        schema inference.  Use when the schema is known.
    nullstr:
        String to interpret as NULL.  Default ``''`` (empty string).
    ignore_errors:
        Skip malformed rows rather than raising.  Default ``False``.
    """

    def __init__(
        self,
        path: str | list[str],
        delimiter: str = ",",
        header: bool = True,
        encoding: str = "utf-8",
        columns: dict | None = None,
        nullstr: str = "",
        ignore_errors: bool = False,
    ):
        if isinstance(path, list):
            quoted = ", ".join(f"'{p}'" for p in path)
            self._expr = f"[{quoted}]"
        else:
            self._expr = f"'{path}'"
        self._delimiter    = delimiter
        self._header       = str(header).upper()
        self._encoding     = encoding
        self._columns      = columns
        self._nullstr      = nullstr
        self._ignore_errors = ignore_errors

    def as_sql(self, con) -> str:
        opts = [
            f"delim='{self._delimiter}'",
            f"header={self._header}",
            f"encoding='{self._encoding}'",
            f"nullstr='{self._nullstr}'",
            f"ignore_errors={'true' if self._ignore_errors else 'false'}",
        ]
        if self._columns is not None:
            col_defs = ", ".join(
                f"'{name}': '{dtype}'" for name, dtype in self._columns.items()
            )
            opts.append(f"columns={{{col_defs}}}")
            opts.append("auto_detect=false")
        return f"read_csv({self._expr}, {', '.join(opts)})"

"""sqldim/sources/delta.py"""


class DeltaSource:
    """
    Read from a Delta Lake table via DuckDB's delta extension.

        DeltaSource("/data/delta/empresa")
        DeltaSource("s3://my-bucket/delta/empresa")

    Parameters
    ----------
    path:
        Local path or cloud URI (s3://, abfss://, gs://).
    use_attach:
        Use ``ATTACH`` mode instead of ``delta_scan()``.  ``ATTACH`` enables
        metadata caching across repeated scans of the same table, which is
        significant for repeated scans of the same table in a streaming loop.
        Default ``True``.
    alias:
        ``ATTACH`` alias.  Only used when ``use_attach=True``.
    """

    def __init__(
        self,
        path: str,
        use_attach: bool = True,
        alias: str = "sqldim_delta_src",
    ):
        self._path      = path
        self._use_attach = use_attach
        self._alias     = alias
        self._attached  = False

    def as_sql(self, con) -> str:
        if not self._use_attach:
            con.execute("INSTALL delta; LOAD delta;")
            return f"delta_scan('{self._path}')"

        if not self._attached:
            con.execute("INSTALL delta; LOAD delta;")
            try:
                con.execute(f"ATTACH '{self._path}' AS {self._alias} (TYPE delta)")
            except Exception:
                # ATTACH may fail for certain delta setups — fall back to delta_scan
                return f"delta_scan('{self._path}')"
            self._attached = True

        return f"SELECT * FROM {self._alias}"

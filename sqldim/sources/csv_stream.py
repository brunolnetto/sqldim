"""
sqldim/sources/csv_stream.py

Chunked CSV streaming source for large files processed in row-batches.
"""
from __future__ import annotations


class CSVStreamSource:
    """Stream a large CSV file in fixed-size row chunks.

    Uses DuckDB rowid-based pagination — no OFFSET scan penalty because DuckDB
    evaluates the full CSV view lazily and the rowid filter pushes down into
    the scan.

    Use with ``process_stream()`` for 60M+ row files::

        source = CSVStreamSource("data/*.csv", delimiter=";")
        result = proc.process_stream(source, "dim_empresa", batch_size=1_000_000)

    Parameters
    ----------
    path      : Glob pattern or file path accepted by DuckDB ``read_csv``.
    delimiter : Field delimiter (default ``","``).
    encoding  : Character encoding (default ``"utf-8"``).
    header    : Whether the file has a header row (default ``True``).
    """

    def __init__(
        self,
        path: str,
        delimiter: str = ",",
        encoding: str = "utf-8",
        header: bool = True,
    ):
        self._path      = path
        self._delimiter = delimiter
        self._encoding  = encoding
        self._header    = header
        self._offset    = 0
        self._total: int | None = None

    def stream(self, con, batch_size: int = 1_000_000):
        """Yield SQL fragments covering successive row windows of *batch_size*.

        Each yielded fragment is a ``SELECT … WHERE rowid BETWEEN …`` expression
        that DuckDB pushes into its CSV scan — no Python-side data movement.
        """
        con.execute(f"""
            CREATE OR REPLACE VIEW _csv_stream_src AS
            SELECT row_number() OVER () - 1 AS _sqldim_rowid, *
            FROM read_csv(
                '{self._path}',
                delim='{self._delimiter}',
                encoding='{self._encoding}',
                header={str(self._header).upper()},
                parallel=true
            )
        """)
        if self._total is None:
            self._total = con.execute(
                "SELECT count(*) FROM _csv_stream_src"
            ).fetchone()[0]

        while self._offset < self._total:
            end = self._offset + batch_size
            yield (
                f"SELECT * EXCLUDE (_sqldim_rowid) FROM _csv_stream_src "
                f"WHERE _sqldim_rowid >= {self._offset} "
                f"AND _sqldim_rowid < {end}"
            )
            self._offset = end

    def checkpoint(self) -> int:
        """Return the current byte/row offset (number of rows consumed so far)."""
        return self._offset

    def commit(self, offset: int) -> None:
        """Advance the committed offset to *offset*."""
        self._offset = offset

    def reset(self) -> None:
        """Reset the stream to the beginning."""
        self._offset = 0
        self._total  = None

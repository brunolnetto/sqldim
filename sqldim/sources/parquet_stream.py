"""
sqldim/sources/parquet_stream.py

Chunked Parquet streaming source for large datasets processed in row-batches.
"""

from __future__ import annotations


class ParquetStreamSource:
    """Stream a large Parquet dataset in fixed-size row chunks.

    Supports glob patterns and hive-partitioned datasets.

    Use with ``process_stream()`` for 60M+ row datasets::

        source = ParquetStreamSource("data/2024-12/*.parquet")
        result = proc.process_stream(source, "dim_socios", batch_size=2_000_000)

    Parameters
    ----------
    path              : Glob pattern or file path accepted by DuckDB ``read_parquet``.
    hive_partitioning : Enable hive-style partition pruning (default ``False``).
    order_by          : Optional SQL column expression to sort within the view
                        before applying rowid pagination.  When ``None`` the
                        natural file order is used.
    """

    def __init__(
        self,
        path: str,
        hive_partitioning: bool = False,
        order_by: str | None = None,
    ):
        self._path = path
        self._hive = hive_partitioning
        self._order = order_by
        self._offset = 0
        self._total: int | None = None

    def stream(self, con, batch_size: int = 1_000_000):
        """Yield SQL fragments covering successive row windows of *batch_size*.

        Each yielded fragment is a ``SELECT … WHERE rowid BETWEEN …`` expression
        that DuckDB pushes into its Parquet scan.
        """
        hive = ", hive_partitioning=true" if self._hive else ""
        order = f"ORDER BY {self._order}" if self._order else ""
        con.execute(f"""
            CREATE OR REPLACE VIEW _parquet_stream_src AS
            SELECT row_number() OVER ({order}) - 1 AS _sqldim_rowid, *
            FROM read_parquet('{self._path}'{hive})
        """)
        if self._total is None:
            self._total = con.execute(
                "SELECT count(*) FROM _parquet_stream_src"
            ).fetchone()[0]

        while self._offset < self._total:
            end = self._offset + batch_size
            yield (
                f"SELECT * EXCLUDE (_sqldim_rowid) FROM _parquet_stream_src "
                f"WHERE _sqldim_rowid >= {self._offset} "
                f"AND _sqldim_rowid < {end}"
            )
            self._offset = end

    def checkpoint(self) -> int:
        """Return the number of rows consumed so far."""
        return self._offset

    def commit(self, offset: int) -> None:
        """Advance the committed offset to *offset*."""
        self._offset = offset

    def reset(self) -> None:
        """Reset the stream to the beginning."""
        self._offset = 0
        self._total = None

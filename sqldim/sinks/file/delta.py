"""
sqldim/sinks/delta.py

Delta Lake sink via DuckDB's delta extension (§6.2 *Delta writes*).

``delta_scan(path)`` is read-only.  A write attaches the table
(``ATTACH … (TYPE delta)``) and appends to it with ``INSERT INTO``; that is
the only write DuckDB's delta extension makes.  ``UPDATE``, ``DELETE`` and
``MERGE INTO`` are rejected by the extension, so every operation that changes
written rows — closing SCD2 versions, SCD1/SCD3 updates, milestones — raises
:class:`~sqldim.sinks.SinkUnsupported` and sends nothing (#178).  On DuckDB
1.5.0 the table must already exist: creating a Delta table needs DuckDB 2.0.
"""

from uuid import uuid4

import duckdb

from sqldim.sinks._connection import make_connection
from sqldim.sinks._errors import SinkUnsupported

_APPEND_ONLY = (
    "§6.2 Delta writes: DuckDB's delta extension appends only (INSERT INTO an "
    "attached table); UPDATE, DELETE and MERGE INTO are rejected, so a Delta "
    "table's rows cannot be changed in place. Nothing was written"
)


def _ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


class DeltaLakeSink:
    """
    Sink backed by Delta Lake via DuckDB's delta extension: APPEND only.

    ``write()`` appends; every in-place change raises ``SinkUnsupported``.
    ``natural_key`` is kept for compatibility; an append does not use it.
    """

    def __init__(self, path: str, natural_key: str = "id"):
        self._path = path
        self._natural_key = natural_key
        self._con: duckdb.DuckDBPyConnection | None = None

    def _table_uri(self, table_name: str) -> str:
        return f"{self._path}/{table_name}"

    # ── SinkAdapter core ──────────────────────────────────────────────────

    def current_state_sql(self, table_name: str) -> str:
        """Return a DuckDB FROM-source expression for the current Delta table.

        Uses DuckDB's built-in ``delta_scan()`` function so the table is read
        lazily without loading the full dataset into memory.
        """
        return f"delta_scan('{self._table_uri(table_name)}')"

    def write(
        self,
        con: duckdb.DuckDBPyConnection,
        view_name: str,
        table_name: str,
        batch_size: int = 100_000,
    ) -> int:
        """Append the rows of *view_name* to the existing Delta table.

        The table is attached under a fresh alias, its columns are read in
        order, and ``INSERT INTO <alias> SELECT <those columns> FROM view``
        appends one Delta commit; a column missing from the view fails the
        write.  Returns the rows appended.
        """
        alias = f"sqldim_delta_{uuid4().hex[:12]}"
        con.execute(f"ATTACH '{self._table_uri(table_name)}' AS {alias} (TYPE delta)")
        try:
            cols = [
                r[0]
                for r in con.execute(
                    "SELECT column_name FROM duckdb_columns() "
                    f"WHERE database_name = '{alias}' ORDER BY column_index"
                ).fetchall()
            ]
            if not cols:
                raise SinkUnsupported(
                    f"{self._table_uri(table_name)!r} has no Delta table to append "
                    "to; creating one needs DuckDB 2.0 (§6.2). Nothing was written"
                )
            n = (con.execute(f"SELECT count(*) FROM {view_name}").fetchone() or (0,))[0]
            select = ", ".join(_ident(c) for c in cols)
            con.execute(f"INSERT INTO {alias} SELECT {select} FROM {view_name}")
        finally:
            con.execute(f"DETACH {alias}")
        return n

    def close_versions(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        nk_col: str,
        nk_view: str,
        valid_to: str,
    ) -> int:
        """Refused: closing a version updates written rows (``SinkUnsupported``)."""
        raise SinkUnsupported(f"DeltaLakeSink.close_versions: {_APPEND_ONLY}")

    # ── SinkAdapter extended ──────────────────────────────────────────────

    def update_attributes(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        nk_col: str,
        updates_view: str,
        update_cols: list[str],
    ) -> int:
        """Refused: an SCD1 update changes written rows (``SinkUnsupported``)."""
        raise SinkUnsupported(f"DeltaLakeSink.update_attributes: {_APPEND_ONLY}")

    def rotate_attributes(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        nk_col: str,
        rotations_view: str,
        column_pairs: list[tuple[str, str]],
    ) -> int:
        """Refused: an SCD3 rotation changes written rows (``SinkUnsupported``)."""
        raise SinkUnsupported(f"DeltaLakeSink.rotate_attributes: {_APPEND_ONLY}")

    def update_milestones(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        match_col: str,
        updates_view: str,
        milestone_cols: list[str],
    ) -> int:
        """Refused: patching milestones changes written rows (``SinkUnsupported``)."""
        raise SinkUnsupported(f"DeltaLakeSink.update_milestones: {_APPEND_ONLY}")

    # ── Context manager ───────────────────────────────────────────────────

    def __enter__(self) -> "DeltaLakeSink":
        self._con = make_connection()
        self._con.execute("INSTALL delta; LOAD delta;")
        return self

    def __exit__(self, *_) -> None:
        if self._con:
            self._con.close()

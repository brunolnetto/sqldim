"""
sqldim/sinks/delta.py

Delta Lake sink via DuckDB's delta extension (§6.2 *Delta writes*).

``delta_scan(path)`` is read-only.  A write attaches the table
(``ATTACH … (TYPE delta)``) and appends to it with ``INSERT INTO … BY NAME``;
when the path holds no Delta table yet, the first write creates it with
``CREATE TABLE <alias> AS`` (DuckDB 2.0.1: the table takes the name of the
attached alias).  ``UPDATE``, ``DELETE`` and ``MERGE INTO`` are still rejected
by the extension on DuckDB 2.0.1 ("Can only update base table"), so every
operation that changes written rows — closing SCD2 versions, SCD1/SCD3
updates, milestones — raises :class:`~sqldim.sinks.SinkUnsupported` and sends
nothing (#178, #193).
"""

from uuid import uuid4

import duckdb

from sqldim.exceptions import SchemaError
from sqldim.sinks._connection import OwnsConnection, make_connection
from sqldim.sinks._errors import SinkUnsupported

_APPEND_ONLY = (
    "§6.2 Delta writes: DuckDB's delta extension appends only (INSERT INTO an "
    "attached table); UPDATE, DELETE and MERGE INTO are rejected (verified on "
    "DuckDB 2.0.1), so a Delta table's rows cannot be changed in place. "
    "Nothing was written"
)


def _columns(con: duckdb.DuckDBPyConnection, relation: str) -> list[str]:
    return [
        str(r[0]) for r in con.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()
    ]


class DeltaLakeSink(OwnsConnection):
    """
    Sink backed by Delta Lake via DuckDB's delta extension: APPEND only.

    ``write()`` appends, creating the table on the first write; every in-place
    change raises ``SinkUnsupported``.
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
        """Append the rows of *view_name* to the Delta table, one Delta commit.

        The table is attached under a fresh alias.  If it exists, the rows are
        inserted by name (``INSERT INTO <alias> BY NAME SELECT *``): a table
        column the view lacks is written as ``NULL``, and a view column the
        table lacks raises :class:`~sqldim.exceptions.SchemaError` before
        anything is written, as in ``DuckDBSink`` (#192).  If the path holds
        no Delta table, ``CREATE TABLE <alias> AS`` creates it with the
        view's columns and rows.  Returns the rows written.
        """
        alias = f"sqldim_delta_{uuid4().hex[:12]}"
        con.execute(f"ATTACH '{self._table_uri(table_name)}' AS {alias} (TYPE delta)")
        try:
            exists = (
                con.execute(
                    "SELECT count(*) FROM duckdb_tables() "
                    f"WHERE database_name = '{alias}'"
                ).fetchone()
                or (0,)
            )[0]
            n = (con.execute(f"SELECT count(*) FROM {view_name}").fetchone() or (0,))[0]
            if not exists:
                con.execute(
                    f"CREATE TABLE {alias}.{alias} AS SELECT * FROM {view_name}"
                )
                return n
            have = {c.lower() for c in _columns(con, alias)}
            extra = [c for c in _columns(con, view_name) if c.lower() not in have]
            if extra:
                raise SchemaError(
                    f"{self._table_uri(table_name)!r} has no column {extra}: "
                    "DeltaLakeSink inserts by name, and every column of the rows "
                    "written must exist in the Delta table; nothing was written"
                )
            con.execute(f"INSERT INTO {alias} BY NAME SELECT * FROM {view_name}")
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
            self._con = None

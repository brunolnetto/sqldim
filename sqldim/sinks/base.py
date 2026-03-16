"""
sqldim/sinks/base.py

SinkAdapter protocol — the only boundary between the DuckDB execution engine
and any storage backend.

Rules enforced by convention:
  - current_state_sql() must return a SQL fragment DuckDB can use in FROM.
  - write() is the ONLY allowed materialisation point.
  - close_versions() must never load full rows into Python.
  - update_attributes(), rotate_attributes(), update_milestones() likewise.
  - No implementation may import psycopg2/pyarrow/delta-rs in module scope
    — only inside the method that uses them.
"""

from typing import Protocol, runtime_checkable
import duckdb


@runtime_checkable
class SinkAdapter(Protocol):
    """
    Abstracts the read/write boundary between the DuckDB execution engine
    and any storage backend.

    Core SCD2 interface
    -------------------
    current_state_sql  — lazy FROM fragment for current dimension state
    write              — the ONLY point where data leaves DuckDB
    close_versions     — mark current rows as historical (NK list, not rows)

    Extended interface for SCD1 / SCD3 / accumulating snapshots
    -----------------------------------------------------------
    update_attributes  — overwrite attribute columns in place (SCD Type 1)
    rotate_attributes  — shift current→previous columns (SCD Type 3)
    update_milestones  — update non-null milestone columns (accumulating snap)
    """

    # ── Core (SCD2) ────────────────────────────────────────────────────────

    def current_state_sql(self, table_name: str) -> str:
        """
        SQL fragment for reading current dimension state.
        Must yield at minimum (natural_key, checksum, is_current).

        Returns a string usable directly inside a DuckDB FROM clause:

            CREATE VIEW current_checksums AS
            SELECT nk, checksum
            FROM ({self.sink.current_state_sql(table_name)})
            WHERE is_current = TRUE
        """
        ...

    def write(
        self,
        con: duckdb.DuckDBPyConnection,
        view_name: str,
        table_name: str,
        batch_size: int = 100_000,
    ) -> int:
        """
        Stream DuckDB view `view_name` into this sink.
        This is the ONLY place data may leave DuckDB into Python.
        Returns rows written.
        """
        ...

    def write_named(
        self,
        con: duckdb.DuckDBPyConnection,
        view_name: str,
        table_name: str,
        columns: list[str],
        batch_size: int = 100_000,
    ) -> int:
        """
        Insert only the listed *columns* from *view_name* into *table_name*.

        Unlike ``write`` (which does ``SELECT *``), this form is required when
        the target table has auto-generated columns (e.g. ``sk BIGSERIAL``) that
        must not appear in the INSERT column list.

        All column names in *columns* must have already been validated by
        ``_safe()`` before being passed here.
        Returns rows written.
        """
        ...

    def close_versions(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        nk_col: str,
        nk_view: str,
        valid_to: str,
    ) -> int:
        """
        Mark current rows as historical.
        `nk_view` is a DuckDB view name containing the NKs to close.

        Mutable sinks (PG, DuckDB):  UPDATE in place.
        Immutable sinks (Parquet, Iceberg): overwrite partition.

        Never read full rows — work only with the NK list.
        Returns rows closed.
        """
        ...

    # ── Extended (SCD1 / SCD3 / accumulating) ─────────────────────────────

    def update_attributes(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        nk_col: str,
        updates_view: str,
        update_cols: list[str],
    ) -> int:
        """
        Overwrite attribute columns in place for SCD Type 1 / hybrid Type 6.
        `updates_view` must contain (nk_col, *update_cols).

        Mutable sinks: single UPDATE … FROM.
        Immutable sinks: partition rewrite inside DuckDB.
        Returns rows updated.
        """
        ...

    def rotate_attributes(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        nk_col: str,
        rotations_view: str,
        column_pairs: list[tuple[str, str]],
    ) -> int:
        """
        Column rotation for SCD Type 3.
        `column_pairs` is a list of (current_col, previous_col) tuples.
        For each matched NK: previous_col = current_col, current_col = incoming.
        `rotations_view` must contain (nk_col, *[c for c, _ in column_pairs]).
        Returns rows updated.
        """
        ...

    def update_milestones(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        match_col: str,
        updates_view: str,
        milestone_cols: list[str],
    ) -> int:
        """
        Patch non-null milestone columns for accumulating snapshot facts.
        `updates_view` must contain (match_col, *milestone_cols).
        Only columns that are non-NULL in the source are written.
        Returns rows updated.
        """
        ...

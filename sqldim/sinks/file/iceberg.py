"""
sqldim/sinks/iceberg.py

Apache Iceberg sink for sqldim via PyIceberg + DuckDB.

Requires
--------
* ``pyiceberg >= 0.5.0``  (``pip install "pyiceberg[hive,s3]"`` for cloud)
* ``pyarrow``
* DuckDB's built-in Iceberg extension (loaded automatically on ``__enter__``)

Usage
-----
::

    from sqldim.sinks import IcebergSink

    catalog_cfg = {
        "uri": "thrift://hive-metastore:9083",
        "s3.endpoint": "http://minio:9000",
        "s3.access-key-id": "...",
        "s3.secret-access-key": "...",
    }

    with IcebergSink(
        catalog_name="hive",
        namespace="warehouse",
        catalog_config=catalog_cfg,
    ) as sink:
        proc = LazySCDProcessor("id", ["name", "email"], sink, con=sink._con)
        proc.process(source, "dim_customer")

Local / REST catalog example::

    with IcebergSink(
        catalog_name="rest",
        namespace="default",
        catalog_config={"uri": "http://localhost:8181"},
    ) as sink:
        ...
"""

from __future__ import annotations

from typing import Any

import duckdb

from sqldim.sinks._connection import make_connection
from sqldim.sinks.file._iceberg_mutate import _IcebergMutateMixin  # noqa: F401


class IcebergSink(_IcebergMutateMixin):
    """
    SinkAdapter implementation for Apache Iceberg tables.

    Reads use DuckDB's native ``iceberg_scan()`` extension; writes use
    PyIceberg's ``Table.append()`` (Arrow-backed) so the Iceberg catalog
    and commit chain are kept consistent.

    Parameters
    ----------
    catalog_name   : PyIceberg catalog name (matches catalog config key
                     in ``~/.pyiceberg.yaml`` or supplied via
                     *catalog_config*).
    namespace      : Iceberg namespace (database) that holds the tables.
    catalog_config : Optional dict of catalog properties forwarded to
                     ``pyiceberg.catalog.load_catalog()``.  Takes
                     precedence over the YAML config file.
    table_location_base : Base path/URI prefix used to derive per-table
                     warehouse locations (e.g. ``"s3://bucket/warehouse"``).
                     Leave *None* to rely on catalog defaults.
    """

    def __init__(
        self,
        catalog_name: str,
        namespace: str,
        catalog_config: dict | None = None,
        table_location_base: str | None = None,
        max_python_rows: int = 500_000,
    ) -> None:
        self._catalog_name = catalog_name
        self._namespace = namespace
        self._catalog_config = catalog_config or {}
        self._location_base = table_location_base
        self._catalog: Any = None
        self._con: Any = None
        self._max_python_rows = max_python_rows

    # ── Internal helpers ──────────────────────────────────────────────────

    def _check_table_row_count(self, iceberg_table, table_name: str) -> int:
        """Raise MemoryError if the table exceeds ``_max_python_rows``.

        Mutation methods that load the full table via ``scan().to_arrow()``
        call this first to prevent OOM.  Returns the row count so callers
        can skip the guard result.
        """
        row_count = iceberg_table.scan().count()
        if row_count > self._max_python_rows:
            raise MemoryError(
                f"IcebergSink: table '{table_name}' has {row_count:,} rows which "
                f"exceeds the safe Python-materialisation limit of "
                f"{self._max_python_rows:,}. "
                f"Use IcebergSink(max_python_rows=N) to raise the limit or "
                f"migrate to a DuckDB-native write path."
            )
        return row_count

    def _table_location(self, table_name: str) -> str:
        """Derive the underlying file-system location for a table."""
        if self._location_base:
            return f"{self._location_base.rstrip('/')}/{table_name}"
        # Fall back to catalog metadata (populated after table is created)
        tbl = self._catalog.load_table(f"{self._namespace}.{table_name}")
        return tbl.location()

    def _load_catalog(self):
        """Lazy-import and initialise the PyIceberg catalog."""
        try:
            from pyiceberg.catalog import load_catalog  # type: ignore[import]
        except ImportError as exc:
            raise ImportError(
                "IcebergSink requires pyiceberg. "
                'Install it with: pip install "pyiceberg[hive,s3]"'
            ) from exc
        return load_catalog(self._catalog_name, **self._catalog_config)

    # ── SinkAdapter core ──────────────────────────────────────────────────

    def current_state_sql(self, table_name: str) -> str:
        """Read the current Iceberg table via DuckDB's iceberg_scan()."""
        location = self._table_location(table_name)
        return f"SELECT * FROM iceberg_scan('{location}')"

    def write(
        self,
        con: duckdb.DuckDBPyConnection,
        view_name: str,
        table_name: str,
        batch_size: int = 100_000,
    ) -> int:
        """Append new rows to the Iceberg table using PyIceberg + Arrow."""
        try:
            import pyarrow as pa  # type: ignore[import]
        except ImportError as exc:
            raise ImportError(
                "IcebergSink requires pyarrow. Install it with: pip install pyarrow"
            ) from exc

        arrow_batch: pa.Table = con.execute(f"SELECT * FROM {view_name}").arrow()
        if not isinstance(arrow_batch, pa.Table):
            arrow_batch = arrow_batch.read_all()
        iceberg_table = self._catalog.load_table(f"{self._namespace}.{table_name}")
        iceberg_table.append(arrow_batch)
        return len(arrow_batch)

    def close_versions(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        nk_col: str,
        nk_view: str,
        valid_to: str,
    ) -> int:
        """Close SCD2 open versions via Iceberg overwrite (read-modify-write)."""
        try:
            import pyarrow.compute as pc  # type: ignore[import]
        except ImportError as exc:
            raise ImportError("IcebergSink requires pyarrow") from exc

        iceberg_table = self._catalog.load_table(f"{self._namespace}.{table_name}")
        self._check_table_row_count(iceberg_table, table_name)
        df = iceberg_table.scan().to_arrow()

        # Natural keys that need version closure
        nk_values = set(
            con.execute(f"SELECT {nk_col} FROM {nk_view}").fetchdf()[nk_col].tolist()
        )

        import pyarrow as pa

        mask = pc.and_(
            pc.is_in(df[nk_col], value_set=pa.array(list(nk_values))),
            pc.equal(df["is_current"], True),
        )
        not_mask = pc.invert(mask)

        unchanged = df.filter(not_mask)
        to_close = df.filter(mask)

        if len(to_close) == 0:
            return 0

        # Update is_current and valid_to for the closing rows
        updated_cols = {
            col: to_close.column(col)
            for col in to_close.schema.names
            if col not in ("is_current", "valid_to")
        }
        updated_cols["is_current"] = pa.array([False] * len(to_close))
        updated_cols["valid_to"] = pa.array([valid_to] * len(to_close))
        closed = pa.table(updated_cols)

        new_table = pa.concat_tables([unchanged, closed])
        iceberg_table.overwrite(new_table)
        return len(to_close)

    # ── SinkAdapter extended ──────────────────────────────────────────────

    def update_attributes(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        nk_col: str,
        updates_view: str,
        update_cols: list[str],
    ) -> int:
        """Apply SCD1-style attribute updates via Iceberg read-modify-write."""
        try:
            import pyarrow as pa
            import pyarrow.compute as pc
        except ImportError as exc:
            raise ImportError("IcebergSink requires pyarrow") from exc

        iceberg_table = self._catalog.load_table(f"{self._namespace}.{table_name}")
        self._check_table_row_count(iceberg_table, table_name)
        existing = iceberg_table.scan().to_arrow()

        updates_df = con.execute(
            f"SELECT {nk_col}, {', '.join(update_cols)} FROM {updates_view}"
        ).fetchdf()

        update_map = {
            row[nk_col]: {c: row[c] for c in update_cols}
            for _, row in updates_df.iterrows()
        }
        nk_list = list(update_map.keys())

        mask = pc.is_in(existing[nk_col], value_set=pa.array(nk_list))
        to_update = existing.filter(mask)
        unchanged = existing.filter(pc.invert(mask))

        updated_cols = self._build_updated_cols(
            to_update, nk_col, update_cols, update_map
        )
        new_table = pa.concat_tables([unchanged, pa.table(updated_cols)])
        iceberg_table.overwrite(new_table)
        return len(to_update)

    def _build_updated_cols(
        self, to_update, nk_col: str, update_cols: list[str], update_map: dict
    ) -> dict:
        """Build a column dict applying *update_map* values for *update_cols*.

        Walks every column in *to_update*; columns listed in *update_cols* are
        replaced with the corresponding value from *update_map*, all others are
        passed through unchanged.
        """
        import pyarrow as pa

        updated = {}
        nk_list = to_update.column(nk_col).to_pylist()
        for col in to_update.schema.names:
            if col in update_cols:
                updated[col] = pa.array(
                    [
                        update_map.get(nk, {}).get(
                            col, to_update.column(col)[i].as_py()
                        )
                        for i, nk in enumerate(nk_list)
                    ]
                )
            else:
                updated[col] = to_update.column(col)
        return updated

    # ── Context manager ───────────────────────────────────────────────────

    def __enter__(self) -> "IcebergSink":
        """Initialise the PyIceberg catalog and a DuckDB connection."""
        self._catalog = self._load_catalog()
        self._con = make_connection()
        try:
            self._con.execute("LOAD iceberg")
        except Exception:
            self._con.execute("INSTALL iceberg; LOAD iceberg")
        return self

    def __exit__(self, *_) -> None:
        """Close the DuckDB connection and release the catalog reference."""
        if self._con:
            self._con.close()
            self._con = None
        self._catalog = None

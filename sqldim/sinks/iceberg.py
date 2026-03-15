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

import duckdb


class IcebergSink:
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
    ) -> None:
        self._catalog_name   = catalog_name
        self._namespace      = namespace
        self._catalog_config = catalog_config or {}
        self._location_base  = table_location_base
        self._catalog        = None
        self._con: duckdb.DuckDBPyConnection | None = None

    # ── Internal helpers ──────────────────────────────────────────────────

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
                "Install it with: pip install \"pyiceberg[hive,s3]\""
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
                "IcebergSink requires pyarrow. "
                "Install it with: pip install pyarrow"
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
        to_close  = df.filter(mask)

        if len(to_close) == 0:
            return 0

        # Update is_current and valid_to for the closing rows
        updated_cols = {
            col: to_close.column(col)
            for col in to_close.schema.names
            if col not in ("is_current", "valid_to")
        }
        updated_cols["is_current"] = pa.array([False] * len(to_close))
        updated_cols["valid_to"]   = pa.array([valid_to] * len(to_close))
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
        existing = iceberg_table.scan().to_arrow()

        updates_df = con.execute(
            f"SELECT {nk_col}, {', '.join(update_cols)} FROM {updates_view}"
        ).fetchdf()

        update_map = {
            row[nk_col]: {c: row[c] for c in update_cols}
            for _, row in updates_df.iterrows()
        }
        nk_list = list(update_map.keys())

        mask        = pc.is_in(existing[nk_col], value_set=pa.array(nk_list))
        to_update   = existing.filter(mask)
        unchanged   = existing.filter(pc.invert(mask))

        updated_cols = self._build_updated_cols(to_update, nk_col, update_cols, update_map)
        new_table = pa.concat_tables([unchanged, pa.table(updated_cols)])
        iceberg_table.overwrite(new_table)
        return len(to_update)

    def _build_updated_cols(
        self, to_update, nk_col: str, update_cols: list[str], update_map: dict
    ) -> dict:
        import pyarrow as pa
        updated = {}
        nk_list = to_update.column(nk_col).to_pylist()
        for col in to_update.schema.names:
            if col in update_cols:
                updated[col] = pa.array([
                    update_map.get(nk, {}).get(col, to_update.column(col)[i].as_py())
                    for i, nk in enumerate(nk_list)
                ])
            else:
                updated[col] = to_update.column(col)
        return updated

    def rotate_attributes(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        nk_col: str,
        rotations_view: str,
        column_pairs: list[tuple[str, str]],
    ) -> int:
        """Rotate SCD3 current/previous column pairs via Iceberg overwrite."""
        try:
            import pyarrow as pa
            import pyarrow.compute as pc
        except ImportError as exc:
            raise ImportError("IcebergSink requires pyarrow") from exc

        iceberg_table = self._catalog.load_table(f"{self._namespace}.{table_name}")
        existing = iceberg_table.scan().to_arrow()

        curr_cols = [curr for curr, _ in column_pairs]
        rotations_df = con.execute(
            f"SELECT {nk_col}, {', '.join(curr_cols)} FROM {rotations_view}"
        ).fetchdf()
        rot_map = {
            row[nk_col]: {c: row[c] for c in curr_cols}
            for _, row in rotations_df.iterrows()
        }
        nk_list   = list(rot_map.keys())
        mask      = pc.is_in(existing[nk_col], value_set=pa.array(nk_list))
        to_rot    = existing.filter(mask)
        unchanged = existing.filter(pc.invert(mask))

        updated_cols = self._build_rotation_cols(to_rot, nk_col, column_pairs, rot_map)
        new_table = pa.concat_tables([unchanged, pa.table(updated_cols)])
        iceberg_table.overwrite(new_table)
        return len(to_rot)

    @staticmethod
    def _rotation_col_value(col: str, nk_list: list, to_rot, rot_map: dict) -> list:
        import pyarrow as pa
        return pa.array([
            rot_map.get(nk, {}).get(col, to_rot.column(col)[i].as_py())
            for i, nk in enumerate(nk_list)
        ])

    @staticmethod
    def _rotation_name_maps(column_pairs: list[tuple[str, str]]) -> tuple[set, dict]:
        curr_names = {curr for curr, _ in column_pairs}
        prev_names = {prev: curr for curr, prev in column_pairs}
        return curr_names, prev_names

    def _build_rotation_cols(
        self, to_rot, nk_col: str, column_pairs: list[tuple[str, str]], rot_map: dict
    ) -> dict:
        curr_names, prev_names = self._rotation_name_maps(column_pairs)
        nk_list = to_rot.column(nk_col).to_pylist()
        updated: dict = {}
        for col in to_rot.schema.names:
            if col in prev_names:
                updated[col] = to_rot.column(prev_names[col])
            elif col in curr_names:
                updated[col] = self._rotation_col_value(col, nk_list, to_rot, rot_map)
            else:
                updated[col] = to_rot.column(col)
        return updated

    def update_milestones(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        match_col: str,
        updates_view: str,
        milestone_cols: list[str],
    ) -> int:
        """Fill NULL milestone timestamps for accumulating snapshots."""
        try:
            import pyarrow as pa
            import pyarrow.compute as pc
        except ImportError as exc:
            raise ImportError("IcebergSink requires pyarrow") from exc

        iceberg_table = self._catalog.load_table(f"{self._namespace}.{table_name}")
        existing = iceberg_table.scan().to_arrow()

        updates_df = con.execute(
            f"SELECT {match_col}, {', '.join(milestone_cols)} FROM {updates_view}"
        ).fetchdf()
        upd_map = {
            row[match_col]: {c: row[c] for c in milestone_cols}
            for _, row in updates_df.iterrows()
        }
        match_list = list(upd_map.keys())
        mask       = pc.is_in(existing[match_col], value_set=pa.array(match_list))
        to_upd     = existing.filter(mask)
        unchanged  = existing.filter(pc.invert(mask))

        updated_cols = self._build_milestone_cols(to_upd, match_col, milestone_cols, upd_map)
        new_table = pa.concat_tables([unchanged, pa.table(updated_cols)])
        iceberg_table.overwrite(new_table)
        return len(to_upd)

    def _build_milestone_cols(
        self, to_upd, match_col: str, milestone_cols: list[str], upd_map: dict
    ) -> dict:
        import pyarrow as pa
        mk_list = to_upd.column(match_col).to_pylist()
        updated: dict = {}
        for col in to_upd.schema.names:
            if col in milestone_cols:
                updated[col] = pa.array([
                    self._milestone_val(to_upd.column(col)[i].as_py(), upd_map.get(mk, {}).get(col))
                    for i, mk in enumerate(mk_list)
                ])
            else:
                updated[col] = to_upd.column(col)
        return updated

    @staticmethod
    def _milestone_val(existing_val, new_val):
        return new_val if (existing_val is None and new_val is not None) else existing_val

    # ── Context manager ───────────────────────────────────────────────────

    def __enter__(self) -> "IcebergSink":
        self._catalog = self._load_catalog()
        self._con = duckdb.connect()
        try:
            self._con.execute("LOAD iceberg")
        except Exception:
            self._con.execute("INSTALL iceberg; LOAD iceberg")
        return self

    def __exit__(self, *_) -> None:
        if self._con:
            self._con.close()
            self._con = None
        self._catalog = None

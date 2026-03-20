"""IcebergSink rotation and milestone helper methods.

Extracted from iceberg.py to keep file sizes manageable.
These methods are mixed into :class:`IcebergSink` via inheritance.
"""

from __future__ import annotations


import duckdb


class _IcebergMutateMixin:
    """Column-rotation and milestone update helpers for IcebergSink."""

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
        self._check_table_row_count(iceberg_table, table_name)
        existing = iceberg_table.scan().to_arrow()

        curr_cols = [curr for curr, _ in column_pairs]
        rotations_df = con.execute(
            f"SELECT {nk_col}, {', '.join(curr_cols)} FROM {rotations_view}"
        ).fetchdf()
        rot_map = {
            row[nk_col]: {c: row[c] for c in curr_cols}
            for _, row in rotations_df.iterrows()
        }
        nk_list = list(rot_map.keys())
        mask = pc.is_in(existing[nk_col], value_set=pa.array(nk_list))
        to_rot = existing.filter(mask)
        unchanged = existing.filter(pc.invert(mask))

        updated_cols = self._build_rotation_cols(to_rot, nk_col, column_pairs, rot_map)
        new_table = pa.concat_tables([unchanged, pa.table(updated_cols)])
        iceberg_table.overwrite(new_table)
        return len(to_rot)

    @staticmethod
    def _rotation_col_value(col: str, nk_list: list, to_rot, rot_map: dict) -> list:
        """Return a PyArrow array with the rotated values for a single column.

        For each row keyed by *nk_list*, the new value comes from *rot_map* if
        present; otherwise the existing value from *to_rot* is preserved.
        """
        import pyarrow as pa

        return pa.array(
            [
                rot_map.get(nk, {}).get(col, to_rot.column(col)[i].as_py())
                for i, nk in enumerate(nk_list)
            ]
        )

    @staticmethod
    def _rotation_name_maps(column_pairs: list[tuple[str, str]]) -> tuple[set, dict]:
        """Derive lookup structures from *column_pairs* for SCD3 rotation.

        Returns a set of current column names and a dict mapping each previous
        column name to its corresponding current column name.
        """
        curr_names = {curr for curr, _ in column_pairs}
        prev_names = {prev: curr for curr, prev in column_pairs}
        return curr_names, prev_names

    def _build_rotation_cols(
        self, to_rot, nk_col: str, column_pairs: list[tuple[str, str]], rot_map: dict
    ) -> dict:
        """Build a column dict that applies SCD3 rotations to *to_rot*.

        Previous columns receive the old value of their paired current column;
        current columns receive the fresh value from *rot_map*; all other
        columns are passed through unchanged.
        """
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
        self._check_table_row_count(iceberg_table, table_name)
        existing = iceberg_table.scan().to_arrow()

        updates_df = con.execute(
            f"SELECT {match_col}, {', '.join(milestone_cols)} FROM {updates_view}"
        ).fetchdf()
        upd_map = {
            row[match_col]: {c: row[c] for c in milestone_cols}
            for _, row in updates_df.iterrows()
        }
        match_list = list(upd_map.keys())
        mask = pc.is_in(existing[match_col], value_set=pa.array(match_list))
        to_upd = existing.filter(mask)
        unchanged = existing.filter(pc.invert(mask))

        updated_cols = self._build_milestone_cols(
            to_upd, match_col, milestone_cols, upd_map
        )
        new_table = pa.concat_tables([unchanged, pa.table(updated_cols)])
        iceberg_table.overwrite(new_table)
        return len(to_upd)

    def _build_milestone_cols(
        self, to_upd, match_col: str, milestone_cols: list[str], upd_map: dict
    ) -> dict:
        """Build a column dict patching NULL milestone values in *to_upd*.

        For each row keyed by *match_col*, milestone columns receive the new
        value from *upd_map* only when the existing value is ``None``; already-
        stamped columns and non-milestone columns are passed through intact.
        """
        import pyarrow as pa

        mk_list = to_upd.column(match_col).to_pylist()
        updated: dict = {}
        for col in to_upd.schema.names:
            if col in milestone_cols:
                updated[col] = pa.array(
                    [
                        self._milestone_val(
                            to_upd.column(col)[i].as_py(), upd_map.get(mk, {}).get(col)
                        )
                        for i, mk in enumerate(mk_list)
                    ]
                )
            else:
                updated[col] = to_upd.column(col)
        return updated

    @staticmethod
    def _milestone_val(existing_val, new_val):
        """Return *new_val* only when *existing_val* is None, else keep *existing_val*."""
        return (
            new_val if (existing_val is None and new_val is not None) else existing_val
        )

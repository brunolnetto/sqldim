"""SCD metadata processor write helper methods.

Extracted from _lazy_metadata.py to keep file sizes manageable.
These methods are mixed into :class:`LazySCDMetadataProcessor` via inheritance.
"""

from __future__ import annotations


class _MetadataWriteMixin:
    """Write helper methods for LazySCDMetadataProcessor."""

    def _nk_join_fragments(self) -> tuple:
        """Return (nk_d_sel, join_cnd, nk_cl_sel, cl_join) built from *_nk_cols*."""
        cols = self._nk_cols
        return (
            ", ".join(f"d.{c}" for c in cols),
            " AND ".join(f"d.{c} = c.{c}" for c in cols),
            ", ".join(f"cl.{c}" for c in cols),
            " AND ".join(f"cl.{c} = ms.{c}" for c in cols),
        )

    def _write_changed(self, table_name: str, now: str, count: int) -> int:
        """Expire changed rows and insert new versions with ``metadata_diff``."""
        from sqldim.core.kimball.dimensions.scd.processors.lazy.metadata._lazy_metadata import (
            _as_subquery,
        )

        if count == 0:
            return 0
        nk_select = ", ".join(self._nk_cols)
        nk_d_sel, join_cnd, nk_cl_sel, cl_join = self._nk_join_fragments()

        # Step 1 — materialise changed NKs (small subset of classified)
        self._con.execute(f"""
            CREATE OR REPLACE TABLE changed_nks AS
            SELECT {nk_select}
            FROM classified
            WHERE _scd_class = 'changed'
        """)

        # Step 2 — fetch old metadata only for changed NKs (targeted PG read)
        db_sql = self.sink.current_state_sql(table_name)
        self._con.execute(f"""
            CREATE OR REPLACE TABLE old_metadata_snapshot AS
            SELECT {nk_d_sel},
                   d.metadata AS _old_metadata
            FROM {_as_subquery(db_sql)} d
            JOIN changed_nks c
              ON {join_cnd}
            WHERE d.is_current = TRUE
        """)

        # Step 3 — expire old versions
        self.sink.close_versions(
            self._con, table_name, self._nk_cols, "changed_nks", now
        )
        self._con.execute("DROP TABLE IF EXISTS changed_nks")

        # Step 4 — insert new versions with metadata_diff = old snapshot
        cols = self._nk_cols + self._EXTRA_COLS
        self._con.execute(f"""
            CREATE OR REPLACE VIEW new_versions AS
            SELECT {nk_cl_sel},
                   '{now}'::TIMESTAMPTZ  AS valid_from,
                   NULL                  AS valid_to,
                   TRUE                  AS is_current,
                   cl._metadata          AS metadata,
                   ms._old_metadata      AS metadata_diff,
                   cl._row_hash          AS row_hash
            FROM classified cl
            JOIN old_metadata_snapshot ms
              ON {cl_join}
            WHERE cl._scd_class = 'changed'
        """)
        written = self.sink.write_named(
            self._con, "new_versions", table_name, cols, self.batch_size
        )
        self._con.execute("DROP TABLE IF EXISTS old_metadata_snapshot")
        return written

    # ── Streaming helpers ─────────────────────────────────────────────────

    def _register_source_from_sql(self, sql_fragment: str) -> None:
        """Materialise *sql_fragment* as the ``incoming`` TABLE (streaming variant).

        Identical to :meth:`_register_source` but accepts a pre-resolved SQL
        string instead of a source object.  Used by :meth:`process_stream` to
        bypass :func:`~sqldim.sources.coerce_source`.
        """
        nk_select = ", ".join(self._nk_cols)

        if self._meta_cols:
            pairs = ", ".join(f"{c} := {c}" for c in self._meta_cols)
            meta_sql = f"to_json(struct_pack({pairs}))"
        else:
            meta_sql = "'{}'"

        nk_not_null = " AND ".join(f"{c} IS NOT NULL" for c in self._nk_cols)

        self._con.execute(f"""
            CREATE OR REPLACE TABLE incoming AS
            WITH _src AS (
                SELECT {nk_select},
                       {meta_sql} AS _metadata
                FROM ({sql_fragment})
                WHERE {nk_not_null}
            )
            SELECT {nk_select},
                   _metadata,
                   md5(cast(_metadata AS varchar)) AS _row_hash
            FROM _src
        """)

    def _update_local_hashes_after_batch(self) -> None:
        """Sync local current_hashes TABLE with rows written in this batch.

        Enables correct cross-batch NK detection without re-scanning the
        remote sink.
        """
        nk_select = ", ".join(self._nk_cols)
        nk_match = " AND ".join(f"ch.{c} = cl.{c}" for c in self._nk_cols)
        self._con.execute(f"""
            INSERT INTO current_hashes ({nk_select}, _row_hash)
            SELECT {nk_select}, _row_hash
            FROM classified
            WHERE _scd_class = 'new'
        """)
        self._con.execute(f"""
            UPDATE current_hashes ch
            SET _row_hash = cl._row_hash
            FROM classified cl
            WHERE {nk_match}
            AND cl._scd_class = 'changed'
        """)

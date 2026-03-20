"""SCD Type 6 lazy processor built on DuckDB SQL.

:class:`LazyType6Processor` applies Type-1 overwrite for slow-changing
attributes and Type-2 versioning for fast-changing ones.
Delegates all storage to a :class:`SinkAdapter`.
"""

from __future__ import annotations

from sqldim.core.kimball.dimensions.scd.handler import SCDResult


def _as_subquery(sql: str) -> str:
    """Wrap *sql* for use in a DuckDB FROM clause (see _lazy_type2 for details)."""
    return f"({sql})" if sql.strip().upper().startswith("SELECT") else sql


class LazyType6Processor:
    """
    SCD Type 6 (hybrid) processor.

    ``type1_columns`` — overwritten in place when changed (no new row).
    ``type2_columns`` — trigger a new versioned row when changed.

    When only type1 columns change: UPDATE existing row.
    When any type2 column changes: close current row, insert new version.
    When both change simultaneously: Type 2 behaviour for the whole row
    (new version carries the updated type1 values too).

    Uses only DuckDB SQL; storage delegated to SinkAdapter.
    """

    def __init__(
        self,
        natural_key: str,
        type1_columns: list[str],
        type2_columns: list[str],
        sink,
        batch_size: int = 100_000,
        con=None,
    ):
        import duckdb as _duckdb

        self.natural_key = natural_key
        self.type1_columns = sorted(type1_columns)
        self.type2_columns = sorted(type2_columns)
        self.sink = sink
        self.batch_size = batch_size
        self._con = con or _duckdb.connect()

    def process(self, source, table_name: str) -> SCDResult:
        """Detect type1/type2 changes and apply correct versioning strategy.

        Type-1-only changes are applied in place; type-2 changes close the
        current row and insert a new version.  Returns an
        :class:`~sqldim.scd.handler.SCDResult` with row counts.
        """
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc).isoformat()

        self._register_source(source)
        self._register_current_state(table_name)
        self._classify()

        result = SCDResult()
        result.inserted = self._write_new(table_name, now)
        result.versioned = self._write_type2_changed(table_name, now)
        result.versioned += self._apply_type1_only(table_name)
        result.unchanged = self._count_unchanged()
        self._con.execute("DROP VIEW IF EXISTS incoming")
        self._con.execute("DROP TABLE IF EXISTS current_state")
        self._con.execute("DROP TABLE IF EXISTS classified")
        return result

    def _register_source(self, source) -> None:
        """Register *source* as a DuckDB view with separate Type-1 and Type-2 checksums.

        Two MD5 columns (``_t1_checksum``, ``_t2_checksum``) are computed so
        the classifier can distinguish which change type applies per row.
        """
        from sqldim.sources import coerce_source

        _sql = coerce_source(source).as_sql(self._con)
        t1_hash = " || '|' || ".join(
            f"coalesce(cast({c} as varchar), '')" for c in self.type1_columns
        )
        t2_hash = " || '|' || ".join(
            f"coalesce(cast({c} as varchar), '')" for c in self.type2_columns
        )
        self._con.execute(f"""
            CREATE OR REPLACE VIEW incoming AS
            SELECT *,
                   md5({t1_hash}) AS _t1_checksum,
                   md5({t2_hash}) AS _t2_checksum
            FROM {_as_subquery(_sql)}
        """)

    def _register_current_state(self, table_name: str) -> None:
        nk = self.natural_key
        sql = self.sink.current_state_sql(table_name)
        t1_hash = " || '|' || ".join(
            f"coalesce(cast({c} as varchar), '')" for c in self.type1_columns
        )
        t2_hash = " || '|' || ".join(
            f"coalesce(cast({c} as varchar), '')" for c in self.type2_columns
        )
        self._con.execute(f"""
            CREATE OR REPLACE TABLE current_state AS
            SELECT {nk},
                   md5({t1_hash}) AS _t1_checksum,
                   md5({t2_hash}) AS _t2_checksum
            FROM {_as_subquery(sql)}
            WHERE is_current = TRUE
        """)

    def _classify(self) -> None:
        """Classify each row as ``new``, ``type2_changed``, ``type1_only``, or ``unchanged``."""
        nk = self.natural_key
        self._con.execute(f"""
            CREATE OR REPLACE TABLE classified AS
            SELECT
                i.*,
                CASE
                    WHEN c.{nk} IS NULL                                          THEN 'new'
                    WHEN i._t2_checksum != c._t2_checksum                        THEN 'type2_changed'
                    WHEN i._t2_checksum  = c._t2_checksum
                     AND i._t1_checksum != c._t1_checksum                        THEN 'type1_only'
                    ELSE                                                               'unchanged'
                END AS _scd_class
            FROM incoming i
            LEFT JOIN current_state c
                   ON cast(i.{nk} as varchar) = cast(c.{nk} as varchar)
        """)

    def _write_new(self, table_name: str, now: str) -> int:
        """Insert brand-new natural keys as SCD-2 rows into *table_name*.

        All type-1 and type-2 columns are written; ``valid_from`` is set to
        *now* with ``is_current = TRUE``.
        """
        all_cols = self.type1_columns + self.type2_columns
        full_hash = " || '|' || ".join(
            f"coalesce(cast({c} as varchar), '')" for c in all_cols
        )
        self._con.execute(f"""
            CREATE OR REPLACE VIEW new_rows AS
            SELECT * EXCLUDE (_scd_class, _t1_checksum, _t2_checksum),
                   '{now}'::varchar AS valid_from,
                   NULL             AS valid_to,
                   TRUE             AS is_current,
                   md5({full_hash}) AS checksum
            FROM classified
            WHERE _scd_class = 'new'
        """)
        return self.sink.write(self._con, "new_rows", table_name, self.batch_size)

    def _write_type2_changed(self, table_name: str, now: str) -> int:
        """Close current row and insert a new version for type-2 attribute changes.

        Calls :meth:`SinkAdapter.close_versions` then writes a new row with
        the updated attribute values and ``is_current = TRUE``.
        """
        nk = self.natural_key
        all_cols = self.type1_columns + self.type2_columns
        full_hash = " || '|' || ".join(
            f"coalesce(cast({c} as varchar), '')" for c in all_cols
        )
        self._con.execute(f"""
            CREATE OR REPLACE VIEW t2_nks AS
            SELECT {nk} FROM classified WHERE _scd_class = 'type2_changed'
        """)
        self.sink.close_versions(self._con, table_name, nk, "t2_nks", now)
        self._con.execute(f"""
            CREATE OR REPLACE VIEW new_versions AS
            SELECT * EXCLUDE (_scd_class, _t1_checksum, _t2_checksum),
                   '{now}'::varchar AS valid_from,
                   NULL             AS valid_to,
                   TRUE             AS is_current,
                   md5({full_hash}) AS checksum
            FROM classified
            WHERE _scd_class = 'type2_changed'
        """)
        return self.sink.write(self._con, "new_versions", table_name, self.batch_size)

    def _apply_type1_only(self, table_name: str) -> int:
        """Overwrite type-1 columns for rows where only type-1 attributes changed.

        No version row is created; the existing current row is updated via
        :meth:`SinkAdapter.update_attributes`.
        """
        nk = self.natural_key
        t1_cols = self.type1_columns
        col_list = ", ".join(t1_cols)
        self._con.execute(f"""
            CREATE OR REPLACE VIEW type1_updates AS
            SELECT {nk}, {col_list}
            FROM classified
            WHERE _scd_class = 'type1_only'
        """)
        return self.sink.update_attributes(
            self._con, table_name, nk, "type1_updates", t1_cols
        )

    def _count_unchanged(self) -> int:
        """Return the count of rows with no type-1 or type-2 attribute changes."""
        return self._con.execute(
            "SELECT count(*) FROM classified WHERE _scd_class = 'unchanged'"
        ).fetchone()[0]

    def _update_local_state_after_batch(self) -> None:
        """Sync the local ``current_state`` TABLE with rows written in this batch."""
        nk = self.natural_key
        # Add new rows (both checksum columns).
        self._con.execute(f"""
            INSERT INTO current_state ({nk}, _t1_checksum, _t2_checksum)
            SELECT {nk}, _t1_checksum, _t2_checksum
            FROM classified
            WHERE _scd_class = 'new'
        """)
        # type2_changed: new version inserted with both checksums updated.
        self._con.execute(f"""
            UPDATE current_state cs
            SET _t1_checksum = cl._t1_checksum,
                _t2_checksum = cl._t2_checksum
            FROM classified cl
            WHERE cs.{nk} = cl.{nk}
            AND cl._scd_class = 'type2_changed'
        """)
        # type1_only: only the type-1 checksum changed (updated in place).
        self._con.execute(f"""
            UPDATE current_state cs
            SET _t1_checksum = cl._t1_checksum
            FROM classified cl
            WHERE cs.{nk} = cl.{nk}
            AND cl._scd_class = 'type1_only'
        """)

    # ── streaming helpers ──────────────────────────────────────────────────

    def _register_source_from_sql(self, sql_fragment: str) -> None:
        """Register an already-resolved SQL fragment as the ``incoming`` view.

        Computes separate type-1 and type-2 checksum columns so the Type-6
        classifier can distinguish which kind of change applies per row.
        """
        t1_hash = " || '|' || ".join(
            f"coalesce(cast({c} as varchar), '')" for c in self.type1_columns
        )
        t2_hash = " || '|' || ".join(
            f"coalesce(cast({c} as varchar), '')" for c in self.type2_columns
        )
        self._con.execute(f"""
            CREATE OR REPLACE VIEW incoming AS
            SELECT *,
                   md5({t1_hash}) AS _t1_checksum,
                   md5({t2_hash}) AS _t2_checksum
            FROM ({sql_fragment})
        """)

    def _drop_stream_views(self) -> None:
        """Clean up per-batch working views and tables between micro-batches.

        ``current_state`` TABLE is bootstrapped once before the batch loop
        and dropped by :meth:`process_stream` after all batches complete.
        """
        for v in [
            "incoming",
            "new_rows",
            "new_versions",
            "t2_nks",
            "type1_updates",
        ]:
            try:
                self._con.execute(f"DROP VIEW IF EXISTS {v}")
            except Exception:
                pass
        try:
            self._con.execute("DROP TABLE IF EXISTS classified")
        except Exception:
            pass

    def _resolve_dedup_sql(
        self, sql_fragment: str, deduplicate_by: "str | None"
    ) -> str:
        """Return *sql_fragment* wrapped in a deduplication QUALIFY clause when requested."""
        if not deduplicate_by:
            return sql_fragment
        nk = self.natural_key
        return (
            f"SELECT * FROM ({sql_fragment})"
            f" QUALIFY ROW_NUMBER() OVER ("
            f"PARTITION BY {nk} ORDER BY {deduplicate_by} DESC) = 1"
        )

    def _run_stream_batch(
        self, i, dedup_sql, table_name, now, on_batch, source, result, _log
    ):
        """Process one micro-batch inside the streaming loop (Type 6)."""
        from sqldim.core.kimball.dimensions.scd.handler import SCDResult as _SCDResult

        try:
            self._register_source_from_sql(dedup_sql)
            self._classify()

            batch_result = _SCDResult()
            batch_result.inserted = self._write_new(table_name, now)
            batch_result.versioned = self._write_type2_changed(table_name, now)
            batch_result.versioned += self._apply_type1_only(table_name)
            batch_result.unchanged = self._count_unchanged()

            self._update_local_state_after_batch()

            source.commit(source.checkpoint())
            result.accumulate(batch_result)
            result.batches_processed += 1

            if on_batch:
                on_batch(i, batch_result)
        except Exception as exc:
            result.batches_failed += 1
            _log.error("Batch %d failed for table %s: %s", i, table_name, exc)
        finally:
            self._drop_stream_views()

    def process_stream(
        self,
        source,
        table_name: str,
        batch_size: int = 10_000,
        max_batches: int | None = None,
        on_batch=None,
        deduplicate_by: str | None = None,
    ):
        """Process a streaming source micro-batch by micro-batch (Type 6 — hybrid).

        Returns
        -------
        :class:`~sqldim.sources.stream.StreamResult`
        """
        import logging
        from datetime import datetime, timezone
        from sqldim.sources.stream import StreamResult

        _log = logging.getLogger(__name__)
        result = StreamResult()

        self._register_current_state(table_name)

        for i, sql_fragment in enumerate(source.stream(self._con, batch_size)):
            if max_batches is not None and i >= max_batches:
                break
            now = datetime.now(timezone.utc).isoformat()
            self._run_stream_batch(
                i,
                self._resolve_dedup_sql(sql_fragment, deduplicate_by),
                table_name,
                now,
                on_batch,
                source,
                result,
                _log,
            )

        self._con.execute("DROP TABLE IF EXISTS current_state")
        return result

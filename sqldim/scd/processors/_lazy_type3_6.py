"""SCD Type 3 and Type 6 lazy processors built on DuckDB SQL.

:class:`LazyType3Processor` rotates ``(current_col, previous_col)`` pairs
in place.  :class:`LazyType6Processor` applies Type-1 overwrite for slow-
changing attributes and Type-2 versioning for fast-changing ones.  Both
delegate all storage to a :class:`SinkAdapter`.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import narwhals as nw

from sqldim.scd.handler import SCDResult


def _as_subquery(sql: str) -> str:
    """Wrap *sql* for use in a DuckDB FROM clause (see _lazy_type2 for details)."""
    return f"({sql})" if sql.strip().upper().startswith("SELECT") else sql


class LazyType3Processor:
    """
    SCD Type 3 processor. Rotates columns: previous_col = current_col,
    current_col = incoming value. Only the current and one prior value
    are preserved.

    ``column_pairs`` is a list of (current_col, previous_col) tuples,
    e.g. [("region", "prev_region"), ("manager", "prev_manager")].

    Uses only DuckDB SQL; storage delegated to SinkAdapter.
    """

    def __init__(
        self,
        natural_key: str,
        column_pairs: list[tuple[str, str]],
        sink,
        batch_size: int = 100_000,
        con=None,
    ):
        import duckdb as _duckdb

        self.natural_key  = natural_key
        self.column_pairs = column_pairs          # [(curr, prev), ...]
        self.track_cols   = [c for c, _ in column_pairs]
        self.sink         = sink
        self.batch_size   = batch_size
        self._con         = con or _duckdb.connect()

    def process(self, source, table_name: str) -> SCDResult:
        """Detect changes and rotate column history; insert new rows into *table_name*.

        Returns an :class:`~sqldim.scd.handler.SCDResult` with inserted /
        versioned / unchanged row counts.
        """
        self._register_source(source)
        self._register_current_checksums(table_name)
        self._classify()

        result           = SCDResult()
        result.inserted  = self._write_new(table_name)
        result.versioned = self._rotate_changed(table_name)
        result.unchanged = self._count_unchanged()
        self._con.execute("DROP VIEW IF EXISTS incoming")
        self._con.execute("DROP TABLE IF EXISTS current_checksums")
        self._con.execute("DROP TABLE IF EXISTS classified")
        return result

    def _register_source(self, source) -> None:
        """Register *source* as a DuckDB ``incoming`` view with an MD5 ``_checksum`` column.

        The checksum is computed over all ``track_cols`` so unchanged rows
        are never re-processed by the SCD logic.
        """
        from sqldim.sources import coerce_source
        _sql = coerce_source(source).as_sql(self._con)
        cols = " || '|' || ".join(
            f"coalesce(cast({c} as varchar), '')"
            for c in self.track_cols
        )
        self._con.execute(f"""
            CREATE OR REPLACE VIEW incoming AS
            SELECT *,
                   md5({cols}) AS _checksum
            FROM ({_sql})
        """)

    def _register_current_checksums(self, table_name: str) -> None:
        """Materialise a slim (NK + checksum) TABLE from the sink's current state."""
        nk = self.natural_key
        sql = self.sink.current_state_sql(table_name)
        self._con.execute(f"""
            CREATE OR REPLACE TABLE current_checksums AS
            SELECT {nk}, checksum AS _checksum
            FROM {_as_subquery(sql)}
            WHERE is_current = TRUE
        """)

    def _classify(self) -> None:
        """Classify each incoming row as ``new``, ``changed``, or ``unchanged``.

        Results land in a ``classified`` DuckDB table used by all downstream
        writer methods.
        """
        nk = self.natural_key
        self._con.execute(f"""
            CREATE OR REPLACE TABLE classified AS
            SELECT
                i.*,
                CASE
                    WHEN c.{nk}       IS NULL           THEN 'new'
                    WHEN i._checksum != c._checksum      THEN 'changed'
                    ELSE                                      'unchanged'
                END AS _scd_class
            FROM incoming i
            LEFT JOIN current_checksums c
                   ON cast(i.{nk} as varchar) = cast(c.{nk} as varchar)
        """)

    def _write_new(self, table_name: str) -> int:
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc).isoformat()
        # For new rows, previous columns are NULL
        prev_nulls = ", ".join(
            f"NULL::varchar AS {prev}" for _, prev in self.column_pairs
        )
        self._con.execute(f"""
            CREATE OR REPLACE VIEW new_rows AS
            SELECT * EXCLUDE (_scd_class, _checksum),
                   {prev_nulls},
                   '{now}'::varchar AS valid_from,
                   NULL             AS valid_to,
                   TRUE             AS is_current,
                   _checksum        AS checksum
            FROM classified
            WHERE _scd_class = 'new'
        """)
        return self.sink.write(self._con, "new_rows", table_name, self.batch_size)

    def _rotate_changed(self, table_name: str) -> int:
        """Apply column rotation to rows where tracked attributes changed.

        Delegates to :meth:`SinkAdapter.rotate_attributes`, which executes
        a single ``UPDATE … SET prev_col = curr_col, curr_col = incoming``.
        """
        nk = self.natural_key
        curr_cols = ", ".join(f"{c}" for c, _ in self.column_pairs)
        self._con.execute(f"""
            CREATE OR REPLACE VIEW changed_rotations AS
            SELECT {nk}, {curr_cols}
            FROM classified
            WHERE _scd_class = 'changed'
        """)
        return self.sink.rotate_attributes(
            self._con, table_name, nk, "changed_rotations", self.column_pairs
        )

    def _count_unchanged(self) -> int:
        """Return the count of rows that had no attribute changes."""
        return self._con.execute(
            "SELECT count(*) FROM classified WHERE _scd_class = 'unchanged'"
        ).fetchone()[0]
    def _update_local_fingerprint_after_batch(self) -> None:
        """Sync the local ``current_checksums`` TABLE with rows written in this batch."""
        nk = self.natural_key
        self._con.execute(f"""
            INSERT INTO current_checksums ({nk}, _checksum)
            SELECT {nk}, _checksum
            FROM classified
            WHERE _scd_class = 'new'
        """)
        self._con.execute(f"""
            UPDATE current_checksums cs
            SET _checksum = cl._checksum
            FROM classified cl
            WHERE cs.{nk} = cl.{nk}
            AND cl._scd_class = 'changed'
        """)
    # ── streaming helpers ──────────────────────────────────────────────────

    def _register_source_from_sql(self, sql_fragment: str) -> None:
        """Register an already-resolved SQL fragment as the ``incoming`` view."""
        cols = " || '|' || ".join(
            f"coalesce(cast({c} as varchar), '')"
            for c in self.track_cols
        )
        self._con.execute(f"""
            CREATE OR REPLACE VIEW incoming AS
            SELECT *, md5({cols}) AS _checksum
            FROM ({sql_fragment})
        """)

    def _drop_stream_views(self) -> None:
        """Clean up per-batch working views and tables between micro-batches.

        ``current_checksums`` TABLE is bootstrapped once before the batch loop
        and dropped by :meth:`process_stream` after all batches complete.
        """
        for v in [
            "incoming",
            "new_rows", "changed_rotations",
        ]:
            try:
                self._con.execute(f"DROP VIEW IF EXISTS {v}")
            except Exception:
                pass
        try:
            self._con.execute("DROP TABLE IF EXISTS classified")
        except Exception:
            pass

    def process_stream(
        self,
        source,
        table_name: str,
        batch_size: int = 10_000,
        max_batches: int | None = None,
        on_batch=None,
        deduplicate_by: str | None = None,
    ):
        """Process a streaming source micro-batch by micro-batch (Type 3 — rotate).

        Returns
        -------
        :class:`~sqldim.sources.stream.StreamResult`
        """
        import logging
        from sqldim.sources.stream import StreamResult
        from sqldim.scd.handler import SCDResult as _SCDResult

        _log = logging.getLogger(__name__)
        result = StreamResult()

        # Phase 1: Bootstrap slim fingerprint once — remote source scanned exactly once.
        self._register_current_checksums(table_name)

        # Phase 2: Stream batches, each classified against the local TABLE.
        for i, sql_fragment in enumerate(source.stream(self._con, batch_size)):
            if max_batches is not None and i >= max_batches:
                break

            if deduplicate_by:
                nk = self.natural_key
                dedup_sql = (
                    f"SELECT * FROM ({sql_fragment})"
                    f" QUALIFY ROW_NUMBER() OVER ("
                    f"PARTITION BY {nk} ORDER BY {deduplicate_by} DESC) = 1"
                )
            else:
                dedup_sql = sql_fragment

            try:
                self._register_source_from_sql(dedup_sql)
                self._classify()

                batch_result           = _SCDResult()
                batch_result.inserted  = self._write_new(table_name)
                batch_result.versioned = self._rotate_changed(table_name)
                batch_result.unchanged = self._count_unchanged()

                # Keep local fingerprint in sync for cross-batch NK detection.
                self._update_local_fingerprint_after_batch()

                offset = source.checkpoint()
                source.commit(offset)

                result.accumulate(batch_result)
                result.batches_processed += 1

                if on_batch:
                    on_batch(i, batch_result)

            except Exception as exc:
                result.batches_failed += 1
                _log.error("Batch %d failed for table %s: %s", i, table_name, exc)

            finally:
                self._drop_stream_views()

        # Phase 3: Drop bootstrap TABLE after all batches.
        self._con.execute("DROP TABLE IF EXISTS current_checksums")
        return result


# ---------------------------------------------------------------------------
# LazyType6Processor  (SCD Type 6 — hybrid Type 1 + Type 2)
# ---------------------------------------------------------------------------

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

        self.natural_key    = natural_key
        self.type1_columns  = sorted(type1_columns)
        self.type2_columns  = sorted(type2_columns)
        self.sink           = sink
        self.batch_size     = batch_size
        self._con           = con or _duckdb.connect()

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

        result           = SCDResult()
        result.inserted  = self._write_new(table_name, now)
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
        nk = self.natural_key
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
            "new_rows", "new_versions", "t2_nks", "type1_updates",
        ]:
            try:
                self._con.execute(f"DROP VIEW IF EXISTS {v}")
            except Exception:
                pass
        try:
            self._con.execute("DROP TABLE IF EXISTS classified")
        except Exception:
            pass

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
        from sqldim.scd.handler import SCDResult as _SCDResult

        _log = logging.getLogger(__name__)
        result = StreamResult()

        # Phase 1: Bootstrap slim fingerprint once — remote source scanned exactly once.
        self._register_current_state(table_name)

        # Phase 2: Stream batches, each classified against the local TABLE.
        for i, sql_fragment in enumerate(source.stream(self._con, batch_size)):
            if max_batches is not None and i >= max_batches:
                break

            now = datetime.now(timezone.utc).isoformat()

            if deduplicate_by:
                nk = self.natural_key
                dedup_sql = (
                    f"SELECT * FROM ({sql_fragment})"
                    f" QUALIFY ROW_NUMBER() OVER ("
                    f"PARTITION BY {nk} ORDER BY {deduplicate_by} DESC) = 1"
                )
            else:
                dedup_sql = sql_fragment

            try:
                self._register_source_from_sql(dedup_sql)
                self._classify()

                batch_result           = _SCDResult()
                batch_result.inserted  = self._write_new(table_name, now)
                batch_result.versioned = self._write_type2_changed(table_name, now)
                batch_result.versioned += self._apply_type1_only(table_name)
                batch_result.unchanged = self._count_unchanged()

                # Keep local state in sync for cross-batch NK detection.
                self._update_local_state_after_batch()

                offset = source.checkpoint()
                source.commit(offset)

                result.accumulate(batch_result)
                result.batches_processed += 1

                if on_batch:
                    on_batch(i, batch_result)

            except Exception as exc:
                result.batches_failed += 1
                _log.error("Batch %d failed for table %s: %s", i, table_name, exc)

            finally:
                self._drop_stream_views()

        # Phase 3: Drop bootstrap TABLE after all batches.
        self._con.execute("DROP TABLE IF EXISTS current_state")
        return result


# ---------------------------------------------------------------------------
# LazyType4Processor  (SCD Type 4 — mini-dimension split)
# ---------------------------------------------------------------------------


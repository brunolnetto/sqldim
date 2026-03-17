"""SCD Type 2 and Type 1 lazy processors built on DuckDB SQL.

:class:`LazySCDProcessor` implements the full SCD-2 pipeline
(detect → close old versions → insert new versions) using only
DuckDB SQL statements.  :class:`LazyType1Processor` overwrites
attributes in place.  Both delegate storage to a :class:`SinkAdapter`.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import narwhals as nw

from sqldim.scd.handler import SCDResult


def _as_subquery(sql: str) -> str:
    """Wrap *sql* for use in a DuckDB FROM clause.

    DuckDB 1.5+ rejects ``FROM (table_function())`` — parentheses are only
    allowed around subqueries (``SELECT …``).  Table-function calls and bare
    table/view names must appear directly in the FROM clause.
    """
    return f"({sql})" if sql.strip().upper().startswith("SELECT") else sql


class LazySCDProcessor:
    """
    SCD Type 2 processor. Speaks only DuckDB SQL — no Python data.
    Storage is entirely delegated to a SinkAdapter.

    Usage::

        from sqldim.sinks import DuckDBSink

        with DuckDBSink("/tmp/dev.duckdb") as sink:
            proc = LazySCDProcessor(
                natural_key="product_id",
                track_columns=["name", "price"],
                sink=sink,
            )
            result = proc.process("products.parquet", "dim_product")

    Composite natural key example::

        proc = LazySCDProcessor(
            natural_key=["order_id", "line_no"],
            track_columns=["qty", "price"],
            sink=sink,
        )
    """

    def __init__(
        self,
        natural_key: str | list[str],
        track_columns: list[str],
        sink,
        batch_size: int = 100_000,
        con=None,
    ):
        import duckdb as _duckdb

        self.natural_key   = natural_key
        self._nk_cols      = [natural_key] if isinstance(natural_key, str) else list(natural_key)
        self.track_columns = sorted(track_columns)
        self.sink          = sink
        self.batch_size    = batch_size
        self._con          = con or _duckdb.connect()

    def process(self, source, table_name: str) -> SCDResult:
        """Register source, detect changes, write new/changed rows to *table_name*.

        Returns an :class:`~sqldim.scd.handler.SCDResult` with inserted /
        versioned / unchanged row counts.
        """
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc).isoformat()

        self._register_source(source)
        self._register_current_checksums(table_name)
        self._classify()

        result           = SCDResult()
        result.inserted  = self._write_new(table_name, now)
        result.versioned = self._write_changed(table_name, now)
        result.unchanged = self._count_unchanged()
        self._con.execute("DROP VIEW IF EXISTS incoming")
        self._con.execute("DROP TABLE IF EXISTS current_checksums")
        self._con.execute("DROP TABLE IF EXISTS classified")
        return result

    def _register_source(self, source) -> None:
        """Register *source* as a DuckDB ``incoming`` view with an MD5 ``_checksum`` column.

        Concatenates all ``track_columns`` with a ``|`` delimiter before
        hashing so identical attribute sets always produce the same checksum.
        """
        from sqldim.sources import coerce_source
        _sql = coerce_source(source).as_sql(self._con)
        cols = " || '|' || ".join(
            f"coalesce(cast({c} as varchar), '')"
            for c in self.track_columns
        )
        self._con.execute(f"""
            CREATE OR REPLACE VIEW incoming AS
            SELECT *,
                   md5({cols}) AS _checksum
            FROM {_as_subquery(_sql)}
        """)

    def _register_current_checksums(self, table_name: str) -> None:
        """Materialise a slim (NK + checksum) TABLE from the sink's current state.

        A TABLE is used instead of a VIEW so the remote source (PostgreSQL,
        MotherDuck…) is scanned exactly once.  All downstream references to
        ``current_checksums`` read from local DuckDB storage and the TABLE is
        spill-eligible when memory is constrained.
        """
        nk_select = ", ".join(self._nk_cols)
        sql = self.sink.current_state_sql(table_name)
        self._con.execute(f"""
            CREATE OR REPLACE TABLE current_checksums AS
            SELECT {nk_select}, checksum AS _checksum
            FROM {_as_subquery(sql)}
            WHERE is_current = TRUE
        """)

    def _classify(self) -> None:
        """Classify each incoming row as ``new``, ``changed``, or ``unchanged``.

        Builds a ``classified`` DuckDB table by joining ``incoming`` against
        ``current_checksums`` on natural key and checksum equality.
        Supports both single and composite natural keys.
        """
        join_cond  = " AND ".join(
            f"cast(i.{c} as varchar) = cast(c.{c} as varchar)"
            for c in self._nk_cols
        )
        null_check = f"c.{self._nk_cols[0]} IS NULL"
        self._con.execute(f"""
            CREATE OR REPLACE TABLE classified AS
            SELECT
                i.*,
                CASE
                    WHEN {null_check}              THEN 'new'
                    WHEN i._checksum != c._checksum THEN 'changed'
                    ELSE                                 'unchanged'
                END AS _scd_class
            FROM incoming i
            LEFT JOIN current_checksums c
                   ON {join_cond}
        """)

    def _write_new(self, table_name: str, now: str) -> int:
        """Insert brand-new rows from ``classified`` into *table_name*.

        Sets ``valid_from`` to *now*, ``valid_to`` to NULL, and
        ``is_current`` to TRUE for every ``_scd_class = 'new'`` row.
        """
        self._con.execute(f"""
            CREATE OR REPLACE VIEW new_rows AS
            SELECT * EXCLUDE (_scd_class, _checksum),
                   '{now}'::varchar AS valid_from,
                   NULL             AS valid_to,
                   TRUE             AS is_current,
                   _checksum        AS checksum
            FROM classified
            WHERE _scd_class = 'new'
        """)
        return self.sink.write(self._con, "new_rows", table_name, self.batch_size)

    def _write_changed(self, table_name: str, now: str) -> int:
        nk_select = ", ".join(self._nk_cols)
        self._con.execute(f"""
            CREATE OR REPLACE VIEW changed_nks AS
            SELECT {nk_select} FROM classified WHERE _scd_class = 'changed'
        """)
        self.sink.close_versions(self._con, table_name, self._nk_cols, "changed_nks", now)
        self._con.execute(f"""
            CREATE OR REPLACE VIEW new_versions AS
            SELECT * EXCLUDE (_scd_class, _checksum),
                   '{now}'::varchar AS valid_from,
                   NULL             AS valid_to,
                   TRUE             AS is_current,
                   _checksum        AS checksum
            FROM classified
            WHERE _scd_class = 'changed'
        """)
        return self.sink.write(self._con, "new_versions", table_name, self.batch_size)

    def _count_unchanged(self) -> int:
        """Return the count of rows that had no attribute changes."""
        return self._con.execute(
            "SELECT count(*) FROM classified WHERE _scd_class = 'unchanged'"
        ).fetchone()[0]
    def _update_local_fingerprint_after_batch(self) -> None:
        """Sync the local ``current_checksums`` TABLE with rows written in this batch.

        Called after each batch's writes in :meth:`process_stream` so subsequent
        batches correctly classify already-processed NKs without re-scanning the
        remote sink.
        """
        nk_select = ", ".join(self._nk_cols)
        nk_match  = " AND ".join(f"cs.{c} = cl.{c}" for c in self._nk_cols)
        self._con.execute(f"""
            INSERT INTO current_checksums ({nk_select}, _checksum)
            SELECT {nk_select}, _checksum
            FROM classified
            WHERE _scd_class = 'new'
        """)
        self._con.execute(f"""
            UPDATE current_checksums cs
            SET _checksum = cl._checksum
            FROM classified cl
            WHERE {nk_match}
            AND cl._scd_class = 'changed'
        """)
    # ── streaming helpers ──────────────────────────────────────────────────

    def _register_source_from_sql(self, sql_fragment: str) -> None:
        """Register an already-resolved SQL fragment as the ``incoming`` view.

        Used by :meth:`process_stream` to bypass :func:`~sqldim.sources.coerce_source`.
        """
        cols = " || '|' || ".join(
            f"coalesce(cast({c} as varchar), '')"
            for c in self.track_columns
        )
        self._con.execute(f"""
            CREATE OR REPLACE VIEW incoming AS
            SELECT *, md5({cols}) AS _checksum
            FROM ({sql_fragment})
        """)

    def _drop_stream_views(self) -> None:
        """Clean up per-batch working views and tables between micro-batches.

        ``current_checksums`` TABLE is intentionally excluded here — it is
        bootstrapped once before the batch loop and dropped by
        :meth:`process_stream` after all batches complete.
        """
        for v in [
            "incoming",
            "new_rows", "new_versions", "changed_nks",
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
        """Process a streaming source micro-batch by micro-batch.

        Parameters
        ----------
        source         : :class:`~sqldim.sources.stream.StreamSourceAdapter`
        table_name     : Target table in the sink.
        batch_size     : Rows per micro-batch (passed to ``source.stream``).
        max_batches    : Stop after *N* batches; ``None`` = run until exhausted.
        on_batch       : ``callback(batch_num: int, result: SCDResult)`` invoked
                         after each successful write.
        deduplicate_by : Timestamp column name; when set, keeps only the latest
                         event per natural key within each micro-batch using
                         ``QUALIFY ROW_NUMBER() OVER (...) = 1``.

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
        self._register_current_checksums(table_name)

        # Phase 2: Stream batches, each classified against the local TABLE.
        for i, sql_fragment in enumerate(source.stream(self._con, batch_size)):
            if max_batches is not None and i >= max_batches:
                break

            now = datetime.now(timezone.utc).isoformat()

            if deduplicate_by:
                nk = ", ".join(self._nk_cols)
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
                batch_result.versioned = self._write_changed(table_name, now)
                batch_result.unchanged = self._count_unchanged()

                # Keep local fingerprint in sync so subsequent batches
                # correctly classify already-processed NKs.
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
# LazyType1Processor  (SCD Type 1 — overwrite in place)
# ---------------------------------------------------------------------------

class LazyType1Processor:
    """
    SCD Type 1 processor. Overwrites tracked attributes in place.
    No versioning — only the latest value is kept.

    Uses only DuckDB SQL; storage delegated to SinkAdapter.
    """

    def __init__(
        self,
        natural_key: str,
        track_columns: list[str],
        sink,
        batch_size: int = 100_000,
        con=None,
    ):
        import duckdb as _duckdb

        self.natural_key   = natural_key
        self.track_columns = sorted(track_columns)
        self.sink          = sink
        self.batch_size    = batch_size
        self._con          = con or _duckdb.connect()

    def process(self, source, table_name: str) -> SCDResult:
        """Detect and overwrite changed rows; insert new rows into *table_name*.

        Unlike SCD-2, no version rows are created — changes update the
        existing row in place.  Returns an :class:`~sqldim.scd.handler.SCDResult`.
        """
        self._register_source(source)
        self._register_current_checksums(table_name)
        self._classify()

        result           = SCDResult()
        result.inserted  = self._write_new(table_name)
        result.versioned = self._overwrite_changed(table_name)
        result.unchanged = self._count_unchanged()
        self._con.execute("DROP VIEW IF EXISTS incoming")
        self._con.execute("DROP TABLE IF EXISTS current_checksums")
        self._con.execute("DROP TABLE IF EXISTS classified")
        return result

    def _register_source(self, source) -> None:
        """Register *source* as a DuckDB ``incoming`` view with an MD5 ``_checksum`` column."""
        from sqldim.sources import coerce_source
        _sql = coerce_source(source).as_sql(self._con)
        cols = " || '|' || ".join(
            f"coalesce(cast({c} as varchar), '')"
            for c in self.track_columns
        )
        self._con.execute(f"""
            CREATE OR REPLACE VIEW incoming AS
            SELECT *,
                   md5({cols}) AS _checksum
            FROM {_as_subquery(_sql)}
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
        """Create a ``classified`` DuckDB table tagging each row as new/changed/unchanged."""
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
        self._con.execute(f"""
            CREATE OR REPLACE VIEW new_rows AS
            SELECT * EXCLUDE (_scd_class, _checksum),
                   '{now}'::varchar AS valid_from,
                   NULL             AS valid_to,
                   TRUE             AS is_current,
                   _checksum        AS checksum
            FROM classified
            WHERE _scd_class = 'new'
        """)
        return self.sink.write(self._con, "new_rows", table_name, self.batch_size)

    def _overwrite_changed(self, table_name: str) -> int:
        nk = self.natural_key
        self._con.execute(f"""
            CREATE OR REPLACE VIEW changed_updates AS
            SELECT {nk}, {", ".join(self.track_columns)}
            FROM classified
            WHERE _scd_class = 'changed'
        """)
        return self.sink.update_attributes(
            self._con, table_name, nk, "changed_updates", self.track_columns
        )

    def _count_unchanged(self) -> int:
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
            for c in self.track_columns
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
        for v in ["incoming", "new_rows", "changed_updates"]:
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
        """Process a streaming source micro-batch by micro-batch (Type 1 — overwrite).

        Parameters are identical to :meth:`LazySCDProcessor.process_stream`.

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
                batch_result.versioned = self._overwrite_changed(table_name)
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
# LazyType3Processor  (SCD Type 3 — current + previous columns)
# ---------------------------------------------------------------------------


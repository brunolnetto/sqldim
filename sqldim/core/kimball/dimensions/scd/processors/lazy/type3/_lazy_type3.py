"""SCD Type 3 lazy processor built on DuckDB SQL.

:class:`LazyType3Processor` rotates ``(current_col, previous_col)`` pairs
in place.  Delegates all storage to a :class:`SinkAdapter`.
"""

from __future__ import annotations

from sqldim.core.kimball.dimensions.scd.handler import SCDResult


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

        self.natural_key = natural_key
        self.column_pairs = column_pairs  # [(curr, prev), ...]
        self.track_cols = [c for c, _ in column_pairs]
        self.sink = sink
        self.batch_size = batch_size
        self._con = con or _duckdb.connect()

    def process(self, source, table_name: str) -> SCDResult:
        """Detect changes and rotate column history; insert new rows into *table_name*.

        Returns an :class:`~sqldim.scd.handler.SCDResult` with inserted /
        versioned / unchanged row counts.
        """
        self._register_source(source)
        self._register_current_checksums(table_name)
        self._classify()

        result = SCDResult()
        result.inserted = self._write_new(table_name)
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
            f"coalesce(cast({c} as varchar), '')" for c in self.track_cols
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
        return (
            self._con.execute(
                "SELECT count(*) FROM classified WHERE _scd_class = 'unchanged'"
            ).fetchone()
            or (0,)
        )[0]

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
            f"coalesce(cast({c} as varchar), '')" for c in self.track_cols
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
            "new_rows",
            "changed_rotations",
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
        self, i, dedup_sql, table_name, on_batch, source, result, _log
    ):
        """Process one micro-batch inside the streaming loop (Type 3)."""
        from sqldim.core.kimball.dimensions.scd.handler import SCDResult as _SCDResult

        try:
            self._register_source_from_sql(dedup_sql)
            self._classify()

            batch_result = _SCDResult()
            batch_result.inserted = self._write_new(table_name)
            batch_result.versioned = self._rotate_changed(table_name)
            batch_result.unchanged = self._count_unchanged()

            self._update_local_fingerprint_after_batch()

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
        """Process a streaming source micro-batch by micro-batch (Type 3 — rotate).

        Returns
        -------
        :class:`~sqldim.sources.streaming.stream.StreamResult`
        """
        import logging
        from sqldim.sources.streaming.stream import StreamResult

        _log = logging.getLogger(__name__)
        result = StreamResult()

        self._register_current_checksums(table_name)

        for i, sql_fragment in enumerate(source.stream(self._con, batch_size)):
            if max_batches is not None and i >= max_batches:
                break
            self._run_stream_batch(
                i,
                self._resolve_dedup_sql(sql_fragment, deduplicate_by),
                table_name,
                on_batch,
                source,
                result,
                _log,
            )

        self._con.execute("DROP TABLE IF EXISTS current_checksums")
        return result

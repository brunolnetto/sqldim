"""SCD Type 2 and Type 1 lazy processors built on DuckDB SQL.

:class:`LazySCDProcessor` implements the full SCD-2 pipeline
(detect → close old versions → insert new versions) using only
DuckDB SQL statements.  :class:`LazyType1Processor` (re-exported from :mod:`._lazy_type1`).
Both delegate storage to a :class:`SinkAdapter`.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


from sqldim.core.kimball.dimensions.scd.handler import SCDResult
from sqldim.core.kimball.dimensions.scd.processors.lazy.type2._lazy_type2_stream import (
    _Type2StreamMixin,
)  # noqa: F401


def _as_subquery(sql: str) -> str:
    """Wrap *sql* for use in a DuckDB FROM clause.

    DuckDB 1.5+ rejects ``FROM (table_function())`` — parentheses are only
    allowed around subqueries (``SELECT …``).  Table-function calls and bare
    table/view names must appear directly in the FROM clause.
    """
    return f"({sql})" if sql.strip().upper().startswith("SELECT") else sql


class LazySCDProcessor(_Type2StreamMixin):
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
        lineage_emitter: Any = None,
    ):
        import duckdb as _duckdb

        self.natural_key = natural_key
        self._nk_cols = (
            [natural_key] if isinstance(natural_key, str) else list(natural_key)
        )
        self.track_columns = sorted(track_columns)
        self.sink = sink
        self.batch_size = batch_size
        self._con = con or _duckdb.connect()
        self._lineage_emitter = lineage_emitter

    def process(self, source, table_name: str) -> SCDResult:
        """Register source, detect changes, write new/changed rows to *table_name*.

        Returns an :class:`~sqldim.scd.handler.SCDResult` with inserted /
        versioned / unchanged row counts.
        """

        now = datetime.now(timezone.utc).isoformat()

        self._register_source(source)
        self._register_current_checksums(table_name)
        self._classify()

        result = SCDResult()
        result.inserted = self._write_new(table_name, now)
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
            f"coalesce(cast({c} as varchar), '')" for c in self.track_columns
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
        """Classify each incoming row as ``new``, ``changed``, ``reconnect``, or ``unchanged``.

        Builds a ``classified`` DuckDB table by joining ``incoming`` against
        ``current_checksums`` on natural key and checksum equality.
        Supports both single and composite natural keys.

        When the current dimension row has ``is_inferred = TRUE`` and the incoming
        checksum differs, the row is classified as ``reconnect`` (late-arriving
        dimension reconnection) instead of plain ``changed``.  This allows
        :meth:`_write_changed` to emit the correct lineage event.
        """
        join_cond = " AND ".join(
            f"cast(i.{c} as varchar) = cast(c.{c} as varchar)" for c in self._nk_cols
        )
        null_check = f"c.{self._nk_cols[0]} IS NULL"
        # current_checksums may or may not have is_inferred column
        try:
            self._con.execute("SELECT is_inferred FROM current_checksums LIMIT 0")
            has_inferred = True
        except Exception:
            has_inferred = False

        if has_inferred:
            inferred_case = (
                "WHEN i._checksum != c._checksum AND c.is_inferred = TRUE THEN 'reconnect'\n"
                "                    WHEN i._checksum != c._checksum THEN 'changed'"
            )
        else:
            inferred_case = "WHEN i._checksum != c._checksum THEN 'changed'"

        self._con.execute(f"""
            CREATE OR REPLACE TABLE classified AS
            SELECT
                i.*,
                CASE
                    WHEN {null_check}              THEN 'new'
                    {inferred_case}
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
        # Both 'changed' and 'reconnect' rows need a new version written.
        # 'reconnect' rows additionally clear the is_inferred flag.
        self._con.execute(f"""
            CREATE OR REPLACE VIEW changed_nks AS
            SELECT {nk_select} FROM classified
            WHERE _scd_class IN ('changed', 'reconnect')
        """)
        self.sink.close_versions(
            self._con, table_name, self._nk_cols, "changed_nks", now
        )

        # Apply is_inferred override when column exists in the target
        try:
            self._con.execute(f"SELECT is_inferred FROM {table_name} LIMIT 0")
            self._con.execute(f"""
                CREATE OR REPLACE VIEW new_versions AS
                SELECT
                    * EXCLUDE (_scd_class, _checksum),
                    '{now}'::varchar AS valid_from,
                    NULL             AS valid_to,
                    TRUE             AS is_current,
                    _checksum        AS checksum,
                    CASE WHEN _scd_class = 'reconnect' THEN FALSE
                         ELSE FALSE
                    END              AS is_inferred
                FROM classified
                WHERE _scd_class IN ('changed', 'reconnect')
            """)
        except Exception:
            # Table doesn't have is_inferred — plain view without override
            self._con.execute(f"""
                CREATE OR REPLACE VIEW new_versions AS
                SELECT
                    * EXCLUDE (_scd_class, _checksum),
                    '{now}'::varchar AS valid_from,
                    NULL             AS valid_to,
                    TRUE             AS is_current,
                    _checksum        AS checksum
                FROM classified
                WHERE _scd_class IN ('changed', 'reconnect')
            """)

        written = self.sink.write(
            self._con, "new_versions", table_name, self.batch_size
        )

        # Emit reconnection lineage events
        if self._lineage_emitter is not None:
            reconnect_rows = self._con.execute(
                f"SELECT {nk_select} FROM classified WHERE _scd_class = 'reconnect'"
            ).fetchall()
            if reconnect_rows:
                from sqldim.lineage.events import (
                    InferredMemberEvent,
                    InferredMemberEventType,
                )

                for row in reconnect_rows:
                    self._lineage_emitter(
                        InferredMemberEvent(
                            event_type=InferredMemberEventType.RECONNECTED,
                            dimension=table_name,
                            natural_key=str(row[0]),
                            versions_merged=1,
                        )
                    )

        return written

    def _count_unchanged(self) -> int:
        """Return the count of rows that had no attribute changes."""
        return (
            self._con.execute(
                "SELECT count(*) FROM classified WHERE _scd_class = 'unchanged'"
            ).fetchone()
            or (0,)
        )[0]

    def _update_local_fingerprint_after_batch(self) -> None:
        """Sync the local ``current_checksums`` TABLE with rows written in this batch.

        Called after each batch's writes in :meth:`process_stream` so subsequent
        batches correctly classify already-processed NKs without re-scanning the
        remote sink.
        """
        nk_select = ", ".join(self._nk_cols)
        nk_match = " AND ".join(f"cs.{c} = cl.{c}" for c in self._nk_cols)
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
            AND cl._scd_class IN ('changed', 'reconnect')
        """)

    # ── streaming helpers ──────────────────────────────────────────────────

    def _register_source_from_sql(self, sql_fragment: str) -> None:
        """Register an already-resolved SQL fragment as the ``incoming`` view.

        Used by :meth:`process_stream` to bypass :func:`~sqldim.sources.coerce_source`.
        """
        cols = " || '|' || ".join(
            f"coalesce(cast({c} as varchar), '')" for c in self.track_columns
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
            "new_rows",
            "new_versions",
            "changed_nks",
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
        nk = ", ".join(self._nk_cols)
        return (
            f"SELECT * FROM ({sql_fragment})"
            f" QUALIFY ROW_NUMBER() OVER ("
            f"PARTITION BY {nk} ORDER BY {deduplicate_by} DESC) = 1"
        )


# ---------------------------------------------------------------------------
# LazyType1Processor re-exported from _lazy_type1 for backward compatibility
# ---------------------------------------------------------------------------
from sqldim.core.kimball.dimensions.scd.processors.lazy.type1 import (  # noqa: E402, F401
    LazyType1Processor as LazyType1Processor,
)

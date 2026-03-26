"""SCD metadata processor stream-batch methods.

Extracted from _lazy_metadata.py to keep file sizes manageable.
These methods are mixed into :class:`LazySCDMetadataProcessor` via inheritance.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

_log = logging.getLogger(__name__)


class _MetadataStreamMixin:
    """Stream-batch processing methods for LazySCDMetadataProcessor."""

    if TYPE_CHECKING:
        _register_current_hashes: Any
        _con: Any
        _update_local_hashes_after_batch: Any

    def _commit_batch(self, i, source, result, batch_result, on_batch) -> None:
        """Commit *batch_result* into *result* and invoke the optional callback."""
        self._update_local_hashes_after_batch()
        self._con.execute("DROP TABLE IF EXISTS classified")
        source.commit(source.checkpoint())
        result.accumulate(batch_result)
        result.batches_processed += 1
        if on_batch:
            on_batch(i, batch_result)

    def _run_stream_batch(
        self, i, sql_fragment, table_name, now, on_batch, source, result, _log
    ):
        """Execute one streaming micro-batch for the metadata SCD-2 cycle."""
        from sqldim.core.kimball.dimensions.scd.handler import SCDResult as _SCDResult

        try:
            self._register_source_from_sql(sql_fragment)
            self._classify()

            self._con.execute("DROP TABLE IF EXISTS incoming")

            stats = self._con.execute("""
                SELECT countif(_scd_class = 'new')       AS n_new,
                       countif(_scd_class = 'changed')   AS n_changed,
                       countif(_scd_class = 'unchanged') AS n_unchanged
                FROM classified
            """).fetchone()
            n_new, n_changed, n_unchanged = stats[0], stats[1], stats[2]
            _log.info(
                f"[sqldim] {table_name}: batch {i + 1} — "
                f"{n_new:,} new, {n_changed:,} changed, {n_unchanged:,} unchanged"
            )

            batch_result = _SCDResult()
            batch_result.inserted = self._write_new(table_name, now, n_new)
            batch_result.versioned = self._write_changed(table_name, now, n_changed)
            batch_result.unchanged = n_unchanged

            self._commit_batch(i, source, result, batch_result, on_batch)

        except Exception as exc:
            result.batches_failed += 1
            _log.error("Batch %d failed for table %s: %s", i, table_name, exc)
            self._con.execute("DROP TABLE IF EXISTS incoming")
            self._con.execute("DROP TABLE IF EXISTS classified")

    def process_stream(
        self,
        source,
        table_name: str,
        batch_size: int = 1_000_000,
        max_batches: int | None = None,
        on_batch=None,
        now: str | None = None,
    ):
        """Run the SCD-2 metadata cycle in streaming mode.

        Two-phase approach:

        * **Phase 1 (bootstrap)** — Materialise ``current_hashes`` TABLE once.
          Remote source (PostgreSQL, MotherDuck) is scanned exactly once.
        * **Phase 2 (stream)** — Each batch is classified against the local
          TABLE; no further remote scans occur regardless of batch count.

        Parameters
        ----------
        source      : Any object with a ``.stream(con, batch_size)`` generator
                      (e.g. :class:`~sqldim.sources.streaming.csv_stream.CSVStreamSource`).
        table_name  : Target dimension table in the sink.
        batch_size  : Rows per micro-batch.
        max_batches : Stop after *N* batches; ``None`` = run until exhausted.
        on_batch    : ``callback(batch_num: int, result: SCDResult)`` invoked
                      after each successful write.
        now         : Fixed ISO-8601 timestamp for ``valid_from`` / ``valid_to``;
                      defaults to current UTC time.

        Returns
        -------
        :class:`~sqldim.sources.streaming.stream.StreamResult`
        """
        import logging
        from sqldim.sources.streaming.stream import StreamResult
        from sqldim.core.kimball.dimensions.scd.processors.lazy.metadata._lazy_metadata import (
            _safe,
        )

        _log = logging.getLogger(__name__)
        _safe(table_name)

        if now is None:
            now = datetime.now(timezone.utc).isoformat()

        result = StreamResult()

        _log.info(f"[sqldim] {table_name}: bootstrapping hash fingerprint …")
        self._register_current_hashes(table_name)

        for i, sql_fragment in enumerate(source.stream(self._con, batch_size)):
            if max_batches is not None and i >= max_batches:
                break
            self._run_stream_batch(
                i, sql_fragment, table_name, now, on_batch, source, result, _log
            )

        self._con.execute("DROP TABLE IF EXISTS current_hashes")
        return result

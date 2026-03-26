"""SCD Type2 processor stream-batch methods.

Extracted from _lazy_type2.py to keep file sizes manageable.
These methods are mixed into :class:`LazySCDProcessor` via inheritance.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

_log = logging.getLogger(__name__)


class _Type2StreamMixin:
    """Stream-batch processing methods for LazySCDProcessor."""

    if TYPE_CHECKING:
        _register_current_checksums: Any
        _con: Any
        _resolve_dedup_sql: Any

    def _run_stream_batch(
        self, i, dedup_sql, table_name, now, on_batch, source, result, _log
    ):
        """Process one micro-batch inside the streaming loop (SCD-2)."""
        from sqldim.core.kimball.dimensions.scd.handler import SCDResult as _SCDResult

        try:
            self._register_source_from_sql(dedup_sql)
            self._classify()

            batch_result = _SCDResult()
            batch_result.inserted = self._write_new(table_name, now)
            batch_result.versioned = self._write_changed(table_name, now)
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
        """Process a streaming source micro-batch by micro-batch.

        Parameters
        ----------
        source         : :class:`~sqldim.sources.streaming.stream.StreamSourceAdapter`
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
        :class:`~sqldim.sources.streaming.stream.StreamResult`
        """
        import logging
        from datetime import datetime, timezone
        from sqldim.sources.streaming.stream import StreamResult

        _log = logging.getLogger(__name__)
        result = StreamResult()

        self._register_current_checksums(table_name)

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

        self._con.execute("DROP TABLE IF EXISTS current_checksums")
        return result

    # ---------------------------------------------------------------------------

    # ---------------------------------------------------------------------------
    # LazyType1Processor  (SCD Type 1 — re-exported from _lazy_type1)
    # ---------------------------------------------------------------------------
    from sqldim.core.kimball.dimensions.scd.processors.lazy.type1._lazy_type1 import (  # noqa: F401
        LazyType1Processor,
    )

"""
sqldim/sources/stream.py

StreamSourceAdapter protocol — the streaming read boundary.
StreamResult — accumulated counts across all micro-batches.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterator, Protocol, runtime_checkable

import duckdb

from sqldim.core.kimball.dimensions.scd.handler import SCDResult


@runtime_checkable
class StreamSourceAdapter(Protocol):
    """
    Streaming counterpart to SourceAdapter.

    Yields SQL fragments one micro-batch at a time.  Each fragment is
    registered as a DuckDB VIEW by the processor, classified, written
    to the sink, then committed.

    Rules:
      - stream() never loads rows into Python — it yields SQL fragments.
      - commit() is called only after sink.write() succeeds.
      - checkpoint() returns an opaque offset the caller can persist
        for resumability across process restarts.
      - Implementations may call con.execute("LOAD kafka") etc. on
        first iteration — one-time extension setup is acceptable.
    """

    def stream(
        self,
        con: duckdb.DuckDBPyConnection,
        batch_size: int = 10_000,
    ) -> Iterator[str]:
        """
        Yield SQL fragments, one per micro-batch.

        Each fragment is usable directly inside a DuckDB FROM clause::

            CREATE VIEW incoming AS SELECT * FROM ({fragment})

        Raises ``StopIteration`` when the stream is exhausted (finite sources)
        or runs indefinitely (live streams).
        """
        ...

    def commit(self, offset: Any) -> None:
        """
        Acknowledge that the batch at *offset* was fully written to the sink.

        Called after every successful ``sink.write()``.  Implementations
        persist this to their offset store.
        """
        ...

    def checkpoint(self) -> Any:
        """
        Return the current offset or watermark.

        Callers persist this between restarts to resume from the last
        committed position.
        """
        ...


@dataclass
class StreamResult:
    """Accumulated result across all micro-batches."""

    inserted: int = 0
    versioned: int = 0
    unchanged: int = 0
    batches_processed: int = 0
    batches_failed: int = 0

    def accumulate(self, r: SCDResult) -> None:
        """Add per-batch counts from *r* into the running totals."""
        self.inserted += r.inserted
        self.versioned += r.versioned
        self.unchanged += r.unchanged

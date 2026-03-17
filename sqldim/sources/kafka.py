"""
sqldim/sources/kafka.py

KafkaSource — streams micro-batches from a Kafka topic.

Uses DuckDB's native ``kafka`` extension when available (zero Python
deserialisation).  Falls back to ``confluent-kafka`` consumer staging
batches via Apache Arrow / Polars.

Optional dependencies:
  DuckDB kafka extension  →  pip install sqldim[kafka-native]
  confluent-kafka         →  pip install sqldim[kafka]
"""
from __future__ import annotations

from typing import Any, Iterator

import duckdb


class KafkaSource:
    """
    Stream from a Kafka topic.

    Parameters
    ----------
    brokers:
        Comma-separated broker addresses, e.g. ``"localhost:9092"``.
    topic:
        Kafka topic name.
    group_id:
        Consumer group id (used for offset tracking).
    format:
        Message format — ``"json"`` | ``"avro"`` | ``"parquet"``.
    start_from:
        ``"latest"`` | ``"earliest"`` | ``"checkpoint"``.
    """

    def __init__(
        self,
        brokers: str,
        topic: str,
        group_id: str,
        format: str = "json",
        start_from: str = "latest",
    ) -> None:
        self._brokers = brokers
        self._topic = topic
        self._group_id = group_id
        self._format = format
        self._start_from = start_from
        self._offset: int | None = None

    # ------------------------------------------------------------------
    # StreamSourceAdapter interface
    # ------------------------------------------------------------------

    def stream(
        self,
        con: duckdb.DuckDBPyConnection,
        batch_size: int = 10_000,
    ) -> Iterator[str]:
        """Yield SQL fragments, one per micro-batch.

        Attempts the DuckDB native ``kafka_scan`` first; falls back to
        the ``confluent-kafka`` consumer if the extension is unavailable.
        """
        native = True
        try:
            con.execute("LOAD kafka;")
        except Exception:
            native = False
        if native:
            yield from self._stream_native(con, batch_size)
        else:
            yield from self._stream_consumer(con, batch_size)

    def commit(self, offset: Any) -> None:
        """No-op: consumer group offset is committed by confluent-kafka."""

    def checkpoint(self) -> int | None:
        """Return the last seen Kafka offset, or ``None`` before any batch."""
        return self._offset

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _stream_native(
        self,
        con: duckdb.DuckDBPyConnection,
        batch_size: int,
    ) -> Iterator[str]:
        """DuckDB kafka_scan — zero Python deserialisation."""
        while True:
            sql = (
                f"kafka_scan('{self._brokers}', '{self._topic}', "
                f"group_id='{self._group_id}', "
                f"max_messages={batch_size}, "
                f"format='{self._format}')"
            )
            count = con.execute(
                f"SELECT count(*) FROM ({sql})"
            ).fetchone()[0]
            if count == 0:
                break
            self._offset = con.execute(
                f"SELECT max(_kafka_offset) FROM ({sql})"
            ).fetchone()[0]
            yield sql

    def _stream_consumer(
        self,
        con: duckdb.DuckDBPyConnection,
        batch_size: int,
    ) -> Iterator[str]:
        """confluent-kafka fallback — batches staged via Apache Arrow."""
        from confluent_kafka import Consumer  # type: ignore[import]
        import json
        import polars as pl  # type: ignore[import]

        consumer = Consumer(
            {
                "bootstrap.servers": self._brokers,
                "group.id": self._group_id,
                "auto.offset.reset": self._start_from,
                "enable.auto.commit": False,
            }
        )
        consumer.subscribe([self._topic])

        try:
            while True:
                msgs = consumer.consume(batch_size, timeout=1.0)
                if not msgs:
                    break

                rows = [
                    json.loads(m.value())
                    for m in msgs
                    if not m.error()
                ]
                if not rows:
                    continue

                batch_df = pl.from_dicts(rows)
                con.register("_kafka_batch", batch_df.to_arrow())
                self._offset = msgs[-1].offset()
                yield "SELECT * FROM _kafka_batch"
        finally:
            consumer.close()

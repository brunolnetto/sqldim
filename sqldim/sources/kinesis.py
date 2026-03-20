"""
sqldim/sources/kinesis.py

KinesisSource — streams micro-batches from an AWS Kinesis Data Stream.

Uses boto3 ``GetRecords`` and stages each batch as an Apache Arrow table
registered with DuckDB.

Optional dependencies:
  pip install sqldim[kinesis]   # installs boto3 + polars
"""

from __future__ import annotations

from typing import Any, Iterator

import duckdb


class KinesisSource:
    """
    Stream from an AWS Kinesis Data Stream.

    Parameters
    ----------
    stream_name:
        Kinesis stream name.
    region:
        AWS region (default ``"us-east-1"``).
    shard_iterator_type:
        ``"LATEST"`` | ``"TRIM_HORIZON"`` | ``"AT_SEQUENCE_NUMBER"``.
    """

    def __init__(
        self,
        stream_name: str,
        region: str = "us-east-1",
        shard_iterator_type: str = "LATEST",
    ) -> None:
        self._stream = stream_name
        self._region = region
        self._iter_type = shard_iterator_type
        self._seq: str | None = None

    # ------------------------------------------------------------------
    # StreamSourceAdapter interface
    # ------------------------------------------------------------------

    def stream(
        self,
        con: duckdb.DuckDBPyConnection,
        batch_size: int = 1_000,
    ) -> Iterator[str]:
        """Yield SQL fragments, one per Kinesis shard batch."""
        import boto3  # type: ignore[import]
        import json
        import polars as pl  # type: ignore[import]

        client = boto3.client("kinesis", region_name=self._region)
        shards = client.list_shards(StreamName=self._stream)["Shards"]

        for shard in shards:
            iterator = client.get_shard_iterator(
                StreamName=self._stream,
                ShardId=shard["ShardId"],
                ShardIteratorType=self._iter_type,
            )["ShardIterator"]

            while iterator:
                resp = client.get_records(ShardIterator=iterator, Limit=batch_size)
                records = resp.get("Records", [])
                iterator = resp.get("NextShardIterator")

                if not records:
                    break

                rows = [json.loads(r["Data"]) for r in records]
                self._seq = records[-1]["SequenceNumber"]

                batch_df = pl.from_dicts(rows)
                con.register("_kinesis_batch", batch_df.to_arrow())
                yield "SELECT * FROM _kinesis_batch"

    def commit(self, offset: Any) -> None:
        """Persist the sequence number externally for resumability."""

    def checkpoint(self) -> str | None:
        """Return the last seen Kinesis sequence number, or ``None``."""
        return self._seq

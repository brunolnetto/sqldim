"""Streaming sources: Kafka, Kinesis, CSV stream, Parquet stream, base stream."""
from sqldim.sources.streaming.stream import StreamSourceAdapter, StreamResult  # noqa: F401
from sqldim.sources.streaming.kafka import KafkaSource  # noqa: F401
from sqldim.sources.streaming.kinesis import KinesisSource  # noqa: F401
from sqldim.sources.streaming.csv_stream import CSVStreamSource  # noqa: F401
from sqldim.sources.streaming.parquet_stream import ParquetStreamSource  # noqa: F401

__all__ = [
    "StreamSourceAdapter",
    "StreamResult",
    "KafkaSource",
    "KinesisSource",
    "CSVStreamSource",
    "ParquetStreamSource",
]

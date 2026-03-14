from sqldim.sinks.base import SinkAdapter
from sqldim.sinks.duckdb import DuckDBSink
from sqldim.sinks.postgresql import PostgreSQLSink
from sqldim.sinks.parquet import ParquetSink
from sqldim.sinks.delta import DeltaLakeSink

__all__ = [
    "SinkAdapter",
    "DuckDBSink",
    "PostgreSQLSink",
    "ParquetSink",
    "DeltaLakeSink",
]

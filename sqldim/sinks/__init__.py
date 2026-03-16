"""Storage sinks for sqldim dimensional loaders.

Each sink implements the :class:`SinkAdapter` protocol so loaders
are storage-agnostic.  Available backends: :class:`DuckDBSink`,
:class:`PostgreSQLSink`, :class:`ParquetSink`, :class:`DeltaLakeSink`,
:class:`MotherDuckSink`, and :class:`IcebergSink`.
"""
from sqldim.sinks.base import SinkAdapter
from sqldim.sinks.duckdb import DuckDBSink
from sqldim.sinks.postgresql import PostgreSQLSink
from sqldim.sinks.parquet import ParquetSink
from sqldim.sinks.delta import DeltaLakeSink
from sqldim.sinks.motherduck import MotherDuckSink
from sqldim.sinks.iceberg import IcebergSink

__all__ = [
    "SinkAdapter",
    "DuckDBSink",
    "PostgreSQLSink",
    "ParquetSink",
    "DeltaLakeSink",
    "MotherDuckSink",
    "IcebergSink",
]

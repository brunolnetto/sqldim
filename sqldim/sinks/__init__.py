"""Storage sinks for sqldim dimensional loaders.

Each sink implements the :class:`SinkAdapter` protocol so loaders
are storage-agnostic.  Available backends: :class:`DuckDBSink`,
:class:`PostgreSQLSink`, :class:`ParquetSink`, :class:`DeltaLakeSink`,
:class:`MotherDuckSink`, and :class:`IcebergSink`.
"""

from sqldim.sinks.base import SinkAdapter
from sqldim.sinks.file.parquet import ParquetSink
from sqldim.sinks.file.delta import DeltaLakeSink
from sqldim.sinks.file.iceberg import IcebergSink
from sqldim.sinks.sql.duckdb import DuckDBSink
from sqldim.sinks.sql.motherduck import MotherDuckSink
from sqldim.sinks.sql.postgresql import PostgreSQLSink

__all__ = [
    "SinkAdapter",
    "DuckDBSink",
    "PostgreSQLSink",
    "ParquetSink",
    "DeltaLakeSink",
    "MotherDuckSink",
    "IcebergSink",
]

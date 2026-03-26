"""File-based sinks: Parquet, Delta Lake, Iceberg."""

from sqldim.sinks.file.parquet import ParquetSink  # noqa: F401
from sqldim.sinks.file.delta import DeltaLakeSink  # noqa: F401
from sqldim.sinks.file.iceberg import IcebergSink  # noqa: F401

__all__ = ["ParquetSink", "DeltaLakeSink", "IcebergSink"]

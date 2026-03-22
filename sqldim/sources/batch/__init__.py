"""Batch / file-based sources: CSV, Parquet, Delta, Iceberg, DuckDB, SQL."""
from sqldim.sources.batch.csv import CSVSource  # noqa: F401
from sqldim.sources.batch.parquet import ParquetSource  # noqa: F401
from sqldim.sources.batch.delta import DeltaSource  # noqa: F401
from sqldim.sources.batch.iceberg import IcebergSource  # noqa: F401
from sqldim.sources.batch.duckdb_source import DuckDBSource  # noqa: F401
from sqldim.sources.batch.sql import SQLSource  # noqa: F401

__all__ = [
    "CSVSource",
    "ParquetSource",
    "DeltaSource",
    "IcebergSource",
    "DuckDBSource",
    "SQLSource",
]

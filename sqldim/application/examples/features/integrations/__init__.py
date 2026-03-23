"""Showcase: dlt, Parquet, and Iceberg integration patterns.

Demonstrates writing dimension and fact tables to :class:`ParquetSink`,
an Iceberg catalog, and a dlt pipeline target.
"""

from sqldim.application.examples.features.integrations.showcase import run_showcase

__all__ = ["run_showcase"]

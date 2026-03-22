"""Specialized fact and dimension loaders for common data warehousing patterns.

Each loader encapsulates a single Kimball loading strategy — snapshot,
accumulating, cumulative, bitmask, array metric, or edge projection —
and exposes a ``Lazy*`` DuckDB-first variant that never materialises the
full dataset in Python memory.

All loaders accept a ``SinkAdapter`` so the underlying storage engine
(DuckDB, PostgreSQL, Parquet, etc.) is swapped without code changes.
"""

from sqldim.core.loaders.fact.snapshot import (
    SnapshotLoader,
    LazyTransactionLoader,
    LazySnapshotLoader,
)
from sqldim.core.loaders.fact.accumulating import AccumulatingLoader, LazyAccumulatingLoader
from sqldim.core.loaders.fact.cumulative import LazyCumulativeLoader
from sqldim.core.loaders.dimension.bitmask import LazyBitmaskLoader
from sqldim.core.loaders.dimension.array_metric import LazyArrayMetricLoader
from sqldim.core.loaders.dimension.dimensional import DimensionalLoader
from sqldim.core.loaders.dimension.edge_projection import LazyEdgeProjectionLoader

__all__ = [
    "SnapshotLoader",
    "LazyTransactionLoader",
    "LazySnapshotLoader",
    "AccumulatingLoader",
    "LazyAccumulatingLoader",
    "LazyCumulativeLoader",
    "LazyBitmaskLoader",
    "LazyArrayMetricLoader",
    "LazyEdgeProjectionLoader",
    "DimensionalLoader",
]

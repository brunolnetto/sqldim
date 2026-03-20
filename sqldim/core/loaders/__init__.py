"""Specialized fact and dimension loaders for common data warehousing patterns.

Each loader encapsulates a single Kimball loading strategy — snapshot,
accumulating, cumulative, bitmask, array metric, or edge projection —
and exposes both eager and ``Lazy*`` variants for deferred execution.

All loaders accept a ``SinkAdapter`` so the underlying storage engine
(DuckDB, PostgreSQL, Parquet, etc.) is swapped without code changes.
"""

from sqldim.core.loaders.snapshot import (
    SnapshotLoader,
    LazyTransactionLoader,
    LazySnapshotLoader,
)
from sqldim.core.loaders.accumulating import AccumulatingLoader, LazyAccumulatingLoader
from sqldim.core.loaders.cumulative import CumulativeLoader, LazyCumulativeLoader
from sqldim.core.loaders.bitmask import BitmaskerLoader, LazyBitmaskLoader
from sqldim.core.loaders.array_metric import ArrayMetricLoader, LazyArrayMetricLoader
from sqldim.core.loaders.edge_projection import (
    EdgeProjectionLoader,
    LazyEdgeProjectionLoader,
)

__all__ = [
    "SnapshotLoader",
    "LazyTransactionLoader",
    "LazySnapshotLoader",
    "AccumulatingLoader",
    "LazyAccumulatingLoader",
    "CumulativeLoader",
    "LazyCumulativeLoader",
    "BitmaskerLoader",
    "LazyBitmaskLoader",
    "ArrayMetricLoader",
    "LazyArrayMetricLoader",
    "EdgeProjectionLoader",
    "LazyEdgeProjectionLoader",
]

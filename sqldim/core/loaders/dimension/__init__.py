"""Dimension loaders: SCD, bitmask, array metric, edge projection patterns."""

from sqldim.core.loaders.dimension.dimensional import DimensionalLoader  # noqa: F401
from sqldim.core.loaders.dimension.bitmask import LazyBitmaskLoader  # noqa: F401
from sqldim.core.loaders.dimension.array_metric import LazyArrayMetricLoader  # noqa: F401
from sqldim.core.loaders.dimension.edge_projection import LazyEdgeProjectionLoader  # noqa: F401

__all__ = [
    "DimensionalLoader",
    "LazyBitmaskLoader",
    "LazyArrayMetricLoader",
    "LazyEdgeProjectionLoader",
]

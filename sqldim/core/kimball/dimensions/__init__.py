"""Pre-built time dimensions: DateDimension, TimeDimension, and junk dimension helpers.

Canonical location within the ``sqldim.core`` domain.

Exports calendar spine (:class:`DateDimension`, :class:`TimeDimension`) and
junk-dimension factories (:func:`make_junk_dimension`,
:func:`populate_junk_dimension`, :func:`populate_junk_dimension_lazy`).

Also exports recursive hierarchy support (:class:`HierarchyMixin`,
:class:`HierarchyRoller`, and the three strategy implementations).
"""

from sqldim.core.kimball.dimensions.date import DateDimension
from sqldim.core.kimball.dimensions.time import TimeDimension
from sqldim.core.kimball.dimensions.junk import (
    make_junk_dimension,
    populate_junk_dimension,
    populate_junk_dimension_lazy,
)
from sqldim.core.kimball.dimensions.hierarchy import (
    HierarchyMixin,
    HierarchyStrategy,
    AdjacencyListStrategy,
    MaterializedPathStrategy,
    ClosureTableStrategy,
    HierarchyRoller,
)

__all__ = [
    "DateDimension",
    "TimeDimension",
    "make_junk_dimension",
    "populate_junk_dimension",
    "populate_junk_dimension_lazy",
    "HierarchyMixin",
    "HierarchyStrategy",
    "AdjacencyListStrategy",
    "MaterializedPathStrategy",
    "ClosureTableStrategy",
    "HierarchyRoller",
]

"""Pre-built time dimensions: DateDimension, TimeDimension, and junk dimension helpers.

Canonical location within the ``sqldim.core`` domain.

Exports calendar spine (:class:`DateDimension`, :class:`TimeDimension`) and
junk-dimension factories (:func:`make_junk_dimension`,
:func:`populate_junk_dimension`, :func:`populate_junk_dimension_lazy`).
"""
from sqldim.core.dimensions.date import DateDimension
from sqldim.core.dimensions.time import TimeDimension
from sqldim.core.dimensions.junk import (
    make_junk_dimension,
    populate_junk_dimension,
    populate_junk_dimension_lazy,
)

__all__ = [
    "DateDimension",
    "TimeDimension",
    "make_junk_dimension",
    "populate_junk_dimension",
    "populate_junk_dimension_lazy",
]

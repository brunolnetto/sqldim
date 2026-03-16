"""Pre-built time dimensions: DateDimension, TimeDimension, and junk dimension helpers.

Exports calendar spine (:class:`DateDimension`, :class:`TimeDimension`) and
junk-dimension factories (:func:`make_junk_dimension`,
:func:`populate_junk_dimension`, :func:`populate_junk_dimension_lazy`).
"""
from sqldim.dimensions.date import DateDimension
from sqldim.dimensions.time import TimeDimension
from sqldim.dimensions.junk import make_junk_dimension, populate_junk_dimension, populate_junk_dimension_lazy

__all__ = [
    "DateDimension",
    "TimeDimension",
    "make_junk_dimension",
    "populate_junk_dimension",
    "populate_junk_dimension_lazy",
]

"""Backward-compatible shim — canonical location is sqldim.core.dimensions."""
from sqldim.core.dimensions import (  # noqa: F401
    DateDimension,
    TimeDimension,
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

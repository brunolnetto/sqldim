"""Backward-compatible shim — canonical location is sqldim.core.dimensions.junk."""
from sqldim.core.dimensions.junk import (  # noqa: F401
    _flag_value_sql,
    make_junk_dimension,
    populate_junk_dimension,
    populate_junk_dimension_lazy,
)

__all__ = [
    "_flag_value_sql",
    "make_junk_dimension",
    "populate_junk_dimension",
    "populate_junk_dimension_lazy",
]

"""Backward-compatible shim — canonical location is sqldim.core.dimensions.date."""
from sqldim.core.dimensions.date import DateDimension  # noqa: F401

__all__ = ["DateDimension"]

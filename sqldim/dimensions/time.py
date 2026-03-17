"""Backward-compatible shim — canonical location is sqldim.core.dimensions.time."""
from sqldim.core.dimensions.time import TimeDimension  # noqa: F401

__all__ = ["TimeDimension"]

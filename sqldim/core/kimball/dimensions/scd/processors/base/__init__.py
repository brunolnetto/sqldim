"""Narwhals-native SCD engine base classes."""

from sqldim.core.kimball.dimensions.scd.processors.base._scd_base import (
    NarwhalsHashStrategy,
    NarwhalsSCDProcessor,
)

__all__ = ["NarwhalsHashStrategy", "NarwhalsSCDProcessor"]

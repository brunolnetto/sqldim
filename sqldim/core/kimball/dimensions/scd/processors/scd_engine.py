"""
Narwhals SCD Engine — vectorized change detection and SCD2 processing.

Replaces Python-loop row-by-row comparisons in SCDHandler with
single-join DataFrame operations.

This module is a thin re-exporter. Implementation is split into:
  _scd_base.py       — shared utilities, NarwhalsHashStrategy, NarwhalsSCDProcessor
  _lazy_type2.py     — LazySCDProcessor (Type 2), LazyType1Processor
  _lazy_type3_6.py   — LazyType3Processor, LazyType6Processor
  _lazy_type4_5.py   — LazyType4Processor, LazyType5Processor
"""
from __future__ import annotations

from sqldim.core.kimball.dimensions.scd.processors._scd_base import (
    NarwhalsHashStrategy,
    NarwhalsSCDProcessor,
)
from sqldim.core.kimball.dimensions.scd.processors._lazy_type2 import (
    LazySCDProcessor,
    LazyType1Processor,
)
from sqldim.core.kimball.dimensions.scd.processors._lazy_type3_6 import (
    LazyType3Processor,
    LazyType6Processor,
)
from sqldim.core.kimball.dimensions.scd.processors._lazy_type4_5 import (
    LazyType4Processor,
    LazyType5Processor,
)
from sqldim.core.kimball.dimensions.scd.processors._lazy_metadata import LazySCDMetadataProcessor

__all__ = [
    "NarwhalsHashStrategy",
    "NarwhalsSCDProcessor",
    "LazySCDProcessor",
    "LazyType1Processor",
    "LazyType3Processor",
    "LazyType6Processor",
    "LazyType4Processor",
    "LazyType5Processor",
    "LazySCDMetadataProcessor",
]

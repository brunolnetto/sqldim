"""
Narwhals SCD Engine — vectorized change detection and SCD2 processing.

Replaces Python-loop row-by-row comparisons in SCDHandler with
single-join DataFrame operations.

This module is a thin re-exporter. Implementation is split into:
  base/          — shared utilities, NarwhalsHashStrategy, NarwhalsSCDProcessor
  lazy/type2/    — LazySCDProcessor (Type 2), LazyType1Processor
  lazy/type3/    — LazyType3Processor
  lazy/type6/    — LazyType6Processor
  lazy/type4_5/  — LazyType4Processor, LazyType5Processor
  lazy/metadata/ — LazySCDMetadataProcessor
"""

from __future__ import annotations

from sqldim.core.kimball.dimensions.scd.processors.base import (
    NarwhalsHashStrategy,
    NarwhalsSCDProcessor,
)
from sqldim.core.kimball.dimensions.scd.processors.lazy.type2 import (
    LazySCDProcessor,
    LazyType1Processor,
)
from sqldim.core.kimball.dimensions.scd.processors.lazy.type3 import (
    LazyType3Processor,
)
from sqldim.core.kimball.dimensions.scd.processors.lazy.type6 import (
    LazyType6Processor,
)
from sqldim.core.kimball.dimensions.scd.processors.lazy.type4_5 import (
    LazyType4Processor,
    LazyType5Processor,
)
from sqldim.core.kimball.dimensions.scd.processors.lazy.metadata import (
    LazySCDMetadataProcessor,
)

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

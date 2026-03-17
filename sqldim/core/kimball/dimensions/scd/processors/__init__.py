"""
Narwhals integration + lazy DuckDB-first processors.

Optional accelerator: ``pip install sqldim[narwhals]``.
Existing list[dict] paths are unchanged.

Exports eager (``NarwhalsAdapter``, ``NarwhalsSCDProcessor``) and lazy
(``LazySCDProcessor``, ``LazyType1Processor`` … ``LazyType6Processor``)
processors.  The lazy variants speak only DuckDB SQL — no Python rows.
Also exports ``NarwhalsSKResolver`` and ``LazySKResolver`` for
surrogate-key resolution without a SQLModel session.
"""
from sqldim.core.kimball.dimensions.scd.processors.adapter import NarwhalsAdapter
from sqldim.core.kimball.dimensions.scd.processors.transforms import col, TransformPipeline
from sqldim.core.kimball.dimensions.scd.processors.backfill import backfill_scd2_narwhals, lazy_backfill_scd2
from sqldim.core.kimball.dimensions.scd.processors.scd_engine import (
    NarwhalsHashStrategy,
    NarwhalsSCDProcessor,
    LazySCDProcessor,
    LazyType1Processor,
    LazyType3Processor,
    LazyType6Processor,
    LazyType4Processor,
    LazyType5Processor,
    LazySCDMetadataProcessor,
)
from sqldim.core.kimball.dimensions.scd.processors.sk_resolver import NarwhalsSKResolver, LazySKResolver

__all__ = [
    "NarwhalsAdapter",
    "col",
    "TransformPipeline",
    "backfill_scd2_narwhals",
    "lazy_backfill_scd2",
    "NarwhalsHashStrategy",
    "NarwhalsSCDProcessor",
    "LazySCDProcessor",
    "LazyType1Processor",
    "LazyType3Processor",
    "LazyType6Processor",
    "LazyType4Processor",
    "LazyType5Processor",
    "LazySCDMetadataProcessor",
    "NarwhalsSKResolver",
    "LazySKResolver",
]

"""
Narwhals integration + lazy DuckDB-first processors.

Optional accelerator: ``pip install sqldim[narwhals]``.
Existing list[dict] paths are unchanged.
"""
from sqldim.processors.adapter import NarwhalsAdapter
from sqldim.processors.transforms import col, TransformPipeline
from sqldim.processors.backfill import backfill_scd2_narwhals, lazy_backfill_scd2
from sqldim.processors.scd_engine import (
    NarwhalsHashStrategy,
    NarwhalsSCDProcessor,
    LazySCDProcessor,
    LazyType1Processor,
    LazyType3Processor,
    LazyType6Processor,
    LazyType4Processor,
    LazyType5Processor,
)
from sqldim.processors.sk_resolver import NarwhalsSKResolver, LazySKResolver

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
    "NarwhalsSKResolver",
    "LazySKResolver",
]

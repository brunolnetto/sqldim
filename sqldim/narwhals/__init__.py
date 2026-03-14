"""
Narwhals integration + lazy DuckDB-first processors.

Optional accelerator: ``pip install sqldim[narwhals]``.
Existing list[dict] paths are unchanged.
"""
from sqldim.narwhals.adapter import NarwhalsAdapter
from sqldim.narwhals.transforms import col, TransformPipeline
from sqldim.narwhals.backfill import backfill_scd2_narwhals, lazy_backfill_scd2
from sqldim.narwhals.scd_engine import (
    NarwhalsHashStrategy,
    NarwhalsSCDProcessor,
    LazySCDProcessor,
    LazyType1Processor,
    LazyType3Processor,
    LazyType6Processor,
)
from sqldim.narwhals.sk_resolver import NarwhalsSKResolver, LazySKResolver

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
    "NarwhalsSKResolver",
    "LazySKResolver",
]

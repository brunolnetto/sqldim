"""Backward-compatible shim — canonical location is sqldim.scd.processors."""
from sqldim.scd.processors import (  # noqa: F401
    NarwhalsAdapter,
    col,
    TransformPipeline,
    backfill_scd2_narwhals,
    lazy_backfill_scd2,
    NarwhalsHashStrategy,
    NarwhalsSCDProcessor,
    LazySCDProcessor,
    LazyType1Processor,
    LazyType3Processor,
    LazyType6Processor,
    LazyType4Processor,
    LazyType5Processor,
    LazySCDMetadataProcessor,
    NarwhalsSKResolver,
    LazySKResolver,
)

__all__ = [
    "NarwhalsAdapter", "col", "TransformPipeline",
    "backfill_scd2_narwhals", "lazy_backfill_scd2",
    "NarwhalsHashStrategy", "NarwhalsSCDProcessor", "LazySCDProcessor",
    "LazyType1Processor", "LazyType3Processor", "LazyType6Processor",
    "LazyType4Processor", "LazyType5Processor", "LazySCDMetadataProcessor",
    "NarwhalsSKResolver", "LazySKResolver",
]

"""
Phase 7 — Narwhals integration.

Optional accelerator: ``pip install sqldim[narwhals]``.
Existing list[dict] paths are unchanged.
"""
from sqldim.narwhals.adapter import NarwhalsAdapter
from sqldim.narwhals.transforms import col, TransformPipeline
from sqldim.narwhals.backfill import backfill_scd2_narwhals

__all__ = [
    "NarwhalsAdapter",
    "col",
    "TransformPipeline",
    "backfill_scd2_narwhals",
]

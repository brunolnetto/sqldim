"""Showcase: composable SCD pipelines across multiple dimension types.

Each example composes SCD-2, SCD-3, and SCD-6 handlers in a
single pipeline, demonstrating how mixins layer cleanly together.
"""
from sqldim.examples.features.composition.showcase import run_showcase

__all__ = ["run_showcase"]

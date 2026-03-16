"""Showcase: pre-built date, time, and junk dimension generation and usage.

Populates :class:`DateDimension` and :class:`TimeDimension` tables and
creates an example junk dimension via :func:`make_junk_dimension`.
"""
from sqldim.examples.features.prebuilt_dims.showcase import run_showcase

__all__ = ["run_showcase"]

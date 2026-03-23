"""Showcase: SCD type 1, 2, 3, and 6 change detection and versioning.

Each SCD variant is exercised end-to-end: detect changes, close old
versions, insert new rows, and validate the resulting history table.
"""

from sqldim.application.examples.features.scd_types.showcase import run_showcase

__all__ = ["run_showcase"]

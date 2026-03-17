"""sqldim.scd — SCD handlers, detection, audit, backfill, and processors.

processors/ subpackage contains the Narwhals + lazy DuckDB SCD processors.
"""
from sqldim.core.kimball.dimensions.scd.handler import SCDHandler, SCDResult
from sqldim.core.kimball.dimensions.scd import processors  # noqa: F401 — expose as subpackage

__all__ = ["SCDHandler", "SCDResult", "processors"]

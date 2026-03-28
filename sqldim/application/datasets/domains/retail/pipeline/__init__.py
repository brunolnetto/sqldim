"""retail pipeline package."""

from sqldim.application.datasets.domains.retail.pipeline.builder import (
    build_pipeline,
    GOLD_TABLES,
)

__all__ = ["build_pipeline", "GOLD_TABLES"]

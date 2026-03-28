"""RetailMedallionPipelineSource — wires the retail medallion pipeline into the NL graph.

This source builds all three layers (bronze → silver → gold) in an ephemeral
in-memory DuckDB connection and exposes only the gold-layer tables to the NL
agent.  It implements the same ``PipelineSource`` protocol used by the rest
of the eval/ask infrastructure, so it is a drop-in replacement for
``DatasetPipelineSource`` for the retail domain.

Usage inside eval cases::

    from sqldim.application.datasets.domains.retail.sources.medallion import (
        RetailMedallionPipelineSource,
    )

    EvalCase(
        id="retail.01.revenue_by_segment",
        dataset="retail",
        pipeline_source_factory=RetailMedallionPipelineSource,
        ...
    )
"""

from __future__ import annotations

import duckdb

from sqldim.application._pipeline_sources import PipelineSource
from sqldim.application.datasets.domains.retail.pipeline.builder import (
    GOLD_TABLES,
    build_pipeline,
)


class RetailMedallionPipelineSource(PipelineSource):
    """Builds the full retail medallion pipeline and exposes its gold layer.

    Bronze → silver → gold transforms are executed inside ``setup()`` in
    an ephemeral in-memory DuckDB connection.

    ``get_table_names()`` returns only the gold-layer tables so the NL agent
    reasons exclusively over the business-ready semantic layer:
    ``dim_customer``, ``fct_daily_sales``, and ``fct_cohort_retention``.

    Parameters
    ----------
    seed:
        RNG seed forwarded to the pipeline builder for reproducible data.
    """

    def __init__(self, seed: int = 42) -> None:
        self._seed = seed
        self._con: duckdb.DuckDBPyConnection | None = None

    def setup(self) -> None:
        """Build all three medallion layers in a fresh in-memory DuckDB."""
        self._con = duckdb.connect(":memory:")
        build_pipeline(self._con, seed=self._seed)

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        if self._con is None:
            raise RuntimeError("setup() must be called before get_connection()")
        return self._con

    def get_table_names(self) -> list[str]:
        """Return gold-layer table names only."""
        return list(GOLD_TABLES)

    def teardown(self) -> None:
        if self._con is not None:
            self._con.close()
            self._con = None

    @property
    def label(self) -> str:
        return "medallion:retail:gold"

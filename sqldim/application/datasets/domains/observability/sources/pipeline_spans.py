"""PipelineSpanSource — real pipeline execution spans from OTelCollector.

Instead of synthetic Faker data, this source **runs the other domain
pipelines** with an :class:`~sqldim.observability.OTelCollector` and
materialises the captured :class:`~sqldim.observability.PipelineSpan`
objects into a DuckDB table.
"""

from __future__ import annotations

from typing import Any

import duckdb

from sqldim.application.datasets.base import (
    BaseSource,
    DatasetFactory,
    SourceProvider,
)


_SPAN_DDL = """
CREATE TABLE IF NOT EXISTS {{table}} (
    span_id        INTEGER PRIMARY KEY,
    name           VARCHAR NOT NULL,
    status         VARCHAR NOT NULL DEFAULT 'ok',
    duration_s     DOUBLE  NOT NULL DEFAULT 0.0,
    start_time     VARCHAR,
    end_time       VARCHAR,
    error          VARCHAR,
    pipeline       VARCHAR,
    domain         VARCHAR
)
"""


@DatasetFactory.register("pipeline_spans")
class PipelineSpanSource(BaseSource):
    """Materialises real :class:`PipelineSpan` telemetry into DuckDB.

    Data is self-populated in :meth:`setup` by running every discoverable
    domain pipeline under an :class:`OTelCollector`.  No synthetic rows.

    Staging schema::

        span_id     INTEGER
        name        VARCHAR   -- span name (e.g. build_pipeline qualname)
        status      VARCHAR   -- 'ok' | 'error'
        duration_s  DOUBLE    -- wall-clock seconds
        start_time  VARCHAR   -- ISO timestamp
        end_time    VARCHAR   -- ISO timestamp
        error       VARCHAR   -- error message or NULL
        pipeline    VARCHAR   -- domain pipeline name
        domain      VARCHAR   -- domain identifier
    """

    DIM_DDL = _SPAN_DDL

    provider = SourceProvider(
        name="sqldim OTelCollector",
        description="Pipeline execution spans captured by running domain pipelines with instrumentation.",
        url="",
        auth_required=False,
        requires=[],
    )

    def setup(self, con: duckdb.DuckDBPyConnection, table: str = "pipeline_spans") -> None:
        con.execute(_SPAN_DDL.replace("{{table}}", table))
        from sqldim.application.datasets.domains.observability.sources._collector import (
            collect_telemetry,
        )

        spans, _ = collect_telemetry()
        if not spans:
            return
        rows = [
            (
                i + 1,
                s.name,
                s.status.value,
                s.duration_s,
                s.start_time.isoformat() if s.start_time else None,
                s.end_time.isoformat() if s.end_time else None,
                s.error,
                s.attributes.get("pipeline", ""),
                s.attributes.get("domain", ""),
            )
            for i, s in enumerate(spans)
        ]
        con.executemany(f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)

    def teardown(self, con: duckdb.DuckDBPyConnection, table: str = "pipeline_spans") -> None:
        con.execute(f"DROP TABLE IF EXISTS {table}")

    def snapshot(self) -> Any:
        raise NotImplementedError("PipelineSpanSource self-populates in setup()")

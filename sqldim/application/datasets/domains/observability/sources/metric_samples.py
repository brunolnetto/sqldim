"""MetricSampleSource — real metric observations from OTelCollector.

Instead of synthetic Faker data, this source runs the other domain
pipelines with an :class:`~sqldim.observability.OTelCollector` and
materialises the captured :class:`~sqldim.observability.MetricSample`
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


_METRIC_DDL = """
CREATE TABLE IF NOT EXISTS {{table}} (
    sample_id    INTEGER PRIMARY KEY,
    name         VARCHAR NOT NULL,
    value        DOUBLE  NOT NULL,
    kind         VARCHAR NOT NULL DEFAULT 'gauge',
    label_key    VARCHAR,
    label_value  VARCHAR,
    timestamp    VARCHAR,
    domain       VARCHAR
)
"""


@DatasetFactory.register("metric_samples")
class MetricSampleSource(BaseSource):
    """Materialises real :class:`MetricSample` telemetry into DuckDB.

    Data is self-populated in :meth:`setup` by running every discoverable
    domain pipeline under an :class:`OTelCollector`.  No synthetic rows.

    Staging schema::

        sample_id    INTEGER
        name         VARCHAR   -- metric name
        value        DOUBLE    -- observed numeric value
        kind         VARCHAR   -- gauge / counter / histogram
        label_key    VARCHAR   -- first label key (flattened)
        label_value  VARCHAR   -- first label value (flattened)
        timestamp    VARCHAR   -- ISO timestamp
        domain       VARCHAR   -- domain identifier (from labels)
    """

    DIM_DDL = _METRIC_DDL

    provider = SourceProvider(
        name="sqldim OTelCollector",
        description="Metric samples captured by running domain pipelines with instrumentation.",
        url="",
        auth_required=False,
        requires=[],
    )

    def setup(self, con: duckdb.DuckDBPyConnection, table: str = "metric_samples") -> None:
        con.execute(_METRIC_DDL.replace("{{table}}", table))
        from sqldim.application.datasets.domains.observability.sources._collector import (
            collect_telemetry,
        )

        _, metrics = collect_telemetry()
        if not metrics:
            return
        rows = []
        for i, m in enumerate(metrics):
            # Flatten labels to first key/value pair for SQL-friendliness.
            label_key = next(iter(m.labels), None)
            label_value = m.labels.get(label_key, None) if label_key else None
            rows.append((
                i + 1,
                m.name,
                m.value,
                m.kind.value,
                label_key,
                label_value,
                m.timestamp.isoformat() if m.timestamp else None,
                m.labels.get("domain", ""),
            ))
        con.executemany(f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)

    def teardown(self, con: duckdb.DuckDBPyConnection, table: str = "metric_samples") -> None:
        con.execute(f"DROP TABLE IF EXISTS {table}")

    def snapshot(self) -> Any:
        raise NotImplementedError("MetricSampleSource self-populates in setup()")

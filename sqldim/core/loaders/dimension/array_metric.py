"""
LazyArrayMetricLoader — month-partitioned array metrics via DuckDB SQL.
Reproduces array_metrics_analysis.sql and generate_monthly_array_metrics.sql.
"""

from __future__ import annotations

import asyncio
from sqldim.core.loaders._utils import _resolve_table, _assert_not_dimension


# ---------------------------------------------------------------------------
# Lazy (DuckDB-first) loader — no Python data, no OOM risk
# ---------------------------------------------------------------------------


class LazyArrayMetricLoader:
    """
    Array metric loader using DuckDB ``list_transform`` operations.

    Builds a 31-element array for each row, placing *value_column* at the
    correct day offset inside DuckDB SQL — no Python element-wise loop.

    Usage::

        with DuckDBSink("/tmp/dev.duckdb") as sink:
            loader = LazyArrayMetricLoader(
                table_name="fact_monthly_metrics",
                partition_key="user_id",
                value_column="revenue",
                metric_name="monthly_revenue",
                month_start=date(2024, 1, 1),
                sink=sink,
            )
            rows = loader.process("daily_revenue.parquet", target_date=date(2024, 1, 15))
    """

    def __init__(
        self,
        table: str | type,
        partition_key: str,
        value_column: str,
        metric_name: str,
        month_start,
        sink,
        batch_size: int = 100_000,
        con=None,
    ):
        import duckdb as _duckdb

        _assert_not_dimension(table, "LazyArrayMetricLoader")
        self.table_name = _resolve_table(table)
        self.partition_key = partition_key
        self.value_column = value_column
        self.metric_name = metric_name
        self.month_start = month_start
        self.sink = sink
        self.batch_size = batch_size
        self._con = con or _duckdb.connect()
        self._model_cls: type | None = (
            None  # set by factory when created via as_loader()
        )

    def process(self, source, target_date) -> int:
        """
        Read *source*, build a 31-element array with the metric value at
        *target_date*'s day offset, and write to *table_name*.
        Returns rows written.
        """
        day_offset = (target_date - self.month_start).days
        ms = self.month_start
        pk = self.partition_key
        vc = self.value_column

        from sqldim.sources import coerce_source

        _sql = coerce_source(source).as_sql(self._con)
        self._con.execute(f"""
            CREATE OR REPLACE VIEW incoming AS
            SELECT * FROM ({_sql})
        """)

        # DuckDB list_transform: build [0.0, ..., value_at_offset, ..., 0.0]
        self._con.execute(f"""
            CREATE OR REPLACE VIEW array_metric_view AS
            SELECT
                {pk},
                '{self.metric_name}' AS metric_name,
                '{ms}'::DATE         AS month_start,
                list_transform(
                    range(31),
                    i -> CASE
                             WHEN i = {day_offset}
                             THEN CAST({vc} AS DOUBLE)
                             ELSE 0.0
                         END
                ) AS metric_array
            FROM incoming
        """)
        return self.sink.write(
            self._con, "array_metric_view", self.table_name, self.batch_size
        )

    async def aload(self, source, target_date) -> int:
        """Async wrapper — runs :meth:`process` in a thread pool executor."""
        return await asyncio.to_thread(self.process, source, target_date)

    #: Alias for :meth:`process` — unified sync entry point across all loaders.
    load = process

"""
LazyCumulativeLoader — incremental FULL OUTER JOIN history via DuckDB SQL.
Reproduces the pipeline_query.sql and user_cumulated_populate.sql patterns.
"""

from __future__ import annotations

import asyncio
from sqldim.core.loaders._utils import _resolve_table, _assert_not_dimension


# ---------------------------------------------------------------------------
# Lazy (DuckDB-first) loader — no Python data, no OOM risk
# ---------------------------------------------------------------------------


class LazyCumulativeLoader:
    """
    Cumulative history loader using DuckDB FULL OUTER JOIN.

    Yesterday's state is read from the sink (no Python load).
    Today's batch comes from a Parquet file.
    The merge and history-array accumulation are expressed as a single
    DuckDB SQL VIEW — the result is written back via the sink.

    DuckDB ``struct_pack`` builds the per-period stats struct and
    ``list_append`` grows the history array — no Python element-wise loop.

    Usage::

        with DuckDBSink("/tmp/dev.duckdb") as sink:
            loader = LazyCumulativeLoader(
                table_name="player_seasons_cumulated",
                partition_key="player_name",
                cumulative_column="seasons",
                metric_columns=["pts", "ast", "reb"],
                sink=sink,
            )
            rows = loader.process("today_stats.parquet", target_period=2024)
    """

    def __init__(
        self,
        table: str | type,
        partition_key: str,
        cumulative_column: str,
        metric_columns: list[str],
        sink,
        current_period_column: str = "current_season",
        batch_size: int = 100_000,
        con=None,
    ):
        import duckdb as _duckdb

        _assert_not_dimension(table, "LazyCumulativeLoader")
        self.table_name = _resolve_table(table)
        self.partition_key = partition_key
        self.cumulative_column = cumulative_column
        self.metric_columns = metric_columns
        self.sink = sink
        self.current_period_column = current_period_column
        self.batch_size = batch_size
        self._con = con or _duckdb.connect()
        self._model_cls: type | None = (
            None  # set by factory when created via as_loader()
        )

    def process(self, today_source, target_period) -> int:
        """
        Full-outer-join yesterday (from sink) with today (from Parquet).
        Appends today's metrics to the history array for active members;
        increments ``years_since_last_active`` for dormant members.
        Returns rows written.
        """
        tn = self.table_name
        pk = self.partition_key
        cc = self.cumulative_column
        pcc = self.current_period_column

        yesterday_sql = self.sink.current_state_sql(tn)

        from sqldim.sources import coerce_source

        def _as_subquery(sql: str) -> str:
            return f"({sql})" if sql.strip().upper().startswith("SELECT") else sql

        _src_sql = coerce_source(today_source).as_sql(self._con)
        self._con.execute(f"""
            CREATE OR REPLACE VIEW today_incoming AS
            SELECT * FROM ({_src_sql})
        """)

        # Materialise yesterday's state as a TABLE so the remote source is
        # scanned exactly once (VIEW would re-scan on every reference).
        self._con.execute(f"""
            CREATE OR REPLACE TABLE yesterday AS
            SELECT * FROM {_as_subquery(yesterday_sql)}
        """)

        # Build a struct_pack expression for today's metrics
        struct_fields = ", ".join(f"{c} := t.{c}" for c in self.metric_columns)

        self._con.execute(f"""
            CREATE OR REPLACE VIEW cumulated AS
            SELECT
                COALESCE(y.{pk}, t.{pk}) AS {pk},
                CASE
                    WHEN t.{pk} IS NOT NULL
                    THEN list_append(
                        COALESCE(y.{cc}, []),
                        struct_pack({struct_fields}, period := '{target_period}')
                    )
                    ELSE y.{cc}
                END AS {cc},
                CASE WHEN t.{pk} IS NOT NULL THEN TRUE ELSE FALSE END AS is_active,
                CASE
                    WHEN t.{pk} IS NOT NULL THEN 0
                    ELSE COALESCE(y.years_since_last_active, 0) + 1
                END AS years_since_last_active,
                '{target_period}' AS {pcc}
            FROM yesterday y
            FULL OUTER JOIN today_incoming t
                        ON y.{pk} = t.{pk}
        """)
        rows = self.sink.write(self._con, "cumulated", tn, self.batch_size)
        self._con.execute("DROP VIEW IF EXISTS today_incoming")
        self._con.execute("DROP TABLE IF EXISTS yesterday")
        self._con.execute("DROP VIEW IF EXISTS cumulated")
        return rows

    async def aload(self, today_source, target_period) -> int:
        """Async wrapper — runs :meth:`process` in a thread pool executor."""
        return await asyncio.to_thread(self.process, today_source, target_period)

    #: Alias for :meth:`process` — unified sync entry point across all loaders.
    load = process

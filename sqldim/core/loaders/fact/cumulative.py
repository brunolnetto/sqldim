"""
LazyCumulativeLoader — incremental FULL OUTER JOIN history via DuckDB SQL.
Reproduces the pipeline_query.sql and user_cumulated_populate.sql patterns.
"""

from __future__ import annotations

import asyncio

from sqldim.core.loaders._utils import _assert_not_dimension, _resolve_table
from sqldim.exceptions import IdempotencyError, LoadError
from sqldim.sinks._connection import connection_for
from sqldim.sources.base import from_item


def _lit(value: object) -> str:
    """*value* as a SQL text literal (periods are stored as text)."""
    return "'" + str(value).replace("'", "''") + "'"


# ---------------------------------------------------------------------------
# Lazy (DuckDB-first) loader — no Python data, no OOM risk
# ---------------------------------------------------------------------------


class LazyCumulativeLoader:
    """
    Cumulative history loader using DuckDB FULL OUTER JOIN.

    The table holds one row per partition key **per period**, as in the
    patterns this loader reproduces: each run reads the rows of the previous
    period from the sink ("yesterday"), joins them with today's batch, and
    appends the rows of *target_period*, each carrying the whole history
    array.  Earlier periods' rows are kept; the latest state of a key is its
    row with the latest ``current_period_column``.

    The merge and history-array accumulation are expressed as a single
    DuckDB SQL VIEW — the result is written back via the sink.  DuckDB
    ``struct_pack`` builds the per-period stats struct and ``list_append``
    grows the history array — no Python element-wise loop.

    Usage::

        with DuckDBSink("/tmp/dev.duckdb") as sink:
            loader = LazyCumulativeLoader(
                table="player_seasons_cumulated",
                partition_key="player_name",
                cumulative_column="seasons",
                metric_columns=["pts", "ast", "reb"],
                sink=sink,
            )
            loader.process("stats_2023.parquet", target_period=2023)
            loader.process("stats_2024.parquet", target_period=2024,
                           previous_period=2023)
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
        _assert_not_dimension(table, "LazyCumulativeLoader")
        self.table_name = _resolve_table(table)
        self.partition_key = partition_key
        self.cumulative_column = cumulative_column
        self.metric_columns = metric_columns
        self.sink = sink
        self.current_period_column = current_period_column
        self.batch_size = batch_size
        self._con = connection_for(sink, con)
        self._model_cls: type | None = (
            None  # set by factory when created via as_loader()
        )

    def _previous(self, state: str, target_period, previous_period) -> str | None:
        """The period whose rows are yesterday, as stored text, or ``None``.

        Raises :class:`IdempotencyError` when *target_period* is already
        stored (a re-run would add a second row per key), and
        :class:`LoadError` when the previous period is not determined: the
        table holds several periods and *previous_period* is not given, or
        it holds no row of the *previous_period* given.
        """
        pcc = self.current_period_column
        stored = {
            r[0]
            for r in self._con.execute(
                f"SELECT DISTINCT CAST({pcc} AS VARCHAR) FROM {state}"
            ).fetchall()
            if r[0] is not None
        }
        if str(target_period) in stored:
            raise IdempotencyError(
                f"{self.table_name} already holds period {str(target_period)!r}; "
                "loading it again would add a second row per key"
            )
        if previous_period is not None:
            if str(previous_period) not in stored:
                raise LoadError(
                    f"{self.table_name} holds no row of period "
                    f"{str(previous_period)!r}; stored: {sorted(stored)}"
                )
            return str(previous_period)
        if len(stored) > 1:
            raise LoadError(
                f"{self.table_name} holds periods {sorted(stored)}; pass "
                "previous_period to say which one target_period follows"
            )
        return next(iter(stored), None)

    def process(self, today_source, target_period, previous_period=None) -> int:
        """
        Full-outer-join the rows of the previous period (from the sink) with
        today's batch, and append the rows of *target_period*.

        Appends today's metrics to the history array for active members;
        increments ``years_since_last_active`` for dormant members.
        *previous_period* names the period read as yesterday; it may be
        omitted while the table holds at most one period.  See
        :meth:`_previous` for the errors.  Returns rows written.
        """
        tn = self.table_name
        pk = self.partition_key
        cc = self.cumulative_column
        pcc = self.current_period_column

        state = from_item(self.sink.current_state_sql(tn))
        previous = self._previous(state, target_period, previous_period)

        from sqldim.sources import coerce_source

        _src_sql = coerce_source(today_source).as_sql(self._con)
        self._con.execute(f"""
            CREATE OR REPLACE VIEW today_incoming AS
            SELECT * FROM {from_item(_src_sql)}
        """)

        # Materialise yesterday's state as a TABLE so the remote source is
        # scanned exactly once (VIEW would re-scan on every reference).
        keep = (
            "FALSE"
            if previous is None
            else f"CAST({pcc} AS VARCHAR) = {_lit(previous)}"
        )
        self._con.execute(f"""
            CREATE OR REPLACE TABLE yesterday AS
            SELECT * FROM {state} WHERE {keep}
        """)

        # Build a struct_pack expression for today's metrics
        struct_fields = ", ".join(f"{c} := t.{c}" for c in self.metric_columns)
        period = _lit(target_period)

        self._con.execute(f"""
            CREATE OR REPLACE VIEW cumulated AS
            SELECT
                COALESCE(y.{pk}, t.{pk}) AS {pk},
                CASE
                    WHEN t.{pk} IS NOT NULL
                    THEN list_append(
                        COALESCE(y.{cc}, []),
                        struct_pack({struct_fields}, period := {period})
                    )
                    ELSE y.{cc}
                END AS {cc},
                CASE WHEN t.{pk} IS NOT NULL THEN TRUE ELSE FALSE END AS is_active,
                CASE
                    WHEN t.{pk} IS NOT NULL THEN 0
                    ELSE COALESCE(y.years_since_last_active, 0) + 1
                END AS years_since_last_active,
                {period} AS {pcc}
            FROM yesterday y
            FULL OUTER JOIN today_incoming t
                        ON y.{pk} = t.{pk}
        """)
        rows = self.sink.write(self._con, "cumulated", tn, self.batch_size)
        self._con.execute("DROP VIEW IF EXISTS today_incoming")
        self._con.execute("DROP TABLE IF EXISTS yesterday")
        self._con.execute("DROP VIEW IF EXISTS cumulated")
        return rows

    async def aload(self, today_source, target_period, previous_period=None) -> int:
        """Async wrapper — runs :meth:`process` in a thread pool executor."""
        return await asyncio.to_thread(
            self.process, today_source, target_period, previous_period
        )

    #: Alias for :meth:`process` — unified sync entry point across all loaders.
    load = process

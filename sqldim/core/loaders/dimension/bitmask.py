"""
LazyBitmaskLoader — activity bitmask facts via DuckDB bitwise aggregation.
Reproduces generate_datelist.sql and analyze_datelist.sql patterns.
"""

from __future__ import annotations

import asyncio
from sqldim.core.loaders._utils import _resolve_table, _assert_not_dimension



# ---------------------------------------------------------------------------
# Lazy (DuckDB-first) loader — no Python data, no OOM risk
# ---------------------------------------------------------------------------


class LazyBitmaskLoader:
    """
    Activity bitmask loader using DuckDB bitwise aggregation.

    Reads a Parquet source whose column *dates_column* is an array/list
    of ISO date strings.  DuckDB unnests the array, computes the offset
    from *reference_date*, and produces the bitmask as a SUM of bit-shifts
    — entirely inside the query engine.

    Usage::

        with DuckDBSink("/tmp/dev.duckdb") as sink:
            loader = LazyBitmaskLoader(
                table_name="fact_activity_bitmask",
                partition_key="user_id",
                dates_column="dates_active",
                reference_date="2024-01-31",
                window_days=32,
                sink=sink,
            )
            rows = loader.process("user_activity.parquet")
    """

    def __init__(
        self,
        table: str | type,
        partition_key: str,
        dates_column: str,
        reference_date,
        sink,
        window_days: int = 32,
        batch_size: int = 100_000,
        con=None,
    ):
        import duckdb as _duckdb

        _assert_not_dimension(table, "LazyBitmaskLoader")
        self.table_name = _resolve_table(table)
        self.partition_key = partition_key
        self.dates_column = dates_column
        self.reference_date = reference_date
        self.sink = sink
        self.window_days = window_days
        self.batch_size = batch_size
        self._con = con or _duckdb.connect()

    def process(self, source) -> int:
        """
        Compute bitmask for each partition key and write to *table_name*.

        Logic:
            UNNEST dates → DATEDIFF → SUM(1 << (window - 1 - offset))
        Returns rows written.
        """
        pk = self.partition_key
        dc = self.dates_column
        rd = self.reference_date
        wd = self.window_days

        from sqldim.sources import coerce_source

        _sql = coerce_source(source).as_sql(self._con)
        self._con.execute(f"""
            CREATE OR REPLACE VIEW incoming AS
            SELECT * FROM ({_sql})
        """)

        self._con.execute(f"""
            CREATE OR REPLACE VIEW bitmask_view AS
            SELECT
                {pk},
                CAST(SUM(
                    CASE
                        WHEN days_since >= 0 AND days_since < {wd}
                        THEN CAST(1 AS BIGINT) << ({wd} - 1 - days_since)
                        ELSE 0
                    END
                ) AS BIGINT) AS datelist_int
            FROM (
                SELECT
                    {pk},
                    DATEDIFF('day', TRY_CAST(date_col AS DATE), '{rd}'::DATE) AS days_since
                FROM (
                    SELECT {pk}, UNNEST({dc}) AS date_col
                    FROM incoming
                )
            )
            GROUP BY {pk}
        """)
        return self.sink.write(
            self._con, "bitmask_view", self.table_name, self.batch_size
        )

    async def aload(self, source) -> int:
        """Async wrapper — runs :meth:`process` in a thread pool executor."""
        return await asyncio.to_thread(self.process, source)

    #: Alias for :meth:`process` — unified sync entry point across all loaders.
    load = process

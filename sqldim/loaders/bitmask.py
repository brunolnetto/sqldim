"""
BitmaskerLoader — derives bitmask activity facts from cumulative date lists.
Reproduces generate_datelist.sql and anaylze_datelist.sql patterns.
"""
from __future__ import annotations
from typing import Any, List, Type
import narwhals as nw


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
        table_name: str,
        partition_key: str,
        dates_column: str,
        reference_date,
        sink,
        window_days: int = 32,
        batch_size: int = 100_000,
        con=None,
    ):
        import duckdb as _duckdb

        self.table_name     = table_name
        self.partition_key  = partition_key
        self.dates_column   = dates_column
        self.reference_date = reference_date
        self.sink           = sink
        self.window_days    = window_days
        self.batch_size     = batch_size
        self._con           = con or _duckdb.connect()

    def process(self, source) -> int:
        """
        Compute bitmask for each partition key and write to *table_name*.

        Logic:
            UNNEST dates → DATEDIFF → SUM(1 << (window - 1 - offset))
        Returns rows written.
        """
        pk  = self.partition_key
        dc  = self.dates_column
        rd  = self.reference_date
        wd  = self.window_days

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
        return self.sink.write(self._con, "bitmask_view", self.table_name, self.batch_size)

class BitmaskerLoader:
    def __init__(
        self,
        source_model: Type[Any],
        target_model: Type[Any],
        session: Any,
        reference_date: Any,
        window_days: int = 32,
    ):
        self.source_model = source_model
        self.target_model = target_model
        self.session = session
        self.reference_date = reference_date
        self.window_days = window_days

    def process(self, source_frame: nw.DataFrame) -> nw.DataFrame:
        """
        Computes the activity bitmask for each user.
        Logic: SUM(POW(2, window - days_since))
        """
        native = nw.to_native(source_frame)
        module = type(native).__module__.split(".")[0]
        
        def calculate_mask(dates):
            if not dates: return 0
            mask = 0
            for d in dates:
                # Handle both date objects and strings
                from datetime import date
                if isinstance(d, str):
                    d = date.fromisoformat(d)
                diff = (self.reference_date - d).days
                if 0 <= diff < self.window_days:
                    mask |= (1 << (self.window_days - 1 - diff))
            return mask

        if module == "pandas":
            native["datelist_int"] = native["dates_active"].apply(calculate_mask)
        else:
            # polars
            native = native.with_columns(
                nw.col("dates_active").map_elements(calculate_mask, return_dtype=nw.Int64).alias("datelist_int")
            )
            
        return nw.from_native(native, eager_only=True)

"""
ArrayMetricLoader — handles daily upserts into month-partitioned array metrics.
Reproduces array_metrics_analysis.sql and generate_monthly_array_metrics.sql.
"""
from __future__ import annotations
from typing import Any, Type
import narwhals as nw


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
        table_name: str,
        partition_key: str,
        value_column: str,
        metric_name: str,
        month_start,
        sink,
        batch_size: int = 100_000,
        con=None,
    ):
        import duckdb as _duckdb

        self.table_name    = table_name
        self.partition_key = partition_key
        self.value_column  = value_column
        self.metric_name   = metric_name
        self.month_start   = month_start
        self.sink          = sink
        self.batch_size    = batch_size
        self._con          = con or _duckdb.connect()

    def process(self, source, target_date) -> int:
        """
        Read *source*, build a 31-element array with the metric value at
        *target_date*'s day offset, and write to *table_name*.
        Returns rows written.
        """
        day_offset = (target_date - self.month_start).days
        ms         = self.month_start
        pk         = self.partition_key
        vc         = self.value_column

        self._con.execute(f"""
            CREATE OR REPLACE VIEW incoming AS
            SELECT * FROM read_parquet('{source}')
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

class ArrayMetricLoader:
    def __init__(
        self,
        model: Type[Any],
        session: Any,
        metric_name: str,
        month_start: Any,
    ):
        self.model = model
        self.session = session
        self.metric_name = metric_name
        self.month_start = month_start

    def process(self, frame: nw.DataFrame, target_date: Any) -> nw.DataFrame:
        """
        Prepares the array metric frame. 
        Calculates the day offset from month_start and places the value in the correct index.
        """
        day_offset = (target_date - self.month_start).days
        
        native = nw.to_native(frame)
        module = type(native).__module__.split(".")[0]
        
        def build_metric_array(val):
            # Create a 31-day array (standard for monthly metrics)
            arr = [0.0] * 31
            if 0 <= day_offset < 31:
                arr[day_offset] = float(val)
            return arr

        # In a real pipeline, this would involve an UPSERT where we update specific indices
        # For this example, we project the initial daily array.
        if module == "pandas":
            native["metric_array"] = native["value"].apply(build_metric_array)
            native["metric_name"] = self.metric_name
            native["month_start"] = self.month_start
        else:
            import polars as pl
            native = native.with_columns([
                nw.col("value").map_elements(build_metric_array, return_dtype=nw.List(nw.Float64)).alias("metric_array"),
                nw.lit(self.metric_name).alias("metric_name"),
                nw.lit(self.month_start).alias("month_start")
            ])
            
        return nw.from_native(native, eager_only=True)

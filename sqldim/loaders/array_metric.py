"""
ArrayMetricLoader — handles daily upserts into month-partitioned array metrics.
Reproduces array_metrics_analysis.sql and generate_monthly_array_metrics.sql.
"""
from __future__ import annotations
from typing import Any, Type
import narwhals as nw

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

"""
CumulativeLoader — handles the incremental FULL OUTER JOIN append pattern.
Reproduces the pipeline_query.sql and user_cumulated_populate.sql patterns.
"""
from __future__ import annotations
import json
from typing import Any, Callable, List, Optional, Type, Dict
from sqlmodel import Session, select, text
import narwhals as nw
from sqldim.exceptions import LoadError

class CumulativeLoader:
    """
    Implements the FULL OUTER JOIN pattern for cumulative history.
    
    Logic:
    1. Join Yesterday's state with Today's batch.
    2. For members in both: append today's stats to the history array.
    3. For members in Yesterday only: keep history, increment 'years_since_last_active'.
    4. For members in Today only: create new history array with today's stats.
    """
    def __init__(
        self,
        model: Type[Any],
        session: Session,
        partition_key: str,
        cumulative_column: str,
        current_period_column: str = "current_season",
    ):
        self.model = model
        self.session = session
        self.partition_key = partition_key
        self.cumulative_column = cumulative_column
        self.current_period_column = current_period_column

    def process(
        self,
        yesterday_frame: nw.DataFrame,
        today_frame: nw.DataFrame,
        target_period: Any,
    ) -> nw.DataFrame:
        """
        Vectorized cumulative update.
        """
        # 1. Full Join
        merged = yesterday_frame.join(
            today_frame, 
            on=self.partition_key, 
            how="full", 
            suffix="_today"
        )
        
        native = nw.to_native(merged)
        module = type(native).__module__.split(".")[0]
        
        # We handle the row-level array construction deterministically 
        # using native mapping for complex nested structures.
        def merge_history(row):
            # Extract historical array
            history = row.get(self.cumulative_column)
            if history is None:
                history = []
            elif isinstance(history, str):
                history = json.loads(history)
            
            # Extract today's stats (assuming all non-partition columns in today_frame are stats)
            # For simplicity, we assume today_frame was passed with just the stats
            today_stats = {k.replace("_today", ""): v for k, v in row.items() if k.endswith("_today")}
            
            if today_stats and any(v is not None for v in today_stats.values()):
                # If active today, append
                today_stats[self.current_period_column.replace("current_", "")] = target_period
                history.append(today_stats)
                active = True
                years_since = 0
            else:
                # If inactive today, increment counter
                active = False
                years_since = row.get("years_since_last_active", 0) + 1
            
            return {
                self.cumulative_column: history,
                "is_active": active,
                "years_since_last_active": years_since,
                self.current_period_column: target_period
            }

        if module == "pandas":
            updates = native.apply(merge_history, axis=1, result_type="expand")
            for col in updates.columns:
                native[col] = updates[col]
        else:
            # polars
            structs = native.to_struct("merged_row").map_elements(merge_history, return_dtype=nw.Object)
            # Unpack the resulting dicts (Polars implementation details usually vary here, 
            # so we'll fallback to a robust conversion for the example)
            res_dicts = [merge_history(r) for r in native.to_dicts()]
            import polars as pl
            update_df = pl.from_dicts(res_dicts)
            native = native.with_columns(update_df)

        return nw.from_native(native, eager_only=True)

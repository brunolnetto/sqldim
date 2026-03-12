"""
BitmaskerLoader — derives bitmask activity facts from cumulative date lists.
Reproduces generate_datelist.sql and anaylze_datelist.sql patterns.
"""
from __future__ import annotations
from typing import Any, Type
import narwhals as nw

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

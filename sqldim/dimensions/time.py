from datetime import time
from typing import List
from sqlmodel import Session
from sqldim import DimensionModel, Field

class TimeDimension(DimensionModel, table=True):
    """
    Pre-built Time-of-Day Dimension for intra-day analysis.
    Granularity: one row per minute (1440 rows total).
    """
    __natural_key__ = ["time_value"]

    id: int = Field(primary_key=True, surrogate_key=True)
    time_value: str = Field(natural_key=True, index=True, unique=True)  # HH:MM
    hour: int
    minute: int
    period: str   # AM | PM
    hour_12: int
    time_of_day: str  # Morning | Afternoon | Evening | Night

    @classmethod
    def _from_time(cls, h: int, m: int) -> "TimeDimension":
        t = time(h, m)
        period = "AM" if h < 12 else "PM"
        hour_12 = h % 12 or 12
        if 5 <= h < 12:
            tod = "Morning"
        elif 12 <= h < 17:
            tod = "Afternoon"
        elif 17 <= h < 21:
            tod = "Evening"
        else:
            tod = "Night"
        return cls(
            time_value=t.strftime("%H:%M"),
            hour=h,
            minute=m,
            period=period,
            hour_12=hour_12,
            time_of_day=tod,
        )

    @classmethod
    def generate(cls, session: Session) -> List["TimeDimension"]:
        """Generate all 1440 minute-level time rows (idempotent)."""
        rows = []
        for h in range(24):
            for m in range(60):
                row = cls._from_time(h, m)
                session.add(row)
                rows.append(row)
        session.commit()
        return rows

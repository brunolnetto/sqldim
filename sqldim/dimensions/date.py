from datetime import date, timedelta
from typing import Optional, List
from sqlmodel import Session
from sqldim import DimensionModel, Field

class DateDimension(DimensionModel, table=True):
    """Pre-built Date Dimension — generate a full date spine for any range."""
    __natural_key__ = ["date_value"]

    id: int = Field(primary_key=True, surrogate_key=True)
    date_value: str = Field(natural_key=True, index=True, unique=True)  # YYYY-MM-DD
    year: int
    quarter: int
    month: int
    month_name: str
    day_of_month: int
    day_of_week: int        # 0=Monday, 6=Sunday
    day_name: str
    week_of_year: int
    is_weekend: bool
    is_leap_year: bool

    @classmethod
    def _from_date(cls, d: date) -> "DateDimension":
        return cls(
            date_value=d.isoformat(),
            year=d.year,
            quarter=(d.month - 1) // 3 + 1,
            month=d.month,
            month_name=d.strftime("%B"),
            day_of_month=d.day,
            day_of_week=d.weekday(),
            day_name=d.strftime("%A"),
            week_of_year=int(d.strftime("%W")),
            is_weekend=d.weekday() >= 5,
            is_leap_year=d.year % 4 == 0 and (d.year % 100 != 0 or d.year % 400 == 0),
        )

    @classmethod
    def generate(cls, start: str, end: str, session: Session) -> List["DateDimension"]:
        """
        Generate and persist all DateDimension rows for [start, end] inclusive.
        Skips dates already present (idempotent).
        """
        start_date = date.fromisoformat(start)
        end_date = date.fromisoformat(end)
        rows = []
        current = start_date
        while current <= end_date:
            row = cls._from_date(current)
            session.add(row)
            rows.append(row)
            current += timedelta(days=1)
        session.commit()
        return rows

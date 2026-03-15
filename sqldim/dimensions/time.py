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
    def _time_of_day(cls, h: int) -> str:
        if 5 <= h < 12:
            return "Morning"
        if 12 <= h < 17:
            return "Afternoon"
        if 17 <= h < 21:
            return "Evening"
        return "Night"

    @classmethod
    def _from_time(cls, h: int, m: int) -> "TimeDimension":
        t = time(h, m)
        return cls(
            time_value=t.strftime("%H:%M"),
            hour=h,
            minute=m,
            period="AM" if h < 12 else "PM",
            hour_12=h % 12 or 12,
            time_of_day=cls._time_of_day(h),
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

    @classmethod
    def generate_lazy(
        cls,
        table_name: str,
        sink,
        batch_size: int = 100_000,
        con=None,
    ) -> int:
        """
        Generate all 1440 minute-level rows using DuckDB ``generate_series``.
        No Python loop — the full time spine is computed and written inside
        DuckDB.  Returns the number of rows written.
        """
        import duckdb as _duckdb

        _con = con or _duckdb.connect()

        _con.execute(f"""
            CREATE OR REPLACE VIEW time_spine AS
            SELECT
                printf('%02d:%02d', h, m)              AS time_value,
                h                                      AS hour,
                m                                      AS minute,
                CASE WHEN h < 12 THEN 'AM' ELSE 'PM' END AS period,
                CASE WHEN h % 12 = 0 THEN 12
                     ELSE h % 12 END                   AS hour_12,
                CASE
                    WHEN h >= 5  AND h < 12 THEN 'Morning'
                    WHEN h >= 12 AND h < 17 THEN 'Afternoon'
                    WHEN h >= 17 AND h < 21 THEN 'Evening'
                    ELSE 'Night'
                END                                    AS time_of_day
            FROM generate_series(0, 23) AS t1(h)
            CROSS JOIN generate_series(0, 59) AS t2(m)
        """)
        return sink.write(_con, "time_spine", table_name, batch_size)

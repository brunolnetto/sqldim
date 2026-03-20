"""Pre-built Time-of-Day Dimension for intra-day analytics.

Generates all 1 440 minute-level rows (00:00 – 23:59) with
period labels (AM/PM) and time-of-day buckets (Morning / Afternoon /
Evening / Night).  Use :meth:`TimeDimension.generate` to populate.
"""

from datetime import time
from typing import ClassVar, List
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
    period: str  # AM | PM
    hour_12: int
    time_of_day: str  # Morning | Afternoon | Evening | Night

    _DEFAULT_BINS: ClassVar[dict] = {
        "Morning": (5, 12),
        "Afternoon": (12, 17),
        "Evening": (17, 21),
    }

    @classmethod
    def _time_of_day(cls, h: int, bins: dict | None = None) -> str:
        """Map the hour (0–23) to a human-readable time-of-day bucket."""
        active = bins if bins is not None else cls._DEFAULT_BINS
        for name, (start, end) in active.items():
            if start <= h < end:
                return name
        if bins is not None:
            return cls._time_of_day(h)
        return "Night"

    @classmethod
    def bucket_sql(cls, hour_col: str = "hour", bins: dict | None = None) -> str:
        """Return a SQL CASE expression for time-of-day bucketing.

        Parameters
        ----------
        hour_col:
            Column name containing the hour value (0–23).
        bins:
            Optional mapping of bucket name → ``(start, end)`` hour tuple.
            When ``None``, uses the classic 4-bucket scheme.

        Returns
        -------
        str
            A DuckDB-compatible ``CASE WHEN … END`` expression that can be
            embedded in any SELECT.

        Example
        -------
        .. code-block:: python

            sql = f"SELECT {TimeDimension.bucket_sql('hour')} AS time_of_day FROM ..."
        """
        if bins is None:
            bins = dict(cls._DEFAULT_BINS)
            default_bin = "Night"
        else:
            default_bin = list(bins.keys())[-1] if bins else "'Unknown'"

        when_clauses = "\n            ".join(
            f"WHEN {hour_col} >= {start} AND {hour_col} < {end} THEN '{name}'"
            for name, (start, end) in bins.items()
        )
        return f"CASE\n            {when_clauses}\n            ELSE '{default_bin}'\n        END"

    @classmethod
    def _from_time(cls, h: int, m: int) -> "TimeDimension":
        """Construct a ``TimeDimension`` instance for the given hour and minute.

        Derives the 12-hour clock value, AM/PM period, and time-of-day bucket
        from *h* and *m*, then returns an unsaved model instance.
        """
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
        time_of_day_bins: dict | None = None,
    ) -> int:
        """
        Generate all 1440 minute-level rows using DuckDB ``generate_series``.
        No Python loop — the full time spine is computed and written inside
        DuckDB.  Returns the number of rows written.

        Parameters
        ----------
        time_of_day_bins:
            Optional mapping of bucket name → ``(start, end)`` hour tuple.
            When ``None``, uses the classic Morning/Afternoon/Evening/Night
            scheme.  The generated SQL CASE expression is built dynamically
            from the provided mapping so custom fiscal or operational periods
            are supported without code changes.
        """
        import duckdb as _duckdb

        _con = con or _duckdb.connect()
        tod_case = cls.bucket_sql("h", bins=time_of_day_bins)

        _con.execute(f"""
            CREATE OR REPLACE VIEW time_spine AS
            SELECT
                printf('%02d:%02d', h, m)              AS time_value,
                h                                      AS hour,
                m                                      AS minute,
                CASE WHEN h < 12 THEN 'AM' ELSE 'PM' END AS period,
                CASE WHEN h % 12 = 0 THEN 12
                     ELSE h % 12 END                   AS hour_12,
                {tod_case}                             AS time_of_day
            FROM generate_series(0, 23) AS t1(h)
            CROSS JOIN generate_series(0, 59) AS t2(m)
        """)
        return sink.write(_con, "time_spine", table_name, batch_size)

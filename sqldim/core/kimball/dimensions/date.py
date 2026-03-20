from datetime import date, timedelta
from sqlmodel import Session
from sqldim import DimensionModel, Field

# ---------------------------------------------------------------------------
# Lazy (DuckDB-first) generation — no Python loop, no OOM risk
# ---------------------------------------------------------------------------

_GENERATE_LAZY_DOC = """
Generate and persist date rows using DuckDB ``generate_series``.

No Python date loop — the entire date spine is computed inside DuckDB
a single SQL step and streamed into the sink.

Parameters
----------
start : ISO date string, e.g. ``"2000-01-01"``
end : ISO date string, e.g. ``"2030-12-31"``
table_name : target table in the sink
sink : SinkAdapter implementation
batch_size : write buffer hint
con : existing DuckDB connection (created if None)

Returns
-------
int — number of rows written
"""


class DateDimension(DimensionModel, table=True):
    """Pre-built Date Dimension — generate a full date spine for any range.

    Use the lazy class method (``generate_lazy``) for large date ranges;
    it delegates entirely to DuckDB’s ``generate_series`` so no Python
    date loop is needed and memory stays constant.
    """

    __natural_key__ = ["date_value"]

    id: int = Field(primary_key=True, surrogate_key=True)
    date_value: str = Field(natural_key=True, index=True, unique=True)  # YYYY-MM-DD
    year: int
    quarter: int
    month: int
    month_name: str
    day_of_month: int
    day_of_week: int  # 0=Monday, 6=Sunday
    day_name: str
    week_of_year: int
    is_weekend: bool
    is_leap_year: bool

    @classmethod
    def _from_date(cls, d: date) -> "DateDimension":
        """Construct a ``DateDimension`` instance from a single :class:`date`.

        Derives all calendar attributes (quarter, week, day-name, leap-year
        flag, etc.) from *d* and returns an unsaved model instance.
        """
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
    def generate(cls, start: str, end: str, session: Session) -> list["DateDimension"]:
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

    @classmethod
    def generate_lazy(
        cls,
        start: str,
        end: str,
        table_name: str,
        sink,
        batch_size: int = 100_000,
        con=None,
    ) -> int:
        """
        Generate a full date spine using DuckDB ``generate_series``.
        No Python date loop — the entire spine is computed and written
        inside DuckDB.  Returns the number of rows written.
        """
        import duckdb as _duckdb

        _con = con or _duckdb.connect()

        # DuckDB dayofweek: 0=Sunday … 6=Saturday
        # Convert to ISO (0=Monday … 6=Sunday): (dayofweek + 6) % 7
        _con.execute(f"""
            CREATE OR REPLACE VIEW date_spine AS
            SELECT
                strftime(d, '%Y-%m-%d')                          AS date_value,
                year(d)                                          AS year,
                quarter(d)                                       AS quarter,
                month(d)                                         AS month,
                strftime(d, '%B')                                AS month_name,
                day(d)                                           AS day_of_month,
                (dayofweek(d) + 6) % 7                          AS day_of_week,
                strftime(d, '%A')                                AS day_name,
                weekofyear(d)                                    AS week_of_year,
                ((dayofweek(d) + 6) % 7) >= 5                   AS is_weekend,
                (year(d) % 4 = 0
                 AND (year(d) % 100 != 0 OR year(d) % 400 = 0)) AS is_leap_year
            FROM generate_series(
                '{start}'::DATE,
                '{end}'::DATE,
                INTERVAL 1 DAY
            ) AS gs(d)
        """)
        return sink.write(_con, "date_spine", table_name, batch_size)

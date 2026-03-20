import pytest
from sqlalchemy.pool import StaticPool
from datetime import date
from sqlmodel import Session, create_engine, SQLModel
from sqldim.core.kimball.dimensions.date import DateDimension
from sqldim.core.kimball.dimensions.time import TimeDimension
from sqldim.core.kimball.dimensions.junk import populate_junk_dimension_lazy

@pytest.fixture(scope="module")
def session():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        yield s
    engine.dispose()

def test_generate_date_range(session):
    rows = DateDimension.generate("2024-01-01", "2024-01-07", session)
    assert len(rows) == 7

def test_date_fields(session):
    rows = DateDimension.generate("2024-03-15", "2024-03-15", session)
    r = rows[0]
    assert r.date_value == "2024-03-15"
    assert r.year == 2024
    assert r.quarter == 1
    assert r.month == 3
    assert r.month_name == "March"
    assert r.day_of_month == 15
    assert r.day_name == "Friday"
    assert r.is_weekend is False

def test_weekend_detection(session):
    rows = DateDimension.generate("2024-03-16", "2024-03-17", session)
    assert rows[0].is_weekend is True   # Saturday
    assert rows[1].is_weekend is True   # Sunday

def test_leap_year(session):
    rows = DateDimension.generate("2024-02-29", "2024-02-29", session)
    assert rows[0].is_leap_year is True

def test_non_leap_year(session):
    rows = DateDimension.generate("2023-03-01", "2023-03-01", session)
    assert rows[0].is_leap_year is False

def test_century_non_leap():
    # 1900 is divisible by 100 but not 400 → not a leap year
    r = DateDimension._from_date(date(1900, 1, 1))
    assert r.is_leap_year is False

def test_400_year_leap():
    # 2000 is divisible by 400 → leap year
    r = DateDimension._from_date(date(2000, 1, 1))
    assert r.is_leap_year is True

def test_quarter_boundaries(session):
    cases = [("2024-01-01", 1), ("2024-04-01", 2), ("2024-07-01", 3), ("2024-10-01", 4)]
    for ds, expected_q in cases:
        r = DateDimension._from_date(date.fromisoformat(ds))
        assert r.quarter == expected_q


# ---------------------------------------------------------------------------
# Lazy dimension generation (DuckDB sink)
# ---------------------------------------------------------------------------

class _SimpleSink:
    """Minimal sink that writes a DuckDB VIEW to a Table and returns row count."""

    def write(self, con, view_name: str, table_name: str, batch_size: int = 100_000) -> int:
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM {view_name}")
        return con.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]


class TestLazyDimensions:
    def test_date_dimension_generate_lazy(self):
        import duckdb
        con = duckdb.connect()
        sink = _SimpleSink()
        n = DateDimension.generate_lazy(
            "2024-01-01", "2024-01-03", "test_lazy_date_dim", sink, con=con
        )
        assert n == 3
        con.close()

    def test_time_dimension_generate_lazy(self):
        import duckdb
        con = duckdb.connect()
        sink = _SimpleSink()
        n = TimeDimension.generate_lazy("test_lazy_time_dim", sink, con=con)
        assert n == 1440
        con.close()

    def test_junk_dimension_populate_lazy(self):
        import duckdb
        con = duckdb.connect()
        sink = _SimpleSink()
        n = populate_junk_dimension_lazy(
            {"promo": [True, False], "channel": ["web", "store"]},
            "test_lazy_junk_dim",
            sink,
            con=con,
        )
        assert n == 4
        con.close()

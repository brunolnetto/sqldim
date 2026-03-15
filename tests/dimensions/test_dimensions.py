import pytest
from sqlalchemy.pool import StaticPool
from datetime import date
from sqlmodel import Session, create_engine, SQLModel
from sqldim.dimensions.date import DateDimension

@pytest.fixture
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

def test_century_non_leap(session):
    # 1900 is divisible by 100 but not 400 → not a leap year
    r = DateDimension._from_date(date(1900, 1, 1))
    assert r.is_leap_year is False

def test_400_year_leap(session):
    # 2000 is divisible by 400 → leap year
    r = DateDimension._from_date(date(2000, 1, 1))
    assert r.is_leap_year is True

def test_quarter_boundaries(session):
    cases = [("2024-01-01", 1), ("2024-04-01", 2), ("2024-07-01", 3), ("2024-10-01", 4)]
    for ds, expected_q in cases:
        r = DateDimension._from_date(date.fromisoformat(ds))
        assert r.quarter == expected_q

import pytest
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine, SQLModel
from sqldim.core.kimball.dimensions.time import TimeDimension

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

def test_generate_creates_1440_rows(session):
    rows = TimeDimension.generate(session)
    assert len(rows) == 1440

def test_midnight():
    row = TimeDimension._from_time(0, 0)
    assert row.time_value == "00:00"
    assert row.hour == 0
    assert row.minute == 0
    assert row.period == "AM"
    assert row.hour_12 == 12
    assert row.time_of_day == "Night"

def test_noon():
    row = TimeDimension._from_time(12, 0)
    assert row.period == "PM"
    assert row.hour_12 == 12
    assert row.time_of_day == "Afternoon"

def test_morning():
    row = TimeDimension._from_time(8, 30)
    assert row.time_of_day == "Morning"
    assert row.period == "AM"
    assert row.hour_12 == 8

def test_afternoon():
    row = TimeDimension._from_time(14, 0)
    assert row.time_of_day == "Afternoon"
    assert row.period == "PM"
    assert row.hour_12 == 2

def test_evening():
    row = TimeDimension._from_time(19, 0)
    assert row.time_of_day == "Evening"

def test_night_late():
    row = TimeDimension._from_time(23, 59)
    assert row.time_of_day == "Night"
    assert row.time_value == "23:59"

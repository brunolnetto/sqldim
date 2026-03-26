from sqldim.core.kimball.dimensions.time import TimeDimension


def test_generate_creates_1440_rows():
    from unittest.mock import MagicMock

    session = MagicMock()
    rows = TimeDimension.generate(session)
    assert len(rows) == 1440
    assert session.add.call_count == 1440
    session.commit.assert_called_once()


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


def test_time_of_day_custom_bins_match():
    """Lines 43-45: custom bins loop should return matching bucket name."""
    custom_bins = {"Deep Night": (0, 6), "Daylight": (6, 20), "Dusk": (20, 24)}
    result = TimeDimension._time_of_day(3, bins=custom_bins)
    assert result == "Deep Night"
    result = TimeDimension._time_of_day(10, bins=custom_bins)
    assert result == "Daylight"


def test_time_of_day_custom_bins_fall_through():
    """Custom bins with no match falls through to default buckets."""
    # hour=2 doesn't match bins covering (6, 20)
    result = TimeDimension._time_of_day(2, bins={"Daylight": (6, 20)})
    # Falls through to default: 5<=h<12 Morning, else Night
    assert result == "Night"


def test_bucket_sql_custom_bins():
    """Line 87: custom bins branch — last key becomes the ELSE default."""
    custom_bins = {"Early": (0, 6), "Mid": (6, 18), "Late": (18, 24)}
    sql = TimeDimension.bucket_sql("h", bins=custom_bins)
    assert "WHEN h >= 0 AND h < 6 THEN 'Early'" in sql
    assert "ELSE 'Late'" in sql


def test_bucket_sql_default_bins():
    sql = TimeDimension.bucket_sql()
    assert "Morning" in sql
    assert "Night" in sql
    assert "CASE" in sql

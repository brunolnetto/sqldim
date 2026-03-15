"""Tests for backfill_scd2_narwhals — Task 7.5."""
import pytest
import polars as pl
import pandas as pd
import narwhals as nw

from sqldim.processors.backfill import backfill_scd2_narwhals


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _snapshot_pl():
    """Flat snapshot: player changes scoring_class between seasons."""
    return pl.DataFrame({
        "player_name":    ["Jordan", "Jordan", "Jordan", "Bird", "Bird"],
        "season":         [1990,     1991,     1992,     1990,   1991],
        "scoring_class":  ["Star",   "Star",   "Elite",  "Star", "Star"],
        "is_active":      [True,     True,     True,     True,   False],
    })


def _snapshot_pd():
    import pandas as pd
    return pd.DataFrame({
        "player_name":   ["Jordan", "Jordan", "Jordan"],
        "season":        [1990,     1991,     1992],
        "scoring_class": ["Star",   "Elite",  "Elite"],
    })


# ---------------------------------------------------------------------------
# Basic functionality
# ---------------------------------------------------------------------------

def test_dry_run_returns_dataframe():
    result = backfill_scd2_narwhals(
        _snapshot_pl(),
        partition_by="player_name",
        order_by="season",
        track_columns=["scoring_class"],
        dry_run=True,
    )
    assert isinstance(result, nw.DataFrame)


def test_dry_run_false_returns_int():
    result = backfill_scd2_narwhals(
        _snapshot_pl(),
        partition_by="player_name",
        order_by="season",
        track_columns=["scoring_class"],
        dry_run=False,
    )
    assert isinstance(result, int)
    assert result > 0


def test_streak_detection_jordan():
    """Jordan has Star(1990-1991) then Elite(1992) → 2 SCD2 rows."""
    result = backfill_scd2_narwhals(
        _snapshot_pl(),
        partition_by="player_name",
        order_by="season",
        track_columns=["scoring_class"],
        dry_run=True,
    )
    jordan = result.filter(nw.col("player_name") == "Jordan")
    assert len(jordan) == 2


def test_no_change_produces_one_row():
    """Bird is always Star → only 1 SCD2 row."""
    result = backfill_scd2_narwhals(
        _snapshot_pl(),
        partition_by="player_name",
        order_by="season",
        track_columns=["scoring_class"],
        dry_run=True,
    )
    bird = result.filter(nw.col("player_name") == "Bird")
    assert len(bird) == 1


def test_valid_from_is_first_season():
    result = backfill_scd2_narwhals(
        _snapshot_pl(),
        partition_by="player_name",
        order_by="season",
        track_columns=["scoring_class"],
        dry_run=True,
    )
    jordan = result.filter(nw.col("player_name") == "Jordan").sort("valid_from")
    assert jordan["valid_from"].to_list()[0] == 1990


def test_valid_to_is_last_season_of_streak():
    result = backfill_scd2_narwhals(
        _snapshot_pl(),
        partition_by="player_name",
        order_by="season",
        track_columns=["scoring_class"],
        dry_run=True,
    )
    jordan = result.filter(nw.col("player_name") == "Jordan").sort("valid_from")
    rows = nw.to_native(jordan).to_dicts()
    # First streak: 1990-1991 (Star)
    assert rows[0]["valid_from"] == 1990
    assert rows[0]["valid_to"] == 1991
    # Second streak: 1992 (Elite)
    assert rows[1]["valid_from"] == 1992
    assert rows[1]["valid_to"] == 1992


def test_result_sorted_by_partition_and_valid_from():
    result = backfill_scd2_narwhals(
        _snapshot_pl(),
        partition_by="player_name",
        order_by="season",
        track_columns=["scoring_class"],
        dry_run=True,
    )
    rows = nw.to_native(result).to_dicts()
    # Should be sorted: Bird first (alphabetical), then Jordan
    names = [r["player_name"] for r in rows]
    assert names == sorted(names)


def test_works_with_pandas_source():
    result = backfill_scd2_narwhals(
        _snapshot_pd(),
        partition_by="player_name",
        order_by="season",
        track_columns=["scoring_class"],
        dry_run=True,
    )
    assert isinstance(result, nw.DataFrame)
    jordan = result.filter(nw.col("player_name") == "Jordan")
    assert len(jordan) == 2  # Star(1990), Elite(1991-1992)


def test_multiple_track_columns():
    """Track both scoring_class and is_active."""
    result = backfill_scd2_narwhals(
        _snapshot_pl(),
        partition_by="player_name",
        order_by="season",
        track_columns=["scoring_class", "is_active"],
        dry_run=True,
    )
    # Bird changes is_active in 1991 → 2 rows
    bird = result.filter(nw.col("player_name") == "Bird")
    assert len(bird) == 2


def test_output_contains_valid_from_and_valid_to():
    result = backfill_scd2_narwhals(
        _snapshot_pl(),
        partition_by="player_name",
        order_by="season",
        track_columns=["scoring_class"],
        dry_run=True,
    )
    assert "valid_from" in result.columns
    assert "valid_to" in result.columns


def test_output_contains_partition_column():
    result = backfill_scd2_narwhals(
        _snapshot_pl(),
        partition_by="player_name",
        order_by="season",
        track_columns=["scoring_class"],
        dry_run=True,
    )
    assert "player_name" in result.columns


def test_total_row_count():
    result = backfill_scd2_narwhals(
        _snapshot_pl(),
        partition_by="player_name",
        order_by="season",
        track_columns=["scoring_class"],
        dry_run=True,
    )
    # Jordan: 2 rows. Bird: 1 row = 3 total
    assert len(result) == 3

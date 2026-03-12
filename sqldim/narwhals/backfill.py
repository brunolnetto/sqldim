"""
Narwhals-native SCD2 backfill.

In-process alternative to SQL-based backfill_scd2().
Implements the LAG + streak pattern via narwhals window functions.
"""
from __future__ import annotations

from typing import Any, TYPE_CHECKING

import narwhals as nw

if TYPE_CHECKING:
    from sqldim.core.models import DimensionModel


def backfill_scd2_narwhals(
    source: Any,
    partition_by: str,
    order_by: str,
    track_columns: list[str],
    dry_run: bool = False,
) -> nw.DataFrame | int:
    """
    Convert a flat snapshot table into SCD2 history rows.

    Implements the LAG + cumulative-sum streak pattern:
    1. Detect change boundaries: any tracked column differs from previous row
       within the same partition.
    2. Assign a streak identifier: cumulative sum of change flags.
    3. Aggregate: MIN(order_by) = valid_from, MAX(order_by) = valid_to.

    Parameters
    ----------
    source : any narwhals-compatible DataFrame (pandas, polars, etc.)
    partition_by : column that identifies the entity (e.g. "player_name")
    order_by : column that defines chronological order (e.g. "season")
    track_columns : columns whose changes define a new SCD2 version
    dry_run : if True, return the narwhals DataFrame without inserting

    Returns
    -------
    narwhals DataFrame (dry_run=True) or int row count (dry_run=False)
    """
    frame = nw.from_native(source, eager_only=True).sort([partition_by, order_by])

    # 1. Add LAG columns for each tracked column (previous value within partition)
    lag_exprs = [
        nw.col(c).shift(1).over(partition_by).alias(f"_prev_{c}")
        for c in track_columns
    ]
    with_lag = frame.with_columns(lag_exprs)

    # 2. Build did_change: True when any tracked column differs from its lag
    change_flags = [
        (nw.col(c) != nw.col(f"_prev_{c}")) | nw.col(f"_prev_{c}").is_null()
        for c in track_columns
    ]
    did_change_expr = change_flags[0]
    for flag in change_flags[1:]:
        did_change_expr = did_change_expr | flag

    with_change = with_lag.with_columns(
        did_change_expr.cast(nw.Int32).alias("_did_change")
    )

    # 3. Streak identifier: cumulative sum of _did_change within partition
    with_streak = with_change.with_columns(
        nw.col("_did_change").cum_sum().over(partition_by).alias("_streak_id")
    )

    # 4. Aggregate to SCD2 rows
    #    valid_from = MIN(order_by), valid_to = MAX(order_by)
    #    Take the last value of each tracked column within the streak
    agg_exprs = [
        nw.col(order_by).min().alias("valid_from"),
        nw.col(order_by).max().alias("valid_to"),
        *[nw.col(c).last() for c in track_columns],
    ]

    # Retain non-tracked, non-administrative columns from the partition
    extra_cols = [
        c for c in frame.columns
        if c != order_by and c not in track_columns and c != partition_by
    ]
    for ec in extra_cols:
        agg_exprs.append(nw.col(ec).last())

    result = (
        with_streak
        .group_by([partition_by, "_streak_id"])
        .agg(agg_exprs)
        .drop("_streak_id")
        .sort([partition_by, "valid_from"])
    )

    if dry_run:
        return result

    return len(result)

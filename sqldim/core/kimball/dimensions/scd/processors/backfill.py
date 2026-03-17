"""
Narwhals-native SCD2 backfill.

In-process alternative to SQL-based backfill_scd2().
Implements the LAG + streak pattern via narwhals window functions.
"""
from __future__ import annotations

from typing import Any, TYPE_CHECKING

import narwhals as nw


def _combine_change_flags(with_lag: nw.DataFrame, track_columns: list[str]) -> Any:
    change_flags = [
        (nw.col(c) != nw.col(f"_prev_{c}")) | nw.col(f"_prev_{c}").is_null()
        for c in track_columns
    ]
    expr = change_flags[0]
    for flag in change_flags[1:]:
        expr = expr | flag
    return expr


def _is_extra_col(c: str, partition_by: str, order_by: str, track_columns: list[str]) -> bool:
    return c != order_by and c not in track_columns and c != partition_by


def _build_agg_exprs(
    frame: nw.DataFrame,
    partition_by: str,
    order_by: str,
    track_columns: list[str],
) -> list[Any]:
    extra_cols = [c for c in frame.columns if _is_extra_col(c, partition_by, order_by, track_columns)]
    return [
        nw.col(order_by).min().alias("valid_from"),
        nw.col(order_by).max().alias("valid_to"),
        *[nw.col(c).last() for c in track_columns],
        *[nw.col(ec).last() for ec in extra_cols],
    ]

if TYPE_CHECKING:
    from sqldim.core.kimball.models import DimensionModel


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
    lag_exprs = [nw.col(c).shift(1).over(partition_by).alias(f"_prev_{c}") for c in track_columns]
    with_lag = frame.with_columns(lag_exprs)
    did_change_expr = _combine_change_flags(with_lag, track_columns)
    with_change = with_lag.with_columns(did_change_expr.cast(nw.Int32).alias("_did_change"))
    with_streak = with_change.with_columns(
        nw.col("_did_change").cum_sum().over(partition_by).alias("_streak_id")
    )
    agg_exprs = _build_agg_exprs(frame, partition_by, order_by, track_columns)
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


# ---------------------------------------------------------------------------
# lazy_backfill_scd2 — DuckDB SQL, no Python data
# ---------------------------------------------------------------------------


def lazy_backfill_scd2(
    source,
    partition_by: str,
    order_by: str,
    track_columns: list[str],
    table_name: str,
    sink,
    batch_size: int = 100_000,
    con=None,
) -> int:
    """
    DuckDB SQL-based SCD2 backfill using LAG + cumulative-sum-streak pattern.

    Converts a flat snapshot table into SCD2 history rows without loading
    any data into Python.

    Steps (all inside DuckDB):

    1. Register the source as a view.
    2. Add LAG columns for each tracked column within the partition.
    3. Derive a ``_did_change`` flag (any tracked column differs from lag).
    4. Accumulate a ``_streak_id`` via a running SUM of ``_did_change``.
    5. GROUP BY (partition, streak) to emit (valid_from, valid_to, …) rows.
    6. Write the result via the sink.

    Parameters
    ----------
    source : Parquet path (or any DuckDB-readable URI)
    partition_by : column that identifies the entity
    order_by : column that defines chronological ordering
    track_columns : columns whose changes trigger a new SCD2 version
    table_name : target table in the sink
    sink : SinkAdapter implementation
    batch_size : write buffer size hint
    con : existing DuckDB connection (created if None)

    Returns
    -------
    int — number of rows written
    """
    import duckdb as _duckdb

    _con = con or _duckdb.connect()

    from sqldim.sources import coerce_source
    _src_sql = coerce_source(source).as_sql(_con)
    _con.execute(f"""
        CREATE OR REPLACE VIEW backfill_source AS
        SELECT * FROM ({_src_sql})
    """)

    # Step 1: LAG columns
    lag_exprs = ",\n               ".join(
        f"LAG({c}) OVER (PARTITION BY {partition_by} ORDER BY {order_by}) AS _prev_{c}"
        for c in track_columns
    )
    _con.execute(f"""
        CREATE OR REPLACE VIEW backfill_with_lag AS
        SELECT *,
               {lag_exprs}
        FROM backfill_source
    """)

    # Step 2: did_change flag
    change_conds = " OR ".join(
        f"({c} IS DISTINCT FROM _prev_{c})" for c in track_columns
    )
    _con.execute(f"""
        CREATE OR REPLACE VIEW backfill_with_change AS
        SELECT *,
               CASE WHEN {change_conds} THEN 1 ELSE 0 END AS _did_change
        FROM backfill_with_lag
    """)

    # Step 3: streak_id via running SUM
    _con.execute(f"""
        CREATE OR REPLACE VIEW backfill_with_streak AS
        SELECT *,
               SUM(_did_change) OVER (
                   PARTITION BY {partition_by}
                   ORDER BY {order_by}
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
               ) AS _streak_id
        FROM backfill_with_change
    """)

    # Step 4: group by (partition, streak) → (valid_from, valid_to, tracked cols)
    track_agg = ",\n               ".join(
        f"LAST({c} ORDER BY {order_by}) AS {c}" for c in track_columns
    )
    _con.execute(f"""
        CREATE OR REPLACE VIEW backfill_result AS
        SELECT
            {partition_by},
            MIN({order_by}) AS valid_from,
            MAX({order_by}) AS valid_to,
            {track_agg},
            TRUE            AS is_current
        FROM backfill_with_streak
        GROUP BY {partition_by}, _streak_id
        ORDER BY {partition_by}, valid_from
    """)

    return sink.write(_con, "backfill_result", table_name, batch_size)

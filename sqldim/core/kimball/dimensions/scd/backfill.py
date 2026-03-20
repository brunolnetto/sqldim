"""SCD Type 2 and cumulative backfill utilities.

Provides SQL-generating helpers for converting flat snapshot tables into
SCD-2 history rows and for bulk-loading cumulative window-aggregation data
from a source staging table.
"""

from typing import List, Type, Union
from sqlmodel import Session, text
from sqldim.core.kimball.models import DimensionModel


def backfill_scd2(
    source_table: str,
    target_model: type[DimensionModel],
    partition_by: str,
    order_by: str,
    track_columns: list[str],
    session: Session,
    dry_run: bool = False,
) -> Union[str, int]:
    """
    Generates and optionally executes the streak-based SCD2 backfill SQL.
    Useful for converting flat snapshot tables into history.
    """
    cols = ", ".join(track_columns)
    table_name = (
        target_model.__tablename__
        if hasattr(target_model, "__tablename__")
        else target_model.__name__.lower()
    )

    sql = f"""
    WITH streak_started AS (
        SELECT 
            *,
            CASE WHEN LAG({track_columns[0]}) OVER (PARTITION BY {partition_by} ORDER BY {order_by}) <> {track_columns[0]} 
                 THEN 1 ELSE 0 END as did_change
        FROM {source_table}
    ),
    streak_identified AS (
        SELECT 
            *,
            SUM(did_change) OVER (PARTITION BY {partition_by} ORDER BY {order_by}) as streak_id
        FROM streak_started
    ),
    aggregated AS (
        SELECT 
            {partition_by},
            {cols},
            MIN({order_by}) as valid_from,
            MAX({order_by}) as valid_to,
            CASE WHEN MAX({order_by}) = (SELECT MAX({order_by}) FROM {source_table}) 
                 THEN 1 ELSE 0 END as is_current
        FROM streak_identified
        GROUP BY {partition_by}, streak_id, {cols}
    )
    INSERT INTO {table_name} ({partition_by}, {cols}, valid_from, valid_to, is_current)
    SELECT * FROM aggregated
    """

    if dry_run:
        return sql

    result = session.execute(text(sql))
    session.commit()
    return result.rowcount or 0


def backfill_cumulative(
    source_table: str,
    target_model: type[DimensionModel],
    partition_by: str,
    order_by: str,
    stats_columns: list[str],
    session: Session,
) -> int:
    """
    Bulk historical cumulative load using window functions.
    Reproduces load_players_table_day2.sql.
    """
    # SQL generation for window-based array aggregation
    # Using Postgres-specific ARRAY_AGG for the cumulative effect
    stats_json = ", ".join([f"'{c}', {c}" for c in stats_columns])

    sql = f"""
    INSERT INTO {target_model.__tablename__}
    SELECT 
        {partition_by},
        MAX({order_by}) as current_season,
        ARRAY_AGG(
            JSON_BUILD_OBJECT({stats_json})
            ORDER BY {order_by}
        ) as seasons
    FROM {source_table}
    GROUP BY {partition_by}
    """
    result = session.execute(text(sql))
    session.commit()
    return result.rowcount or 0

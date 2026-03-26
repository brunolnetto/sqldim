"""
Pre-built Dimensions — Examples 9 and 10
==========================================

9. Date + Time spine   — DateDimension.generate_lazy() + TimeDimension.generate_lazy()
10. Sales channel flags — JunkDimension via populate_junk_dimension_lazy()

Both are DuckDB-first: no Python loops over rows.  The full Cartesian
product / date range is computed inside DuckDB via generate_series and
CROSS JOIN.

Run with:
    PYTHONPATH=. python -m sqldim.application.examples.features.prebuilt_dims.showcase
"""

from __future__ import annotations

import os

import duckdb

from sqldim.core.kimball.dimensions.date import DateDimension
from sqldim.core.kimball.dimensions.time import TimeDimension
from sqldim.core.kimball.dimensions.junk import populate_junk_dimension_lazy
from sqldim.sinks import DuckDBSink
from sqldim.application.examples.features.utils import make_tmp_db


def _tmp_db() -> str:
    return make_tmp_db()


# ---------------------------------------------------------------------------
# Example 9: Date + Time Spine
# ---------------------------------------------------------------------------


def example_09_date_time_spine() -> None:
    """
    Generate a full date spine for a calendar year and a full time spine
    (1 440 minute-level rows) using DuckDB generate_series — no Python loop.
    """
    print("\n── Example 9: Date + Time Spine ────────────────────────────────")

    path = _tmp_db()
    setup = duckdb.connect(path)
    setup.execute("""
        CREATE TABLE dim_date (
            date_value   VARCHAR,
            year         INTEGER,
            quarter      INTEGER,
            month        INTEGER,
            month_name   VARCHAR,
            day_of_month INTEGER,
            day_of_week  INTEGER,
            day_name     VARCHAR,
            week_of_year INTEGER,
            is_weekend   BOOLEAN,
            is_leap_year BOOLEAN
        )
    """)
    setup.execute("""
        CREATE TABLE dim_time (
            time_value  VARCHAR,
            hour        INTEGER,
            minute      INTEGER,
            period      VARCHAR,
            hour_12     INTEGER,
            time_of_day VARCHAR
        )
    """)
    setup.close()

    with DuckDBSink(path) as sink:
        date_rows = DateDimension.generate_lazy(
            "2024-01-01", "2024-12-31", "dim_date", sink, con=sink._con
        )
        time_rows = TimeDimension.generate_lazy("dim_time", sink, con=sink._con)

    con = duckdb.connect(path)

    # Date spine stats
    q_quarter = con.execute(
        "SELECT quarter, COUNT(*) AS days FROM dim_date GROUP BY quarter ORDER BY quarter"
    ).fetchall()
    weekends = (
        con.execute("SELECT COUNT(*) FROM dim_date WHERE is_weekend").fetchone() or (0,)
    )[0]

    # Time spine stats
    periods = con.execute(
        "SELECT time_of_day, COUNT(*) AS minutes FROM dim_time GROUP BY time_of_day ORDER BY MIN(hour)"
    ).fetchall()
    con.close()

    print(f"  Date spine: {date_rows} rows (2024-01-01 → 2024-12-31)")
    for q, days in q_quarter:
        print(f"    Q{q}: {days} days")
    print(f"    Weekends: {weekends}  Weekdays: {date_rows - weekends}")

    print(f"\n  Time spine: {time_rows} rows (1 440 minute-level entries)")
    for tod, minutes in periods:
        print(f"    {tod:<12} {minutes} minutes")

    os.unlink(path)


# ---------------------------------------------------------------------------
# Example 10: Sales Channel Flags — Junk Dimension
# ---------------------------------------------------------------------------


def example_10_sales_channel_flags() -> None:
    """
    A junk dimension collapses multiple low-cardinality flags into a
    single pre-populated table.  Every combination of
    (is_promo × channel × is_return) gets its own surrogate key.

    populate_junk_dimension_lazy() computes the full Cartesian product
    inside DuckDB via CROSS JOIN — no Python itertools.product.
    """
    print("\n── Example 10: Sales Channel Flags (Junk Dimension) ────────────")

    flags = {
        "is_promo": [True, False],
        "channel": ["web", "in-store", "phone"],
        "is_return": [True, False],
    }
    expected = 2 * 3 * 2  # 12 combinations

    path = _tmp_db()
    setup = duckdb.connect(path)
    setup.execute("""
        CREATE TABLE dim_sales_flags (
            is_promo  BOOLEAN,
            channel   VARCHAR,
            is_return BOOLEAN
        )
    """)
    setup.close()

    with DuckDBSink(path) as sink:
        n = populate_junk_dimension_lazy(flags, "dim_sales_flags", sink, con=sink._con)  # type: ignore[arg-type]

    con = duckdb.connect(path)
    rows = con.execute(
        "SELECT is_promo, channel, is_return FROM dim_sales_flags ORDER BY channel, is_promo, is_return"
    ).fetchall()
    con.close()

    print(f"  Populated {n} rows ({expected} expected = 2 × 3 × 2):")
    for r in rows:
        print(f"    promo={str(r[0]):<5}  channel={r[1]:<10}  return={r[2]}")

    os.unlink(path)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


EXAMPLE_METADATA = {
    "name": "prebuilt-dims",
    "title": "Pre-built Dimensions",
    "description": "Examples 9-10: date/time spine + junk dimension",
    "entry_point": "run_showcase",
}


def run_showcase() -> None:
    print("Pre-built Dimensions Showcase")
    print("=============================")
    example_09_date_time_spine()
    example_10_sales_channel_flags()
    print("\nDone.\n")


if __name__ == "__main__":  # pragma: no cover
    run_showcase()

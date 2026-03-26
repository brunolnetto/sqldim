"""
sqldim Showcase: User Activity — Bitmask Retention & Growth Accounting
=======================================================================

Applies sqldim's bitmask datelist pattern and the vectorized growth
accounting state machine to the ``user_activity`` domain dataset.

Data flow
---------
Source layer (DuckDB in-memory):
    devices     (50)         ← DevicesSource
    page_events (10 000)     ← EventsSource  (user_id 1–500, event_time)

Analytical demonstrations
-------------------------
1. Staging layer     — load both sources via DuckDB
2. Bitmask retention — LazyBitmaskLoader → 32-bit datelist → L7 / L28
3. Growth accounting — vectorized New/Retained/Resurrected/Churned/Stale
                       single SQL statement, no Python loop
4. Retention cohort  — DGMQuery B2: % active by days-since-first-active
5. Rolling WAU       — DGMQuery B3: 7-day rolling + week-over-week QUALIFY
"""

from __future__ import annotations

import duckdb

from sqldim.application.datasets.domains.user_activity.sources import (
    DevicesSource,
    EventsSource,
)
from sqldim.core.loaders.dimension.bitmask import LazyBitmaskLoader
from sqldim import DGMQuery
from sqldim.core.query.dgm.preds._core import RawPred
from sqldim.application.examples.utils import section, banner


# ── in-memory sink ────────────────────────────────────────────────────────────


class _InMemorySink:
    """Minimal SinkAdapter backed by the shared in-memory DuckDB connection."""

    def current_state_sql(self, table_name: str) -> str:  # pragma: no cover
        return f"SELECT * FROM {table_name}"

    def write(
        self,
        con: "duckdb.DuckDBPyConnection",
        view_name: str,
        table_name: str,
        batch_size: int = 100_000,
    ) -> int:
        n = (con.execute(f"SELECT count(*) FROM {view_name}").fetchone() or (0,))[0]
        try:
            con.execute(f"INSERT INTO {table_name} BY NAME SELECT * FROM {view_name}")
        except Exception:
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {view_name}")
        return n


# ── helpers ───────────────────────────────────────────────────────────────────


def _col_width(h: str, rows: list, i: int, max_rows: int = 10) -> int:
    return max(len(h), max((len(str(r[i])) for r in rows[:max_rows]), default=0))


def _fmt_row(vals, widths: list) -> str:
    return "  " + "  ".join(str(v).ljust(w) for v, w in zip(vals, widths))


def _print_table(headers: list[str], rows: list[tuple], max_rows: int = 10) -> None:
    widths = [_col_width(h, rows, i) for i, h in enumerate(headers)]
    sep = "  " + "  ".join("-" * w for w in widths)
    print(_fmt_row(headers, widths))
    print(sep)
    for row in rows[:max_rows]:
        print(_fmt_row(row, widths))
    if len(rows) > max_rows:  # pragma: no cover
        print(f"  … ({len(rows) - max_rows} more rows)")


# ── Stage 1: staging layer ────────────────────────────────────────────────────


def demo_staging_layer(con: duckdb.DuckDBPyConnection) -> None:
    """Load DevicesSource and EventsSource into in-memory DuckDB tables."""
    with section("1. Staging Layer — DevicesSource + EventsSource"):
        DevicesSource(n=50, seed=42).setup(con, "devices")

        src = EventsSource(n=500, seed=42)
        con.execute(
            f"CREATE TABLE page_events AS SELECT * FROM ({src.snapshot().as_sql(con)})"
        )

        for tbl in ("devices", "page_events"):
            n = (con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone() or (0,))[0]
            print(f"  {tbl:<14}  {n:>7} rows")


# ── Stage 2: bitmask retention ────────────────────────────────────────────────


def demo_bitmask_retention(con: duckdb.DuckDBPyConnection) -> None:
    """
    Encode per-user page-view activity into a 32-bit integer bitmask.

    Each bit represents one calendar day within the trailing 32-day window.
    Computing L7 / L28 then requires a single bitwise AND — no date joins.

    Pattern from lecture (Module 2, user_datelist_int.sql):
        datelist_int encoded as BIT(32) / INTEGER bitmask
        L7  = BIT_COUNT(datelist_int & 0b1111111)
        L28 = BIT_COUNT(datelist_int & 0xFFFFFFF)
    """
    with section("2. Bitmask Datelist — L7 / L28 Retention (LazyBitmaskLoader)"):
        # Collapse raw events → per-user sorted list of active dates (ISO strings)
        con.execute("""
            CREATE TABLE users_active AS
            SELECT user_id::VARCHAR AS user_id, LIST(event_date) AS dates_active
            FROM (
                SELECT DISTINCT user_id, event_time::DATE::VARCHAR AS event_date
                FROM page_events
            )
            GROUP BY user_id
        """)

        ref_date = (
            con.execute("SELECT MAX(event_time::DATE) FROM page_events").fetchone()
            or (0,)
        )[0]

        sink = _InMemorySink()
        loader = LazyBitmaskLoader(
            table="users_bitmask",
            partition_key="user_id",
            dates_column="dates_active",
            reference_date=ref_date,
            window_days=32,
            sink=sink,
            con=con,
        )
        loader.process("users_active")

        sample = con.execute("""
            SELECT
                user_id,
                datelist_int,
                BIT_COUNT(datelist_int & 127)  AS l7_days,
                BIT_COUNT(datelist_int)         AS l32_days
            FROM users_bitmask
            ORDER BY l32_days DESC
            LIMIT 8
        """).fetchall()
        _print_table(["user_id", "bitmask_int", "l7_days", "l32_days"], sample)
        print(
            "\n  l7_days = active days in last 7  ·  l32_days = active days in last 32"
        )


# ── Stage 3: growth accounting ────────────────────────────────────────────────


def demo_growth_accounting(con: duckdb.DuckDBPyConnection) -> None:
    """
    Classify every (user, day) as New / Retained / Resurrected / Churned /
    Stale using a single vectorized DuckDB SQL statement.

    Replaces the lecture's incremental day-by-day Python loop
    (growth_accounting.sql + generate_series loop) with a window function
    that computes all state transitions in one pass:

        MAX(CASE WHEN is_active THEN date END)
            OVER (PARTITION BY user_id ORDER BY date
                  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
        → last_active_before

    State derivation mirrors the lecture state machine exactly.
    """
    with section("3. Growth Accounting — Vectorized State Machine"):
        con.execute("""
            CREATE TABLE daily_events AS
            SELECT DISTINCT
                user_id::VARCHAR AS user_id,
                event_time::DATE AS event_date
            FROM page_events
        """)

        con.execute("""
            CREATE TABLE user_growth_accounting AS
            WITH
              date_spine AS (
                  SELECT UNNEST(GENERATE_SERIES(
                      (SELECT MIN(event_date) FROM daily_events),
                      (SELECT MAX(event_date) FROM daily_events),
                      INTERVAL '1 day'
                  ))::DATE AS dt
              ),
              user_first AS (
                  SELECT user_id, MIN(event_date) AS first_active_date
                  FROM daily_events
                  GROUP BY user_id
              ),
              user_day_spine AS (
                  SELECT
                      uf.user_id,
                      uf.first_active_date,
                      d.dt AS date,
                      a.event_date IS NOT NULL AS is_active
                  FROM user_first uf
                  CROSS JOIN date_spine d
                  LEFT JOIN daily_events a
                         ON uf.user_id = a.user_id AND a.event_date = d.dt
                  WHERE d.dt >= uf.first_active_date
              ),
              with_lags AS (
                  SELECT
                      user_id, first_active_date, date, is_active,
                      MAX(CASE WHEN is_active THEN date END) OVER (
                          PARTITION BY user_id
                          ORDER BY date
                          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                      ) AS last_active_before
                  FROM user_day_spine
              )
            SELECT
                user_id,
                first_active_date,
                date,
                CASE
                    WHEN last_active_before IS NULL                AND is_active     THEN 'New'
                    WHEN last_active_before = date - 1             AND is_active     THEN 'Retained'
                    WHEN last_active_before IS NOT NULL
                         AND last_active_before < date - 1         AND is_active     THEN 'Resurrected'
                    WHEN last_active_before = date - 1             AND NOT is_active THEN 'Churned'
                    ELSE 'Stale'
                END AS daily_active_state
            FROM with_lags
            WHERE last_active_before IS NOT NULL OR is_active
        """)

        dist = con.execute("""
            SELECT
                daily_active_state,
                COUNT(*)                                              AS user_days,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1)   AS pct
            FROM user_growth_accounting
            GROUP BY daily_active_state
            ORDER BY user_days DESC
        """).fetchall()
        _print_table(["state", "user_days", "pct_%"], dist)


# ── Stage 4: retention cohort ─────────────────────────────────────────────────


def demo_retention_cohort(con: duckdb.DuckDBPyConnection) -> None:
    """
    Cohort retention curve — % of users still active by days since first active.

    DGMQuery B2: group_by(days_since_first_active) + agg(pct_active).
    Maps directly to the retention_analysis.sql lecture query, re-expressed
    via the sqldim query algebra.
    """
    with section("4. Retention Cohort — % Active by Days Since First Active"):
        q = (
            DGMQuery()
            .anchor("user_growth_accounting", "ga")
            .group_by("(ga.date - ga.first_active_date)")
            .agg(
                total_users="COUNT(DISTINCT ga.user_id)",
                active_users=(
                    "COUNT(DISTINCT CASE WHEN ga.daily_active_state IN "
                    "('New','Retained','Resurrected') THEN ga.user_id END)"
                ),
                pct_active=(
                    "ROUND(COUNT(DISTINCT CASE WHEN ga.daily_active_state IN "
                    "('New','Retained','Resurrected') THEN ga.user_id END)"
                    " * 100.0 / COUNT(DISTINCT ga.user_id), 1)"
                ),
            )
        )
        rows = q.execute(con)
        _print_table(
            ["days_since_first", "total_users", "active_users", "pct_%"],
            rows,
            max_rows=12,
        )


# ── Stage 5: rolling WAU + WoW ────────────────────────────────────────────────


def demo_rolling_window(con: duckdb.DuckDBPyConnection) -> None:
    """
    Rolling 7-day active users (WAU) + week-over-week growth filter.

    DGMQuery B2 (DAU per calendar day) composed with B3 (rolling window
    expression + QUALIFY). Only dates where current WAU > prior 7-day WAU
    are returned — the sqldim way to express the window_based_analysis.sql
    lecture query without a CTE.
    """
    with section("5. Rolling WAU + Week-over-Week Growth (DGMQuery B3)"):
        q = (
            DGMQuery()
            .anchor("user_growth_accounting", "ga")
            .group_by("ga.date")
            .agg(
                dau=(
                    "COUNT(DISTINCT CASE WHEN ga.daily_active_state IN "
                    "('New','Retained','Resurrected') THEN ga.user_id END)"
                )
            )
            .window(
                wau_7d=(
                    "SUM(COUNT(DISTINCT CASE WHEN ga.daily_active_state IN "
                    "('New','Retained','Resurrected') THEN ga.user_id END))"
                    " OVER (ORDER BY ga.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)"
                ),
                prev_wau_7d=(
                    "SUM(COUNT(DISTINCT CASE WHEN ga.daily_active_state IN "
                    "('New','Retained','Resurrected') THEN ga.user_id END))"
                    " OVER (ORDER BY ga.date ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING)"
                ),
            )
            .qualify(RawPred("wau_7d > prev_wau_7d"))
        )
        rows = q.execute(con)
        print(f"  {len(rows)} dates with positive week-over-week WAU growth\n")
        if rows:
            _print_table(["date", "dau", "wau_7d", "prev_wau_7d"], rows, max_rows=8)


# ── Entry point ───────────────────────────────────────────────────────────────


EXAMPLE_METADATA = {
    "name": "user-activity",
    "title": "User Activity",
    "description": (
        "Bitmask datelist encoding (L7/L28) + vectorized growth accounting "
        "state machine (New/Retained/Resurrected/Churned/Stale) applied to "
        "page-view events from the user_activity domain dataset."
    ),
    "entry_point": "run_showcase",
}


async def run_showcase() -> None:
    """Run the full user activity showcase in a single in-memory DuckDB session."""
    banner(
        "User Activity — Bitmask Retention & Growth Accounting",
        "sqldim  ·  bitmask datelist  ·  growth state machine  ·  DGMQuery B3",
    )

    con = duckdb.connect()

    demo_staging_layer(con)
    demo_bitmask_retention(con)
    demo_growth_accounting(con)
    demo_retention_cohort(con)
    demo_rolling_window(con)

    con.close()
    print("\nUser activity showcase complete.\n")


if __name__ == "__main__":  # pragma: no cover
    import asyncio

    asyncio.run(run_showcase())

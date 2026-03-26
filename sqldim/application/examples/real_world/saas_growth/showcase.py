"""
sqldim Showcase: SaaS Growth Analytics
=======================================

Applies SCD-2 plan-tier tracking and the vectorized growth accounting state
machine to the ``saas_growth`` domain dataset (users + sessions).

Data flow
---------
Source layer (DuckDB in-memory):
    saas_users    (200)     ← SaaSUsersSource
    saas_sessions (3 000)   ← SaaSSessionsSource  (user_id 1–200, event_date)

Analytical demonstrations
-------------------------
1. Staging layer     — load both sources via DuckDB
2. Plan-tier upgrades — SCD-2 event batch delta: who upgraded from free → pro
3. Growth accounting — vectorized New/Retained/Resurrected/Churned/Stale
4. Retention by tier — DGMQuery B2: activity rate segmented by plan_tier
5. Funnel analysis   — new-user → D7 active → paid conversion
"""

from __future__ import annotations

import duckdb

from sqldim.application.datasets.domains.saas_growth.sources import (
    SaaSUsersSource,
    SaaSSessionsSource,
)
from sqldim import DGMQuery
from sqldim.application.examples.utils import section, banner


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
    """Load SaaSUsersSource and SaaSSessionsSource into DuckDB staging tables."""
    with section("1. Staging Layer — SaaSUsersSource + SaaSSessionsSource"):
        src_users = SaaSUsersSource(n=200, seed=42)
        src_sessions = SaaSSessionsSource(n=200, seed=42)

        con.execute(
            f"CREATE TABLE saas_users AS SELECT * FROM ({src_users.snapshot().as_sql(con)})"
        )
        con.execute(
            f"CREATE TABLE saas_sessions AS SELECT * FROM ({src_sessions.snapshot().as_sql(con)})"
        )

        for tbl in ("saas_users", "saas_sessions"):
            n = (con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone() or (0,))[0]
            print(f"  {tbl:<16}  {n:>6} rows")

        tiers = con.execute("""
            SELECT plan_tier, COUNT(*) AS users,
                   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
            FROM saas_users
            GROUP BY plan_tier
            ORDER BY users DESC
        """).fetchall()
        print()
        _print_table(["plan_tier", "users", "pct_%"], tiers)


# ── Stage 2: plan-tier upgrade delta ─────────────────────────────────────────


def demo_plan_tier_upgrades(
    con: duckdb.DuckDBPyConnection,
    src_users: SaaSUsersSource,
) -> None:
    """
    Show which users upgraded their plan tier in the next event batch.

    SaaSUsersSource.event_batch() applies the upgrade ChangeRules (~30 % of
    free users → pro, ~10 % of pro → enterprise).  Comparing original to
    batch reveals the SCD-2 delta — the rows that need a new version in
    ``dim_user``.

    In production:
        NarwhalsSCDProcessor("user_id", ["plan_tier"]).process(batch, "dim_user")
    """
    with section("2. Plan-Tier Upgrades — SCD-2 Event Batch Delta"):
        original = {
            row[0]: row[2]
            for row in con.execute(
                "SELECT user_id, email, plan_tier FROM saas_users"
            ).fetchall()
        }
        batch_sql = src_users.event_batch().as_sql(con)
        updated = {
            row[0]: row[1]
            for row in con.execute(
                f"SELECT user_id, plan_tier FROM ({batch_sql})"
            ).fetchall()
        }
        upgrades = [
            (uid, original[uid], updated[uid])
            for uid in updated
            if updated.get(uid) != original.get(uid)
        ]
        print(f"  {len(upgrades)} plan upgrades detected in next event batch\n")
        _print_table(["user_id", "original_tier", "upgraded_tier"], upgrades[:8])
        print(
            "\n  → In production: feed event batch into NarwhalsSCDProcessor to build\n"
            "    versioned dim_user rows (valid_from / valid_to SCD-2 history)."
        )


# ── Stage 3: growth accounting ────────────────────────────────────────────────


def demo_growth_accounting(con: duckdb.DuckDBPyConnection) -> None:
    """
    Classify every (user, day) as New / Retained / Resurrected / Churned /
    Stale using a single vectorized DuckDB SQL statement.

    Source: ``saas_sessions.event_date`` (one row per session, deduplicated
    to one row per user-day before the state machine runs).

    The window function approach eliminates the Python loop from the lecture
    — all state transitions are computed in one SQL pass.
    """
    with section("3. Growth Accounting — Subscription Engagement States"):
        con.execute("""
            CREATE TABLE daily_sessions AS
            SELECT DISTINCT
                user_id::VARCHAR AS user_id,
                event_date::DATE AS event_date
            FROM saas_sessions
        """)

        con.execute("""
            CREATE TABLE user_growth_accounting AS
            WITH
              date_spine AS (
                  SELECT UNNEST(GENERATE_SERIES(
                      (SELECT MIN(event_date) FROM daily_sessions),
                      (SELECT MAX(event_date) FROM daily_sessions),
                      INTERVAL '1 day'
                  ))::DATE AS dt
              ),
              user_first AS (
                  SELECT user_id, MIN(event_date) AS first_active_date
                  FROM daily_sessions
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
                  LEFT JOIN daily_sessions a
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


# ── Stage 4: retention by plan tier ──────────────────────────────────────────


def demo_retention_by_tier(con: duckdb.DuckDBPyConnection) -> None:
    """
    Segment growth accounting states by plan tier.

    DGMQuery B2 — joins ``user_growth_accounting`` with ``saas_users`` and
    groups by ``plan_tier`` to reveal whether enterprise users engage more
    consistently than free-tier users.
    """
    with section("4. Retention by Plan Tier (DGMQuery B2)"):
        con.execute("""
            CREATE VIEW growth_with_tier AS
            SELECT ga.*, u.plan_tier
            FROM user_growth_accounting ga
            JOIN saas_users u ON ga.user_id = u.user_id::VARCHAR
        """)

        q = (
            DGMQuery()
            .anchor("growth_with_tier", "g")
            .group_by("g.plan_tier")
            .agg(
                total_user_days="COUNT(*)",
                active_days=(
                    "COUNT(CASE WHEN g.daily_active_state IN "
                    "('New','Retained','Resurrected') THEN 1 END)"
                ),
                retention_pct=(
                    "ROUND(COUNT(CASE WHEN g.daily_active_state IN "
                    "('New','Retained','Resurrected') THEN 1 END)"
                    " * 100.0 / COUNT(*), 1)"
                ),
            )
        )
        rows = q.execute(con)
        _print_table(["plan_tier", "user_days", "active_days", "retention_%"], rows)
        print("\n  → Higher plan tiers correlate with stronger day-over-day engagement")


# ── Stage 5: funnel analysis ──────────────────────────────────────────────────


def demo_funnel_analysis(con: duckdb.DuckDBPyConnection) -> None:
    """
    Subscription conversion funnel: new signup → D7 active → paid tier.

    Self-join on ``user_growth_accounting`` to track each user's cohort
    through the first 7 days.  Paid conversion is checked against the
    latest plan tier in ``saas_users``.

    Maps to funnel_analysis.sql from lecture 4: step 1 (New) → step 2
    (Retained within N days), with a third step for paid conversion.
    """
    with section("5. Funnel — New Signup → D7 Active → Paid Conversion"):
        rows = con.execute("""
            WITH new_users AS (
                SELECT user_id, date AS signup_day
                FROM user_growth_accounting
                WHERE daily_active_state = 'New'
            ),
            d7_activity AS (
                SELECT
                    nu.user_id,
                    nu.signup_day,
                    MAX(
                        CASE WHEN ga.date BETWEEN nu.signup_day + 1
                                              AND nu.signup_day + 7
                             AND ga.daily_active_state IN ('Retained', 'Resurrected')
                             THEN 1 ELSE 0 END
                    ) AS active_d7
                FROM new_users nu
                LEFT JOIN user_growth_accounting ga USING (user_id)
                GROUP BY nu.user_id, nu.signup_day
            )
            SELECT
                COUNT(*)                                                      AS total_new,
                SUM(active_d7)                                                AS d7_retained,
                ROUND(SUM(active_d7) * 100.0 / NULLIF(COUNT(*), 0), 1)       AS d7_rate_pct,
                SUM(
                    CASE WHEN u.plan_tier IN ('pro', 'enterprise') THEN 1 END
                )                                                             AS paid_users
            FROM d7_activity d7
            JOIN saas_users u ON d7.user_id = u.user_id::VARCHAR
        """).fetchall()
        _print_table(
            ["total_new", "d7_retained", "d7_rate_%", "paid_users"],
            rows,
        )
        print(
            "\n  total_new   = users who had their first session\n"
            "  d7_retained = still active in days 1–7 after signup\n"
            "  paid_users  = currently on pro or enterprise plan"
        )


# ── Entry point ───────────────────────────────────────────────────────────────


EXAMPLE_METADATA = {
    "name": "saas",
    "title": "SaaS Growth",
    "description": (
        "SCD-2 plan-tier tracking + vectorized growth accounting state machine "
        "(New/Retained/Resurrected/Churned/Stale) + funnel analysis applied to "
        "the saas_growth domain dataset (users + sessions)."
    ),
    "entry_point": "run_showcase",
}


async def run_showcase() -> None:
    """Run the full SaaS growth showcase in a single in-memory DuckDB session."""
    banner(
        "SaaS Growth — Plan-Tier SCD-2 & Growth Accounting",
        "sqldim  ·  SCD-2 delta  ·  growth state machine  ·  funnel analysis",
    )

    con = duckdb.connect()
    src_users = SaaSUsersSource(n=200, seed=42)

    demo_staging_layer(con)
    demo_plan_tier_upgrades(con, src_users)
    demo_growth_accounting(con)
    demo_retention_by_tier(con)
    demo_funnel_analysis(con)

    con.close()
    print("\nSaaS growth showcase complete.\n")


# Backward-compat alias used by old tests
run_saas_showcase = run_showcase


if __name__ == "__main__":  # pragma: no cover
    import asyncio

    asyncio.run(run_showcase())

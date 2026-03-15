"""
Fact Patterns — Examples 5 through 8
======================================

sqldim handles the common fact table loading patterns that sit between
OLTP event streams and the analytical data warehouse.

5. E-commerce orders  — Accumulating snapshot  (pipeline: placed→paid→shipped→delivered)
6. Daily balances     — Periodic snapshot      (one row per account per day)
7. NBA player seasons — Cumulative fact        (→ examples/real_world/nba_analytics/)
8. User daily activity— Bitmask loader         (→ examples/real_world/user_activity/)

Run:
    PYTHONPATH=. python -m sqldim.examples.features.fact_patterns.showcase
"""
from __future__ import annotations

import os

import duckdb

from sqldim.loaders.accumulating import LazyAccumulatingLoader
from sqldim.loaders.snapshot import LazySnapshotLoader
from sqldim.sinks import DuckDBSink

from sqldim.examples.datasets.ecommerce  import OrdersSource
from sqldim.examples.datasets.enterprise import AccountsSource
from sqldim.examples.utils import make_tmp_db


def _tmp_db() -> str:
    return make_tmp_db()


# ── Example 5 ─────────────────────────────────────────────────────────────────

_STAGES = ("placed", "paid", "shipped", "delivered")


def _stage_label(s: str, v) -> str:
    return s if v else ""


def _order_summary(r: tuple) -> str:
    vals    = r[1:5]
    filled  = " → ".join(filter(None, (_stage_label(s, v) for s, v in zip(_STAGES, vals))))
    pending = [s for s, v in zip(_STAGES, vals) if not v]
    suffix  = "  ✓ COMPLETE" if not pending else f"  (pending: {', '.join(pending)})"
    return f"    order {r[0]}: {filled}{suffix}"


def _print_order_state(r: tuple) -> None:
    print(_order_summary(r))


def example_05_ecommerce_orders() -> None:
    """
    Accumulating snapshot — one row per order, updated in-place as it moves
    through the fulfilment pipeline.  Only non-null milestone values
    overwrite existing nulls (idempotent merges).

    OLTP event stream:
      OrdersSource.placed_events()    → T1: orders created
      OrdersSource.paid_events()      → T2: payment confirmed
      OrdersSource.shipped_events()   → T3: handed to carrier
      OrdersSource.delivered_events() → T4: delivery confirmed
    """
    print("\n── Example 5: E-commerce Orders (Accumulating Snapshot) ────────")

    src  = OrdersSource(n=6, seed=42)
    path = _tmp_db()

    setup_con = duckdb.connect(path)
    src.setup(setup_con, "fact_order_pipeline")
    setup_con.close()

    with DuckDBSink(path) as sink:
        loader = LazyAccumulatingLoader(
            table_name       = "fact_order_pipeline",
            match_column     = "order_id",
            milestone_columns= ["paid_at", "shipped_at", "delivered_at"],
            sink             = sink,
            con              = sink._con,
        )
        r = loader.process(src.placed_events());    print(f"  T1 placed    → {r}")
        r = loader.process(src.paid_events());      print(f"  T2 paid      → {r}")
        r = loader.process(src.shipped_events());   print(f"  T3 shipped   → {r}")
        r = loader.process(src.delivered_events()); print(f"  T4 delivered → {r}")

    con  = duckdb.connect(path)
    rows = con.execute(
        "SELECT order_id, placed_at, paid_at, shipped_at, delivered_at "
        "FROM fact_order_pipeline ORDER BY order_id"
    ).fetchall()
    src.teardown(con, "fact_order_pipeline")
    con.close()
    os.unlink(path)

    print("  Final pipeline state:")
    for r in rows:
        _print_order_state(r)


# ── Example 6 ─────────────────────────────────────────────────────────────────

def example_06_daily_account_balances() -> None:
    """
    Periodic snapshot — one analytical fact row per (account, day).
    ``LazySnapshotLoader`` injects the ``snapshot_date`` column automatically.

    OLTP event stream:
      AccountsSource.snapshot_for_date(d) → daily balance extract from OLTP
    """
    print("\n── Example 6: Daily Account Balances (Periodic Snapshot) ───────")

    src  = AccountsSource(n=5, seed=42)
    path = _tmp_db()

    setup_con = duckdb.connect(path)
    src.setup(setup_con, "fact_account_balance")
    setup_con.close()

    snap_dates = ["2024-03-29", "2024-03-30", "2024-03-31"]

    with DuckDBSink(path) as sink:
        for snap_date in snap_dates:
            loader = LazySnapshotLoader(sink, snapshot_date=snap_date, con=sink._con)
            n = loader.load(src.snapshot_for_date(snap_date), "fact_account_balance")
            print(f"  {snap_date} → {n} rows loaded")

    con    = duckdb.connect(path)
    total  = con.execute("SELECT COUNT(*) FROM fact_account_balance").fetchone()[0]
    sample = con.execute(
        "SELECT account_id, account_type, balance, snapshot_date "
        "FROM fact_account_balance WHERE snapshot_date = '2024-03-31' ORDER BY account_id"
    ).fetchall()
    src.teardown(con, "fact_account_balance")
    con.close()
    os.unlink(path)

    n_accts = len(src.accounts)
    print(f"  Total rows: {total} ({n_accts} accounts × {len(snap_dates)} days)")
    print("  March 31 snapshot:")
    for r in sample:
        print(f"    account {r[0]}  ({r[1]:<12})  ${r[2]:>10,.2f}  @ {r[3]}")


# ── Examples 7 & 8 (pointers) ────────────────────────────────────────────────

def example_07_08_pointers() -> None:
    print("\n── Example 7: NBA Player Seasons (Cumulative Fact) ─────────────")
    print("  → Full implementation in examples/real_world/nba_analytics/")
    print("    Key class: CumulativeMixin — appends one JSON struct per season\n")
    print("── Example 8: User Daily Activity (Bitmask Loader) ─────────────")
    print("  → Full implementation in examples/real_world/user_activity/")
    print("    Key class: LazyBitmaskLoader / BitmaskerLoader")
    print("    32-bit integer encodes up to 28 days; L7/L28 = bit-shift test")


# ── Entry point ───────────────────────────────────────────────────────────────

def run_showcase() -> None:
    print("Fact Patterns Showcase")
    print("======================")
    example_05_ecommerce_orders()
    example_06_daily_account_balances()
    example_07_08_pointers()
    print("\nDone.\n")


if __name__ == "__main__":  # pragma: no cover
    run_showcase()

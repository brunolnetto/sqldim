"""
SCD Types — Examples 1 through 4
=================================

sqldim sits as the **dimensional layer between an OLTP system and the
analytical warehouse**.  Each example loads an initial OLTP snapshot,
then processes a batch of change events, demonstrating how sqldim builds
and maintains slowly-changing dimension tables.

1. Product catalogue  — SCD Type 2  (price / name change → new version)
2. Employee directory — SCD Type 6  (promotion: T1 overwrite ; transfer: T2)
3. Customer addresses — SCD Type 3  (keep one layer of address history)
4. Retail stores      — SCD Type 6  (phone: T1 overwrite ; city: T2 version)

Run:
    PYTHONPATH=. python -m sqldim.examples.features.scd_types.showcase
"""
from __future__ import annotations

import os

import duckdb

from sqldim.core.kimball.dimensions.scd.processors.scd_engine import (
    LazySCDProcessor,
    LazyType3Processor,
    LazyType6Processor,
)
from sqldim.sinks import DuckDBSink

from sqldim.examples.datasets.ecommerce  import ProductsSource, CustomersSource, StoresSource
from sqldim.examples.datasets.enterprise import EmployeesSource
from sqldim.examples.utils import make_tmp_db


def _tmp_db() -> str:
    return make_tmp_db()


# ── Example 1 ─────────────────────────────────────────────────────────────────

def example_01_product_catalogue() -> None:
    """
    SCD Type 2 — price and name changes create a new dimension row.

    OLTP → sqldim pipeline:
      ProductsSource.snapshot()      → initial product catalogue
      ProductsSource.event_batch(1)  → price increases + name revisions
    """
    print("\n── Example 1: Product Catalogue (SCD Type 2) ───────────────────")

    src  = ProductsSource(n=5, seed=42)
    path = _tmp_db()

    # Create sqldim-managed SCD2 dimension (empty target)
    setup_con = duckdb.connect(path)
    src.setup(setup_con, "dim_product")
    setup_con.close()

    with DuckDBSink(path) as sink:
        proc = LazySCDProcessor("product_id", ["name", "price"], sink, con=sink._con)
        # T0: initial OLTP extract
        r1 = proc.process(src.snapshot(), "dim_product")
        # T1: changed records arrive from OLTP (price hikes + name revisions)
        r2 = proc.process(src.event_batch(1), "dim_product")

    print(f"  T0 snapshot  → inserted={r1.inserted}")
    print(f"  T1 events    → inserted={r2.inserted}, versioned={r2.versioned}, unchanged={r2.unchanged}")

    con  = duckdb.connect(path)
    rows = con.execute(
        "SELECT product_id, name, price, is_current FROM dim_product ORDER BY product_id, valid_from"
    ).fetchall()
    src.teardown(con, "dim_product")
    con.close()
    os.unlink(path)

    print("  Current state:")
    for r in rows:
        flag = "✓ current" if r[3] else "  history"
        print(f"    [{flag}]  id={r[0]}  {r[1]:<28}  ${r[2]:.2f}")


# ── Example 2 ─────────────────────────────────────────────────────────────────

def example_02_employee_directory() -> None:
    """
    SCD Type 6 (hybrid) — promotions overwrite title in-place (Type 1);
    department transfers create a new SCD2 row (Type 2).

    OLTP → sqldim pipeline:
      EmployeesSource.snapshot()     → initial HR extract
      EmployeesSource.event_batch(1) → promotions + transfers
    """
    print("\n── Example 2: Employee Directory (SCD Type 6: title T1 + dept T2) ─")

    src  = EmployeesSource(n=5, seed=42)
    path = _tmp_db()

    setup_con = duckdb.connect(path)
    src.setup(setup_con, "dim_employee")
    setup_con.close()

    with DuckDBSink(path) as sink:
        proc = LazyType6Processor(
            natural_key   = "employee_id",
            type1_columns = ["title"],
            type2_columns = ["department"],
            sink          = sink,
            con           = sink._con,
        )
        r1 = proc.process(src.snapshot(),     "dim_employee")
        r2 = proc.process(src.event_batch(1), "dim_employee")

    print(f"  T0 snapshot → inserted={r1.inserted}")
    print(f"  T1 events   → inserted={r2.inserted}, versioned={r2.versioned}, unchanged={r2.unchanged}")

    con  = duckdb.connect(path)
    rows = con.execute(
        "SELECT employee_id, full_name, title, department, is_current "
        "FROM dim_employee ORDER BY employee_id, valid_from"
    ).fetchall()
    src.teardown(con, "dim_employee")
    con.close()
    os.unlink(path)

    print("  Current state:")
    for r in rows:
        flag = "✓" if r[4] else "H"
        print(f"    [{flag}] id={r[0]}  {r[1]:<24}  {r[2]:<24}  {r[3]}")


# ── Example 3 ─────────────────────────────────────────────────────────────────

def example_03_customer_addresses() -> None:
    """
    SCD Type 3 — one layer of address history kept in ``prev_*`` columns.
    No new rows created; the dimension is updated in-place with old values
    rotated into ``prev_address`` / ``prev_city``.

    OLTP → sqldim pipeline:
      CustomersSource.snapshot()     → initial customer addresses
      CustomersSource.event_batch(1) → customers who have moved
    """
    print("\n── Example 3: Customer Address Book (SCD Type 3) ───────────────")

    src  = CustomersSource(n=5, seed=42)
    path = _tmp_db()

    setup_con = duckdb.connect(path)
    src.setup(setup_con, "dim_customer")
    setup_con.close()

    with DuckDBSink(path) as sink:
        proc = LazyType3Processor(
            natural_key  = "customer_id",
            column_pairs = [("address", "prev_address"), ("city", "prev_city")],
            sink         = sink,
            con          = sink._con,
        )
        r1 = proc.process(src.snapshot(),     "dim_customer")
        r2 = proc.process(src.event_batch(1), "dim_customer")

    print(f"  T0 snapshot    → inserted={r1.inserted}")
    print(f"  T1 move events → inserted={r2.inserted}, rotated={r2.versioned}, unchanged={r2.unchanged}")

    con  = duckdb.connect(path)
    rows = con.execute(
        "SELECT customer_id, full_name, address, city, prev_address, prev_city "
        "FROM dim_customer ORDER BY customer_id"
    ).fetchall()
    src.teardown(con, "dim_customer")
    con.close()
    os.unlink(path)

    print("  Current addresses (with previous retained):")
    for r in rows:
        prev = f"  ← was: {r[4]}, {r[5]}" if r[4] else ""
        print(f"    id={r[0]}  {r[1]:<24}  now: {r[2]}, {r[3]}{prev}")


# ── Example 4 ─────────────────────────────────────────────────────────────────

def example_04_retail_stores() -> None:
    """
    SCD Type 6 — phone number updated in-place (Type 1);
    city/state change creates a new SCD2 row (Type 2).

    OLTP → sqldim pipeline:
      StoresSource.snapshot()     → initial store directory
      StoresSource.event_batch(1) → phone changes + relocations
    """
    print("\n── Example 4: Retail Store Dimension (SCD Type 6) ──────────────")

    src  = StoresSource(n=5, seed=42)
    path = _tmp_db()

    setup_con = duckdb.connect(path)
    src.setup(setup_con, "dim_store")
    setup_con.close()

    with DuckDBSink(path) as sink:
        proc = LazyType6Processor(
            natural_key   = "store_id",
            type1_columns = ["phone"],
            type2_columns = ["city", "state"],
            sink          = sink,
            con           = sink._con,
        )
        r1 = proc.process(src.snapshot(),     "dim_store")
        r2 = proc.process(src.event_batch(1), "dim_store")

    print(f"  T0 snapshot → inserted={r1.inserted}")
    print(f"  T1 events   → inserted={r2.inserted}, versioned={r2.versioned}, unchanged={r2.unchanged}")

    con  = duckdb.connect(path)
    rows = con.execute(
        "SELECT store_id, store_name, phone, email, city, state, is_current "
        "FROM dim_store ORDER BY store_id, valid_from"
    ).fetchall()
    src.teardown(con, "dim_store")
    con.close()
    os.unlink(path)

    print("  Full history:")
    for r in rows:
        flag = "✓" if r[6] else "H"
        print(f"    [{flag}] id={r[0]}  {r[1]:<20}  {r[2]}  {r[4]}, {r[5]}")


# ── Entry point ───────────────────────────────────────────────────────────────

def run_showcase() -> None:
    print("SCD Types Showcase")
    print("==================")
    example_01_product_catalogue()
    example_02_employee_directory()
    example_03_customer_addresses()
    example_04_retail_stores()
    print("\nDone.\n")


if __name__ == "__main__":  # pragma: no cover
    run_showcase()

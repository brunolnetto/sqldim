"""retail medallion pipeline — staging → bronze → silver → gold → semantic.

Mirrors the patterns from:
  aula 08 – Fato e Dimensão na Prática (Caso Varejo + SCDs Clássicos)

Layer design
------------
Staging (raw OLTP, Faker-generated):
    raw_customers   customer master data
    raw_products    product catalogue
    raw_orders      transactional order events

Bronze (dimensional models / historised — dim, fact, bridge):
    silver_dim_customer   SCD2 customer dimension with surrogate key,
                          valid_from / valid_to / is_current audit columns.
                          Some customers change segment over time, creating
                          multiple versions (mirrors dim_cliente_type2).
    silver_dim_product    SCD1 product dimension — price overwrites in place.
    silver_fact_orders    Order fact with customer_sk resolved via Point-In-Time
                          join against silver_dim_customer.

Silver (pre-aggregated, snapshot):
    dim_customer          Current-snapshot customer attributes.
                          Derived from silver_dim_customer WHERE is_current = TRUE.

Gold (aggregated metrics):
    fct_daily_sales       OBT: one row per (order_date, segment) with
                          total_orders, total_revenue, new_customers,
                          returning_customers.
                          Mirrors varejo.fct_metricas_diarias.
    fct_cohort_retention  Weekly cohort J-curve mart: D1 / D7 / D30 retention.
                          Mirrors varejo.mart_jcurve_retencao.

Semantic (business views — exposed to NL agent / BI):
    All gold tables are re-exposed as the consumption layer for NL queries.

Usage
-----
    import duckdb
    from sqldim.application.datasets.domains.retail.pipeline.builder import build_pipeline

    con = duckdb.connect(":memory:")
    build_pipeline(con)
    # gold tables are now queryable:
    con.execute("SELECT * FROM fct_daily_sales LIMIT 5").fetchall()
"""

from __future__ import annotations

import random
from datetime import date, timedelta
from typing import Any

import duckdb

# ── constants ──────────────────────────────────────────────────────────────

_SEED = 42
_N_CUSTOMERS = 200
_N_PRODUCTS = 30
_N_DAYS = 90          # order history window
_START_DATE = date(2024, 1, 1)
_SEGMENTS = ["premium", "standard", "trial"]
_REGIONS = ["NORTH", "SOUTH", "EAST", "WEST"]
_CATEGORIES = ["Electronics", "Apparel", "Home", "Sports", "Books"]

# Orders-per-day per segment (range) — trial users are most numerous
_ORD_LO = {"premium": 2, "standard": 8, "trial": 20}
_ORD_HI = {"premium": 8, "standard": 25, "trial": 60}

# Unit price ranges per product category
_PRICE_RANGE = {
    "Electronics": (50.0, 500.0),
    "Apparel": (20.0, 150.0),
    "Home": (15.0, 200.0),
    "Sports": (10.0, 120.0),
    "Books": (8.0, 40.0),
}


# ── bronze layer ────────────────────────────────────────────────────────────


def _build_bronze(con: duckdb.DuckDBPyConnection, *, seed: int = _SEED) -> None:
    """Populate the three raw OLTP tables with Faker-backed synthetic data."""
    from faker import Faker

    rng = random.Random(seed)
    fake = Faker()
    Faker.seed(seed)

    # ── raw_customers ──────────────────────────────────────────────────────
    con.execute("""
        CREATE TABLE raw_customers (
            customer_id INTEGER PRIMARY KEY,
            name        VARCHAR NOT NULL,
            segment     VARCHAR NOT NULL,
            region      VARCHAR NOT NULL,
            joined_date DATE    NOT NULL
        )
    """)
    customers: list[dict[str, Any]] = []
    for i in range(_N_CUSTOMERS):
        customers.append({
            "customer_id": i + 1,
            "name": fake.name(),
            "segment": rng.choices(_SEGMENTS, weights=[1, 3, 6], k=1)[0],
            "region": rng.choice(_REGIONS),
            "joined_date": _START_DATE - timedelta(days=rng.randint(0, 180)),
        })
    con.executemany(
        "INSERT INTO raw_customers VALUES (?, ?, ?, ?, ?)",
        [(r["customer_id"], r["name"], r["segment"], r["region"], r["joined_date"])
         for r in customers],
    )

    # ── raw_products ───────────────────────────────────────────────────────
    con.execute("""
        CREATE TABLE raw_products (
            product_id INTEGER PRIMARY KEY,
            name       VARCHAR NOT NULL,
            category   VARCHAR NOT NULL,
            price      DOUBLE  NOT NULL
        )
    """)
    products: list[tuple[Any, ...]] = []
    for i in range(_N_PRODUCTS):
        cat = rng.choice(_CATEGORIES)
        lo, hi = _PRICE_RANGE[cat]
        products.append((i + 1, fake.catch_phrase(), cat, round(rng.uniform(lo, hi), 2)))
    con.executemany("INSERT INTO raw_products VALUES (?, ?, ?, ?)", products)

    # ── raw_orders ─────────────────────────────────────────────────────────
    # Each day we generate 30-100 orders spread across customers proportionally
    # to their segment's order-rate weight.  Quantities are 1–5.
    con.execute("""
        CREATE TABLE raw_orders (
            order_id    INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL REFERENCES raw_customers(customer_id),
            product_id  INTEGER NOT NULL REFERENCES raw_products(product_id),
            quantity    INTEGER NOT NULL,
            unit_price  DOUBLE  NOT NULL,
            order_date  DATE    NOT NULL
        )
    """)

    # Build per-segment customer pools
    pools: dict[str, list[int]] = {s: [] for s in _SEGMENTS}
    for c in customers:
        pools[c["segment"]].append(c["customer_id"])

    orders: list[tuple[Any, ...]] = []
    order_id = 1
    for day_offset in range(_N_DAYS):
        d = _START_DATE + timedelta(days=day_offset)
        for seg in _SEGMENTS:
            n = rng.randint(_ORD_LO[seg], _ORD_HI[seg])
            for _ in range(n):
                cid = rng.choice(pools[seg])
                pid, _, _, price = rng.choice(products)
                orders.append((order_id, cid, pid, rng.randint(1, 5), price, d))
                order_id += 1
    con.executemany("INSERT INTO raw_orders VALUES (?, ?, ?, ?, ?, ?)", orders)


# ── silver layer ────────────────────────────────────────────────────────────


def _build_silver(con: duckdb.DuckDBPyConnection, *, seed: int = _SEED) -> None:
    """Build SCD2 customer dim, SCD1 product dim, and PIT-resolved fact table."""
    rng = random.Random(seed + 1)

    # ── silver_dim_customer (SCD2) ─────────────────────────────────────────
    # Initial version: all customers active from joined_date.
    # Simulate segment upgrades: ~15% of trial→standard, ~10% of standard→premium
    # after the midpoint of the observation window.  Each upgrade spawns a new
    # SCD2 row and closes the previous one.

    con.execute("""
        CREATE TABLE silver_dim_customer (
            customer_sk  INTEGER PRIMARY KEY,
            customer_id  INTEGER NOT NULL,
            name         VARCHAR NOT NULL,
            segment      VARCHAR NOT NULL,
            region       VARCHAR NOT NULL,
            valid_from   DATE    NOT NULL,
            valid_to     DATE,              -- NULL means active (is_current)
            is_current   BOOLEAN NOT NULL DEFAULT TRUE
        )
    """)

    upgrade_cutoff = _START_DATE + timedelta(days=_N_DAYS // 2)
    upgrade_date = _START_DATE + timedelta(days=_N_DAYS // 2 + 7)

    rows = con.execute(
        "SELECT customer_id, name, segment, region, joined_date FROM raw_customers ORDER BY customer_id"
    ).fetchall()

    sk = 1
    upgrades: list[tuple[int, str]] = []  # (customer_id, new_segment)
    initial_rows: list[tuple[Any, ...]] = []
    for cid, name, segment, region, joined_date in rows:
        # Decide if this customer will upgrade
        new_seg: str | None = None
        if segment == "trial" and rng.random() < 0.15:
            new_seg = "standard"
            upgrades.append((cid, new_seg))
        elif segment == "standard" and rng.random() < 0.10:
            new_seg = "premium"
            upgrades.append((cid, new_seg))

        if new_seg is not None:
            # Version 1: active from joined_date → upgrade_cutoff
            initial_rows.append((sk, cid, name, segment, region, joined_date, upgrade_cutoff, False))
            sk += 1
            # Version 2: active from upgrade_date → NULL (current)
            initial_rows.append((sk, cid, name, new_seg, region, upgrade_date, None, True))
            sk += 1
        else:
            # Single version, still current
            initial_rows.append((sk, cid, name, segment, region, joined_date, None, True))
            sk += 1

    con.executemany(
        "INSERT INTO silver_dim_customer VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        initial_rows,
    )

    # ── silver_dim_product (SCD1 — simple upsert) ─────────────────────────
    con.execute("""
        CREATE TABLE silver_dim_product AS
        SELECT product_id, name, category, price FROM raw_products
    """)

    # ── silver_fact_orders (PIT SK resolution) ────────────────────────────
    # Join each order to the dim_customer version active on the order_date
    # (Point-In-Time): valid_from <= order_date AND (valid_to IS NULL OR valid_to > order_date)
    con.execute("""
        CREATE TABLE silver_fact_orders AS
        SELECT
            o.order_id,
            d.customer_sk,
            o.product_id,
            o.quantity,
            o.unit_price,
            o.order_date
        FROM raw_orders o
        JOIN silver_dim_customer d
            ON  o.customer_id = d.customer_id
            AND o.order_date >= d.valid_from
            AND (d.valid_to IS NULL OR o.order_date < d.valid_to)
    """)


# ── gold layer ──────────────────────────────────────────────────────────────


def _build_gold(con: duckdb.DuckDBPyConnection) -> None:
    """Build the gold/semantic tables exposed to the NL agent."""

    # ── dim_customer (current snapshot, no SCD audit cols) ─────────────
    con.execute("""
        CREATE TABLE dim_customer AS
        SELECT
            customer_id,
            name,
            segment,
            region
        FROM silver_dim_customer
        WHERE is_current = TRUE
    """)

    # ── fct_daily_sales (gold OBT — mirrors fct_metricas_diarias) ─────────
    # Grain: one row per (order_date, segment).
    # new_customers: the order_date equals their very first order date.
    # returning_customers: they ordered before this date.
    con.execute("""
        CREATE TABLE fct_daily_sales AS
        WITH first_order AS (
            SELECT customer_sk, MIN(order_date) AS first_date
            FROM silver_fact_orders
            GROUP BY customer_sk
        )
        SELECT
            f.order_date                                        AS date_ref,
            d.segment,
            COUNT(DISTINCT f.order_id)                         AS total_orders,
            ROUND(SUM(f.quantity * f.unit_price), 2)           AS total_revenue,
            COUNT(DISTINCT CASE
                WHEN fo.first_date = f.order_date THEN f.customer_sk END)  AS new_customers,
            COUNT(DISTINCT CASE
                WHEN fo.first_date < f.order_date  THEN f.customer_sk END) AS returning_customers
        FROM silver_fact_orders f
        JOIN silver_dim_customer d
            ON  f.customer_sk = d.customer_sk
        JOIN first_order fo
            ON  f.customer_sk = fo.customer_sk
        GROUP BY f.order_date, d.segment
        ORDER BY f.order_date, d.segment
    """)

    # ── fct_cohort_retention (J-curve mart — mirrors mart_jcurve_retencao) ─
    # Cohort = customer's first order week.  Retention at D1/D7/D30:
    # a customer is retained_dN if they placed at least one order
    # within [first_date+1, first_date+N] days.
    con.execute("""
        CREATE TABLE fct_cohort_retention AS
        WITH first_order AS (
            SELECT customer_sk, MIN(order_date) AS first_date
            FROM silver_fact_orders
            GROUP BY customer_sk
        ),
        cohort_week AS (
            SELECT
                customer_sk,
                DATE_TRUNC('week', first_date)::DATE AS cohort_date,
                first_date
            FROM first_order
        ),
        activity AS (
            SELECT DISTINCT
                fo.customer_sk,
                fo.cohort_date,
                fo.first_date,
                f.order_date
            FROM silver_fact_orders f
            JOIN cohort_week fo ON f.customer_sk = fo.customer_sk
            WHERE f.order_date > fo.first_date
        )
        SELECT
            cw.cohort_date,
            COUNT(DISTINCT cw.customer_sk)                              AS cohort_size,
            COUNT(DISTINCT CASE
                WHEN a.order_date <= cw.first_date + INTERVAL '1' DAY
                THEN cw.customer_sk END)                                AS retained_d1,
            COUNT(DISTINCT CASE
                WHEN a.order_date <= cw.first_date + INTERVAL '7' DAY
                THEN cw.customer_sk END)                                AS retained_d7,
            COUNT(DISTINCT CASE
                WHEN a.order_date <= cw.first_date + INTERVAL '30' DAY
                THEN cw.customer_sk END)                                AS retained_d30
        FROM cohort_week cw
        LEFT JOIN activity a
            ON  a.customer_sk  = cw.customer_sk
            AND a.cohort_date   = cw.cohort_date
        GROUP BY cw.cohort_date
        ORDER BY cw.cohort_date
    """)


# ── public entry point ───────────────────────────────────────────────────────


#: Gold-layer table names exposed to the NL agent.
GOLD_TABLES: list[str] = ["dim_customer", "fct_daily_sales", "fct_cohort_retention"]


def rebuild_from_bronze(con: duckdb.DuckDBPyConnection, *, seed: int = _SEED) -> None:
    """Rebuild the silver and gold layers from the current bronze tables.

    Call this after any OLTP-level event (mutations to ``raw_customers``,
    ``raw_orders``, or ``raw_products``) to propagate the change through the
    full pipeline without regenerating bronze data.

    Cascade: drops silver tables → drops gold tables → re-runs
    :func:`_build_silver` (SCD2 + SCD1 + PIT fact) → re-runs
    :func:`_build_gold` (semantic layer).

    Parameters
    ----------
    con:
        The live DuckDB connection holding the bronze tables.
    seed:
        RNG seed forwarded to :func:`_build_silver` for reproducible SCD2
        synthetic upgrade decisions.
    """
    # Drop silver (dependency order: fact before dims)
    con.execute("DROP TABLE IF EXISTS silver_fact_orders")
    con.execute("DROP TABLE IF EXISTS silver_dim_product")
    con.execute("DROP TABLE IF EXISTS silver_dim_customer")
    # Drop gold
    con.execute("DROP TABLE IF EXISTS dim_customer")
    con.execute("DROP TABLE IF EXISTS fct_daily_sales")
    con.execute("DROP TABLE IF EXISTS fct_cohort_retention")
    # Rebuild in correct order
    _build_silver(con, seed=seed)
    _build_gold(con)


def rebuild_fact_orders(con: duckdb.DuckDBPyConnection) -> None:
    """Re-derive ``silver_fact_orders`` from the current ``raw_orders`` and
    ``silver_dim_customer``.

    Use this after any event that modifies the raw order rows (e.g. a price
    reprice) without touching the customer dimension.  The SCD2 structure in
    ``silver_dim_customer`` is preserved; only the fact PIT-join is rerun.
    """
    con.execute("DROP TABLE IF EXISTS silver_fact_orders")
    con.execute("""
        CREATE TABLE silver_fact_orders AS
        SELECT
            o.order_id,
            d.customer_sk,
            o.product_id,
            o.quantity,
            o.unit_price,
            o.order_date
        FROM raw_orders o
        JOIN silver_dim_customer d
            ON  o.customer_id = d.customer_id
            AND o.order_date >= d.valid_from
            AND (d.valid_to IS NULL OR o.order_date < d.valid_to)
    """)


def rebuild_gold(con: duckdb.DuckDBPyConnection) -> None:
    """Drop and recreate all three gold tables from the current silver layer.

    Call this after any event that mutates silver tables so the NL agent sees
    the updated data.
    """
    con.execute("DROP TABLE IF EXISTS dim_customer")
    con.execute("DROP TABLE IF EXISTS fct_daily_sales")
    con.execute("DROP TABLE IF EXISTS fct_cohort_retention")
    _build_gold(con)


def build_pipeline(
    con: duckdb.DuckDBPyConnection,
    *,
    seed: int = _SEED,
) -> None:
    """Populate *con* with all three medallion layers.

    After this call the connection contains:
    - Bronze : ``raw_customers``, ``raw_products``, ``raw_orders``
    - Silver : ``silver_dim_customer``, ``silver_dim_product``, ``silver_fact_orders``
    - Gold   : ``dim_customer``, ``fct_daily_sales``, ``fct_cohort_retention``

    Parameters
    ----------
    con:
        An open in-memory (or file-backed) DuckDB connection.
    seed:
        RNG seed for deterministic data generation.
    """
    _build_bronze(con, seed=seed)
    _build_silver(con, seed=seed)
    _build_gold(con)

"""retail domain gold-layer sources.

Models the gold / semantic layer described in:
  aula 08 – Fato e Dimensão na Prática (Caso Varejo + SCDs Clássicos)

Tables
------
dim_customer        SCD2-style customer dimension: one current row per customer
                    with segment (premium / standard / trial) and region.

fct_daily_sales     Gold OBT (One Big Table): one row per day × segment with
                    pre-aggregated business KPIs ready for dashboards.
                    Mirrors ``varejo.fct_metricas_diarias``.

fct_cohort_retention  J-curve retention mart: one row per cohort_date showing
                    how many customers were retained at D1, D7, and D30.
                    Mirrors ``varejo.mart_jcurve_retencao``.

All tables are generated deterministically so eval ground-truth SQL
produces stable expected results across runs.
"""

from __future__ import annotations

import random
from datetime import date, timedelta
from typing import Any

import duckdb

from sqldim.application.datasets.base import BaseSource, DatasetFactory, SourceProvider

# ── Vocabulary ─────────────────────────────────────────────────────────────

_SEGMENTS = ["premium", "standard", "trial"]
_REGIONS = ["NORTH", "SOUTH", "EAST", "WEST"]
_SEED = 42


# ── DimCustomerSource ───────────────────────────────────────────────────────


@DatasetFactory.register("retail_dim_customer")
class DimCustomerSource(BaseSource):
    """Synthetic customer dimension (SCD2-style current snapshot).

    One row per customer with ``segment`` and ``region`` attributes.
    No SCD audit columns — this is the final gold-layer dimension.

    Schema::

        customer_id  INTEGER  PRIMARY KEY
        full_name    VARCHAR
        segment      VARCHAR   -- premium | standard | trial
        region       VARCHAR   -- NORTH | SOUTH | EAST | WEST

    Data is inserted directly in ``setup()``; ``snapshot()`` raises
    ``NotImplementedError`` to signal the static-fixture pattern.
    ``DatasetPipelineSource`` will skip the INSERT-BY-NAME step.
    """

    provider = SourceProvider(
        name="Retail DW – dim_customer (gold layer)",
        description="SCD2-style customer dimension, current snapshot.",
        url=None,
        auth_required=False,
        requires=[],
    )

    _DDL = (
        "CREATE TABLE IF NOT EXISTS {table} ("
        "  customer_id INTEGER PRIMARY KEY,"
        "  full_name   VARCHAR NOT NULL,"
        "  segment     VARCHAR NOT NULL,"
        "  region      VARCHAR NOT NULL"
        ")"
    )

    def __init__(self, n: int = 200, *, seed: int = _SEED) -> None:
        random.seed(seed)
        from faker import Faker

        fake = Faker()
        Faker.seed(seed)
        self._rows: list[tuple[Any, ...]] = [
            (
                i + 1,
                fake.name(),
                random.choices(_SEGMENTS, weights=[1, 3, 6], k=1)[0],
                random.choice(_REGIONS),
            )
            for i in range(n)
        ]

    def snapshot(self):  # noqa: ANN201
        raise NotImplementedError("dim_customer is a static fixture — data inserted in setup()")

    def setup(self, con: duckdb.DuckDBPyConnection, table: str) -> None:
        con.execute(self._DDL.format(table=table))
        con.executemany(
            f"INSERT OR REPLACE INTO {table} VALUES (?, ?, ?, ?)",
            self._rows,
        )

    def teardown(self, con: duckdb.DuckDBPyConnection, table: str) -> None:
        con.execute(f"DROP TABLE IF EXISTS {table}")


# ── FctDailySalesSource ────────────────────────────────────────────────────


@DatasetFactory.register("retail_fct_daily_sales")
class FctDailySalesSource(BaseSource):
    """Gold OBT: daily sales metrics pre-aggregated by date and customer segment.

    Mirrors ``varejo.fct_metricas_diarias`` from the lecture.

    Schema::

        date_ref            DATE     -- business date
        segment             VARCHAR  -- premium | standard | trial
        total_orders        INTEGER  -- orders placed on this day for this segment
        total_revenue       DOUBLE   -- sum of order values
        new_customers       INTEGER  -- customers placing their first-ever order (DAU proxy)
        returning_customers INTEGER  -- customers with prior orders (retained users)

    Grain: one row per (date_ref, segment).
    Data is inserted directly in ``setup()`` (static-fixture pattern).
    """

    provider = SourceProvider(
        name="Retail DW – fct_daily_sales (gold layer)",
        description="Pre-aggregated daily sales OBT by date and segment for dashboards.",
        url=None,
        auth_required=False,
        requires=[],
    )

    _DDL = (
        "CREATE TABLE IF NOT EXISTS {table} ("
        "  date_ref            DATE    NOT NULL,"
        "  segment             VARCHAR NOT NULL,"
        "  total_orders        INTEGER NOT NULL DEFAULT 0,"
        "  total_revenue       DOUBLE  NOT NULL DEFAULT 0.0,"
        "  new_customers       INTEGER NOT NULL DEFAULT 0,"
        "  returning_customers INTEGER NOT NULL DEFAULT 0,"
        "  PRIMARY KEY (date_ref, segment)"
        ")"
    )

    def __init__(self, n_days: int = 90, *, seed: int = _SEED) -> None:
        random.seed(seed)
        start = date(2024, 1, 1)
        self._rows: list[tuple[Any, ...]] = []
        # Revenue ranges per segment — premium orders are fewer but higher-value
        rev_lo = {"premium": 400, "standard": 80, "trial": 8}
        rev_hi = {"premium": 1800, "standard": 600, "trial": 120}
        ord_lo = {"premium": 5, "standard": 15, "trial": 30}
        ord_hi = {"premium": 20, "standard": 60, "trial": 120}

        for day_offset in range(n_days):
            d = start + timedelta(days=day_offset)
            for seg in _SEGMENTS:
                orders = random.randint(ord_lo[seg], ord_hi[seg])
                revenue = round(
                    random.uniform(rev_lo[seg], rev_hi[seg]) * orders, 2
                )
                new_c = max(1, random.randint(0, orders // 4))
                ret_c = max(0, orders - new_c)
                self._rows.append((d, seg, orders, revenue, new_c, ret_c))

    def snapshot(self):  # noqa: ANN201
        raise NotImplementedError("fct_daily_sales is a static fixture — data inserted in setup()")

    def setup(self, con: duckdb.DuckDBPyConnection, table: str) -> None:
        con.execute(self._DDL.format(table=table))
        con.executemany(
            f"INSERT OR REPLACE INTO {table} VALUES (?, ?, ?, ?, ?, ?)",
            self._rows,
        )

    def teardown(self, con: duckdb.DuckDBPyConnection, table: str) -> None:
        con.execute(f"DROP TABLE IF EXISTS {table}")


# ── FctCohortRetentionSource ───────────────────────────────────────────────


@DatasetFactory.register("retail_fct_cohort_retention")
class FctCohortRetentionSource(BaseSource):
    """J-curve retention mart: cohort retention at D1, D7, and D30.

    Mirrors ``varejo.mart_jcurve_retencao`` from the lecture.

    Schema::

        cohort_date     DATE    -- date customers first transacted (cohort entry)
        cohort_size     INTEGER -- total customers in this cohort
        retained_d1     INTEGER -- customers still active on day +1
        retained_d7     INTEGER -- customers still active on day +7
        retained_d30    INTEGER -- customers still active on day +30

    Grain: one row per cohort_date (weekly cohorts).
    Data is inserted directly in ``setup()`` (static-fixture pattern).
    """

    provider = SourceProvider(
        name="Retail DW – fct_cohort_retention (gold layer)",
        description="Weekly cohort J-curve retention mart (D1, D7, D30).",
        url=None,
        auth_required=False,
        requires=[],
    )

    _DDL = (
        "CREATE TABLE IF NOT EXISTS {table} ("
        "  cohort_date  DATE    NOT NULL PRIMARY KEY,"
        "  cohort_size  INTEGER NOT NULL,"
        "  retained_d1  INTEGER NOT NULL,"
        "  retained_d7  INTEGER NOT NULL,"
        "  retained_d30 INTEGER NOT NULL"
        ")"
    )

    def __init__(self, n_cohorts: int = 12, *, seed: int = _SEED) -> None:
        random.seed(seed)
        start = date(2024, 1, 1)
        self._rows: list[tuple[Any, ...]] = []
        for i in range(n_cohorts):
            cohort_date = start + timedelta(weeks=i)
            size = random.randint(40, 200)
            d1 = round(size * random.uniform(0.55, 0.80))
            d7 = round(d1 * random.uniform(0.40, 0.70))
            d30 = round(d7 * random.uniform(0.20, 0.55))
            self._rows.append(
                (cohort_date, size, int(d1), int(d7), int(d30))
            )

    def snapshot(self):  # noqa: ANN201
        raise NotImplementedError(
            "fct_cohort_retention is a static fixture — data inserted in setup()"
        )

    def setup(self, con: duckdb.DuckDBPyConnection, table: str) -> None:
        con.execute(self._DDL.format(table=table))
        con.executemany(
            f"INSERT OR REPLACE INTO {table} VALUES (?, ?, ?, ?, ?)",
            self._rows,
        )

    def teardown(self, con: duckdb.DuckDBPyConnection, table: str) -> None:
        con.execute(f"DROP TABLE IF EXISTS {table}")

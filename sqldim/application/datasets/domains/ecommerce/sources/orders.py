"""
sqldim/examples/datasets/orders.py
====================================
OrdersSource — e-commerce order fulfilment pipeline with milestone timestamps.
"""

from __future__ import annotations

import random
from datetime import date, timedelta
from typing import Any

from faker import Faker

from sqldim.application.datasets.base import (
    BaseSource,
    DatasetFactory,
    SourceProvider,
)
from sqldim.application.datasets.schema import (
    DatasetSpec,
    EntitySchema,
    FieldSpec,
)

_ORDERS_SPEC = DatasetSpec(
    "orders",
    {
        "source": EntitySchema(
            name="order",
            fields=[
                FieldSpec("order_id", "INTEGER"),
                FieldSpec("customer_id", "INTEGER"),
                FieldSpec("amount", "DOUBLE"),
                FieldSpec("status", "VARCHAR"),
                FieldSpec("placed_at", "VARCHAR"),
                FieldSpec("paid_at", "VARCHAR"),
                FieldSpec("shipped_at", "VARCHAR"),
                FieldSpec("delivered_at", "VARCHAR"),
            ],
        ),
        "fact": EntitySchema(
            name="order_fact",
            fields=[
                FieldSpec("order_id", "INTEGER"),
                FieldSpec("customer_id", "INTEGER"),
                FieldSpec("amount", "DOUBLE"),
                FieldSpec("placed_at", "VARCHAR"),
                FieldSpec("paid_at", "VARCHAR"),
                FieldSpec("shipped_at", "VARCHAR"),
                FieldSpec("delivered_at", "VARCHAR"),
            ],
        ),
    },
)


@DatasetFactory.register("orders")
class OrdersSource(BaseSource):
    """
    OLTP order-fulfilment pipeline — feeds ``LazyAccumulatingLoader``.

    Each order is a row in the OLTP ``orders`` table with four progressive
    milestone timestamps.  Events arrive in batches as the order moves
    through the pipeline:

        T1 placed_events()   → placed_at set; paid/shipped/delivered NULL
        T2 paid_events()     → paid_at set for settled orders
        T3 shipped_events()  → shipped_at set for despatched orders
        T4 delivered_events()→ delivered_at set for completed orders

    Kept manual because the milestone-progression model requires knowing the
    full set of timestamps at construction time, which doesn't map naturally
    to a ChangeRule pattern.

    OLTP schema::

        order_id      INTEGER  PRIMARY KEY
        customer_id   INTEGER  FK
        amount        DOUBLE
        status        VARCHAR   -- 'placed' | 'paid' | 'shipped' | 'delivered'
        placed_at     TIMESTAMP
        paid_at       TIMESTAMP  NULL
        shipped_at    TIMESTAMP  NULL
        delivered_at  TIMESTAMP  NULL
    """

    provider = SourceProvider(
        name="Order management system (OMS / ERP)",
        description="Order fulfilment pipeline with milestone timestamps.",
        url="https://docs.kladana.com/api/",
        auth_required=True,
        requires=["dlt", "requests"],
    )

    @property
    def OLTP_DDL(self) -> str:  # noqa: N802
        return _ORDERS_SPEC.source.oltp_ddl()

    @property
    def DIM_DDL(self) -> str:  # noqa: N802
        """Fact target DDL for LazyAccumulatingLoader (no SCD audit columns)."""
        return _ORDERS_SPEC.fact.oltp_ddl()

    def __init__(self, n: int = 6, seed: int = 42) -> None:
        Faker()
        Faker.seed(seed)
        random.seed(seed)

        base = date(2024, 3, 1)
        self._orders: list[dict[str, Any]] = []
        for i in range(n):
            placed = base + timedelta(days=random.randint(0, 10))
            paid = placed + timedelta(days=random.randint(0, 2))
            shipped = paid + timedelta(days=random.randint(1, 3))
            delivered = shipped + timedelta(days=random.randint(1, 4))
            self._orders.append(
                {
                    "order_id": 1000 + i + 1,
                    "customer_id": random.randint(1, 50),
                    "amount": round(random.uniform(9.99, 499.99), 2),
                    "_placed": placed,
                    "_paid": paid if i < n * 3 // 4 else None,  # 75%
                    "_shipped": shipped if i < n * 2 // 4 else None,  # 50%
                    "_delivered": delivered if i < n * 1 // 4 else None,  # 25%
                }
            )

    # ── Progressive event sources ─────────────────────────────────────────

    def snapshot(self):
        """Initial order state — all orders at placement (T0)."""
        return self.placed_events()

    def placed_events(self):
        """All orders just created — only placed_at is known."""
        from sqldim.sources import SQLSource

        return SQLSource(self._batch_sql(self._orders, []))

    def paid_events(self):
        """Orders that have been paid (75% of total)."""
        from sqldim.sources import SQLSource

        return SQLSource(
            self._batch_sql(
                [o for o in self._orders if o["_paid"] is not None], ["paid_at"]
            )
        )

    def shipped_events(self):
        """Orders that have shipped (50% of total)."""
        from sqldim.sources import SQLSource

        return SQLSource(
            self._batch_sql(
                [o for o in self._orders if o["_shipped"] is not None],
                ["paid_at", "shipped_at"],
            )
        )

    def delivered_events(self):
        """Fully delivered orders (25% of total)."""
        from sqldim.sources import SQLSource

        return SQLSource(
            self._batch_sql(
                [o for o in self._orders if o["_delivered"] is not None],
                ["paid_at", "shipped_at", "delivered_at"],
            )
        )

    def _null(self, v: Any) -> str:
        return f"'{v.isoformat()}'" if v is not None else "NULL"

    def _batch_sql(self, orders: list[dict], include_milestones: list[str]) -> str:
        def row_sql(o: dict) -> str:
            milestones = {
                "paid_at": self._null(o["_paid"]),
                "shipped_at": self._null(o["_shipped"]),
                "delivered_at": self._null(o["_delivered"]),
            }
            parts = [
                f"SELECT {o['order_id']} AS order_id",
                f"{o['customer_id']} AS customer_id",
                f"{o['amount']} AS amount",
                f"'{o['_placed'].isoformat()}' AS placed_at",
            ]
            for col in ["paid_at", "shipped_at", "delivered_at"]:
                parts.append(
                    f"{milestones[col] if col in include_milestones else 'NULL'} AS {col}"
                )
            return ", ".join(parts)

        return " UNION ALL ".join(row_sql(o) for o in orders)

    @property
    def orders(self) -> list[dict[str, Any]]:
        return list(self._orders)

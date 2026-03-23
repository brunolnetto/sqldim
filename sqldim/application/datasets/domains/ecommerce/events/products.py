"""Product domain events for the ecommerce domain."""
from __future__ import annotations
from typing import Any
from sqldim.application.datasets.events import AggregateState, DomainEvent


class ProductStockOutEvent(DomainEvent):
    """
    A product's stock has been fully depleted.

    Business rules
    --------------
    - Sets ``stock_qty = 0`` and ``is_active = False`` on the product.
    - Stamps ``updated_at`` on the product row, if that field is present.
    - If any order rows carry a ``product_id`` field that matches, all
      ``placed`` orders for the affected product are cancelled.  Orders
      already past the ``placed`` stage are not touched.

    Parameters (via ``emit``)
    -------------------------
    product_id : int
        The product that ran out of stock.
    event_ts   : str, optional
        Timestamp string written to ``products.updated_at``.
        Defaults to ``"2024-06-01 09:00:00"``.
    """

    name = "product_stock_out"

    def apply(
        self,
        state: AggregateState,
        *,
        product_id: int,
        event_ts: str = "2024-06-01 09:00:00",
    ) -> dict[str, list[dict]]:
        result: dict[str, list[dict]] = {}

        # ── Mark product as inactive ──────────────────────────────────────
        products = state.get("products")
        updated_products = [
            {
                **p,
                "stock_qty": 0,
                "is_active": False,
                **({"updated_at": event_ts} if "updated_at" in p else {}),
            }
            if p.get("product_id") == product_id
            else p
            for p in products
        ]
        changed_products = [
            p
            for orig, p in zip(products, updated_products)
            if orig != p
        ]
        if changed_products:
            state.update("products", updated_products)
            result["products"] = changed_products

        # ── Cancel placed orders containing this product ──────────────────
        # (only when order rows carry a product_id field)
        if "orders" in state.table_names:
            orders = state.get("orders")
            if orders and "product_id" in orders[0]:
                updated_orders = [
                    {**o, "status": "cancelled"}
                    if o.get("product_id") == product_id and o.get("status") == "placed"
                    else o
                    for o in orders
                ]
                changed_orders = [
                    o
                    for orig, o in zip(orders, updated_orders)
                    if orig != o
                ]
                if changed_orders:
                    state.update("orders", updated_orders)
                    result["orders"] = changed_orders

        return result

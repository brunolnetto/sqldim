"""Product domain events for the ecommerce domain."""

from __future__ import annotations
from sqldim.application.datasets.events import AggregateState, DomainEvent


def _has_orders_for_product(orders: list) -> bool:
    return bool(orders) and "product_id" in orders[0]


def _mark_order_cancelled(o: dict, product_id: int) -> dict:
    if o.get("product_id") == product_id and o.get("status") == "placed":
        return {**o, "status": "cancelled"}
    return o


def _changed_rows(orig_list: list, updated_list: list) -> list:
    return [u for orig, u in zip(orig_list, updated_list) if orig != u]


def _apply_stock_out_to_product(p: dict, product_id: int, event_ts: str) -> dict:
    if p.get("product_id") != product_id:
        return p
    return {
        **p,
        "stock_qty": 0,
        "is_active": False,
        **({"updated_at": event_ts} if "updated_at" in p else {}),
    }


def _cancel_product_placed_orders(
    state: AggregateState,
    product_id: int,
) -> list[dict]:
    """Cancel all 'placed' orders for *product_id*; mutates state. Returns changed rows."""
    if "orders" not in state.table_names:
        return []
    orders = state.get("orders")
    if not _has_orders_for_product(orders):
        return []
    updated = [_mark_order_cancelled(o, product_id) for o in orders]
    changed = _changed_rows(orders, updated)
    if changed:
        state.update("orders", updated)
    return changed


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

    def apply(  # type: ignore[override]
        self,
        state: AggregateState,
        *,
        product_id: int,
        event_ts: str = "2024-06-01 09:00:00",
    ) -> dict[str, list[dict]]:
        result: dict[str, list[dict]] = {}
        products = state.get("products")
        updated_products = [
            _apply_stock_out_to_product(p, product_id, event_ts) for p in products
        ]
        changed_products = _changed_rows(products, updated_products)
        if changed_products:
            state.update("products", updated_products)
            result["products"] = changed_products
        changed_orders = _cancel_product_placed_orders(state, product_id)
        if changed_orders:
            result["orders"] = changed_orders
        return result

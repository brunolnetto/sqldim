"""Customer domain events for the ecommerce domain."""

from __future__ import annotations
from sqldim.application.datasets.events import AggregateState, DomainEvent


def _changed_rows(orig_list: list, updated_list: list) -> list:
    return [u for orig, u in zip(orig_list, updated_list) if orig != u]


def _is_cancellable(o: dict) -> bool:
    if "status" in o:
        return o["status"] == "placed"
    return o.get("placed_at") is not None and o.get("paid_at") is None


def _build_cancelled_order(o: dict) -> dict:
    if "status" in o:
        return {**o, "status": "cancelled"}
    return {
        **o,
        "paid_at": None,
        "shipped_at": None,
        "delivered_at": None,
        "_cancelled": True,
    }


def _cancel_placed_orders_for_customer(
    orders: list[dict],
    customer_id: int,
) -> tuple[list[dict], list[dict]]:
    """Return (updated_orders, changed_orders) after cancelling placed orders."""
    updated: list[dict] = []
    changed: list[dict] = []
    for o in orders:
        if o.get("customer_id") != customer_id:
            updated.append(o)
            continue
        if _is_cancellable(o):
            new_o = _build_cancelled_order(o)
            updated.append(new_o)
            changed.append(new_o)
        else:
            updated.append(o)
    return updated, changed


def _stamp_ts_row(c: dict, customer_id: int, event_ts: str) -> dict:
    if c.get("customer_id") == customer_id and "updated_at" in c:
        return {**c, "updated_at": event_ts}
    return c


def _stamp_customer_ts(
    state: AggregateState,
    customer_id: int,
    event_ts: str,
    result: dict[str, list[dict]],
) -> None:
    """Stamp updated_at on the matching customer row if the field is present."""
    if "customers" not in state.table_names:
        return
    customers = state.get("customers")
    updated = [_stamp_ts_row(c, customer_id, event_ts) for c in customers]
    changed = _changed_rows(customers, updated)
    if changed:
        state.update("customers", updated)
        result["customers"] = changed


def _apply_address_to_row(
    c: dict, customer_id: int, new_address: str, new_city: str, event_ts: str
) -> dict:
    if c.get("customer_id") != customer_id:
        return c
    return {
        **c,
        "address": new_address,
        "city": new_city,
        **({"updated_at": event_ts} if "updated_at" in c else {}),
    }


class CustomerBulkCancelEvent(DomainEvent):
    """
    Customer requests bulk cancellation of all their pending orders.

    Business rules
    --------------
    - Only ``placed`` orders are eligible; orders in ``paid``, ``shipped``,
      or ``delivered`` state are already in fulfilment and are left as-is.
    - The customer's ``updated_at`` field, if present, is stamped with
      *event_ts* to reflect the account activity.

    Parameters (via ``emit``)
    -------------------------
    customer_id : int
        The customer whose placed orders should be cancelled.
    event_ts    : str, optional
        Timestamp string written to ``customers.updated_at``.
        Defaults to ``"2024-06-01 09:00:00"``.
    """

    name = "customer_bulk_cancel"

    def apply(  # type: ignore[override]
        self,
        state: AggregateState,
        *,
        customer_id: int,
        event_ts: str = "2024-06-01 09:00:00",
    ) -> dict[str, list[dict]]:
        result: dict[str, list[dict]] = {}
        updated_orders, changed_orders = _cancel_placed_orders_for_customer(
            state.get("orders"),
            customer_id,
        )
        if changed_orders:
            state.update("orders", updated_orders)
            result["orders"] = changed_orders
        _stamp_customer_ts(state, customer_id, event_ts, result)
        return result


class CustomerAddressChangedEvent(DomainEvent):
    """
    Single-table event: customer records a new address.

    Wraps the same mutation as the ``EventSpec``-based CDC event so it can
    participate in the ``EventRepository`` dispatch pattern alongside
    multi-table events.

    Parameters (via ``emit``)
    -------------------------
    customer_id : int
        The customer whose address changed.
    new_address : str
        New street address string.
    new_city    : str
        New city string.
    event_ts    : str, optional
        Timestamp written to ``customers.updated_at`` if present.
        Defaults to ``"2024-06-01 09:00:00"``.
    """

    name = "customer_address_changed"

    def apply(  # type: ignore[override]
        self,
        state: AggregateState,
        *,
        customer_id: int,
        new_address: str,
        new_city: str,
        event_ts: str = "2024-06-01 09:00:00",
    ) -> dict[str, list[dict]]:
        customers = state.get("customers")
        updated = [
            _apply_address_to_row(c, customer_id, new_address, new_city, event_ts)
            for c in customers
        ]
        changed = _changed_rows(customers, updated)
        if changed:
            state.update("customers", updated)
        return {"customers": changed}

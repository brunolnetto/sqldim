"""Customer domain events for the ecommerce domain."""
from __future__ import annotations
from typing import Any
from sqldim.application.datasets.events import AggregateState, DomainEvent


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

    def apply(
        self,
        state: AggregateState,
        *,
        customer_id: int,
        event_ts: str = "2024-06-01 09:00:00",
    ) -> dict[str, list[dict]]:
        result: dict[str, list[dict]] = {}

        # ── Cancel placed orders ──────────────────────────────────────────
        # An order is "cancellable" if:
        #   - it carries an explicit status=="placed" field, OR
        #   - in the accumulating-snapshot model (OrdersSource) it has
        #     placed_at set but paid_at = None (not yet paid)
        orders = state.get("orders")
        updated_orders = []
        changed_orders = []
        for o in orders:
            if o.get("customer_id") != customer_id:
                updated_orders.append(o)
                continue
            # Determine if the order is cancellable
            if "status" in o:
                cancellable = o["status"] == "placed"
            else:
                # Accumulating-snapshot model: placed but not yet paid
                cancellable = o.get("placed_at") is not None and o.get("paid_at") is None
            if cancellable:
                new_o = {**o, "status": "cancelled"} if "status" in o else {
                    **o,
                    "paid_at": None,
                    "shipped_at": None,
                    "delivered_at": None,
                    "_cancelled": True,
                }
                updated_orders.append(new_o)
                changed_orders.append(new_o)
            else:
                updated_orders.append(o)

        if changed_orders:
            state.update("orders", updated_orders)
            result["orders"] = changed_orders

        # ── Stamp customer updated_at ─────────────────────────────────────
        if "customers" in state.table_names:
            customers = state.get("customers")
            updated_customers = [
                {**c, "updated_at": event_ts}
                if c.get("customer_id") == customer_id and "updated_at" in c
                else c
                for c in customers
            ]
            changed_customers = [
                c
                for orig, c in zip(customers, updated_customers)
                if orig != c
            ]
            if changed_customers:
                state.update("customers", updated_customers)
                result["customers"] = changed_customers

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

    def apply(
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
            {
                **c,
                "address": new_address,
                "city": new_city,
                **({"updated_at": event_ts} if "updated_at" in c else {}),
            }
            if c.get("customer_id") == customer_id
            else c
            for c in customers
        ]
        changed = [c for orig, c in zip(customers, updated) if orig != c]
        if changed:
            state.update("customers", updated)
        return {"customers": changed}

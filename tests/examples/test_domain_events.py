"""
tests/examples/test_domain_events.py
=====================================
Unit tests for the domain-event infrastructure and concrete domain events.

Covers:
- AggregateState (get, update, table_names, snapshot, error paths)
- rows_to_sql / _sql_literal utilities
- EventRepository (register, emit, snapshot, as_source, error paths)
- CustomerBulkCancelEvent, CustomerAddressChangedEvent
- ProductStockOutEvent
- UserPlanUpgradedEvent, UserChurnedEvent
"""
from __future__ import annotations

import pytest

from sqldim.application.datasets.events import (
    AggregateState,
    EventRepository,
    rows_to_sql,
    _sql_literal,
)


# ── Helpers ───────────────────────────────────────────────────────────────────


class _FakeSource:
    """Minimal stand-in for BaseSource — returns a static SQLSource."""

    def __init__(self, rows: list[dict]):
        from sqldim.sources import SQLSource
        self._sql = SQLSource(rows_to_sql(rows))

    def snapshot(self):
        return self._sql


# ── AggregateState ────────────────────────────────────────────────────────────


class TestAggregateState:
    def test_get_returns_copy(self):
        state = AggregateState({"users": [{"id": 1}]})
        copy = state.get("users")
        assert copy == [{"id": 1}]
        # mutating the returned copy must not affect the stored state
        copy.append({"id": 2})
        assert state.get("users") == [{"id": 1}]

    def test_get_unknown_table_raises_key_error(self):
        state = AggregateState({"orders": []})
        with pytest.raises(KeyError, match="'x' not in aggregate state"):
            state.get("x")

    def test_update_replaces_rows(self):
        state = AggregateState({"orders": [{"id": 1}]})
        state.update("orders", [{"id": 2}, {"id": 3}])
        assert state.get("orders") == [{"id": 2}, {"id": 3}]

    def test_update_unknown_table_raises_key_error(self):
        state = AggregateState({"orders": []})
        with pytest.raises(KeyError, match="'x' not in aggregate state"):
            state.update("x", [])

    def test_table_names_sorted(self):
        state = AggregateState({"beta": [], "alpha": []})
        assert state.table_names == ["alpha", "beta"]

    def test_snapshot_returns_deep_copy(self):
        state = AggregateState({"t": [{"v": 1}]})
        snap = state.snapshot()
        assert snap == {"t": [{"v": 1}]}
        # mutating snapshot must not affect stored state
        snap["t"].clear()
        assert state.get("t") == [{"v": 1}]


# ── rows_to_sql / _sql_literal ────────────────────────────────────────────────


class TestRowsToSql:
    def test_empty_list_returns_empty_sentinel(self):
        assert rows_to_sql([]) == "SELECT 1 WHERE 1 = 0"

    def test_single_row_produces_select(self):
        sql = rows_to_sql([{"id": 1, "name": "Alice"}])
        assert "SELECT" in sql
        assert "Alice" in sql
        assert "AS id" in sql or "AS name" in sql

    def test_multiple_rows_joined_with_union_all(self):
        sql = rows_to_sql([{"x": 1}, {"x": 2}])
        assert "UNION ALL" in sql


class TestSqlLiteral:
    def test_none_maps_to_null(self):
        assert _sql_literal(None) == "NULL"

    def test_true_maps_to_true(self):
        assert _sql_literal(True) == "TRUE"

    def test_false_maps_to_false(self):
        assert _sql_literal(False) == "FALSE"

    def test_int(self):
        assert _sql_literal(7) == "7"

    def test_float(self):
        assert _sql_literal(1.5) == "1.5"

    def test_plain_string_quoted(self):
        assert _sql_literal("hello") == "'hello'"

    def test_string_with_single_quote_escaped(self):
        assert _sql_literal("it's") == "'it''s'"


# ── EventRepository ───────────────────────────────────────────────────────────


class TestEventRepository:
    """Test EventRepository with a trivial concrete DomainEvent."""

    def _simple_repo(self) -> EventRepository:
        from sqldim.application.datasets.events import DomainEvent

        class BumpEvent(DomainEvent):
            name = "bump"

            def apply(self, state: AggregateState, **kw):
                rows = state.get("items")
                bumped = [{**r, "v": r["v"] + 1} for r in rows]
                state.update("items", bumped)
                return {"items": bumped}

        return (
            EventRepository({"items": _FakeSource([{"v": 10}, {"v": 20}])})
            .register(BumpEvent())
        )

    def test_init_materialises_tables(self):
        repo = self._simple_repo()
        snap = repo.snapshot()
        assert "items" in snap
        assert len(snap["items"]) == 2

    def test_register_and_emit_event(self):
        repo = self._simple_repo()
        changes = repo.emit("bump")
        assert changes == {"items": [{"v": 11}, {"v": 21}]}

    def test_emit_updates_state_in_place(self):
        repo = self._simple_repo()
        repo.emit("bump")
        repo.emit("bump")
        snap = repo.snapshot()
        assert snap["items"] == [{"v": 12}, {"v": 22}]

    def test_emit_unknown_event_raises_key_error(self):
        repo = self._simple_repo()
        with pytest.raises(KeyError, match="No event 'missing'"):
            repo.emit("missing")

    def test_snapshot_is_copy(self):
        repo = self._simple_repo()
        snap = repo.snapshot()
        snap["items"].clear()
        assert len(repo.snapshot()["items"]) == 2

    def test_as_source_returns_sql_source(self):
        from sqldim.sources import SQLSource

        repo = self._simple_repo()
        src = repo.as_source("items")
        assert isinstance(src, SQLSource)


# ── CustomerBulkCancelEvent ───────────────────────────────────────────────────


class TestCustomerBulkCancelEvent:
    def _make_state(self, orders, customers=None):
        tables = {"orders": orders}
        if customers is not None:
            tables["customers"] = customers
        return AggregateState(tables)

    def _event(self):
        from sqldim.application.datasets.domains.ecommerce.events.customers import (
            CustomerBulkCancelEvent,
        )
        return CustomerBulkCancelEvent()

    def test_cancels_placed_orders_with_status_field(self):
        state = self._make_state([
            {"order_id": 1, "customer_id": 42, "status": "placed"},
            {"order_id": 2, "customer_id": 42, "status": "shipped"},
            {"order_id": 3, "customer_id": 99, "status": "placed"},
        ])
        changes = self._event().apply(state, customer_id=42)
        assert len(changes["orders"]) == 1
        assert changes["orders"][0]["status"] == "cancelled"
        # shipped order unchanged; other customer's order unchanged
        remaining = state.get("orders")
        shipped = [o for o in remaining if o["order_id"] == 2]
        assert shipped[0]["status"] == "shipped"

    def test_cancels_placed_orders_accumulating_model(self):
        """Handles order model without status field (placed_at with null paid_at)."""
        state = self._make_state([
            {"order_id": 1, "customer_id": 5, "placed_at": "2024-01-01", "paid_at": None},
            {"order_id": 2, "customer_id": 5, "placed_at": "2024-01-02", "paid_at": "2024-01-03"},
        ])
        changes = self._event().apply(state, customer_id=5)
        assert len(changes["orders"]) == 1
        assert changes["orders"][0]["_cancelled"] is True

    def test_no_changes_when_no_matching_orders(self):
        state = self._make_state([
            {"order_id": 1, "customer_id": 99, "status": "placed"},
        ])
        changes = self._event().apply(state, customer_id=42)
        assert "orders" not in changes

    def test_stamps_customer_updated_at(self):
        state = self._make_state(
            orders=[{"order_id": 1, "customer_id": 1, "status": "placed"}],
            customers=[{"customer_id": 1, "updated_at": "2024-01-01"}],
        )
        changes = self._event().apply(state, customer_id=1, event_ts="2024-06-01 09:00:00")
        assert "customers" in changes
        assert changes["customers"][0]["updated_at"] == "2024-06-01 09:00:00"


# ── CustomerAddressChangedEvent ───────────────────────────────────────────────


class TestCustomerAddressChangedEvent:
    def _event(self):
        from sqldim.application.datasets.domains.ecommerce.events.customers import (
            CustomerAddressChangedEvent,
        )
        return CustomerAddressChangedEvent()

    def test_updates_address_fields(self):
        state = AggregateState({
            "customers": [
                {"customer_id": 1, "address": "Old St", "city": "Old City", "updated_at": "2024-01-01"},
                {"customer_id": 2, "address": "Other St", "city": "Other City", "updated_at": "2024-01-01"},
            ]
        })
        changes = self._event().apply(
            state, customer_id=1, new_address="New St", new_city="New City"
        )
        assert len(changes["customers"]) == 1
        assert changes["customers"][0]["address"] == "New St"
        assert changes["customers"][0]["city"] == "New City"
        # other customer unchanged
        other = state.get("customers")[1]
        assert other["address"] == "Other St"

    def test_no_changes_for_unknown_customer(self):
        state = AggregateState({
            "customers": [{"customer_id": 1, "address": "A", "city": "B"}]
        })
        changes = self._event().apply(state, customer_id=99, new_address="X", new_city="Y")
        assert changes["customers"] == []


# ── ProductStockOutEvent ──────────────────────────────────────────────────────


class TestProductStockOutEvent:
    def _event(self):
        from sqldim.application.datasets.domains.ecommerce.events.products import (
            ProductStockOutEvent,
        )
        return ProductStockOutEvent()

    def test_marks_product_inactive(self):
        state = AggregateState({
            "products": [
                {"product_id": 7, "stock_qty": 100, "is_active": True, "updated_at": "2024-01-01"},
            ]
        })
        changes = self._event().apply(state, product_id=7)
        assert changes["products"][0]["stock_qty"] == 0
        assert changes["products"][0]["is_active"] is False

    def test_cancels_placed_orders_for_product(self):
        state = AggregateState({
            "products": [{"product_id": 3, "stock_qty": 5, "is_active": True}],
            "orders": [
                {"order_id": 1, "product_id": 3, "status": "placed"},
                {"order_id": 2, "product_id": 3, "status": "shipped"},
                {"order_id": 3, "product_id": 99, "status": "placed"},
            ],
        })
        changes = self._event().apply(state, product_id=3)
        assert len(changes["orders"]) == 1
        assert changes["orders"][0]["status"] == "cancelled"

    def test_no_order_changes_when_no_orders_table(self):
        state = AggregateState({
            "products": [{"product_id": 1, "stock_qty": 5, "is_active": True}],
        })
        changes = self._event().apply(state, product_id=1)
        assert "orders" not in changes

    def test_no_order_changes_when_orders_have_no_product_id(self):
        """Orders without product_id field are skipped."""
        state = AggregateState({
            "products": [{"product_id": 1, "stock_qty": 5, "is_active": True}],
            "orders": [{"order_id": 1, "status": "placed"}],
        })
        changes = self._event().apply(state, product_id=1)
        assert "orders" not in changes


# ── UserPlanUpgradedEvent ─────────────────────────────────────────────────────


class TestUserPlanUpgradedEvent:
    def _event(self):
        from sqldim.application.datasets.domains.saas_growth.events.users import (
            UserPlanUpgradedEvent,
        )
        return UserPlanUpgradedEvent()

    def test_upgrades_plan_tier(self):
        state = AggregateState({
            "saas_users": [
                {"user_id": 1, "plan_tier": "free"},
                {"user_id": 2, "plan_tier": "free"},
            ]
        })
        changes = self._event().apply(state, user_id=1, new_tier="pro")
        assert changes["saas_users"][0]["plan_tier"] == "pro"
        # user 2 untouched
        assert state.get("saas_users")[1]["plan_tier"] == "free"

    def test_enterprise_referral_adds_referral_log_row(self):
        state = AggregateState({
            "saas_users": [
                {"user_id": 5, "plan_tier": "pro", "acq_source": "referral"},
            ],
            "referral_log": [],
        })
        changes = self._event().apply(state, user_id=5, new_tier="enterprise")
        assert "referral_log" in changes
        assert changes["referral_log"][0]["event"] == "enterprise_upgrade_bonus"
        assert changes["referral_log"][0]["credit"] == 50

    def test_enterprise_non_referral_no_log_row(self):
        state = AggregateState({
            "saas_users": [
                {"user_id": 5, "plan_tier": "pro", "acq_source": "organic"},
            ],
            "referral_log": [],
        })
        changes = self._event().apply(state, user_id=5, new_tier="enterprise")
        assert "referral_log" not in changes

    def test_no_matching_user(self):
        state = AggregateState({"saas_users": [{"user_id": 99, "plan_tier": "free"}]})
        changes = self._event().apply(state, user_id=1, new_tier="pro")
        assert changes["saas_users"] == []


# ── UserChurnedEvent ──────────────────────────────────────────────────────────


class TestUserChurnedEvent:
    def _event(self):
        from sqldim.application.datasets.domains.saas_growth.events.users import (
            UserChurnedEvent,
        )
        return UserChurnedEvent()

    def test_sets_plan_tier_to_churned(self):
        state = AggregateState({
            "saas_users": [{"user_id": 3, "plan_tier": "pro"}]
        })
        changes = self._event().apply(state, user_id=3)
        assert changes["saas_users"][0]["plan_tier"] == "churned"

    def test_appends_to_churn_log_when_present(self):
        state = AggregateState({
            "saas_users": [{"user_id": 3, "plan_tier": "pro"}],
            "churn_log": [],
        })
        changes = self._event().apply(state, user_id=3, reason="price")
        assert "churn_log" in changes
        assert changes["churn_log"][0]["reason"] == "price"
        assert changes["churn_log"][0]["plan_tier_before"] == "pro"

    def test_no_churn_log_without_table(self):
        state = AggregateState({"saas_users": [{"user_id": 3, "plan_tier": "pro"}]})
        changes = self._event().apply(state, user_id=3)
        assert "churn_log" not in changes

    def test_no_matching_user(self):
        state = AggregateState({"saas_users": [{"user_id": 99, "plan_tier": "pro"}]})
        changes = self._event().apply(state, user_id=1)
        assert changes["saas_users"] == []

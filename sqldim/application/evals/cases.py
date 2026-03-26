"""EvalCase registry — one typed case per dataset/question pair (§11.10 evals).

Each ``EvalCase`` captures:
- the natural-language utterance
- which dataset to run it against
- what constitutes a *passing* result (columns present, rows returned, etc.)
- semantic tags for grouping reports

The ``EVAL_SUITE`` list is the authoritative registry consumed by
:class:`~sqldim.evals.runner.EvalRunner`.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class EvalCase:
    """A single NL interface evaluation case.

    Parameters
    ----------
    id:
        Unique slug, e.g. ``"ecommerce.01.customers_by_tier"``.
    dataset:
        Dataset name as accepted by ``sqldim ask`` (e.g. ``"ecommerce"``).
    utterance:
        Natural-language question posed to the NL interface.
    expect_result:
        When ``True`` the case expects at least one result row.
    expect_columns:
        Subset of column names that must appear in the result (case-insensitive).
        A case with an empty list only checks that a query ran (no column assertion).
    expect_table:
        If set, the generated SQL must reference this table name.
    max_hops:
        Maximum number of graph node hops considered acceptable.
        The happy path is 8 hops (entity_resolution → … → explanation_rendering).
        Queries with clarification loops may use up to 11.
    tags:
        Free-form labels for filtering reports (e.g. ``["entity", "filter"]``).
    """

    id: str
    dataset: str
    utterance: str
    expect_result: bool = True
    expect_columns: list[str] = field(default_factory=list)
    expect_table: str | None = None
    max_hops: int = 11
    tags: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# ecommerce — customers / products / stores / orders
# ---------------------------------------------------------------------------
# Tables:
#   customers  : customer_id, email, full_name, loyalty_tier, country_code,
#                acquisition_channel, valid_from, valid_to, is_current, checksum
#   products   : product_id, sku, product_name, category, unit_price,
#                stock_units, valid_from, valid_to, is_current, checksum
#   stores     : store_id, store_name, phone, email, city, state,
#                valid_from, valid_to, is_current, checksum
#   orders     : order_id, customer_id, product_id, quantity,
#                unit_price_at_order, placed_at, valid_from, valid_to,
#                is_current, checksum

_ECOMMERCE: list[EvalCase] = [
    EvalCase(
        id="ecommerce.01.customers",
        dataset="ecommerce",
        utterance="show me all customers",
        expect_result=True,
        expect_columns=["full_name", "loyalty_tier"],
        expect_table="customers",
        tags=["entity"],
    ),
    EvalCase(
        id="ecommerce.02.customers_by_country",
        dataset="ecommerce",
        utterance="which customers are from the US?",
        expect_result=True,
        expect_columns=["full_name", "country_code"],
        expect_table="customers",
        tags=["entity", "filter"],
    ),
    EvalCase(
        id="ecommerce.03.loyalty_tiers",
        dataset="ecommerce",
        utterance="list customers grouped by loyalty tier",
        expect_result=True,
        expect_columns=["loyalty_tier"],
        expect_table="customers",
        tags=["entity", "aggregation"],
    ),
    EvalCase(
        id="ecommerce.04.products",
        dataset="ecommerce",
        utterance="show me all products",
        expect_result=True,
        expect_columns=["product_name", "category"],
        expect_table="products",
        tags=["entity"],
    ),
    EvalCase(
        id="ecommerce.05.product_prices",
        dataset="ecommerce",
        utterance="what are the unit prices of all products?",
        expect_result=True,
        expect_columns=["product_name", "unit_price"],
        expect_table="products",
        tags=["entity", "filter"],
    ),
    EvalCase(
        id="ecommerce.06.orders",
        dataset="ecommerce",
        utterance="show me all orders",
        expect_result=True,
        expect_columns=["order_id", "quantity"],
        expect_table="orders",
        tags=["entity"],
    ),
    EvalCase(
        id="ecommerce.07.orders_quantity",
        dataset="ecommerce",
        utterance="what is the quantity and unit price for each order?",
        expect_result=True,
        expect_columns=["quantity", "unit_price_at_order"],
        expect_table="orders",
        tags=["entity", "filter"],
    ),
    EvalCase(
        id="ecommerce.08.stores",
        dataset="ecommerce",
        utterance="list all stores with their state",
        expect_result=True,
        expect_columns=["store_name", "state"],
        expect_table="stores",
        tags=["entity"],
    ),
]

# ---------------------------------------------------------------------------
# fintech — accounts / counterparties / transactions
# ---------------------------------------------------------------------------
# Tables:
#   accounts       : account_id, account_number, account_type, risk_tier,
#                    currency, owner_name, valid_from, valid_to, is_current, checksum
#   counterparties : cp_id, bic_code, institution_name, country_code,
#                    is_sanctioned, valid_from, valid_to, is_current, checksum
#   transactions   : txn_id, account_id, counterparty_id, txn_date, amount_usd,
#                    txn_type, channel, valid_from, valid_to, is_current, checksum

_FINTECH: list[EvalCase] = [
    EvalCase(
        id="fintech.01.accounts",
        dataset="fintech",
        utterance="show me all accounts",
        expect_result=True,
        expect_columns=["account_type", "risk_tier"],
        expect_table="accounts",
        tags=["entity"],
    ),
    EvalCase(
        id="fintech.02.high_risk_accounts",
        dataset="fintech",
        utterance="which accounts have high risk tier?",
        expect_result=True,
        expect_columns=["owner_name", "risk_tier"],
        expect_table="accounts",
        tags=["entity", "filter"],
    ),
    EvalCase(
        id="fintech.03.transactions",
        dataset="fintech",
        utterance="list all transactions",
        expect_result=True,
        expect_columns=["txn_id", "amount_usd"],
        expect_table="transactions",
        tags=["entity"],
    ),
    EvalCase(
        id="fintech.04.transaction_amounts",
        dataset="fintech",
        utterance="show transaction amounts and types",
        expect_result=True,
        expect_columns=["amount_usd", "txn_type"],
        expect_table="transactions",
        tags=["entity", "filter"],
    ),
    EvalCase(
        id="fintech.05.counterparties",
        dataset="fintech",
        utterance="show me all counterparties",
        expect_result=True,
        expect_columns=["institution_name", "country_code"],
        expect_table="counterparties",
        tags=["entity"],
    ),
    EvalCase(
        id="fintech.06.sanctioned_counterparties",
        dataset="fintech",
        utterance="which counterparties are sanctioned?",
        expect_result=True,
        expect_columns=["institution_name", "is_sanctioned"],
        expect_table="counterparties",
        tags=["entity", "filter"],
    ),
]

# ---------------------------------------------------------------------------
# saas_growth — saas_users / saas_sessions
# ---------------------------------------------------------------------------
# Tables:
#   saas_users    : user_id, email, plan_tier, acq_source, device, signup_date,
#                   valid_from, valid_to, is_current, checksum
#   saas_sessions : session_id, user_id, event_date, event_type,
#                   valid_from, valid_to, is_current, checksum

_SAAS_GROWTH: list[EvalCase] = [
    EvalCase(
        id="saas_growth.01.users",
        dataset="saas_growth",
        utterance="show me all users",
        expect_result=True,
        expect_columns=["email", "plan_tier"],
        expect_table="saas_users",
        tags=["entity"],
    ),
    EvalCase(
        id="saas_growth.02.plan_distribution",
        dataset="saas_growth",
        utterance="list users by plan tier",
        expect_result=True,
        expect_columns=["plan_tier"],
        expect_table="saas_users",
        tags=["entity", "aggregation"],
    ),
    EvalCase(
        id="saas_growth.03.acquisition_source",
        dataset="saas_growth",
        utterance="how did users find us? show acquisition source",
        expect_result=True,
        expect_columns=["acq_source"],
        expect_table="saas_users",
        tags=["entity", "filter"],
    ),
    EvalCase(
        id="saas_growth.04.sessions",
        dataset="saas_growth",
        utterance="show me all sessions",
        expect_result=True,
        expect_columns=["session_id", "event_type"],
        expect_table="saas_sessions",
        tags=["entity"],
    ),
    EvalCase(
        id="saas_growth.05.session_events",
        dataset="saas_growth",
        utterance="what event types are recorded in sessions?",
        expect_result=True,
        expect_columns=["event_type"],
        expect_table="saas_sessions",
        tags=["entity", "filter"],
    ),
]

# ---------------------------------------------------------------------------
# user_activity — devices / page_events
# ---------------------------------------------------------------------------
# Tables:
#   devices     : device_id, browser_type, os_type, device_type,
#                 valid_from, valid_to, is_current, checksum
#   page_events : event_id, url, referrer, user_id, device_id, host,
#                 event_time, valid_from, valid_to, is_current, checksum

_USER_ACTIVITY: list[EvalCase] = [
    EvalCase(
        id="user_activity.01.devices",
        dataset="user_activity",
        utterance="show me all devices",
        expect_result=True,
        expect_columns=["browser_type", "os_type"],
        expect_table="devices",
        tags=["entity"],
    ),
    EvalCase(
        id="user_activity.02.device_types",
        dataset="user_activity",
        utterance="list devices grouped by device type",
        expect_result=True,
        expect_columns=["device_type"],
        expect_table="devices",
        tags=["entity", "aggregation"],
    ),
    EvalCase(
        id="user_activity.03.page_events",
        dataset="user_activity",
        utterance="show me all page events",
        expect_result=True,
        expect_columns=["url", "event_time"],
        expect_table="page_events",
        tags=["entity"],
    ),
    EvalCase(
        id="user_activity.04.event_urls",
        dataset="user_activity",
        utterance="what URLs were visited?",
        expect_result=True,
        expect_columns=["url"],
        expect_table="page_events",
        tags=["entity", "filter"],
    ),
    EvalCase(
        id="user_activity.05.referrers",
        dataset="user_activity",
        utterance="show page events with referrer information",
        expect_result=True,
        expect_columns=["url", "referrer"],
        expect_table="page_events",
        tags=["entity"],
    ),
]

# ---------------------------------------------------------------------------
# Master registry
# ---------------------------------------------------------------------------

#: All eval cases across all datasets. Consumed by :class:`EvalRunner`.
EVAL_SUITE: list[EvalCase] = _ECOMMERCE + _FINTECH + _SAAS_GROWTH + _USER_ACTIVITY

__all__ = ["EvalCase", "EVAL_SUITE"]

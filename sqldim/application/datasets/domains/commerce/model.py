"""The DGM model the commerce dataset declares (§2.6).

Written from ``sources/``.  The dataset joins three tiers that share no key:

* OLTP: ``customers``, ``products`` and ``stores`` are dimensions; each
  ``orders`` row is one order (``order_id``, unique) that names its customer
  (``customer_id``) and its product (``product_id``).  ``stores`` is referenced
  by no table;
* gold analytics: ``dim_customer`` is a dimension of its own customers (ids
  1–200, unrelated to ``customers``).  ``fct_daily_sales`` holds one row per
  day and segment; ``segment`` is a value of ``dim_customer.segment``, not a
  key, so it is a property.  ``fct_cohort_retention`` holds one row per weekly
  cohort with its D1/D7/D30 milestones.  Neither fact has a foreign key;
* supply chain: ``suppliers``, ``warehouses`` and ``skus`` are dimensions;
  each ``receipts`` row is one goods receipt naming all three.

Grain (§2.4): an order and a receipt are events.  ``fct_daily_sales`` is one
row per segment per day, so PERIOD, which by default forbids a SUM across
days.  ``fct_cohort_retention`` is one lifecycle row per cohort whose
milestone counts fill in as D1, D7 and D30 pass, so ACCUMULATING.  No
dimension holds verb edge types into two fact types, so nothing is
Conformed; there is no bridge edge type.

Additivity (§2.4): quantities, revenue and customer counts are flows,
ADDITIVE; unit prices and costs are NON_ADDITIVE.  ``returning_customers``
counts the distinct returning customers of one day, so it sums across
segments but not across days: SEMI_ADDITIVE over TIME.
"""

from __future__ import annotations

from sqldim.application.datasets.model_decl import (
    ASSESSED_KINDS,
    measures,
    BOOLEAN,
    DATE,
    INTEGER,
    REAL,
    STRING,
    TIMESTAMP,
    table_node,
    verb_fk,
)
from sqldim.core.query.dgm.annotations import Grain, GrainKind
from sqldim.core.query.dgm.model import DGMModel, NodeKind, NodeType

__all__ = ["build_model"]


def _oltp() -> tuple[NodeType, ...]:
    return (
        table_node(
            "Customer",
            NodeKind.DIM,
            "customers",
            ("customer_id",),
            {
                "email": STRING,
                "full_name": STRING,
                "loyalty_tier": STRING,
                "country_code": STRING,
                "acquisition_channel": STRING,
            },
        ),
        table_node(
            "Product",
            NodeKind.DIM,
            "products",
            ("product_id",),
            {
                "sku": STRING,
                "product_name": STRING,
                "category": STRING,
                "unit_price": REAL,
                "stock_units": INTEGER,
            },
        ),
        table_node(
            "Store",
            NodeKind.DIM,
            "stores",
            ("store_id",),
            {
                "store_name": STRING,
                "phone": STRING,
                "email": STRING,
                "city": STRING,
                "state": STRING,
            },
        ),
        table_node(
            "Order",
            NodeKind.FACT,
            "orders",
            ("order_id",),
            {
                "quantity": INTEGER,
                "unit_price_at_order": REAL,
                "placed_at": TIMESTAMP,
            },
            event_time="placed_at",
        ),
    )


def _gold() -> tuple[NodeType, ...]:
    return (
        table_node(
            "RetailCustomer",
            NodeKind.DIM,
            "dim_customer",
            ("customer_id",),
            {"full_name": STRING, "segment": STRING, "region": STRING},
        ),
        table_node(
            "DailySales",
            NodeKind.FACT,
            "fct_daily_sales",
            ("date_ref", "segment"),
            {
                "date_ref": DATE,
                "segment": STRING,
                "total_orders": INTEGER,
                "total_revenue": REAL,
                "new_customers": INTEGER,
                "returning_customers": INTEGER,
            },
            event_time="date_ref",
        ),
        table_node(
            "CohortRetention",
            NodeKind.FACT,
            "fct_cohort_retention",
            ("cohort_date",),
            {
                "cohort_date": DATE,
                "cohort_size": INTEGER,
                "retained_d1": INTEGER,
                "retained_d7": INTEGER,
                "retained_d30": INTEGER,
            },
        ),
    )


def _supply_chain() -> tuple[NodeType, ...]:
    return (
        table_node(
            "Supplier",
            NodeKind.DIM,
            "suppliers",
            ("supplier_id",),
            {
                "supplier_code": STRING,
                "supplier_name": STRING,
                "country_code": STRING,
                "lead_time_days": INTEGER,
                "reliability_score": REAL,
            },
        ),
        table_node(
            "Warehouse",
            NodeKind.DIM,
            "warehouses",
            ("warehouse_id",),
            {
                "warehouse_code": STRING,
                "warehouse_name": STRING,
                "region": STRING,
                "max_capacity_units": INTEGER,
                "is_active": BOOLEAN,
            },
        ),
        table_node(
            "Sku",
            NodeKind.DIM,
            "skus",
            ("sku_id",),
            {
                "sku_code": STRING,
                "product_name": STRING,
                "category": STRING,
                "weight_kg": REAL,
                "hs_code": STRING,
            },
        ),
        table_node(
            "Receipt",
            NodeKind.FACT,
            "receipts",
            ("receipt_id",),
            {
                "receipt_date": STRING,  # an ISO date stored as VARCHAR
                "quantity_received": INTEGER,
                "unit_cost_usd": REAL,
            },
        ),
    )


def build_model() -> DGMModel:
    return DGMModel(
        node_types=(*_oltp(), *_gold(), *_supply_chain()),
        edge_types=(
            verb_fk("Customer", "Order", "placed", "customer_id"),
            verb_fk("Product", "Order", "ordered", "product_id"),
            verb_fk("Supplier", "Receipt", "supplied", "supplier_id"),
            verb_fk("Warehouse", "Receipt", "received", "warehouse_id"),
            verb_fk("Sku", "Receipt", "stocked", "sku_id"),
        ),
        sigma=(
            Grain("Order", GrainKind.EVENT),
            Grain("Receipt", GrainKind.EVENT),
            Grain("DailySales", GrainKind.PERIOD),
            Grain("CohortRetention", GrainKind.ACCUMULATING),
            *measures(
                "Order", additive=("quantity",), non_additive=("unit_price_at_order",)
            ),
            *measures(
                "Receipt",
                additive=("quantity_received",),
                non_additive=("unit_cost_usd",),
            ),
            # Distinct customers of one day: summable across segments, not days.
            *measures(
                "DailySales",
                additive=("total_orders", "total_revenue", "new_customers"),
                semi_over_time=("returning_customers",),
            ),
            *measures(
                "CohortRetention",
                additive=("cohort_size", "retained_d1", "retained_d7", "retained_d30"),
            ),
        ),
        sigma_kinds=ASSESSED_KINDS,
    )

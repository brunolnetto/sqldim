"""commerce domain Dataset — OLTP + gold analytics + supply-chain in one analytical space.

Composes all sources from the former **ecommerce** OLTP pipeline, the former
**retail** gold-layer analytics tier, and the former **supply_chain** domain
into a single :class:`Dataset`.

OLTP tables (ecommerce sources)
--------------------------------
customers       Loyalty customers: customer_id, email, full_name, loyalty_tier,
                country_code, acquisition_channel  (80 rows)
products        Catalog products:  product_id, sku, product_name, category,
                unit_price, stock_units  (40 rows)
stores          Physical stores:   store_id, store_name, phone, email,
                city, state  (US locations)
orders          Fulfillment orders: order_id, customer_id, product_id,
                quantity, unit_price_at_order, placed_at  (100 rows)

Gold analytics tables (retail sources)
----------------------------------------
dim_customer         Current customer dimension with segment & region  (200 rows)
fct_daily_sales      Day × segment grain with pre-aggregated KPIs      (90 × 3 rows)
fct_cohort_retention Weekly cohort J-curve retention mart               (12 cohorts)

Supply-chain tables
--------------------
suppliers       Supplier master: supplier_id, supplier_code, supplier_name,
                country_code, lead_time_days, reliability_score  (20 rows)
warehouses      Warehouse master: warehouse_id, warehouse_code, warehouse_name,
                region, max_capacity_units, is_active  (10 rows)
skus            SKU catalogue: sku_id, sku_code, product_name, category,
                weight_kg, hs_code  (50 rows)
receipts        Goods receipts: receipt_id, warehouse_id, supplier_id, sku_id,
                receipt_date, quantity_received, unit_cost_usd  (150 rows)

Usage::

    import duckdb
    from sqldim.application.datasets.domains.commerce.dataset import commerce_dataset

    con = duckdb.connect()
    commerce_dataset.setup(con)
    for table, rows in commerce_dataset.snapshots().items():
        print(f"{table}: {len(rows)} rows")
    commerce_dataset.teardown(con)
"""

from sqldim.application.datasets.dataset import Dataset
from sqldim.application.datasets.domains.commerce.sources import (
    CatalogProductsSource,
    DimCustomerSource,
    FctCohortRetentionSource,
    FctDailySalesSource,
    FulfillmentOrdersSource,
    LoyaltyCustomersSource,
    ReceiptsSource,
    SKUsSource,
    StoresSource,
    SuppliersSource,
    WarehousesSource,
)

commerce_dataset = Dataset(
    "commerce",
    [
        # ── OLTP layer (ecommerce) ─────────────────────────────────────
        (LoyaltyCustomersSource(), "customers"),
        (CatalogProductsSource(), "products"),
        (StoresSource(), "stores"),
        (FulfillmentOrdersSource(), "orders"),
        # ── Gold analytics layer (retail) ──────────────────────────────
        (DimCustomerSource(), "dim_customer"),
        (FctDailySalesSource(), "fct_daily_sales"),
        (FctCohortRetentionSource(), "fct_cohort_retention"),
        # ── Supply-chain OLTP layer ────────────────────────────────────
        (SuppliersSource(), "suppliers"),
        (WarehousesSource(), "warehouses"),
        (SKUsSource(), "skus"),
        (ReceiptsSource(), "receipts"),
    ],
)

DATASET_METADATA = {
    "name": "commerce",
    "title": "Commerce (OLTP, retail analytics & supply-chain)",
    "description": (
        "Combined OLTP + gold-layer + supply-chain: loyalty customers, catalog products, "
        "stores, fulfillment orders (OLTP); dim_customer, fct_daily_sales, and "
        "fct_cohort_retention (retail analytics); plus suppliers, warehouses, skus, "
        "and receipts (supply-chain)."
    ),
    "dataset_attr": "commerce_dataset",
}

__all__ = ["commerce_dataset", "DATASET_METADATA"]

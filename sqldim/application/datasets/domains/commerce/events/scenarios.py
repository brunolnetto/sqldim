"""Commerce domain — drift event façade.

Re-exports all OLTP mutation functions from their source modules so that
``artifacts/drift.json`` event_fn references resolve via this module.

OLTP mutations (ecommerce layer)
---------------------------------
apply_price_increase  — raise all product ``unit_price`` values (+20% default)
apply_stock_out       — zero out ``stock_units`` for a product category
apply_loyalty_upgrade — bulk-upgrade bronze→silver / silver→gold loyalty tiers

Medallion mutations (retail analytics layer)
---------------------------------------------
apply_segment_upgrade — upgrade raw_customers segments + rebuild silver/gold
apply_medallion_price_increase — reprice raw orders + products + rebuild pipeline

Supply-chain mutations
-----------------------
apply_supplier_disruption — halve reliability scores for a country's suppliers
apply_warehouse_closure   — deactivate all warehouses in a region
apply_cost_shock          — raise all receipt unit costs by a factor
"""

from sqldim.application.datasets.domains.commerce.events.products_drift import (
    apply_price_increase,
    apply_stock_out,
)
from sqldim.application.datasets.domains.commerce.events.customers_drift import (
    apply_loyalty_upgrade,
)
from sqldim.application.datasets.domains.commerce.events.segment_upgrade import (
    apply_segment_upgrade,
)
from sqldim.application.datasets.domains.commerce.events.orders_drift import (
    apply_price_increase as apply_medallion_price_increase,
)
from sqldim.application.datasets.domains.commerce.events.suppliers import (
    apply_supplier_disruption,
)
from sqldim.application.datasets.domains.commerce.events.warehouses import (
    apply_warehouse_closure,
)
from sqldim.application.datasets.domains.commerce.events.receipts import (
    apply_cost_shock,
)

__all__ = [
    "apply_price_increase",
    "apply_stock_out",
    "apply_loyalty_upgrade",
    "apply_segment_upgrade",
    "apply_medallion_price_increase",
    "apply_supplier_disruption",
    "apply_warehouse_closure",
    "apply_cost_shock",
]

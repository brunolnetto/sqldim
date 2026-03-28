"""Ecommerce domain — drift event façade.

Re-exports source-specific OLTP mutation functions from their
respective source modules so that ``artifacts/drift.json`` references
resolve unchanged.
"""

from sqldim.application.datasets.domains.ecommerce.events.products_drift import (
    apply_price_increase,
    apply_stock_out,
)
from sqldim.application.datasets.domains.ecommerce.events.customers_drift import (
    apply_loyalty_upgrade,
)

__all__ = ["apply_price_increase", "apply_loyalty_upgrade", "apply_stock_out"]

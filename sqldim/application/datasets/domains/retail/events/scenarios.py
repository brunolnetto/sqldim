"""Retail medallion domain — drift event façade.

Re-exports source-specific OLTP mutation functions from their
respective source modules so that ``artifacts/drift.json`` references
resolve unchanged.
"""

from sqldim.application.datasets.domains.retail.events.orders import (
    apply_price_increase,
)
from sqldim.application.datasets.domains.retail.events.customers import (
    apply_segment_upgrade,
)

__all__ = ["apply_price_increase", "apply_segment_upgrade"]

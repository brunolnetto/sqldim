"""Supply-chain domain — drift event façade.

Re-exports source-specific OLTP mutation functions from their
respective source modules so that ``artifacts/drift.json`` references
resolve unchanged.
"""

from sqldim.application.datasets.domains.supply_chain.events.suppliers import (
    apply_supplier_disruption,
)
from sqldim.application.datasets.domains.supply_chain.events.receipts import (
    apply_cost_shock,
)
from sqldim.application.datasets.domains.supply_chain.events.warehouses import (
    apply_warehouse_closure,
)

__all__ = [
    "apply_supplier_disruption",
    "apply_cost_shock",
    "apply_warehouse_closure",
]

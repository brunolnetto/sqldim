"""sources sub-package for the supply_chain domain — exposes all source classes."""

from sqldim.application.datasets.domains.supply_chain.sources.suppliers import (
    SuppliersSource,
)
from sqldim.application.datasets.domains.supply_chain.sources.warehouses import (
    WarehousesSource,
)
from sqldim.application.datasets.domains.supply_chain.sources.skus import SKUsSource
from sqldim.application.datasets.domains.supply_chain.sources.receipts import (
    ReceiptsSource,
)

__all__ = [
    "SuppliersSource",
    "WarehousesSource",
    "SKUsSource",
    "ReceiptsSource",
]

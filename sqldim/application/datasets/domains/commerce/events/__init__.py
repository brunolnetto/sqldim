"""commerce domain events — cross-table business rules."""

from sqldim.application.datasets.domains.commerce.events.customers import (
    CustomerBulkCancelEvent,
    CustomerAddressChangedEvent,
)
from sqldim.application.datasets.domains.commerce.events.products import (
    ProductStockOutEvent,
)

__all__ = [
    "CustomerBulkCancelEvent",
    "CustomerAddressChangedEvent",
    "ProductStockOutEvent",
]

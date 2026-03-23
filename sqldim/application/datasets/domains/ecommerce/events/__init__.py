"""ecommerce domain events — cross-table business rules."""
from sqldim.application.datasets.domains.ecommerce.events.customers import (
    CustomerBulkCancelEvent,
    CustomerAddressChangedEvent,
)
from sqldim.application.datasets.domains.ecommerce.events.products import ProductStockOutEvent

__all__ = [
    "CustomerBulkCancelEvent",
    "CustomerAddressChangedEvent",
    "ProductStockOutEvent",
]

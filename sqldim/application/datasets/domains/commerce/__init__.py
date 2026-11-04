"""commerce domain — OLTP + gold analytics combined dataset.

Re-exports the :data:`commerce_dataset` and its sources, events, and metadata
so downstream code can import from the package root::

    from sqldim.application.datasets.domains.commerce import (
        commerce_dataset,
        LoyaltyCustomersSource,
        CatalogProductsSource,
        FulfillmentOrdersSource,
        StoresSource,
        CustomersSource,
        ProductsSource,
        OrdersSource,
        CustomerBulkCancelEvent,
        CustomerAddressChangedEvent,
        ProductStockOutEvent,
    )
"""

from sqldim.application.datasets.domains.commerce.dataset import (
    DATASET_METADATA,
    commerce_dataset,
)
from sqldim.application.datasets.domains.commerce.sources import (
    CatalogProductsSource,
    CustomersSource,
    DimCustomerSource,
    FctCohortRetentionSource,
    FctDailySalesSource,
    FulfillmentOrdersSource,
    LoyaltyCustomersSource,
    OrdersSource,
    StoresSource,
    get_us_states,
    _PRODUCTS_SPEC,
    SuppliersSource,
    WarehousesSource,
    SKUsSource,
    ReceiptsSource,
)
from sqldim.application.datasets.domains.commerce.sources.orders import (
    _ORDERS_SPEC,
)
from sqldim.application.datasets.domains.commerce.events import (
    CustomerBulkCancelEvent,
    CustomerAddressChangedEvent,
    ProductStockOutEvent,
)

# Re-export ProductsSource separately to avoid import collision (also defined
# in sources.customers as a local spec reference)
from sqldim.application.datasets.domains.commerce.sources.products import (
    ProductsSource,
)

__all__ = [
    "commerce_dataset",
    "DATASET_METADATA",
    # OLTP sources
    "LoyaltyCustomersSource",
    "CatalogProductsSource",
    "FulfillmentOrdersSource",
    "StoresSource",
    "get_us_states",
    "CustomersSource",
    "ProductsSource",
    "_PRODUCTS_SPEC",
    "_ORDERS_SPEC",
    "OrdersSource",
    # Gold sources
    "DimCustomerSource",
    "FctDailySalesSource",
    "FctCohortRetentionSource",
    # Supply-chain sources
    "SuppliersSource",
    "WarehousesSource",
    "SKUsSource",
    "ReceiptsSource",
    # Cross-table events
    "CustomerBulkCancelEvent",
    "CustomerAddressChangedEvent",
    "ProductStockOutEvent",
]

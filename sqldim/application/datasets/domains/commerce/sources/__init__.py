"""commerce domain sources — all OLTP and gold-layer source classes."""

from sqldim.application.datasets.domains.commerce.sources.customers import (
    CustomersSource,
)
from sqldim.application.datasets.domains.commerce.sources.products import (
    ProductsSource,
    _PRODUCTS_SPEC,
)
from sqldim.application.datasets.domains.commerce.sources.stores import (
    StoresSource,
    get_us_states,
)
from sqldim.application.datasets.domains.commerce.sources.orders import (
    OrdersSource,
)
from sqldim.application.datasets.domains.commerce.sources.star import (
    LoyaltyCustomersSource,
    CatalogProductsSource,
    FulfillmentOrdersSource,
)
from sqldim.application.datasets.domains.commerce.sources.gold import (
    DimCustomerSource,
    FctDailySalesSource,
    FctCohortRetentionSource,
)
from sqldim.application.datasets.domains.commerce.sources.suppliers import (
    SuppliersSource,
)
from sqldim.application.datasets.domains.commerce.sources.warehouses import (
    WarehousesSource,
)
from sqldim.application.datasets.domains.commerce.sources.skus import SKUsSource
from sqldim.application.datasets.domains.commerce.sources.receipts import (
    ReceiptsSource,
)

# Note: CommerceMedallionPipelineSource is intentionally NOT imported here to
# avoid a circular import via _pipeline_sources.  Import it directly when needed:
#   from sqldim.application.datasets.domains.commerce.sources.medallion import (
#       CommerceMedallionPipelineSource,
#   )

__all__ = [
    "CustomersSource",
    "ProductsSource",
    "_PRODUCTS_SPEC",
    "StoresSource",
    "get_us_states",
    "OrdersSource",
    "LoyaltyCustomersSource",
    "CatalogProductsSource",
    "FulfillmentOrdersSource",
    "DimCustomerSource",
    "FctDailySalesSource",
    "FctCohortRetentionSource",
    "SuppliersSource",
    "WarehousesSource",
    "SKUsSource",
    "ReceiptsSource",
]

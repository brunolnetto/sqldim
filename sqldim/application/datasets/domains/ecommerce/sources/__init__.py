"""ecommerce domain — all OLTP source classes."""

from sqldim.application.datasets.domains.ecommerce.sources.customers import (
    CustomersSource,
)
from sqldim.application.datasets.domains.ecommerce.sources.products import (
    ProductsSource,
    _PRODUCTS_SPEC,
)
from sqldim.application.datasets.domains.ecommerce.sources.stores import (
    StoresSource,
    get_us_states,
)
from sqldim.application.datasets.domains.ecommerce.sources.orders import OrdersSource
from sqldim.application.datasets.domains.ecommerce.sources.star import (
    LoyaltyCustomersSource,
    CatalogProductsSource,
    FulfillmentOrdersSource,
)

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
]

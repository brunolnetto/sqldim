"""
ecommerce domain Dataset — FK-ordered collection of all OLTP sources.

Usage::

    import duckdb
    from sqldim.application.datasets.domains.ecommerce.dataset import ecommerce_dataset

    con = duckdb.connect()
    ecommerce_dataset.setup(con)
    for table, rows in ecommerce_dataset.snapshots().items():
        print(f"{table}: {len(rows)} rows")
    ecommerce_dataset.teardown(con)
"""

from sqldim.application.datasets.dataset import Dataset
from sqldim.application.datasets.domains.ecommerce.sources import (
    LoyaltyCustomersSource,
    CatalogProductsSource,
    StoresSource,
    FulfillmentOrdersSource,
)

ecommerce_dataset = Dataset(
    "ecommerce",
    [
        (LoyaltyCustomersSource(), "customers"),
        (CatalogProductsSource(), "products"),
        (StoresSource(), "stores"),
        (FulfillmentOrdersSource(), "orders"),
    ],
)

DATASET_METADATA = {
    "name": "ecommerce",
    "title": "E-commerce pipeline",
    "description": "Loyalty customers, catalog products, stores, and fulfilment orders",
    "dataset_attr": "ecommerce_dataset",
}

__all__ = ["ecommerce_dataset", "DATASET_METADATA"]

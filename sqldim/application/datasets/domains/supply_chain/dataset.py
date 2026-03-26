"""
supply_chain domain Dataset — FK-ordered collection of all OLTP sources.

Instantiate and use with a DuckDB connection:

    import duckdb
    from sqldim.application.datasets.domains.supply_chain.dataset import supply_chain_dataset

    con = duckdb.connect()
    supply_chain_dataset.setup(con)

    for table, rows in supply_chain_dataset.snapshots().items():
        print(f"{table}: {len(rows)} rows")

    supply_chain_dataset.teardown(con)
"""

from sqldim.application.datasets.dataset import Dataset
from sqldim.application.datasets.domains.supply_chain.sources import (
    SuppliersSource,
    WarehousesSource,
    SKUsSource,
    ReceiptsSource,
)

supply_chain_dataset = Dataset(
    "supply_chain",
    [
        (SuppliersSource(), "suppliers"),
        (WarehousesSource(), "warehouses"),
        (SKUsSource(), "skus"),
        (ReceiptsSource(), "receipts"),
    ],
)

DATASET_METADATA = {
    "name": "supply_chain",
    "title": "Supply-chain operations",
    "description": "Suppliers, warehouses, products, inventory, and shipments",
    "dataset_attr": "supply_chain_dataset",
}

__all__ = ["supply_chain_dataset", "DATASET_METADATA"]

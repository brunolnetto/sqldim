"""
hierarchy domain Dataset — FK-ordered collection of all OLTP sources.

Instantiate and use with a DuckDB connection:

    import duckdb
    from sqldim.application.datasets.domains.hierarchy.dataset import hierarchy_dataset

    con = duckdb.connect()
    hierarchy_dataset.setup(con)

    for table, rows in hierarchy_dataset.snapshots().items():
        print(f"{table}: {len(rows)} rows")

    hierarchy_dataset.teardown(con)
"""

from sqldim.application.datasets.dataset import Dataset
from sqldim.application.datasets.domains.hierarchy.sources import (
    OrgChartSource,
)

hierarchy_dataset = Dataset(
    "hierarchy",
    [
        (OrgChartSource(), "org_dim"),
    ],
)

DATASET_METADATA = {
    "name": "hierarchy",
    "title": "Hierarchy patterns",
    "description": "Organisations and employees with adjacency-list, closure-table, and materialised-path hierarchies",
    "dataset_attr": "hierarchy_dataset",
}

__all__ = ["hierarchy_dataset", "DATASET_METADATA"]

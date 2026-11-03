"""
data_models domain Dataset — DGM showcase + org-chart hierarchy.

Instantiate and use with a DuckDB connection:

    import duckdb
    from sqldim.application.datasets.domains.data_models.dataset import data_models_dataset

    con = duckdb.connect()
    data_models_dataset.setup(con)
    # tables: dgm_showcase_{customer,product,segment,sale,prod_seg},
    #         org_dim, sales_fact, org_dim_closure
    data_models_dataset.teardown(con)
"""

from sqldim.application.datasets.dataset import Dataset
from sqldim.application.datasets.domains.data_models.sources import (
    DGMShowcaseSource,
    OrgChartSource,
)

data_models_dataset = Dataset(
    "data_models",
    [
        (DGMShowcaseSource(), "dgm_showcase"),
        (OrgChartSource(), "org_dim"),
    ],
)

DATASET_METADATA = {
    "name": "data_models",
    "title": "Data modelling patterns (DGM + hierarchy)",
    "description": (
        "Combined dimensional graph model showcase and org-chart hierarchy dataset. "
        "DGM side: 5-table star schema (customers, products, segments, sales, bridge). "
        "Hierarchy side: adjacency-list org chart with closure table and sales facts."
    ),
    "dataset_attr": "data_models_dataset",
}

__all__ = ["data_models_dataset", "DATASET_METADATA"]

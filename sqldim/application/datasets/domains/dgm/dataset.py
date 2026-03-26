"""
dgm domain Dataset — FK-ordered collection of all OLTP sources.

Instantiate and use with a DuckDB connection:

    import duckdb
    from sqldim.application.datasets.domains.dgm.dataset import dgm_dataset

    con = duckdb.connect()
    dgm_dataset.setup(con)

    for table, rows in dgm_dataset.snapshots().items():
        print(f"{table}: {len(rows)} rows")

    dgm_dataset.teardown(con)
"""

from sqldim.application.datasets.dataset import Dataset
from sqldim.application.datasets.domains.dgm.sources import (
    DGMShowcaseSource,
)

dgm_dataset = Dataset(
    "dgm",
    [
        (DGMShowcaseSource(), "dgm_showcase"),
    ],
)

DATASET_METADATA = {
    "name": "dgm",
    "title": "DGM showcase",
    "description": "Dimensional graph model demonstration dataset with formal graph structures",
    "dataset_attr": "dgm_dataset",
}

__all__ = ["dgm_dataset", "DATASET_METADATA"]

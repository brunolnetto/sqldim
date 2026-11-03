"""data_models pipeline builder — DGM showcase + org-chart hierarchy."""

from __future__ import annotations

import duckdb

#: DGM showcase sub-tables (prefixed with alias "dgm_showcase").
DGM_TABLE_NAMES: list[str] = [
    "dgm_showcase_customer",
    "dgm_showcase_product",
    "dgm_showcase_segment",
    "dgm_showcase_sale",
    "dgm_showcase_prod_seg",
]

#: Hierarchy tables created by OrgChartSource.
HIERARCHY_TABLE_NAMES: list[str] = ["org_dim", "sales_fact", "org_dim_closure"]

#: All tables for the data_models domain.
TABLE_NAMES: list[str] = DGM_TABLE_NAMES + HIERARCHY_TABLE_NAMES


def build_pipeline(con: duckdb.DuckDBPyConnection, *, seed: int = 42) -> None:
    """Populate the **data_models** tables in *con*.

    Tables created: ``dgm_showcase_*`` (5 DGM tables) and
    ``org_dim``, ``sales_fact``, ``org_dim_closure`` (hierarchy).
    """
    from sqldim.application.datasets.domains.data_models.dataset import (
        data_models_dataset,
    )

    data_models_dataset.setup(con)

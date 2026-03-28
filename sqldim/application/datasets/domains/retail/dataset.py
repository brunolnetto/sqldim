"""retail domain Dataset — gold-layer analytics tables.

Usage::

    import duckdb
    from sqldim.application.datasets.domains.retail.dataset import retail_dataset

    con = duckdb.connect()
    retail_dataset.setup(con)
    # All three tables are fully populated after setup()
    retail_dataset.teardown(con)
"""

from sqldim.application.datasets.dataset import Dataset
from sqldim.application.datasets.domains.retail.sources.gold import (
    DimCustomerSource,
    FctDailySalesSource,
    FctCohortRetentionSource,
)

retail_dataset = Dataset(
    "retail",
    [
        (DimCustomerSource(), "dim_customer"),
        (FctDailySalesSource(), "fct_daily_sales"),
        (FctCohortRetentionSource(), "fct_cohort_retention"),
    ],
)

DATASET_METADATA = {
    "name": "retail",
    "title": "Retail gold-layer analytics",
    "description": (
        "Pre-materialized gold tables: customer dimension (200 customers, "
        "segments premium/standard/trial), daily sales OBT (90 days × 3 segments), "
        "and cohort retention mart (12 weekly cohorts with D1/D7/D30 retention)"
    ),
    "dataset_attr": "retail_dataset",
}

__all__ = ["retail_dataset", "DATASET_METADATA"]

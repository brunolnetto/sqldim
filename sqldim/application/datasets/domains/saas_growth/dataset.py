"""saas_growth domain Dataset."""

from sqldim.application.datasets.dataset import Dataset
from sqldim.application.datasets.domains.saas_growth.sources import (
    SaaSUsersSource,
    SaaSSessionsSource,
)

saas_growth_dataset = Dataset(
    "saas_growth",
    [
        (SaaSUsersSource(), "saas_users"),
        (SaaSSessionsSource(n=200), "saas_sessions"),
    ],
)

DATASET_METADATA = {
    "name": "saas_growth",
    "title": "SaaS growth metrics",
    "description": "Companies, tenants, subscriptions, events, and revenue metrics",
    "dataset_attr": "saas_growth_dataset",
}

__all__ = ["saas_growth_dataset", "DATASET_METADATA"]

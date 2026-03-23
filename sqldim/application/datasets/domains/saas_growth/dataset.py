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
        (SaaSSessionsSource(n=3_000), "saas_sessions"),
    ],
)

__all__ = ["saas_growth_dataset"]

__all__ = ["saas_growth_dataset"]

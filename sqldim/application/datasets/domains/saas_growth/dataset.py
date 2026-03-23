"""saas_growth domain Dataset."""
from sqldim.application.datasets.dataset import Dataset
from sqldim.application.datasets.domains.saas_growth.sources import SaaSUsersSource

saas_growth_dataset = Dataset(
    "saas_growth",
    [
        (SaaSUsersSource(), "saas_users"),
    ],
)

__all__ = ["saas_growth_dataset"]

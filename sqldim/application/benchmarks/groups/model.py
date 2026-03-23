"""
benchmarks/groups/model.py
==========================
Re-export shim for the **model** group — implementation split into subgroup modules:

* :mod:`model_dims`    — subgroup ``dims``    (J–K): prebuilt dimension
  generation (Date/Time) and graph traversal query builder
* :mod:`model_loaders` — subgroup ``loaders`` (L–M): Narwhals SCD2 backfill
  throughput and ORM/Medallion loader benchmarks
* :mod:`model_drift`   — subgroup ``drift``   (N):   schema/quality drift
  observability (DriftObservatory star schema)

CLI: ``model``, ``model.dims``, ``model.drift.saas_users``, …
"""
from sqldim.application.benchmarks.groups.model_dims import (
    group_j_dim_generation,
    group_k_graph_query,
)
from sqldim.application.benchmarks.groups.model_loaders import (
    group_l_narwhals_backfill,
    group_m_loaders_medallion,
)
from sqldim.application.benchmarks.groups.model_drift import (
    group_n_drift_observatory,
)

__all__ = [
    "group_j_dim_generation",
    "group_k_graph_query",
    "group_l_narwhals_backfill",
    "group_m_loaders_medallion",
    "group_n_drift_observatory",
]

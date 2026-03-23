"""
benchmarks/groups/dgm.py
========================
Re-export shim for the **dgm** group — implementation split into subgroup modules:

* :mod:`dgm_query` — subgroup ``query`` (O–P): DGM three-band query
  builder throughput and BDD predicate compilation
* :mod:`dgm_model` — subgroup ``model`` (Q–S): DGMRecommender annotation,
  DGMPlanner rule cycles, multi-target exporter throughput

CLI: ``dgm``, ``dgm.query``, ``dgm.model.planner``, …
"""
from sqldim.application.benchmarks.groups.dgm_query import (
    group_o_dgm_query,
    group_p_bdd_predicate,
)
from sqldim.application.benchmarks.groups.dgm_model import (
    group_q_recommender,
    group_r_planner,
    group_s_exporter,
)
from sqldim.application.benchmarks.groups.dgm_algebra import (
    group_t_question_algebra,
)

__all__ = [
    "group_o_dgm_query",
    "group_p_bdd_predicate",
    "group_q_recommender",
    "group_r_planner",
    "group_s_exporter",
    "group_t_question_algebra",
]

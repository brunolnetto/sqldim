"""
benchmarks.groups
==================
Benchmark group modules, split by domain:

* :mod:`benchmarks.groups.scd`   — SCD processing benchmarks (A–I)
* :mod:`benchmarks.groups.model` — Dimensional model infrastructure (J–N)
* :mod:`benchmarks.groups.dgm`   — DGM query algebra (O–S)
"""
from sqldim.application.benchmarks.groups.scd import (
    group_a_scan_regression,
    group_b_memory_safety,
    group_c_throughput_scaling,
    group_d_stream_vs_batch,
    group_e_change_rate_sensitivity,
    group_f_processor_comparison,
    group_g_beyond_memory,
    group_h_source_sink_matrix,
    group_i_scd_type_variety,
)
from sqldim.application.benchmarks.groups.model import (
    group_j_dim_generation,
    group_k_graph_query,
    group_l_narwhals_backfill,
    group_m_loaders_medallion,
    group_n_drift_observatory,
)
from sqldim.application.benchmarks.groups.dgm import (
    group_o_dgm_query,
    group_p_bdd_predicate,
    group_q_recommender,
    group_r_planner,
    group_s_exporter,
)

__all__ = [
    "group_a_scan_regression",
    "group_b_memory_safety",
    "group_c_throughput_scaling",
    "group_d_stream_vs_batch",
    "group_e_change_rate_sensitivity",
    "group_f_processor_comparison",
    "group_g_beyond_memory",
    "group_h_source_sink_matrix",
    "group_i_scd_type_variety",
    "group_j_dim_generation",
    "group_k_graph_query",
    "group_l_narwhals_backfill",
    "group_m_loaders_medallion",
    "group_n_drift_observatory",
    "group_o_dgm_query",
    "group_p_bdd_predicate",
    "group_q_recommender",
    "group_r_planner",
    "group_s_exporter",
]

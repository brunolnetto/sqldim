"""
benchmarks/suite.py
====================
Backward-compatible re-export hub for the sqldim benchmark suite.

All shared infrastructure lives in :mod:`benchmarks.infra`;
group functions are organised in :mod:`benchmarks.groups`:

* :mod:`benchmarks.groups.scd`   — Groups A–I  (SCD processing)
* :mod:`benchmarks.groups.model` — Groups J–N  (dimensional model / graph)
* :mod:`benchmarks.groups.dgm`   — Groups O–S  (DGM query algebra)

:mod:`benchmarks.runner` continues to import directly from this module.
"""
from sqldim.application.benchmarks.infra import (  # noqa: F401
    BenchmarkResult,
    SOURCE_NAMES,
    SINK_NAMES,
    _make_source,
    _remove_db,
    _configure,
    _run_scd2_batch,
    _run_metadata_batch,
)
from sqldim.application.benchmarks.groups import (  # noqa: F401
    group_a_scan_regression,
    group_b_memory_safety,
    group_c_throughput_scaling,
    group_d_stream_vs_batch,
    group_e_change_rate_sensitivity,
    group_f_processor_comparison,
    group_g_beyond_memory,
    group_h_source_sink_matrix,
    group_i_scd_type_variety,
    group_j_dim_generation,
    group_k_graph_query,
    group_l_narwhals_backfill,
    group_m_loaders_medallion,
    group_n_drift_observatory,
    group_o_dgm_query,
    group_p_bdd_predicate,
    group_q_recommender,
    group_r_planner,
    group_s_exporter,
)

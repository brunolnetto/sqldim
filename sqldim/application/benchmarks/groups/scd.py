"""
benchmarks/groups/scd.py
========================
Re-export shim for the **scd** group — implementation split into subgroup modules:

* :mod:`scd_regression` — subgroup ``regression`` (A–C): scan count,
  memory safety, throughput scaling
* :mod:`scd_stream`     — subgroup ``stream``     (D–H): stream vs batch,
  change rate, processor comparison, spill, source/sink matrix
* :mod:`scd_types`      — subgroup ``types``      (I):   SCD type variety

CLI: ``scd``, ``scd.regression``, ``scd.regression.products``, …
"""

from sqldim.application.benchmarks.groups.scd_regression import (
    group_a_scan_regression,
    group_b_memory_safety,
    group_c_throughput_scaling,
)
from sqldim.application.benchmarks.groups.scd_stream import (
    group_d_stream_vs_batch,
    group_e_change_rate_sensitivity,
    group_f_processor_comparison,
    group_g_beyond_memory,
    group_h_source_sink_matrix,
)
from sqldim.application.benchmarks.groups.scd_types import (
    group_i_scd_type_variety,
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
]

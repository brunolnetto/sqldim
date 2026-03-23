"""Temporary helper to split large benchmark group files."""
from __future__ import annotations

import pathlib

HERE = pathlib.Path(__file__).parent


def get_lines(filename):
    return (HERE / filename).read_text().splitlines(keepends=True)


def find_line_with(lines, text):
    for i, line in enumerate(lines):
        if text in line:
            return i
    return -1


def write(filename, content):
    (HERE / filename).write_text(content)
    print(f"  {filename}: {len(content.splitlines())} lines")


# ── scd.py ──────────────────────────────────────────────────────────────────

SCD_IMPORTS = """\
from __future__ import annotations

import os
import time
import traceback as _traceback

import duckdb
from sqlalchemy.pool import StaticPool

from sqldim.application.benchmarks._dataset import (
    BenchmarkDatasetGenerator,
    DatasetArtifact,
    SCALE_TIERS,
)
from sqldim.application.benchmarks.infra import (
    BenchmarkResult,
    SOURCE_NAMES,
    _make_source,
    _remove_db,
    _configure,
    _run_scd2_batch,
    _run_metadata_batch,
)
from sqldim.application.benchmarks.memory_probe import MemoryProbe
from sqldim.application.benchmarks.scan_probe import DuckDBObjectTracker

"""

scd_lines = get_lines("scd.py")
scd_a_start = find_line_with(scd_lines, "def group_a_scan_regression")
scd_d_start = find_line_with(scd_lines, "# ── Group D")
scd_i_start = find_line_with(scd_lines, "# ═══════")

print(f"scd boundaries: A={scd_a_start+1}, D={scd_d_start+1}, I={scd_i_start+1}")

# scd_regression.py — Groups A, B, C
write(
    "scd_regression.py",
    '"""SCD regression benchmarks (groups A–C): scan, memory safety, throughput."""\n'
    + SCD_IMPORTS
    + "".join(scd_lines[scd_a_start:scd_d_start]),
)

# scd_stream.py — Groups D, E, F, G, H
write(
    "scd_stream.py",
    '"""SCD streaming and processor benchmarks (groups D–H)."""\n'
    + SCD_IMPORTS
    + "".join(scd_lines[scd_d_start:scd_i_start]),
)

# scd_types.py — Group I
write(
    "scd_types.py",
    '"""SCD type variety benchmarks (group I): Type3, Type4, Type5, Type6."""\n'
    + SCD_IMPORTS
    + "".join(scd_lines[scd_i_start:]),
)


# ── model.py ────────────────────────────────────────────────────────────────

MODEL_IMPORTS = """\
from __future__ import annotations

import os
import time
import traceback as _traceback

import duckdb
from sqlalchemy.pool import StaticPool

from sqldim.application.benchmarks._dataset import (
    BenchmarkDatasetGenerator,
    DatasetArtifact,
    SCALE_TIERS,
)
from sqldim.application.benchmarks.infra import (
    BenchmarkResult,
    SOURCE_NAMES,
    _make_source,
    _remove_db,
    _configure,
    _run_scd2_batch,
    _run_metadata_batch,
)
from sqldim.application.benchmarks.memory_probe import MemoryProbe
from sqldim.application.benchmarks.scan_probe import DuckDBObjectTracker

"""

model_lines = get_lines("model.py")
model_j_start = find_line_with(model_lines, "def group_j_")
model_l_start = find_line_with(model_lines, "def group_l_")
model_n_start = find_line_with(model_lines, "def group_n_")

print(f"model boundaries: J={model_j_start+1}, L={model_l_start+1}, N={model_n_start+1}")

# model_dims.py — Groups J, K
write(
    "model_dims.py",
    '"""Model dimension benchmarks (groups J–K): date/time dims, geo dims."""\n'
    + MODEL_IMPORTS
    + "".join(model_lines[model_j_start:model_l_start]),
)

# model_loaders.py — Groups L, M
write(
    "model_loaders.py",
    '"""Model loader benchmarks (groups L–M): bulk loaders, incremental loaders."""\n'
    + MODEL_IMPORTS
    + "".join(model_lines[model_l_start:model_n_start]),
)

# model_drift.py — Group N
write(
    "model_drift.py",
    '"""Model drift benchmarks (group N): schema drift and type coercion."""\n'
    + MODEL_IMPORTS
    + "".join(model_lines[model_n_start:]),
)


# ── dgm.py ──────────────────────────────────────────────────────────────────

DGM_IMPORTS = """\
from __future__ import annotations

import os
import time
import traceback as _traceback

import duckdb
from sqlalchemy.pool import StaticPool

from sqldim.application.benchmarks._dataset import (
    BenchmarkDatasetGenerator,
    DatasetArtifact,
    SCALE_TIERS,
)
from sqldim.application.benchmarks.infra import (
    BenchmarkResult,
    SOURCE_NAMES,
    _make_source,
    _remove_db,
    _configure,
    _run_scd2_batch,
    _run_metadata_batch,
)
from sqldim.application.benchmarks.memory_probe import MemoryProbe
from sqldim.application.benchmarks.scan_probe import DuckDBObjectTracker

"""

dgm_lines = get_lines("dgm.py")
dgm_o_start = find_line_with(dgm_lines, "def group_o_")
dgm_q_start = find_line_with(dgm_lines, "def group_q_")

print(f"dgm boundaries: O={dgm_o_start+1}, Q={dgm_q_start+1}")

# dgm_query.py — Groups O, P
write(
    "dgm_query.py",
    '"""DGM query benchmarks (groups O–P): query patterns, aggregations."""\n'
    + DGM_IMPORTS
    + "".join(dgm_lines[dgm_o_start:dgm_q_start]),
)

# dgm_model.py — Groups Q, R, S
write(
    "dgm_model.py",
    '"""DGM model benchmarks (groups Q–S): model building, layering, resolution."""\n'
    + DGM_IMPORTS
    + "".join(dgm_lines[dgm_q_start:]),
)

print("Done.")

"""sqldim benchmark dataset generator — DDL resolution, DatasetArtifact, BenchmarkDatasetGenerator.

Profile metadata and DuckDB SQL generators live in _generators.py.
"""
from __future__ import annotations

import importlib
import os
import tempfile
from dataclasses import dataclass, field
from typing import Iterator

import duckdb

from sqldim.application.benchmarks._generators import _PROFILE_META, _SQL_GENERATORS


# ── Scale tiers ───────────────────────────────────────────────────────────

SCALE_TIERS = {
    "xs":  10_000,
    "s":   100_000,
    "m":   1_000_000,
    "l":   5_000_000,
    "xl":  20_000_000,
    "xxl": 60_000_000,
}

# Change rate per tier for event batches
CHANGE_RATES = {
    "xs":  0.30,
    "s":   0.20,
    "m":   0.15,
    "l":   0.10,
    "xl":  0.05,
    "xxl": 0.02,
}


# ── DDL resolver ──────────────────────────────────────────────────────────

def _resolve_ddl(profile: str) -> str:
    meta = _PROFILE_META[profile]
    if meta["source_class"] is None:
        return meta["ddl"]
    module_path, cls_name = meta["source_class"].rsplit(".", 1)
    mod = importlib.import_module(module_path)
    src_cls = getattr(mod, cls_name)
    return src_cls(n=1).DIM_DDL


_DDL_CACHE: dict[str, str] = {}


def _get_ddl(profile: str) -> str:
    if profile not in _DDL_CACHE:
        _DDL_CACHE[profile] = _resolve_ddl(profile)
    return _DDL_CACHE[profile]


# ── DatasetArtifact helpers ────────────────────────────────────────────────

def _remove_path(p: str) -> None:
    if p and os.path.exists(p):
        os.unlink(p)


def _remove_dir(d: str) -> None:
    if d and os.path.exists(d):
        try:
            os.rmdir(d)
        except OSError:
            pass


# ── DatasetArtifact ────────────────────────────────────────────────────────

@dataclass
class DatasetArtifact:
    """A generated dataset available as Parquet files on disk.

    Using Parquet means rescanning is cheap and does not keep the full
    dataset in Python memory — important at m/l/xl/xxl tiers.
    """
    profile:          str
    tier:             str
    n_rows:           int
    n_changed:        int
    snapshot_path:    str  = ""
    events_path:      str  = ""
    ddl:              str  = ""
    natural_key:      str  = ""
    track_columns:    list = field(default_factory=list)
    metadata_columns: list = field(default_factory=list)
    _temp_dir:        str  = field(default="", repr=False)

    def cleanup(self) -> None:
        for p in [self.snapshot_path, self.events_path]:
            _remove_path(p)
        _remove_dir(self._temp_dir)

    def __enter__(self) -> "DatasetArtifact":
        return self

    def __exit__(self, *_) -> None:
        self.cleanup()


# ── Generator ─────────────────────────────────────────────────────────────

class BenchmarkDatasetGenerator:
    """Generates benchmark datasets at scale via DuckDB ``generate_series``.

    Schema metadata (DDL, natural_key, track_columns) is derived from the
    canonical source classes in :mod:`sqldim.application.datasets` where an
    equivalent source exists.  ``cnpj_empresa`` uses an inline DDL because
    it models a Brazil-specific regulatory schema with no examples analogue.

    Usage::

        with BenchmarkDatasetGenerator() as gen:
            with gen.generate("products", tier="s") as ds:
                proc.process(ParquetSource(ds.snapshot_path), "dim_product")
    """

    def __init__(self, tmp_root: str | None = None):
        self._tmp_root = tmp_root or tempfile.gettempdir()
        self._con = duckdb.connect()

    def generate(
        self,
        profile: str,
        tier: str = "s",
        change_rate: float | None = None,
        seed: int = 42,
    ) -> DatasetArtifact:
        """Generate snapshot + event-batch Parquet files for *profile* at *tier* scale."""
        if profile not in _PROFILE_META:
            raise ValueError(f"Unknown profile {profile!r}. Available: {sorted(_PROFILE_META)}")
        if profile not in _SQL_GENERATORS:
            raise ValueError(f"No SQL generator registered for profile {profile!r}.")

        n_rows    = SCALE_TIERS[tier]
        rate      = change_rate if change_rate is not None else CHANGE_RATES[tier]
        n_changed = max(1, int(n_rows * rate))
        meta      = _PROFILE_META[profile]
        sql_gen   = _SQL_GENERATORS[profile]
        tmp_dir   = tempfile.mkdtemp(
            prefix=f"sqldim_bench_{profile}_{tier}_", dir=self._tmp_root
        )
        snap_path   = os.path.join(tmp_dir, "snapshot.parquet")
        events_path = os.path.join(tmp_dir, "events.parquet")

        self._con.execute(sql_gen["snapshot_view"].format(n=n_rows, changed=n_changed, seed=seed))
        self._con.execute(f"COPY _bench_snapshot TO '{snap_path}' (FORMAT parquet)")
        self._con.execute(sql_gen["event_view"].format(n=n_rows, changed=n_changed, seed=seed))
        self._con.execute(f"COPY _bench_events TO '{events_path}' (FORMAT parquet)")

        return DatasetArtifact(
            profile=profile,
            tier=tier,
            n_rows=n_rows,
            n_changed=n_changed,
            snapshot_path=snap_path,
            events_path=events_path,
            ddl=_get_ddl(profile),
            natural_key=meta["natural_key"],
            track_columns=meta.get("track_columns", []),
            metadata_columns=meta.get("metadata_columns", []),
            _temp_dir=tmp_dir,
        )

    def generate_all_tiers(
        self,
        profile: str,
        tiers: list[str] | None = None,
    ) -> Iterator[DatasetArtifact]:
        """Yield artifacts for each tier, cleaning up each after yielding."""
        for tier in (tiers or list(SCALE_TIERS.keys())):
            yield self.generate(profile, tier)

    def setup_dim_table(
        self,
        con: duckdb.DuckDBPyConnection,
        artifact: DatasetArtifact,
        table_name: str,
    ) -> None:
        """Create the empty SCD target table in *con* using the artifact's DDL."""
        con.execute(artifact.ddl.format(table=table_name))

    def close(self) -> None:
        self._con.close()

    def __enter__(self) -> "BenchmarkDatasetGenerator":
        return self

    def __exit__(self, *_) -> None:
        self.close()

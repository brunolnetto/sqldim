"""Tests for DriftObservatory and its helper functions."""

from __future__ import annotations

import os
import pytest
from dataclasses import dataclass, field
from typing import Any


from sqldim.observability.drift import (
    DriftObservatory,
)


# ---------------------------------------------------------------------------
# Helpers / minimal report stubs
# ---------------------------------------------------------------------------


@dataclass
class _Change:
    change_type: str
    column: str = ""
    detail: str = ""


@dataclass
class _EvolutionReport:
    safe_changes: list = field(default_factory=list)
    additive_changes: list = field(default_factory=list)
    breaking_changes: list = field(default_factory=list)


@dataclass
class _Violation:
    rule: str
    severity: str = "error"
    count: int = 1


@dataclass
class _ContractReport:
    violations: list = field(default_factory=list)
    elapsed_s: float = 0.1
    has_errors: Any = lambda: False


# ---------------------------------------------------------------------------
# DriftObservatory.from_path — lines 343-344
# ---------------------------------------------------------------------------


class TestDriftObservatoryFromPath:
    def test_from_path_creates_file_backed_observatory(self, tmp_path):
        """from_path() opens/creates a file-backed DuckDB connection (lines 343-344)."""
        db_path = str(tmp_path / "obs.duckdb")
        obs = DriftObservatory.from_path(db_path)
        # Should be usable
        assert obs.counts() is not None
        assert os.path.exists(db_path)

    def test_from_path_opens_existing_file(self, tmp_path):
        """from_path() can reopen an existing file."""
        db_path = str(tmp_path / "obs2.duckdb")
        obs1 = DriftObservatory.from_path(db_path)
        obs1._con.close()
        # Reopen
        obs2 = DriftObservatory.from_path(db_path)
        assert obs2.counts() is not None


# ---------------------------------------------------------------------------
# ingest_evolution empty report (line 462 — early return 0)
# ---------------------------------------------------------------------------


class TestIngestEvolutionEarlyReturn:
    def test_ingest_evolution_empty_report_returns_zero(self):
        """EvolutionReport with no changes → ingest_evolution returns 0 (line 462)."""
        obs = DriftObservatory.in_memory()
        report = _EvolutionReport()  # no changes
        result = obs.ingest_evolution(
            report,
            dataset="ds",
            run_id="r1",
        )
        assert result == 0

    def test_ingest_evolution_with_changes_returns_count(self):
        """EvolutionReport with one change → returns 1."""
        obs = DriftObservatory.in_memory()
        report = _EvolutionReport(
            safe_changes=[_Change(change_type="added", column="col1")],
        )
        result = obs.ingest_evolution(report, dataset="ds", run_id="r1")
        assert result == 1


# ---------------------------------------------------------------------------
# ingest_quality empty violations (line 503 — early return 0)
# ---------------------------------------------------------------------------


class TestIngestQualityEarlyReturn:
    def test_ingest_quality_no_violations_returns_zero(self):
        """ContractReport with no violations → ingest_quality returns 0 (line 503)."""
        obs = DriftObservatory.in_memory()
        report = _ContractReport(violations=[])
        result = obs.ingest_quality(report, dataset="ds", run_id="r1")
        assert result == 0

    def test_ingest_quality_with_violations_returns_count(self):
        """ContractReport with one violation → returns 1."""
        obs = DriftObservatory.in_memory()
        report = _ContractReport(violations=[_Violation(rule="not_null")])
        result = obs.ingest_quality(report, dataset="ds", run_id="r1")
        assert result == 1


# ---------------------------------------------------------------------------
# Gold-layer analytical query methods (lines 577, 652 etc.)
# ---------------------------------------------------------------------------


def _seeded_observatory():
    """Return an observatory with pre-ingested evolution and quality data."""
    obs = DriftObservatory.in_memory()
    evo_report = _EvolutionReport(
        safe_changes=[_Change(change_type="added", column="new_col")],
        breaking_changes=[_Change(change_type="removed", column="old_col")],
    )
    obs.ingest_evolution(evo_report, dataset="orders", run_id="r1", layer="silver")
    quality_report = _ContractReport(violations=[_Violation(rule="not_null", count=5)])
    obs.ingest_quality(quality_report, dataset="orders", run_id="r1", layer="silver")
    return obs


class TestGoldLayerQueries:
    def test_quality_score_trend_returns_result(self):
        """quality_score_trend() executes and returns a result (line 577)."""
        obs = _seeded_observatory()
        result = obs.quality_score_trend("orders")
        # Returns a DuckDB relation; calling fetchall() gives the rows
        rows = result.fetchall()
        assert isinstance(rows, list)

    def test_quality_score_trend_empty_dataset(self):
        """quality_score_trend() on unknown dataset returns empty result."""
        obs = DriftObservatory.in_memory()
        rows = obs.quality_score_trend("nonexistent").fetchall()
        assert rows == []

    def test_models_returns_list_of_sqlmodel_classes(self):
        """DriftObservatory.models() returns the 6 obs model classes (line 652)."""
        models = DriftObservatory.models()
        assert isinstance(models, list)
        assert len(models) == 6
        # Each entry should be a class
        for m in models:
            assert hasattr(m, "__tablename__")

    def test_breaking_change_rate_runs(self):
        """breaking_change_rate() executes without error."""
        obs = _seeded_observatory()
        rows = obs.breaking_change_rate().fetchall()
        assert isinstance(rows, list)

    def test_worst_quality_datasets_runs(self):
        """worst_quality_datasets() executes without error."""
        obs = _seeded_observatory()
        rows = obs.worst_quality_datasets(top_n=5).fetchall()
        assert isinstance(rows, list)

    def test_drift_velocity_runs(self):
        """drift_velocity() executes without error."""
        obs = _seeded_observatory()
        rows = obs.drift_velocity().fetchall()
        assert isinstance(rows, list)

    def test_migration_backlog_runs(self):
        """migration_backlog() executes without error."""
        obs = _seeded_observatory()
        rows = obs.migration_backlog().fetchall()
        assert isinstance(rows, list)

    def test_rule_failure_heatmap_runs(self):
        """rule_failure_heatmap() executes without error."""
        obs = _seeded_observatory()
        rows = obs.rule_failure_heatmap().fetchall()
        assert isinstance(rows, list)


# ---------------------------------------------------------------------------
# transaction() context manager (drift/__init__.py lines 177-183)
# ---------------------------------------------------------------------------


class TestTransaction:
    def test_transaction_commits_on_success(self):
        """transaction() wraps ingest calls in a single BEGIN/COMMIT block."""
        obs = DriftObservatory.in_memory()
        report = _EvolutionReport(
            safe_changes=[_Change(change_type="added", column="col1")],
        )
        with obs.transaction():
            obs.ingest_evolution(report, dataset="orders", run_id="r1")
        # Work committed — counts() should reflect the inserted rows
        counts = obs.counts()
        assert counts is not None

    def test_transaction_rolls_back_on_exception(self):
        """transaction() issues ROLLBACK when an exception escapes the block."""
        obs = DriftObservatory.in_memory()
        with pytest.raises(ValueError, match="forced error"):
            with obs.transaction():
                raise ValueError("forced error")
        # After rollback the observatory is still usable
        assert obs.counts() is not None


# ---------------------------------------------------------------------------
# _upsert_* DB-cache-hit branches (lines 211-212, 231-232, 254-255)
# ---------------------------------------------------------------------------


class TestUpsertDbCacheHit:
    def test_dataset_db_cache_hit(self):
        """drift lines 211-212: dataset row found in DB but not in memory cache."""
        obs = DriftObservatory.in_memory()
        report = _EvolutionReport(
            safe_changes=[_Change(change_type="added", column="c1")],
        )
        obs.ingest_evolution(report, dataset="ds_cache_test", run_id="r1")
        # Clear in-memory cache to force the DB lookup path
        obs._dataset_cache.clear()
        obs._run_cache.clear()
        # Second call: cache miss → SELECT from DB → hit lines 211-212 and 254-255
        result = obs.ingest_evolution(report, dataset="ds_cache_test", run_id="r1")
        assert result == 1

    def test_rule_db_cache_hit(self):
        """drift lines 231-232: rule row found in DB but not in memory cache."""
        obs = DriftObservatory.in_memory()
        quality_report = _ContractReport(violations=[_Violation(rule="not_null_check")])
        obs.ingest_quality(quality_report, dataset="ds2", run_id="r1")
        # Clear rule cache to force DB lookup
        obs._rule_cache.clear()
        obs._dataset_cache.clear()
        obs._run_cache.clear()
        # Second call: rule cache miss → SELECT → hit lines 231-232
        result = obs.ingest_quality(quality_report, dataset="ds2", run_id="r1")
        assert result == 1

    def test_run_db_cache_hit(self):
        """drift lines 254-255: run row found in DB but not in memory cache."""
        obs = DriftObservatory.in_memory()
        report = _EvolutionReport(
            safe_changes=[_Change(change_type="added", column="c2")],
        )
        obs.ingest_evolution(report, dataset="ds3", run_id="run_cache_test")
        # Clear run cache to force DB lookup
        obs._run_cache.clear()
        obs._dataset_cache.clear()
        # Second call: run cache miss → SELECT → hit lines 254-255
        result = obs.ingest_evolution(report, dataset="ds3", run_id="run_cache_test")
        assert result == 1

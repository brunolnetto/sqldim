"""Drift Observatory dimensional models and row helpers.

Extracted from drift.py to keep file sizes manageable.
Contains the DimensionModel/FactModel tables (dog-fooded) and
pure-function helpers used by DriftObservatory.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from sqldim import DimensionModel
from sqldim.core.kimball.facts import TransactionFact
from sqldim.core.kimball.fields import Field

if TYPE_CHECKING:
    pass  # avoid circular imports; EvolutionReport / ContractReport loaded lazily


# ---------------------------------------------------------------------------
# Native dimensional models  (dog-fooding sqldim's own abstractions)
# ---------------------------------------------------------------------------


class ObsDatasetDim(DimensionModel, table=True):
    """Every tracked table/view — natural key is ``dataset_name``."""

    __tablename__ = "obs_dataset_dim"
    __natural_key__ = ["dataset_name"]
    __scd_type__ = 1

    dataset_id: int | None = Field(
        default=None,
        primary_key=True,
        surrogate_key=True,
    )
    dataset_name: str = Field(natural_key=True)
    layer: str | None = Field(default=None, nullable=True)
    domain: str | None = Field(default=None, nullable=True)
    table_type: str | None = Field(default=None, nullable=True)


class ObsEvolutionTypeDim(DimensionModel, table=True):
    """Taxonomy of schema-change kinds — populated once as reference data."""

    __tablename__ = "obs_evolution_type_dim"
    __natural_key__ = ["change_type"]
    __scd_type__ = 1

    evo_type_id: int | None = Field(
        default=None,
        primary_key=True,
        surrogate_key=True,
    )
    change_type: str = Field(natural_key=True)
    tier: str = Field()
    requires_migration: bool = Field(default=False)
    requires_backfill: bool = Field(default=False)


class ObsRuleDim(DimensionModel, table=True):
    """Contract rule catalog."""

    __tablename__ = "obs_rule_dim"
    __natural_key__ = ["rule_name"]
    __scd_type__ = 1

    rule_id: int | None = Field(default=None, primary_key=True, surrogate_key=True)
    rule_name: str = Field(natural_key=True)
    severity: str = Field()
    category: str | None = Field(default=None, nullable=True)


class ObsPipelineRunDim(DimensionModel, table=True):
    """One row per pipeline execution."""

    __tablename__ = "obs_pipeline_run_dim"
    __natural_key__ = ["run_key"]
    __scd_type__ = 1

    run_id: int | None = Field(default=None, primary_key=True, surrogate_key=True)
    run_key: str = Field(natural_key=True)
    pipeline_name: str | None = Field(default=None, nullable=True)
    triggered_at: str | None = Field(default=None, nullable=True)
    git_sha: str | None = Field(default=None, nullable=True)


class ObsSchemaEvolutionFact(TransactionFact, table=True):
    """One row per column-level schema change event."""

    __tablename__ = "obs_schema_evolution_fact"
    __grain__ = "one row per column-level schema change per pipeline run"
    __strategy__ = "bulk"

    evo_fact_id: int | None = Field(
        default=None, primary_key=True, surrogate_key=True
    )
    run_id: int = Field(foreign_key="obs_pipeline_run_dim.run_id", measure=False)
    dataset_id: int = Field(foreign_key="obs_dataset_dim.dataset_id", measure=False)
    evo_type_id: int | None = Field(
        default=None,
        nullable=True,
        foreign_key="obs_evolution_type_dim.evo_type_id",
        measure=False,
    )
    column_name: str = Field()
    change_detail: str | None = Field(default=None, nullable=True)
    detected_at: str = Field()


class ObsQualityDriftFact(TransactionFact, table=True):
    """One row per contract-rule violation check result."""

    __tablename__ = "obs_quality_drift_fact"
    __grain__ = "one row per rule violation per pipeline run"
    __strategy__ = "bulk"

    quality_fact_id: int | None = Field(
        default=None, primary_key=True, surrogate_key=True
    )
    run_id: int = Field(foreign_key="obs_pipeline_run_dim.run_id", measure=False)
    dataset_id: int = Field(foreign_key="obs_dataset_dim.dataset_id", measure=False)
    rule_id: int = Field(foreign_key="obs_rule_dim.rule_id", measure=False)
    violation_count: int = Field(default=0, measure=True, additive=True)
    check_elapsed_s: float = Field(
        default=0.0, measure=True, additive=False, semi_additive_fallback="avg"
    )
    has_errors: bool = Field(default=False)
    checked_at: str = Field()


# ---------------------------------------------------------------------------
# Schema materialisation helper
# ---------------------------------------------------------------------------

_OBS_MODELS = [
    ObsDatasetDim,
    ObsEvolutionTypeDim,
    ObsRuleDim,
    ObsPipelineRunDim,
    ObsSchemaEvolutionFact,
    ObsQualityDriftFact,
]

#: Reference data — evolution type taxonomy (seed rows for ObsEvolutionTypeDim)
_EVOLUTION_TYPE_SEED = [
    dict(
        evo_type_id=1,
        change_type="added",
        tier="safe",
        requires_migration=False,
        requires_backfill=False,
    ),
    dict(
        evo_type_id=2,
        change_type="widened",
        tier="additive",
        requires_migration=True,
        requires_backfill=False,
    ),
    dict(
        evo_type_id=3,
        change_type="narrowed",
        tier="breaking",
        requires_migration=True,
        requires_backfill=True,
    ),
    dict(
        evo_type_id=4,
        change_type="type_changed",
        tier="breaking",
        requires_migration=True,
        requires_backfill=True,
    ),
    dict(
        evo_type_id=5,
        change_type="renamed",
        tier="breaking",
        requires_migration=True,
        requires_backfill=True,
    ),
    dict(
        evo_type_id=6,
        change_type="removed",
        tier="breaking",
        requires_migration=True,
        requires_backfill=True,
    ),
]


def _model_ddl(model) -> str:
    """
    Compile a ``table=True`` SQLModel class to ``CREATE TABLE IF NOT EXISTS``
    SQL using SQLAlchemy's SQLite dialect (compatible with DuckDB syntax).
    """
    from sqlalchemy.dialects import sqlite as _sqlite
    from sqlalchemy.schema import CreateTable as _CT

    sql = str(_CT(model.__table__).compile(dialect=_sqlite.dialect()))
    return sql.replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS ", 1)


# ---------------------------------------------------------------------------
# Row dataclasses (typed wrappers for the silver-layer adapters)
# ---------------------------------------------------------------------------


@dataclass
class EvolutionRow:
    """One silver-layer row for :class:`ObsSchemaEvolutionFact`."""

    dataset_name: str
    run_key: str
    change_type: str
    column_name: str
    change_detail: str = ""
    detected_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    layer: str = ""
    pipeline_name: str = ""


@dataclass
class QualityRow:
    """One silver-layer row for :class:`ObsQualityDriftFact`."""

    dataset_name: str
    run_key: str
    rule_name: str
    severity: str
    violation_count: int = 0
    check_elapsed_s: float = 0.0
    has_errors: bool = False
    checked_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    layer: str = ""
    pipeline_name: str = ""


# ---------------------------------------------------------------------------
# Pure adapters — no DB dependency
# ---------------------------------------------------------------------------


def evolution_to_rows(
    report: Any,
    *,
    dataset: str,
    run_id: str,
    layer: str = "",
    pipeline_name: str = "",
    detected_at: datetime | None = None,
) -> list[EvolutionRow]:
    """
    Convert an :class:`~sqldim.contracts.engine.EvolutionReport` to a list
    of :class:`EvolutionRow` objects ready for silver-layer ingestion.
    """
    ts = detected_at or datetime.now(timezone.utc)
    rows: list[EvolutionRow] = []

    tier_map = {
        "safe_changes": "added",
        "additive_changes": "widened",
        "breaking_changes": "type_changed",
    }

    for attr in ("safe_changes", "additive_changes", "breaking_changes"):
        for ch in getattr(report, attr, []):
            rows.append(
                EvolutionRow(
                    dataset_name=dataset,
                    run_key=run_id,
                    change_type=getattr(ch, "change_type", tier_map[attr]),
                    column_name=getattr(ch, "column", ""),
                    change_detail=getattr(ch, "detail", ""),
                    detected_at=ts,
                    layer=layer,
                    pipeline_name=pipeline_name,
                )
            )

    return rows


def contract_to_rows(
    report: Any,
    *,
    dataset: str,
    run_id: str,
    layer: str = "",
    pipeline_name: str = "",
    checked_at: datetime | None = None,
) -> list[QualityRow]:
    """
    Convert a :class:`~sqldim.contracts.report.ContractReport` to a list of
    :class:`QualityRow` objects ready for silver-layer ingestion.

    An empty list means clean data (no violations recorded as facts).
    """
    ts = checked_at or datetime.now(timezone.utc)
    return [
        QualityRow(
            dataset_name=dataset,
            run_key=run_id,
            rule_name=getattr(v, "rule", "unknown"),
            severity=getattr(v, "severity", "error"),
            violation_count=getattr(v, "count", 0),
            check_elapsed_s=getattr(report, "elapsed_s", 0.0),
            has_errors=getattr(report, "has_errors", lambda: False)(),
            checked_at=ts,
            layer=layer,
            pipeline_name=pipeline_name,
        )
        for v in getattr(report, "violations", [])
    ]


# ---------------------------------------------------------------------------
# DriftObservatory — high-level facade
# ---------------------------------------------------------------------------

"""DriftObservatory gold-layer analytical query methods.

Extracted from drift.py to keep file sizes manageable.
These methods are mixed into :class:`DriftObservatory` via inheritance.
"""

from __future__ import annotations

from typing import Any

from sqldim.observability.drift._drift_models import _OBS_MODELS, _EVOLUTION_TYPE_SEED  # noqa: F401


class _DriftAnalyticsMixin:
    """Gold-layer analytical queries for DriftObservatory."""

    # Gold-layer analytical queries
    # ------------------------------------------------------------------

    def breaking_change_rate(self) -> Any:
        """Gold: breaking vs. total schema changes, grouped by dataset."""
        return self._con.query("""
            SELECT
                d.dataset_name,
                d.layer,
                COUNT(*)                                                AS total_changes,
                SUM(CASE WHEN e.tier = 'breaking' THEN 1 ELSE 0 END)  AS breaking_changes,
                ROUND(
                    100.0 * SUM(CASE WHEN e.tier = 'breaking' THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(*), 0), 1
                )                                                       AS breaking_pct
            FROM obs_schema_evolution_fact f
            JOIN obs_dataset_dim        d ON d.dataset_id  = f.dataset_id
            JOIN obs_evolution_type_dim e ON e.evo_type_id = f.evo_type_id
            GROUP BY d.dataset_name, d.layer
            ORDER BY breaking_pct DESC NULLS LAST
        """)

    def worst_quality_datasets(self, top_n: int = 10) -> Any:
        """Gold: datasets with highest cumulative violation counts."""
        return self._con.query(f"""
            SELECT
                d.dataset_name,
                d.layer,
                SUM(qf.violation_count)    AS total_violations,
                COUNT(DISTINCT qf.rule_id) AS distinct_rules_violated,
                MAX(qf.checked_at)         AS latest_check
            FROM obs_quality_drift_fact qf
            JOIN obs_dataset_dim d ON d.dataset_id = qf.dataset_id
            GROUP BY d.dataset_name, d.layer
            ORDER BY total_violations DESC
            LIMIT {top_n}
        """)

    def drift_velocity(self, bucket: str = "day") -> Any:
        """Gold: schema-change events bucketed by ``bucket`` (day/week/month)."""
        return self._con.query(f"""
            SELECT
                DATE_TRUNC('{bucket}', f.detected_at::TIMESTAMP) AS period,
                e.tier                                            AS change_tier,
                COUNT(*)                                          AS event_count
            FROM obs_schema_evolution_fact f
            JOIN obs_evolution_type_dim e ON e.evo_type_id = f.evo_type_id
            GROUP BY period, change_tier
            ORDER BY period, change_tier
        """)

    def quality_score_trend(self, dataset: str, bucket: str = "day") -> Any:
        """Gold: rolling quality score for *dataset* over time (score = 0–1)."""
        return self._con.execute(
            f"""
            WITH daily AS (
                SELECT
                    DATE_TRUNC('{bucket}', qf.checked_at::TIMESTAMP) AS period,
                    SUM(qf.violation_count) AS daily_violations
                FROM obs_quality_drift_fact qf
                JOIN obs_dataset_dim d ON d.dataset_id = qf.dataset_id
                WHERE d.dataset_name = ?
                GROUP BY period
            ),
            windowed AS (
                SELECT period, daily_violations,
                       MAX(daily_violations) OVER () AS max_violations
                FROM daily
            )
            SELECT
                period,
                daily_violations AS total_violations,
                ROUND(1.0 - daily_violations / NULLIF(max_violations, 0), 3) AS quality_score
            FROM windowed
            ORDER BY period
        """,
            [dataset],
        )

    def migration_backlog(self) -> Any:
        """Gold: columns requiring migration or backfill, newest-first."""
        return self._con.query("""
            SELECT
                d.dataset_name,
                f.column_name,
                f.change_detail,
                e.change_type,
                e.tier,
                e.requires_migration,
                e.requires_backfill,
                f.detected_at
            FROM obs_schema_evolution_fact f
            JOIN obs_dataset_dim        d ON d.dataset_id  = f.dataset_id
            JOIN obs_evolution_type_dim e ON e.evo_type_id = f.evo_type_id
            WHERE e.requires_migration = TRUE OR e.requires_backfill = TRUE
            ORDER BY f.detected_at DESC
        """)

    def rule_failure_heatmap(self) -> Any:
        """Gold: dataset × rule violation matrix."""
        return self._con.query("""
            SELECT
                d.dataset_name,
                r.rule_name,
                r.severity,
                COUNT(*)                       AS check_count,
                SUM(violation_count)           AS total_violations,
                ROUND(AVG(violation_count), 1) AS avg_violations_per_run
            FROM obs_quality_drift_fact qf
            JOIN obs_dataset_dim d ON d.dataset_id = qf.dataset_id
            JOIN obs_rule_dim    r ON r.rule_id     = qf.rule_id
            GROUP BY d.dataset_name, r.rule_name, r.severity
            ORDER BY total_violations DESC
        """)

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    def counts(self) -> dict[str, int]:
        """Return row counts for all observability tables."""
        return {
            model.__tablename__: (
                self._con.execute(
                    f"SELECT COUNT(*) FROM {model.__tablename__}"
                ).fetchone()
                or [0]
            )[0]
            for model in _OBS_MODELS
        }

    @staticmethod
    def models() -> list:
        """Return the six SQLModel classes that define the observability schema."""
        return list(_OBS_MODELS)

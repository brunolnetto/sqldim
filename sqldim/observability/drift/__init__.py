"""
sqldim/observability/drift.py
==============================
Schema and quality drift modelled as **Kimball dimensional facts**.

This module answers the research question: *"Can observability metadata be
treated as first-class data-warehouse citizens?"*  The answer is yes — every
``EvolutionReport`` and ``ContractReport`` produced by sqldim's own engines is
just structured data that needs the same lineage, trending, and aggregation
capabilities as business data.

Architecture
------------
The observability **star schema** is declared using sqldim's own native
``DimensionModel`` / ``TransactionFact`` base classes — the library
dog-foods itself.  The models are then materialised into a DuckDB connection
(in-memory or file-backed) via SQLAlchemy DDL compilation.

Dimensions
~~~~~~~~~~
* :class:`ObsDatasetDim`       — every tracked table/view
* :class:`ObsEvolutionTypeDim` — taxonomy of schema-change kinds
* :class:`ObsRuleDim`          — contract rule catalog
* :class:`ObsPipelineRunDim`   — one row per pipeline execution

Facts
~~~~~
* :class:`ObsSchemaEvolutionFact` — one row per *column-level* schema change
* :class:`ObsQualityDriftFact`    — one row per contract-rule check result

Medallion mapping
~~~~~~~~~~~~~~~~~
* **Bronze** — raw ``EvolutionReport`` / ``ContractReport`` objects arrive as events
* **Silver** — ``DriftObservatory.ingest_evolution()`` / ``ingest_quality()``
  explode them into the star schema
* **Gold**   — ``DriftObservatory.breaking_change_rate()``,
  ``worst_quality_datasets()``, ``drift_velocity()`` return analytical relations

Adapters
~~~~~~~~
Two pure functions convert sqldim report objects to row dicts::

    rows = evolution_to_rows(report, dataset="orders_dim",
                             run_id="run-001", layer="silver")
    rows = contract_to_rows(report, dataset="orders_dim",
                            run_id="run-001", layer="silver")
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Generator


from sqldim.observability.drift._drift_analytics import _DriftAnalyticsMixin  # noqa: F401
from sqldim.observability.drift._drift_models import (  # noqa: F401
    ObsDatasetDim,
    ObsEvolutionTypeDim,
    ObsRuleDim,
    ObsPipelineRunDim,
    ObsSchemaEvolutionFact,
    ObsQualityDriftFact,
    _model_ddl,
    EvolutionRow,
    QualityRow,
    evolution_to_rows,
    contract_to_rows,
    _OBS_MODELS,
    _EVOLUTION_TYPE_SEED,
)


class DriftObservatory(_DriftAnalyticsMixin):
    """
    Observability star-schema facade using DuckDB as the store.

    The schema is declared using sqldim's own ``DimensionModel`` /
    ``TransactionFact`` classes — the library dog-foods itself.  DDL is
    compiled from those model classes and executed against the DuckDB
    connection on first use.

    Example
    -------
    .. code-block:: python

        from sqldim.observability.drift import DriftObservatory

        obs = DriftObservatory.in_memory()
        obs.ingest_evolution(report, dataset="orders_dim", run_id="run-001")
        obs.ingest_quality(report,   dataset="orders_dim", run_id="run-001")
        print(obs.breaking_change_rate().fetchdf())
    """

    def __init__(self, con: Any) -> None:
        self._con = con
        # Per-connection in-memory caches — eliminate repeat SELECT round-trips
        # for lookup tables that are small and stable within a single observatory.
        self._pk_counters: dict[str, int] = {}
        self._dataset_cache: dict[str, int] = {}
        self._run_cache: dict[str, int] = {}
        self._rule_cache: dict[str, int] = {}
        self._evo_type_cache: dict[str, int | None] = {}
        self._ensure_schema()

    @classmethod
    def in_memory(cls) -> "DriftObservatory":
        """Create a new in-memory DuckDB observatory (useful for testing)."""
        import duckdb

        return cls(duckdb.connect())

    @classmethod
    def from_path(cls, path: str) -> "DriftObservatory":
        """Open or create a file-backed DuckDB observatory at *path*."""
        import duckdb

        return cls(duckdb.connect(path))

    # ------------------------------------------------------------------
    # Schema management
    # ------------------------------------------------------------------

    def _ensure_schema(self) -> None:
        """
        Materialise all six native models as DuckDB tables (idempotent).

        DDL is compiled from the ``DimensionModel`` / ``TransactionFact``
        declarations above via SQLAlchemy — no hand-written DDL strings.
        """
        for model in _OBS_MODELS:
            self._con.execute(_model_ddl(model))

        # Seed the reference dimension (evolution type taxonomy)
        for row in _EVOLUTION_TYPE_SEED:
            exists = self._con.execute(
                "SELECT 1 FROM obs_evolution_type_dim WHERE change_type = ?",
                [row["change_type"]],
            ).fetchone()
            if not exists:
                self._con.execute(
                    """INSERT INTO obs_evolution_type_dim
                       (evo_type_id, change_type, tier, requires_migration, requires_backfill)
                       VALUES (?, ?, ?, ?, ?)""",
                    [
                        row["evo_type_id"],
                        row["change_type"],
                        row["tier"],
                        row["requires_migration"],
                        row["requires_backfill"],
                    ],
                )
        # Pre-populate evo_type cache — only 6 rows, never changes after seeding
        for ct, eid in self._con.execute(
            "SELECT change_type, evo_type_id FROM obs_evolution_type_dim"
        ).fetchall():
            self._evo_type_cache[ct] = eid

    # ------------------------------------------------------------------
    # Transaction helpers
    # ------------------------------------------------------------------

    @contextmanager
    def transaction(self) -> Generator["DriftObservatory", None, None]:
        """Context manager that batches all ingest calls inside a single transaction.

        Dramatically improves bulk-ingest throughput because DuckDB defaults to
        autocommit — without this wrapper every ``ingest_evolution`` /
        ``ingest_quality`` call issues several individual commits.

        Example::

            with obs.transaction():
                for report in reports:
                    obs.ingest_evolution(report, dataset="orders", run_id="r1")
        """
        self._con.execute("BEGIN")
        try:
            yield self
            self._con.execute("COMMIT")
        except Exception:
            self._con.execute("ROLLBACK")
            raise

    # ------------------------------------------------------------------
    # Dimension upserts
    # ------------------------------------------------------------------

    def _next_pk(self, table: str, pk_col: str) -> int:
        """Return the next PK for *table*, using an in-memory counter after the
        first query (avoids a ``MAX`` round-trip on every ingest call)."""
        if table not in self._pk_counters:
            self._pk_counters[table] = (
                self._con.execute(
                    f"SELECT COALESCE(MAX({pk_col}), 0) FROM {table}"
                ).fetchone()
                or [0]
            )[0]
        self._pk_counters[table] += 1
        return self._pk_counters[table]

    def _lookup_or_insert(
        self,
        cache: dict,
        key: str,
        select_sql: str,
        select_params: list,
        table: str,
        pk_col: str,
        insert_sql: str,
        insert_params: list,
    ) -> int:
        """Cache-aside lookup: check cache, then DB, then insert."""
        if key in cache:
            return cache[key]
        row = self._con.execute(select_sql, select_params).fetchone()
        if row:
            cache[key] = row[0]
            return row[0]
        new_id = self._next_pk(table, pk_col)
        self._con.execute(insert_sql, [new_id, *insert_params])
        cache[key] = new_id
        return new_id

    def _upsert_dataset(
        self, name: str, layer: str = "", domain: str = "", table_type: str = ""
    ) -> int:
        return self._lookup_or_insert(
            self._dataset_cache,
            name,
            "SELECT dataset_id FROM obs_dataset_dim WHERE dataset_name = ?",
            [name],
            "obs_dataset_dim",
            "dataset_id",
            "INSERT INTO obs_dataset_dim (dataset_id, dataset_name, layer, domain, table_type)"
            " VALUES (?, ?, ?, ?, ?)",
            [name, layer or None, domain or None, table_type or None],
        )

    def _upsert_rule(
        self, name: str, severity: str = "error", category: str = ""
    ) -> int:
        if name in self._rule_cache:
            return self._rule_cache[name]
        row = self._con.execute(
            "SELECT rule_id FROM obs_rule_dim WHERE rule_name = ?", [name]
        ).fetchone()
        if row:
            self._rule_cache[name] = row[0]
            return row[0]
        new_id = self._next_pk("obs_rule_dim", "rule_id")
        self._con.execute(
            "INSERT INTO obs_rule_dim (rule_id, rule_name, severity, category) VALUES (?, ?, ?, ?)",
            [new_id, name, severity, category or None],
        )
        self._rule_cache[name] = new_id
        return new_id

    def _upsert_run(
        self,
        run_key: str,
        pipeline_name: str = "",
        triggered_at: datetime | None = None,
        git_sha: str = "",
    ) -> int:
        ts = (triggered_at or datetime.now(timezone.utc)).isoformat()
        return self._lookup_or_insert(
            self._run_cache,
            run_key,
            "SELECT run_id FROM obs_pipeline_run_dim WHERE run_key = ?",
            [run_key],
            "obs_pipeline_run_dim",
            "run_id",
            "INSERT INTO obs_pipeline_run_dim (run_id, run_key, pipeline_name, triggered_at, git_sha)"
            " VALUES (?, ?, ?, ?, ?)",
            [run_key, pipeline_name or None, ts, git_sha or None],
        )

    def _evo_type_id(self, change_type: str) -> int | None:
        """Return evo_type_id for *change_type* using the pre-populated cache."""
        return self._evo_type_cache.get(change_type)

    # ------------------------------------------------------------------
    # Silver-layer ingestion
    # ------------------------------------------------------------------

    def ingest_evolution(
        self,
        report: Any,
        *,
        dataset: str,
        run_id: str,
        layer: str = "",
        pipeline_name: str = "",
        domain: str = "",
        detected_at: datetime | None = None,
    ) -> int:
        """Ingest an ``EvolutionReport`` into the star schema.

        Returns the number of :class:`ObsSchemaEvolutionFact` rows inserted.
        """
        rows = evolution_to_rows(
            report,
            dataset=dataset,
            run_id=run_id,
            layer=layer,
            pipeline_name=pipeline_name,
            detected_at=detected_at,
        )
        if not rows:
            return 0

        dataset_pk = self._upsert_dataset(dataset, layer=layer, domain=domain)
        run_pk = self._upsert_run(
            run_id, pipeline_name=pipeline_name, triggered_at=detected_at
        )
        base_id = self._next_pk("obs_schema_evolution_fact", "evo_fact_id")
        # Advance counter for all rows now so the next call gets a fresh base
        if len(rows) > 1:
            self._pk_counters["obs_schema_evolution_fact"] = base_id + len(rows) - 1

        self._con.executemany(
            """INSERT INTO obs_schema_evolution_fact
               (evo_fact_id, run_id, dataset_id, evo_type_id,
                column_name, change_detail, detected_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            [
                [
                    base_id + i,
                    run_pk,
                    dataset_pk,
                    self._evo_type_id(row.change_type),
                    row.column_name,
                    row.change_detail,
                    row.detected_at.isoformat(),
                ]
                for i, row in enumerate(rows)
            ],
        )

        return len(rows)

    def ingest_quality(
        self,
        report: Any,
        *,
        dataset: str,
        run_id: str,
        layer: str = "",
        pipeline_name: str = "",
        domain: str = "",
        checked_at: datetime | None = None,
    ) -> int:
        """Ingest a ``ContractReport`` into the star schema.

        Returns the number of :class:`ObsQualityDriftFact` rows inserted.
        Zero means a clean run (no violations → no facts recorded).
        """
        rows = contract_to_rows(
            report,
            dataset=dataset,
            run_id=run_id,
            layer=layer,
            pipeline_name=pipeline_name,
            checked_at=checked_at,
        )
        if not rows:
            return 0

        dataset_pk = self._upsert_dataset(dataset, layer=layer, domain=domain)
        run_pk = self._upsert_run(
            run_id, pipeline_name=pipeline_name, triggered_at=checked_at
        )
        base_id = self._next_pk("obs_quality_drift_fact", "quality_fact_id")
        if len(rows) > 1:
            self._pk_counters["obs_quality_drift_fact"] = base_id + len(rows) - 1

        self._con.executemany(
            """INSERT INTO obs_quality_drift_fact
               (quality_fact_id, run_id, dataset_id, rule_id,
                violation_count, check_elapsed_s, has_errors, checked_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                [
                    base_id + i,
                    run_pk,
                    dataset_pk,
                    self._upsert_rule(row.rule_name, severity=row.severity),
                    row.violation_count,
                    row.check_elapsed_s,
                    row.has_errors,
                    row.checked_at.isoformat(),
                ]
                for i, row in enumerate(rows)
            ],
        )

        return len(rows)

    # ------------------------------------------------------------------

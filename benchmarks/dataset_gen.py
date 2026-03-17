"""
benchmarks/dataset_gen.py
==========================
Scales sqldim's DatasetFactory sources to arbitrary row counts by writing
synthetic data to temporary Parquet files. This avoids holding millions of
rows in Python memory while still exercising the full pipeline.

Design:
  - Small datasets (< 50K rows)  → in-memory via DatasetFactory
  - Large datasets (50K+)        → generated in chunks, written to Parquet
  - Change batches               → derived from the original, N% rows mutated
  - Schema matches existing sqldim SCD table layouts exactly

Supported profiles:
  - "products"        → ProductsSource schema (product_id, name, category, price)
  - "employees"       → EmployeesSource schema (employee_id, full_name, title, dept)
  - "customers"       → CustomersSource schema (customer_id, full_name, email, addr)
  - "saas_users"      → SaaSUsersSource schema (user_id, email, plan_tier, ...)
  - "player_seasons"  → PlayerSeasonsSource schema (20-column analytics)
  - "cnpj_empresa"    → RFB empresa-style schema (cnpj_basico + metadata columns)
"""
from __future__ import annotations

import os
import random
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator

import duckdb


# ── Scale tiers ───────────────────────────────────────────────────────────

SCALE_TIERS = {
    "xs":  10_000,
    "s":   100_000,
    "m":   1_000_000,
    "l":   5_000_000,
    "xl":  20_000_000,
    "xxl": 60_000_000,
}

# Change rate per tier for event batches (what fraction of rows mutate)
CHANGE_RATES = {
    "xs":  0.30,
    "s":   0.20,
    "m":   0.15,
    "l":   0.10,
    "xl":  0.05,
    "xxl": 0.02,
}


# ── Dataset profiles ──────────────────────────────────────────────────────

# Each profile defines: table DDL, snapshot SQL generator, event SQL generator
# All generators work inside DuckDB using generate_series — no Python row loops.

PROFILES: dict[str, dict] = {

    "products": {
        "ddl": """
            CREATE TABLE IF NOT EXISTS {table} (
                product_id  INTEGER,
                name        VARCHAR,
                category    VARCHAR,
                price       DOUBLE,
                valid_from  VARCHAR,
                valid_to    VARCHAR,
                is_current  BOOLEAN,
                checksum    VARCHAR
            )
        """,
        "snapshot_view": """
            CREATE OR REPLACE VIEW _bench_snapshot AS
            SELECT
                i                                                  AS product_id,
                concat('Product_', i)                              AS name,
                CASE (i % 5)
                    WHEN 0 THEN 'widgets'
                    WHEN 1 THEN 'gadgets'
                    WHEN 2 THEN 'tools'
                    WHEN 3 THEN 'accessories'
                    ELSE        'consumables'
                END                                                AS category,
                round(4.99 + (i % 1000) * 0.195, 2)               AS price
            FROM generate_series(1, {n}) t(i)
        """,
        "event_view": """
            CREATE OR REPLACE VIEW _bench_events AS
            SELECT
                i                                                  AS product_id,
                concat('Product_', i, '_v2')                       AS name,
                CASE (i % 5)
                    WHEN 0 THEN 'widgets'
                    WHEN 1 THEN 'gadgets'
                    WHEN 2 THEN 'tools'
                    WHEN 3 THEN 'accessories'
                    ELSE        'consumables'
                END                                                AS category,
                round(4.99 + (i % 1000) * 0.195 * 1.15, 2)        AS price
            FROM generate_series(1, {changed}) t(i)
        """,
        "natural_key":    "product_id",
        "track_columns":  ["name", "category", "price"],
    },

    "employees": {
        "ddl": """
            CREATE TABLE IF NOT EXISTS {table} (
                employee_id INTEGER,
                full_name   VARCHAR,
                title       VARCHAR,
                department  VARCHAR,
                valid_from  VARCHAR,
                valid_to    VARCHAR,
                is_current  BOOLEAN,
                checksum    VARCHAR
            )
        """,
        "snapshot_view": """
            CREATE OR REPLACE VIEW _bench_snapshot AS
            SELECT
                100 + i                                            AS employee_id,
                concat('Employee_', i)                             AS full_name,
                CASE (i % 5)
                    WHEN 0 THEN 'Engineer'
                    WHEN 1 THEN 'Analyst'
                    WHEN 2 THEN 'Manager'
                    WHEN 3 THEN 'Developer'
                    ELSE        'Associate'
                END                                                AS title,
                CASE (i % 7)
                    WHEN 0 THEN 'Engineering'
                    WHEN 1 THEN 'Finance'
                    WHEN 2 THEN 'Marketing'
                    WHEN 3 THEN 'Product'
                    WHEN 4 THEN 'HR'
                    WHEN 5 THEN 'Legal'
                    ELSE        'Sales'
                END                                                AS department
            FROM generate_series(1, {n}) t(i)
        """,
        "event_view": """
            CREATE OR REPLACE VIEW _bench_events AS
            SELECT
                100 + i                                            AS employee_id,
                concat('Employee_', i)                             AS full_name,
                CASE (i % 5)
                    WHEN 0 THEN 'Senior Engineer'
                    WHEN 1 THEN 'Senior Analyst'
                    WHEN 2 THEN 'Senior Manager'
                    WHEN 3 THEN 'Lead Developer'
                    ELSE        'Senior Associate'
                END                                                AS title,
                CASE ((i + 1) % 7)
                    WHEN 0 THEN 'Engineering'
                    WHEN 1 THEN 'Finance'
                    WHEN 2 THEN 'Marketing'
                    WHEN 3 THEN 'Product'
                    WHEN 4 THEN 'HR'
                    WHEN 5 THEN 'Legal'
                    ELSE        'Sales'
                END                                                AS department
            FROM generate_series(1, {changed}) t(i)
        """,
        "natural_key":    "employee_id",
        "track_columns":  ["title", "department"],
    },

    "cnpj_empresa": {
        # Mimics the RFB empresa table layout (metadata-bag schema)
        "ddl": """
            CREATE TABLE IF NOT EXISTS {table} (
                cnpj_basico         VARCHAR,
                valid_from          TIMESTAMPTZ,
                valid_to            TIMESTAMPTZ,
                is_current          BOOLEAN,
                metadata            JSON,
                metadata_diff       JSON,
                row_hash            VARCHAR
            )
        """,
        "snapshot_view": """
            CREATE OR REPLACE VIEW _bench_snapshot AS
            SELECT
                lpad(cast(i AS VARCHAR), 8, '0')           AS cnpj_basico,
                concat('EMPRESA ', i)                      AS razao_social,
                CASE (i % 10)
                    WHEN 0 THEN '2062'
                    WHEN 1 THEN '2240'
                    WHEN 2 THEN '3034'
                    ELSE         '2011'
                END                                        AS natureza_juridica,
                CASE (i % 5)
                    WHEN 0 THEN '49'
                    WHEN 1 THEN '05'
                    ELSE         '01'
                END                                        AS porte_empresa,
                round(1000.0 + (i % 100000) * 0.1, 2)     AS capital_social
            FROM generate_series(1, {n}) t(i)
        """,
        "event_view": """
            CREATE OR REPLACE VIEW _bench_events AS
            SELECT
                lpad(cast(i AS VARCHAR), 8, '0')           AS cnpj_basico,
                concat('EMPRESA ', i, ' ATUALIZADA')       AS razao_social,
                CASE (i % 10)
                    WHEN 0 THEN '2062'
                    WHEN 1 THEN '2240'
                    WHEN 2 THEN '3034'
                    ELSE         '2011'
                END                                        AS natureza_juridica,
                CASE (i % 5)
                    WHEN 0 THEN '03'
                    WHEN 1 THEN '05'
                    ELSE         '01'
                END                                        AS porte_empresa,
                round(1000.0 + (i % 100000) * 0.1 * 1.1, 2) AS capital_social
            FROM generate_series(1, {changed}) t(i)
        """,
        "natural_key":    "cnpj_basico",
        "metadata_columns": ["razao_social", "natureza_juridica", "porte_empresa", "capital_social"],
        "track_columns":  ["razao_social", "natureza_juridica", "porte_empresa", "capital_social"],
    },

    "saas_users": {
        "ddl": """
            CREATE TABLE IF NOT EXISTS {table} (
                user_id     INTEGER,
                email       VARCHAR,
                plan_tier   VARCHAR,
                acq_source  VARCHAR,
                valid_from  VARCHAR,
                valid_to    VARCHAR,
                is_current  BOOLEAN,
                checksum    VARCHAR
            )
        """,
        "snapshot_view": """
            CREATE OR REPLACE VIEW _bench_snapshot AS
            SELECT
                i                                                  AS user_id,
                concat('user', i, '@bench.io')                     AS email,
                CASE (i % 10)
                    WHEN 0 THEN 'pro'
                    WHEN 1 THEN 'enterprise'
                    ELSE        'free'
                END                                                AS plan_tier,
                CASE (i % 5)
                    WHEN 0 THEN 'organic'
                    WHEN 1 THEN 'ads'
                    WHEN 2 THEN 'referral'
                    WHEN 3 THEN 'content'
                    ELSE        'social'
                END                                                AS acq_source
            FROM generate_series(1, {n}) t(i)
        """,
        "event_view": """
            CREATE OR REPLACE VIEW _bench_events AS
            SELECT
                i                                                  AS user_id,
                concat('user', i, '@bench.io')                     AS email,
                CASE (i % 10)
                    WHEN 0 THEN 'enterprise'
                    WHEN 1 THEN 'enterprise'
                    ELSE        'pro'
                END                                                AS plan_tier,
                CASE (i % 5)
                    WHEN 0 THEN 'organic'
                    WHEN 1 THEN 'ads'
                    WHEN 2 THEN 'referral'
                    WHEN 3 THEN 'content'
                    ELSE        'social'
                END                                                AS acq_source
            FROM generate_series(1, {changed}) t(i)
        """,
        "natural_key":    "user_id",
        "track_columns":  ["plan_tier"],
    },
}


# ── DatasetArtifact ────────────────────────────────────────────────────────

@dataclass
class DatasetArtifact:
    """
    A generated dataset available as a DuckDB VIEW and optionally as
    a Parquet file on disk.

    For benchmarks, always use the Parquet path so re-scanning is
    cheap and doesn't keep the full dataset in Python memory.
    """
    profile:      str
    tier:         str
    n_rows:       int
    n_changed:    int
    snapshot_path: str   = ""   # path to snapshot Parquet
    events_path:   str   = ""   # path to event batch Parquet
    ddl:           str   = ""
    natural_key:   str   = ""
    track_columns: list  = field(default_factory=list)
    metadata_columns: list = field(default_factory=list)
    _temp_dir:     str   = field(default="", repr=False)

    def cleanup(self) -> None:
        """Delete generated Parquet files."""
        for p in [self.snapshot_path, self.events_path]:
            if p and os.path.exists(p):
                os.unlink(p)
        if self._temp_dir and os.path.exists(self._temp_dir):
            try:
                os.rmdir(self._temp_dir)
            except OSError:
                pass

    def __enter__(self) -> "DatasetArtifact":
        return self

    def __exit__(self, *_) -> None:
        self.cleanup()


# ── Generator ─────────────────────────────────────────────────────────────

class BenchmarkDatasetGenerator:
    """
    Generates benchmark datasets at scale using DuckDB generate_series.
    No Python row loops — all data is produced inside DuckDB.

    Usage::

        gen = BenchmarkDatasetGenerator()

        # Small in-memory dataset
        with gen.generate("products", tier="s") as ds:
            proc.process(ds.snapshot_path, "dim_product")

        # Large Parquet-backed dataset
        with gen.generate("cnpj_empresa", tier="l") as ds:
            source = ParquetSource(ds.snapshot_path)
            proc.process(source, "empresa")
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
        """
        Generate snapshot + event batch Parquet files for *profile* at *tier* scale.

        Returns a DatasetArtifact (context manager — call cleanup() or use with-block).
        """
        if profile not in PROFILES:
            raise ValueError(
                f"Unknown profile {profile!r}. "
                f"Available: {sorted(PROFILES)}"
            )
        n_rows   = SCALE_TIERS[tier]
        rate     = change_rate if change_rate is not None else CHANGE_RATES[tier]
        n_changed = max(1, int(n_rows * rate))
        spec     = PROFILES[profile]
        tmp_dir  = tempfile.mkdtemp(prefix=f"sqldim_bench_{profile}_{tier}_", dir=self._tmp_root)
        snap_path   = os.path.join(tmp_dir, "snapshot.parquet")
        events_path = os.path.join(tmp_dir, "events.parquet")

        self._con.execute(spec["snapshot_view"].format(n=n_rows, changed=n_changed, seed=seed))
        self._con.execute(f"COPY _bench_snapshot TO '{snap_path}' (FORMAT parquet)")

        self._con.execute(spec["event_view"].format(n=n_rows, changed=n_changed, seed=seed))
        self._con.execute(f"COPY _bench_events TO '{events_path}' (FORMAT parquet)")

        return DatasetArtifact(
            profile=profile,
            tier=tier,
            n_rows=n_rows,
            n_changed=n_changed,
            snapshot_path=snap_path,
            events_path=events_path,
            ddl=spec["ddl"],
            natural_key=spec["natural_key"],
            track_columns=spec.get("track_columns", []),
            metadata_columns=spec.get("metadata_columns", []),
            _temp_dir=tmp_dir,
        )

    def generate_all_tiers(
        self,
        profile: str,
        tiers: list[str] | None = None,
    ) -> Iterator[DatasetArtifact]:
        """Yield artifacts for each tier, cleaning up each after use."""
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

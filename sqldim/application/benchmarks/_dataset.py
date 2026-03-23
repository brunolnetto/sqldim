"""
sqldim/benchmarks/_dataset.py
==============================
Benchmark dataset generator.

Schema metadata (DDL, natural_key, track/metadata columns) is derived from
the canonical source classes in :mod:`sqldim.application.datasets` — a single
source of truth that prevents schema drift between examples and benchmarks.

Data generation at every scale tier is performed via DuckDB
``generate_series`` SQL — this keeps throughput in the millions-of-rows/sec
range regardless of tier, which is necessary for xl/xxl scale tests.

Supported profiles
------------------
products        → ecommerce.ProductsSource schema
employees       → enterprise.EmployeesSource schema
customers       → ecommerce.CustomersSource schema
saas_users      → saas_growth.SaaSUsersSource schema
player_seasons  → nba_analytics.PlayerSeasonsSource schema
cnpj_empresa    → RFB empresa-style schema (no examples equivalent; DDL inline)
"""
from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass, field
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

# Change rate per tier for event batches
CHANGE_RATES = {
    "xs":  0.30,
    "s":   0.20,
    "m":   0.15,
    "l":   0.10,
    "xl":  0.05,
    "xxl": 0.02,
}


# ── Profile registry ──────────────────────────────────────────────────────
#
# Each entry maps a profile name to the SCD metadata needed by processors.
# DDL is resolved from the corresponding sqldim.application.datasets source
# class so it stays in sync with the rest of the library.

_PROFILE_META: dict[str, dict] = {
    "products": {
        "source_class":     "sqldim.application.datasets.domains.ecommerce.ProductsSource",
        "natural_key":      "product_id",
        "track_columns":    ["name", "category", "price"],
    },
    "employees": {
        "source_class":     "sqldim.application.datasets.domains.enterprise.EmployeesSource",
        "natural_key":      "employee_id",
        "track_columns":    ["title", "department"],
    },
    "customers": {
        "source_class":     "sqldim.application.datasets.domains.ecommerce.CustomersSource",
        "natural_key":      "customer_id",
        "track_columns":    ["full_name", "email"],
    },
    "saas_users": {
        "source_class":     "sqldim.application.datasets.domains.saas_growth.SaaSUsersSource",
        "natural_key":      "user_id",
        "track_columns":    ["plan_tier"],
    },
    "player_seasons": {
        "source_class":     "sqldim.application.datasets.domains.nba_analytics.PlayerSeasonsSource",
        "natural_key":      "player_id",
        "track_columns":    ["scoring_class"],
    },
    # ── cnpj_empresa: no equivalent in sqldim.application.datasets ───────────
    # DDL is defined inline here; all other profiles delegate to their source class.
    "cnpj_empresa": {
        "source_class":     None,
        "natural_key":      "cnpj_basico",
        "track_columns":    ["razao_social", "natureza_juridica",
                             "porte_empresa", "capital_social"],
        "metadata_columns": ["razao_social", "natureza_juridica",
                             "porte_empresa", "capital_social"],
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
    },
}


# ── DuckDB SQL generators ─────────────────────────────────────────────────
#
# Fast in-database generation via generate_series — no Python row loops.
# These cover every scale tier (xs → xxl) so benchmarks can operate on
# hundreds of millions of rows without Faker overhead.

_SQL_GENERATORS: dict[str, dict] = {

    "products": {
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
    },

    "employees": {
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
    },

    "customers": {
        "snapshot_view": """
            CREATE OR REPLACE VIEW _bench_snapshot AS
            SELECT
                i                                                  AS customer_id,
                concat('Customer_', i)                             AS full_name,
                concat('cust', i, '@bench.io')                     AS email,
                concat(i % 50, ' Main St')                         AS addr
            FROM generate_series(1, {n}) t(i)
        """,
        "event_view": """
            CREATE OR REPLACE VIEW _bench_events AS
            SELECT
                i                                                  AS customer_id,
                concat('Customer_', i, '_upd')                     AS full_name,
                concat('cust', i, '_new@bench.io')                 AS email,
                concat(i % 50, ' New Ave')                         AS addr
            FROM generate_series(1, {changed}) t(i)
        """,
    },

    "saas_users": {
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
                END                                                AS acq_source,
                CASE (i % 3)
                    WHEN 0 THEN 'desktop'
                    WHEN 1 THEN 'mobile'
                    ELSE        'tablet'
                END                                                AS device,
                (DATE '2020-01-01' + (i % 1825)::INTEGER)::VARCHAR         AS signup_date
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
                END                                                AS acq_source,
                CASE (i % 3)
                    WHEN 0 THEN 'desktop'
                    WHEN 1 THEN 'mobile'
                    ELSE        'tablet'
                END                                                AS device,
                (DATE '2020-01-01' + (i % 1825)::INTEGER)::VARCHAR         AS signup_date
            FROM generate_series(1, {changed}) t(i)
        """,
    },

    "player_seasons": {
        "snapshot_view": """
            CREATE OR REPLACE VIEW _bench_snapshot AS
            SELECT
                i                                                  AS player_id,
                concat('Player_', i)                               AS player_name,
                2000 + (i % 25)                                    AS season,
                CASE (i % 4)
                    WHEN 0 THEN 'Elite'
                    WHEN 1 THEN 'Star'
                    WHEN 2 THEN 'Starter'
                    ELSE        'Rotation'
                END                                                AS scoring_class,
                round(5.0 + (i % 30), 1)                           AS pts_per_game,
                round(2.0 + (i % 10), 1)                           AS reb_per_game,
                round(1.0 + (i % 8), 1)                            AS ast_per_game
            FROM generate_series(1, {n}) t(i)
        """,
        "event_view": """
            CREATE OR REPLACE VIEW _bench_events AS
            SELECT
                i                                                  AS player_id,
                concat('Player_', i)                               AS player_name,
                2000 + (i % 25)                                    AS season,
                CASE ((i + 1) % 4)
                    WHEN 0 THEN 'Elite'
                    WHEN 1 THEN 'Star'
                    WHEN 2 THEN 'Starter'
                    ELSE        'Rotation'
                END                                                AS scoring_class,
                round(5.0 + (i % 30) * 1.1, 1)                    AS pts_per_game,
                round(2.0 + (i % 10) * 1.05, 1)                   AS reb_per_game,
                round(1.0 + (i % 8) * 1.1, 1)                     AS ast_per_game
            FROM generate_series(1, {changed}) t(i)
        """,
    },

    "cnpj_empresa": {
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
    },
}


# ── DDL resolver ──────────────────────────────────────────────────────────

def _resolve_ddl(profile: str) -> str:
    """Return the DIM_DDL for *profile*, deriving it from the source class
    registered in :mod:`sqldim.application.datasets` where one exists.

    Profiles backed by a source class stay in sync with the rest of the
    library automatically — no duplicate schema definitions.
    """
    meta = _PROFILE_META[profile]

    if meta["source_class"] is None:
        # Inline DDL for profiles without an examples source
        return meta["ddl"]

    # Lazy import — examples sources require faker (dev dependency)
    module_path, cls_name = meta["source_class"].rsplit(".", 1)
    import importlib
    mod = importlib.import_module(module_path)
    src_cls = getattr(mod, cls_name)
    # Instantiate with n=1 (one Faker row) solely to access the DIM_DDL property
    return src_cls(n=1).DIM_DDL


# ── Cache resolved DDLs (computed once per process) ──────────────────────

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
    """
    A generated dataset available as Parquet files on disk.

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
    """
    Generates benchmark datasets at scale via DuckDB ``generate_series``.

    Schema metadata (DDL, natural_key, track_columns) is derived from the
    canonical source classes in :mod:`sqldim.application.datasets` where an
    equivalent source exists.  ``cnpj_empresa`` uses an inline DDL because
    it models a Brazil-specific regulatory schema with no examples analogue.

    Data generation itself always uses DuckDB SQL so row counts in the
    tens-of-millions remain fast (seconds, not minutes).

    Usage::

        gen = BenchmarkDatasetGenerator()

        with gen.generate("products", tier="s") as ds:
            proc.process(ParquetSource(ds.snapshot_path), "dim_product")

        with gen.generate("cnpj_empresa", tier="l") as ds:
            proc.process(ParquetSource(ds.snapshot_path), "empresa")
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
        Generate snapshot + event-batch Parquet files for *profile* at *tier*
        scale and return a :class:`DatasetArtifact` (context manager).
        """
        if profile not in _PROFILE_META:
            raise ValueError(
                f"Unknown profile {profile!r}. "
                f"Available: {sorted(_PROFILE_META)}"
            )
        if profile not in _SQL_GENERATORS:
            raise ValueError(
                f"No SQL generator registered for profile {profile!r}."
            )

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

        self._con.execute(
            sql_gen["snapshot_view"].format(n=n_rows, changed=n_changed, seed=seed)
        )
        self._con.execute(
            f"COPY _bench_snapshot TO '{snap_path}' (FORMAT parquet)"
        )
        self._con.execute(
            sql_gen["event_view"].format(n=n_rows, changed=n_changed, seed=seed)
        )
        self._con.execute(
            f"COPY _bench_events TO '{events_path}' (FORMAT parquet)"
        )

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

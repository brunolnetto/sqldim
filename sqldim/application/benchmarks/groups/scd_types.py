"""SCD type variety benchmarks (group I): Type3, Type4, Type5, Type6."""

from __future__ import annotations

import time
import traceback as _traceback

import duckdb

from sqldim.application.benchmarks._dataset import (
    BenchmarkDatasetGenerator,
)
from sqldim.application.benchmarks.infra import (
    BenchmarkResult,
    _select_tiers,
)
from sqldim.application.benchmarks.memory_probe import MemoryProbe

# ═══════════════════════════════════════════════════════════════════════════
# ── Group I  SCD type variety  (Type3 · Type4) ───────────────────────────
# ═══════════════════════════════════════════════════════════════════════════

_VARIETY_TIERS: dict[str, int] = {"xs": 1_000, "s": 10_000, "m": 100_000}


class _BenchSink:
    """Minimal DuckDB-native sink used by Type3 / Type4 / Type5 benchmarks."""

    def current_state_sql(self, table_name: str) -> str:
        return f"SELECT * FROM {table_name}"

    def write(
        self, con, view_name: str, table_name: str, batch_size: int = 100_000
    ) -> int:
        n = con.execute(f"SELECT count(*) FROM {view_name}").fetchone()[0]
        try:
            con.execute(f"INSERT INTO {table_name} BY NAME SELECT * FROM {view_name}")
        except Exception:
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {view_name}")
        return n

    def close_versions(
        self, con, table_name: str, nk_col: str, nk_view: str, valid_to: str
    ) -> int:
        con.execute(f"""
            UPDATE {table_name}
               SET is_current = FALSE, valid_to = '{valid_to}'
             WHERE {nk_col} IN (SELECT {nk_col} FROM {nk_view})
               AND is_current = TRUE
        """)
        return con.execute(f"SELECT count(*) FROM {nk_view}").fetchone()[0]

    def update_attributes(
        self,
        con,
        table_name: str,
        nk_col: str,
        updates_view: str,
        update_cols: list[str],
    ) -> int:
        for col in update_cols:
            con.execute(f"""
                UPDATE {table_name}
                   SET {col} = (
                       SELECT u.{col} FROM {updates_view} u
                        WHERE cast(u.{nk_col} as varchar)
                              = cast({table_name}.{nk_col} as varchar)
                   )
                 WHERE {nk_col} IN (SELECT {nk_col} FROM {updates_view})
                   AND is_current = TRUE
            """)
        return con.execute(f"SELECT count(*) FROM {updates_view}").fetchone()[0]

    def upsert(
        self,
        con,
        view_name: str,
        table_name: str,
        conflict_cols: list[str],
        returning_col: str,
        output_view: str,
    ) -> int:
        cols_str = ", ".join(conflict_cols)
        inner_join = " AND ".join(f"src.{c} = t.{c}" for c in conflict_cols)
        view_join = " AND ".join(f"t.{c} = v.{c}" for c in conflict_cols)
        src_cols = ", ".join(f"src.{c}" for c in conflict_cols)
        t_cols = ", ".join(f"t.{c}" for c in conflict_cols)
        con.execute(f"""
            INSERT INTO {table_name} ({returning_col}, {cols_str})
            SELECT
                (SELECT COALESCE(MAX({returning_col}), 0) FROM {table_name})
                    + row_number() OVER () AS {returning_col},
                {src_cols}
            FROM (
                SELECT DISTINCT {src_cols}
                FROM {view_name} src
                WHERE NOT EXISTS (
                    SELECT 1 FROM {table_name} t WHERE {inner_join}
                )
            ) src
        """)
        con.execute(f"""
            CREATE OR REPLACE VIEW {output_view} AS
            SELECT t.{returning_col},
                   {t_cols}
            FROM {table_name} t
            INNER JOIN (SELECT DISTINCT {cols_str} FROM {view_name}) v
                ON {view_join}
        """)
        return con.execute(f"SELECT count(*) FROM {output_view}").fetchone()[0]

    def rotate_attributes(
        self,
        con,
        table_name: str,
        nk_col: str,
        rotations_view: str,
        column_pairs: list[tuple[str, str]],
    ) -> int:
        """Rotate current→previous and write incoming for each changed row."""
        for curr_col, prev_col in column_pairs:
            con.execute(f"""
                UPDATE {table_name}
                   SET {prev_col} = {curr_col},
                       {curr_col} = (
                           SELECT r.{curr_col}
                           FROM {rotations_view} r
                           WHERE cast(r.{nk_col} as varchar)
                                 = cast({table_name}.{nk_col} as varchar)
                       )
                 WHERE {nk_col} IN (SELECT {nk_col} FROM {rotations_view})
                   AND is_current = TRUE
            """)
        return con.execute(f"SELECT count(*) FROM {rotations_view}").fetchone()[0]


def _i_type3_case(
    tier: str, n: int, sink: "_BenchSink", temp_dir: str
) -> BenchmarkResult:
    from sqldim.core.kimball.dimensions.scd.processors.lazy.type3._lazy_type3 import (
        LazyType3Processor,
    )

    cid = f"I-type3-{tier}"
    result = BenchmarkResult(
        case_id=cid,
        group="I",
        profile="scd3-employees",
        tier=tier,
        processor="LazyType3Processor",
        sink="InMemory",
        source="synthetic",
        phase="batch",
        n_rows=n,
        n_changed=0,
    )
    try:
        MemoryProbe.check_safe_to_run(label=cid)
        con = duckdb.connect()
        con.execute("""
            CREATE TABLE dim_scd3 (
                emp_id      VARCHAR,
                region      VARCHAR,
                prev_region VARCHAR,
                checksum    VARCHAR,
                is_current  BOOLEAN,
                valid_from  VARCHAR,
                valid_to    VARCHAR
            )
        """)
        con.execute(f"""
            CREATE OR REPLACE VIEW src AS
            SELECT 'emp_' || i::VARCHAR AS emp_id,
                   CASE WHEN i % 3 = 0 THEN 'East'
                        WHEN i % 3 = 1 THEN 'West'
                        ELSE 'North' END AS region
            FROM range(1, {n + 1}) t(i)
        """)
        proc = LazyType3Processor(
            natural_key="emp_id",
            column_pairs=[("region", "prev_region")],
            sink=sink,
            con=con,
        )
        probe = MemoryProbe(temp_dir=temp_dir, label=cid)
        with probe:
            t0 = time.perf_counter()
            scd = proc.process("src", "dim_scd3")
            result.wall_s = time.perf_counter() - t0
        m = probe.report
        result.peak_rss_gb = m.peak_rss_gb
        result.min_sys_avail_gb = m.min_sys_avail_gb
        result.total_spill_gb = m.total_spill_gb
        result.safety_breach = m.safety_breach
        result.breach_detail = m.breach_detail
        result.inserted = scd.inserted
        result.versioned = scd.versioned
        result.unchanged = scd.unchanged
        result.rows_per_sec = n / max(result.wall_s, 0.001)
        con.close()
    except RuntimeError as exc:
        result.ok = False
        result.error = f"SKIPPED: {exc}"
    except Exception as exc:
        result.ok = False
        result.error = f"{type(exc).__name__}: {exc}\n" + _traceback.format_exc()[-600:]
    return result


def _i_type4_case(
    tier: str, n: int, sink: "_BenchSink", temp_dir: str
) -> BenchmarkResult:
    from sqldim.core.kimball.dimensions.scd.processors.scd_engine import (
        LazyType4Processor,
    )

    cid = f"I-type4-{tier}"
    result = BenchmarkResult(
        case_id=cid,
        group="I",
        profile="scd4-customers",
        tier=tier,
        processor="LazyType4Processor",
        sink="InMemory",
        source="synthetic",
        phase="batch",
        n_rows=n,
        n_changed=0,
    )
    try:
        MemoryProbe.check_safe_to_run(label=cid)
        con = duckdb.connect()
        con.execute("""
            CREATE TABLE dim_mini (
                id          INTEGER,
                age_band    VARCHAR,
                income_band VARCHAR
            )
        """)
        con.execute("""
            CREATE TABLE dim_base (
                customer_id VARCHAR,
                name        VARCHAR,
                profile_sk  INTEGER,
                checksum    VARCHAR,
                valid_from  VARCHAR,
                valid_to    VARCHAR,
                is_current  BOOLEAN
            )
        """)
        con.execute(f"""
            CREATE OR REPLACE TABLE src AS
            SELECT 'cust_' || i::VARCHAR AS customer_id,
                   'Customer_' || i::VARCHAR AS name,
                   CASE WHEN i % 3 = 0 THEN 'Young'
                        WHEN i % 3 = 1 THEN 'Middle'
                        ELSE 'Senior' END AS age_band,
                   CASE WHEN i % 2 = 0 THEN 'Low'
                        ELSE 'High' END AS income_band
            FROM range(1, {n + 1}) t(i)
        """)
        proc = LazyType4Processor(
            natural_key="customer_id",
            base_columns=["name"],
            mini_dim_columns=["age_band", "income_band"],
            base_dim_table="dim_base",
            mini_dim_table="dim_mini",
            mini_dim_fk_col="profile_sk",
            mini_dim_id_col="id",
            sink=sink,
            con=con,
        )
        probe = MemoryProbe(temp_dir=temp_dir, label=cid)
        with probe:
            t0 = time.perf_counter()
            res = proc.process("src")
            result.wall_s = time.perf_counter() - t0
        m = probe.report
        result.peak_rss_gb = m.peak_rss_gb
        result.min_sys_avail_gb = m.min_sys_avail_gb
        result.total_spill_gb = m.total_spill_gb
        result.safety_breach = m.safety_breach
        result.breach_detail = m.breach_detail
        result.inserted = res.get("inserted", 0)
        result.versioned = res.get("versioned", 0)
        result.unchanged = res.get("unchanged", 0)
        result.rows_per_sec = n / max(result.wall_s, 0.001)
        con.close()
    except RuntimeError as exc:
        result.ok = False
        result.error = f"SKIPPED: {exc}"
    except Exception as exc:
        result.ok = False
        result.error = f"{type(exc).__name__}: {exc}\n" + _traceback.format_exc()[-600:]
    return result


def group_i_scd_type_variety(
    gen: BenchmarkDatasetGenerator,
    temp_dir: str,
    max_tier: str = "m",
    **_,
) -> list[BenchmarkResult]:
    """Group I — SCD Type3 and Type4 processor throughput at scale."""
    tier_order = ["xs", "s", "m"]
    active_tiers = _select_tiers(tier_order, _VARIETY_TIERS, max_tier)
    sink = _BenchSink()
    results: list[BenchmarkResult] = []
    for tier in active_tiers:
        n = _VARIETY_TIERS[tier]
        results.append(_i_type3_case(tier, n, sink, temp_dir))
        results.append(_i_type4_case(tier, n, sink, temp_dir))
    return results


# ═══════════════════════════════════════════════════════════════════════════
# ── Group J  Prebuilt dimension generation  (Date · Time) ────────────────
# ═══════════════════════════════════════════════════════════════════════════

_DATE_CASES: list[tuple[str, str, str]] = [
    ("J-date-1y", "2024-01-01", "2024-12-31"),  #   366 rows
    ("J-date-5y", "2020-01-01", "2024-12-31"),  # ~1 827 rows
    ("J-date-20y", "2005-01-01", "2024-12-31"),  # ~7 305 rows
    ("J-date-50y", "1975-01-01", "2024-12-31"),  # ~18 263 rows
]

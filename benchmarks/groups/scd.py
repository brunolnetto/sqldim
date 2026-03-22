"""
benchmarks/groups/scd.py
========================
SCD processing benchmarks (groups A–I).

Covers: scan regression, memory safety, throughput scaling,
stream-vs-batch, change rate sensitivity, processor comparison,
beyond-memory, source/sink matrix, SCD type variety.
"""
from __future__ import annotations

import os
import time
import traceback as _traceback

import duckdb
from sqlalchemy.pool import StaticPool

from benchmarks.dataset_gen import BenchmarkDatasetGenerator, DatasetArtifact, SCALE_TIERS
from benchmarks.infra import (
    BenchmarkResult,
    SOURCE_NAMES,
    _make_source,
    _remove_db,
    _configure,
    _run_scd2_batch,
    _run_metadata_batch,
)
from benchmarks.memory_probe import MemoryProbe
from benchmarks.scan_probe import DuckDBObjectTracker


def group_a_scan_regression(gen: BenchmarkDatasetGenerator, temp_dir: str, **_) -> list[BenchmarkResult]:
    results = []
    for profile in ["products", "employees", "saas_users"]:
        ds = gen.generate(profile, tier="xs")
        results.append(_run_scd2_batch(ds, f"A_{profile}_scan_regression", "A_scan_regression", "xs", temp_dir))
        ds.cleanup()
    ds = gen.generate("cnpj_empresa", tier="xs")
    results.append(_run_metadata_batch(ds, "A_cnpj_metadata_scan_regression", "A_scan_regression", "xs", temp_dir))
    ds.cleanup()
    return results


# ── Group B — Memory safety ───────────────────────────────────────────────

def group_b_memory_safety(gen: BenchmarkDatasetGenerator, temp_dir: str, **_) -> list[BenchmarkResult]:
    results = []
    cases = [
        ("products",     "s",  "scd2"),
        ("products",     "m",  "scd2"),
        ("employees",    "s",  "scd2"),
        ("employees",    "m",  "scd2"),
        ("saas_users",   "s",  "scd2"),
        ("saas_users",   "m",  "scd2"),
        ("cnpj_empresa", "s",  "meta"),
        ("cnpj_empresa", "m",  "meta"),
    ]
    for profile, tier, proc_type in cases:
        ds = gen.generate(profile, tier=tier)
        cid = f"B_{profile}_{tier}_memory"
        r = (_run_metadata_batch(ds, cid, "B_memory_safety", tier, temp_dir)
             if proc_type == "meta"
             else _run_scd2_batch(ds, cid, "B_memory_safety", tier, temp_dir))
        ds.cleanup(); results.append(r)
    return results


# ── Group C — Throughput scaling ─────────────────────────────────────────

def group_c_throughput_scaling(gen: BenchmarkDatasetGenerator, temp_dir: str, max_tier: str = "m", **_) -> list[BenchmarkResult]:
    tier_order = list(SCALE_TIERS.keys())
    max_idx    = tier_order.index(max_tier)
    results    = []
    for tier in tier_order[: max_idx + 1]:
        try:
            MemoryProbe.check_safe_to_run(f"C_{tier}")
        except RuntimeError as e:
            results.append(BenchmarkResult(
                case_id=f"C_products_{tier}_throughput", group="C_throughput",
                profile="products", tier=tier, processor="LazySCDProcessor",
                sink="DuckDBSink", phase="batch",
                n_rows=SCALE_TIERS[tier], n_changed=0, ok=False, error=str(e),
            ))
            continue
        ds = gen.generate("products", tier=tier)
        results.append(_run_scd2_batch(ds, f"C_products_{tier}_throughput", "C_throughput", tier, temp_dir))
        ds.cleanup()
    return results


# ── Group D — Streaming vs batch ─────────────────────────────────────────

def group_d_stream_vs_batch(gen: BenchmarkDatasetGenerator, temp_dir: str, **_) -> list[BenchmarkResult]:
    from sqldim.sinks.duckdb import DuckDBSink
    from sqldim.core.kimball.dimensions.scd.processors._lazy_type2 import LazySCDProcessor
    from sqldim.sources.sql import SQLSource
    from sqldim.sources.csv_stream import CSVStreamSource

    results = []
    tier = "m"
    ds   = gen.generate("products", tier=tier)

    r_batch = _run_scd2_batch(ds, "D_products_m_batch", "D_stream_vs_batch", tier, temp_dir)
    r_batch.phase = "batch"; results.append(r_batch)

    db_path  = os.path.join(temp_dir, "D_stream.duckdb")
    r_stream = BenchmarkResult(
        case_id="D_products_m_stream_100k", group="D_stream_vs_batch",
        profile="products", tier=tier, processor="LazySCDProcessor",
        sink="DuckDBSink", phase="stream_100k",
        n_rows=ds.n_rows, n_changed=ds.n_changed,
    )
    try:
        MemoryProbe.check_safe_to_run("D_stream")
        s = duckdb.connect(db_path); _configure(s, temp_dir)
        s.execute(ds.ddl.format(table="dim_products_stream")); s.close()

        with DuckDBSink(db_path) as sink:
            _configure(sink._con, temp_dir)
            proc = LazySCDProcessor(natural_key=ds.natural_key,
                                    track_columns=ds.track_columns,
                                    sink=sink, con=sink._con)
            batch_size = 100_000; offset = 0
            agg_ins = agg_ver = agg_unc = 0
            probe = MemoryProbe(temp_dir=temp_dir, label="D_stream")
            with probe:
                t0 = time.perf_counter()
                while offset < ds.n_rows:
                    frag = (f"SELECT * FROM read_parquet('{ds.snapshot_path}') "
                            f"ORDER BY {ds.natural_key} "
                            f"LIMIT {batch_size} OFFSET {offset}")
                    r = proc.process(SQLSource(frag), "dim_products_stream")
                    agg_ins += r.inserted; agg_ver += r.versioned; agg_unc += r.unchanged
                    offset  += batch_size
                r_stream.wall_s = time.perf_counter() - t0

        m = probe.report
        r_stream.peak_rss_gb      = m.peak_rss_gb
        r_stream.peak_duckdb_gb   = m.peak_duckdb_gb
        r_stream.min_sys_avail_gb = m.min_sys_avail_gb
        r_stream.total_spill_gb   = m.total_spill_gb
        r_stream.safety_breach    = m.safety_breach
        r_stream.inserted  = agg_ins; r_stream.versioned = agg_ver; r_stream.unchanged = agg_unc
        r_stream.rows_per_sec = ds.n_rows / max(r_stream.wall_s, 0.001)
    except RuntimeError as exc:
        r_stream.ok = False; r_stream.error = f"SKIPPED: {exc}"
    except Exception as exc:
        r_stream.ok = False
        r_stream.error = f"{type(exc).__name__}: {exc}\n" + _traceback.format_exc()[-400:]
    finally:
        _remove_db(db_path)

    results.append(r_stream)

    # ── D3: CSVStreamSource (row_number pagination, no OFFSET penalty) ──────
    csv_path    = os.path.join(temp_dir, "D_stream_products.csv")
    db_path_csv = os.path.join(temp_dir, "D_stream_csv.duckdb")
    r_csv = BenchmarkResult(
        case_id="D_products_m_stream_csv", group="D_stream_vs_batch",
        profile="products", tier=tier, processor="LazySCDProcessor",
        sink="DuckDBSink", phase="stream_csv_100k",
        n_rows=ds.n_rows, n_changed=ds.n_changed,
    )
    try:
        MemoryProbe.check_safe_to_run("D_stream_csv")
        # Export Parquet → CSV once so we can benchmark CSV pagination
        _tmp = duckdb.connect()
        _tmp.execute(
            f"COPY (SELECT * FROM read_parquet('{ds.snapshot_path}')) "
            f"TO '{csv_path}' (FORMAT CSV, HEADER TRUE)"
        )
        _tmp.close()

        s = duckdb.connect(db_path_csv); _configure(s, temp_dir)
        s.execute(ds.ddl.format(table="dim_products_csv")); s.close()

        with DuckDBSink(db_path_csv) as sink:
            _configure(sink._con, temp_dir)
            proc = LazySCDProcessor(natural_key=ds.natural_key,
                                    track_columns=ds.track_columns,
                                    sink=sink, con=sink._con)
            source = CSVStreamSource(csv_path)
            agg_ins = agg_ver = agg_unc = 0
            probe = MemoryProbe(temp_dir=temp_dir, label="D_stream_csv")
            with probe:
                t0 = time.perf_counter()
                for frag in source.stream(sink._con, batch_size=100_000):
                    r = proc.process(SQLSource(frag), "dim_products_csv")
                    agg_ins += r.inserted; agg_ver += r.versioned; agg_unc += r.unchanged
                r_csv.wall_s = time.perf_counter() - t0

        m = probe.report
        r_csv.peak_rss_gb      = m.peak_rss_gb
        r_csv.peak_duckdb_gb   = m.peak_duckdb_gb
        r_csv.min_sys_avail_gb = m.min_sys_avail_gb
        r_csv.total_spill_gb   = m.total_spill_gb
        r_csv.safety_breach    = m.safety_breach
        r_csv.inserted  = agg_ins; r_csv.versioned = agg_ver; r_csv.unchanged = agg_unc
        r_csv.rows_per_sec = ds.n_rows / max(r_csv.wall_s, 0.001)
    except RuntimeError as exc:
        r_csv.ok = False; r_csv.error = f"SKIPPED: {exc}"
    except Exception as exc:
        r_csv.ok = False
        r_csv.error = f"{type(exc).__name__}: {exc}\n" + _traceback.format_exc()[-400:]
    finally:
        _remove_db(db_path_csv)
        if os.path.exists(csv_path):
            os.remove(csv_path)

    results.append(r_csv)
    ds.cleanup(); return results


# ── Group E — Change rate sensitivity ────────────────────────────────────

def group_e_change_rate_sensitivity(gen: BenchmarkDatasetGenerator, temp_dir: str, **_) -> list[BenchmarkResult]:
    results = []
    for rate, label in [(0.01, "1pct"), (0.20, "20pct"), (0.99, "99pct")]:
        ds = gen.generate("products", tier="s", change_rate=rate)
        r  = _run_scd2_batch(ds, f"E_products_s_{label}", "E_change_rate", "s", temp_dir)
        r.phase = f"initial_load_{label}"; ds.cleanup(); results.append(r)
    return results


# ── Group F — Processor comparison ───────────────────────────────────────

def group_f_processor_comparison(gen: BenchmarkDatasetGenerator, temp_dir: str, **_) -> list[BenchmarkResult]:
    from sqldim.sinks.duckdb import DuckDBSink
    from sqldim.core.kimball.dimensions.scd.processors._lazy_type3_6 import LazyType6Processor
    from sqldim.sources.parquet import ParquetSource

    results = []
    tier = "s"

    ds = gen.generate("products", tier=tier)
    r  = _run_scd2_batch(ds, "F_products_SCD2", "F_processor_comparison", tier, temp_dir)
    r.processor = "LazySCDProcessor"; results.append(r); ds.cleanup()

    ds = gen.generate("cnpj_empresa", tier=tier)
    r  = _run_metadata_batch(ds, "F_cnpj_Metadata", "F_processor_comparison", tier, temp_dir)
    r.processor = "LazySCDMetadataProcessor"; results.append(r); ds.cleanup()

    ds      = gen.generate("employees", tier=tier)
    db_path = os.path.join(temp_dir, "F_type6.duckdb")
    r6      = BenchmarkResult(
        case_id="F_employees_Type6", group="F_processor_comparison",
        profile="employees", tier=tier, processor="LazyType6Processor",
        sink="DuckDBSink", phase="batch",
        n_rows=ds.n_rows, n_changed=ds.n_changed,
    )
    try:
        MemoryProbe.check_safe_to_run("F_type6")
        s = duckdb.connect(db_path); _configure(s, temp_dir)
        s.execute(ds.ddl.format(table="dim_emp_t6")); s.close()

        with DuckDBSink(db_path) as sink:
            _configure(sink._con, temp_dir)
            tracker = DuckDBObjectTracker(sink._con); tracker.wrap()
            proc  = LazyType6Processor(natural_key="employee_id",
                                       type1_columns=["title"],
                                       type2_columns=["department"],
                                       sink=sink, con=sink._con)
            probe = MemoryProbe(temp_dir=temp_dir, label="F_type6")
            with probe:
                t0  = time.perf_counter()
                scd = proc.process(ParquetSource(ds.snapshot_path), "dim_emp_t6")
                r6.wall_s = time.perf_counter() - t0
            tracker.snapshot(); tracker.unwrap()

        m = probe.report; obj = tracker.report()
        r6.peak_rss_gb = m.peak_rss_gb; r6.peak_duckdb_gb = m.peak_duckdb_gb
        r6.min_sys_avail_gb = m.min_sys_avail_gb; r6.total_spill_gb = m.total_spill_gb
        r6.safety_breach = m.safety_breach; r6.scan_regression = obj["regression_detected"]
        r6.current_state_as_table = obj["current_state_as_table"]
        r6.inserted = scd.inserted; r6.versioned = scd.versioned
        r6.rows_per_sec = ds.n_rows / max(r6.wall_s, 0.001)
    except RuntimeError as exc:
        r6.ok = False; r6.error = f"SKIPPED: {exc}"
    except Exception as exc:
        r6.ok = False
        r6.error = f"{type(exc).__name__}: {exc}\n" + _traceback.format_exc()[-400:]
    finally:
        _remove_db(db_path)

    ds.cleanup(); results.append(r6)
    return results


# ── Group G — Spill simulation ────────────────────────────────────────────

def group_g_beyond_memory(gen: BenchmarkDatasetGenerator, temp_dir: str, max_tier: str = "m", **_) -> list[BenchmarkResult]:
    # Tight DuckDB memory cap: small enough to force spill on m/l/xl tier working
    # sets (typically 200 MB – 4 GB) without being so low that DuckDB can't start.
    SPILL_LIMIT_GB = 0.5
    results = []

    tier_order   = list(SCALE_TIERS.keys())          # ["xs","s","m","l","xl","xxl"]
    max_idx      = tier_order.index(max_tier)
    tiers_to_run = tier_order[:max_idx + 1]

    for tier in tiers_to_run:
        try:
            MemoryProbe.check_safe_to_run(f"G_spill_{tier}")
        except RuntimeError as e:
            results.append(BenchmarkResult(
                case_id=f"G_cnpj_{tier}_spill", group="G_beyond_memory",
                profile="cnpj_empresa", tier=tier,
                processor="LazySCDMetadataProcessor", sink="DuckDBSink",
                phase="batch_tight_memory",
                n_rows=SCALE_TIERS[tier], n_changed=0, ok=False, error=str(e),
            ))
            continue

        spill_dir = os.path.join(temp_dir, f"spill_{tier}")
        os.makedirs(spill_dir, exist_ok=True)
        ds = gen.generate("cnpj_empresa", tier=tier)
        r  = _run_metadata_batch(
            ds, f"G_cnpj_{tier}_spill", "G_beyond_memory", tier,
            temp_dir=temp_dir,
            mem_limit_override_gb=SPILL_LIMIT_GB,
            spill_dir=spill_dir,
        )
        r.phase = f"tight_{SPILL_LIMIT_GB:.1f}GB_limit"
        ds.cleanup(); results.append(r)
        shutil.rmtree(spill_dir, ignore_errors=True)

    return results


# ── Group H — Source / Sink matrix ───────────────────────────────────────

def group_h_source_sink_matrix(
    gen: BenchmarkDatasetGenerator,
    temp_dir: str,
    **_,
) -> list[BenchmarkResult]:
    """Run the same workload for every registered source, DuckDB sink.

    Enables direct apples-to-apples comparison of:
      - ``parquet`` — zero-copy columnar scan, best-case baseline
      - ``csv``     — CSV parsing overhead, reflects ingestion pipelines

    Both share identical SCD logic so any throughput delta is purely the
    source adapter's parse cost.  Fixed at tier ``s`` (100K rows) to keep
    Group H fast.
    """
    results = []
    tier    = "s"
    profile = "products"

    for src in SOURCE_NAMES:
        cid = f"H_{profile}_{tier}_{src}_duckdb"
        try:
            MemoryProbe.check_safe_to_run(cid)
        except RuntimeError as exc:
            results.append(BenchmarkResult(
                case_id=cid, group="H_source_sink_matrix",
                profile=profile, tier=tier,
                processor="LazySCDProcessor", sink="DuckDBSink",
                source=src, phase="h_source_matrix",
                n_rows=SCALE_TIERS[tier], n_changed=0,
                ok=False, error=str(exc),
            ))
            continue
        ds = gen.generate(profile, tier=tier)
        r  = _run_scd2_batch(
            ds, cid, "H_source_sink_matrix", tier, temp_dir,
            source_name=src,
        )
        r.phase = "h_source_matrix"
        ds.cleanup()
        results.append(r)

    return results

# ═══════════════════════════════════════════════════════════════════════════
# ── Group I  SCD type variety  (Type3 · Type4) ───────────────────────────
# ═══════════════════════════════════════════════════════════════════════════

_VARIETY_TIERS: dict[str, int] = {"xs": 1_000, "s": 10_000, "m": 100_000}


class _BenchSink:
    """Minimal DuckDB-native sink used by Type3 / Type4 / Type5 benchmarks."""

    def current_state_sql(self, table_name: str) -> str:
        return f"SELECT * FROM {table_name}"

    def write(self, con, view_name: str, table_name: str,
              batch_size: int = 100_000) -> int:
        n = con.execute(f"SELECT count(*) FROM {view_name}").fetchone()[0]
        try:
            con.execute(
                f"INSERT INTO {table_name} BY NAME SELECT * FROM {view_name}"
            )
        except Exception:
            con.execute(
                f"CREATE TABLE {table_name} AS SELECT * FROM {view_name}"
            )
        return n

    def close_versions(self, con, table_name: str, nk_col: str,
                       nk_view: str, valid_to: str) -> int:
        con.execute(f"""
            UPDATE {table_name}
               SET is_current = FALSE, valid_to = '{valid_to}'
             WHERE {nk_col} IN (SELECT {nk_col} FROM {nk_view})
               AND is_current = TRUE
        """)
        return con.execute(f"SELECT count(*) FROM {nk_view}").fetchone()[0]

    def update_attributes(self, con, table_name: str, nk_col: str,
                          updates_view: str, update_cols: list[str]) -> int:
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

    def upsert(self, con, view_name: str, table_name: str,
               conflict_cols: list[str], returning_col: str,
               output_view: str) -> int:
        cols_str   = ", ".join(conflict_cols)
        inner_join = " AND ".join(f"src.{c} = t.{c}" for c in conflict_cols)
        view_join  = " AND ".join(f"t.{c} = v.{c}" for c in conflict_cols)
        con.execute(f"""
            INSERT INTO {table_name} ({returning_col}, {cols_str})
            SELECT
                (SELECT COALESCE(MAX({returning_col}), 0) FROM {table_name})
                    + row_number() OVER () AS {returning_col},
                {', '.join(f'src.{c}' for c in conflict_cols)}
            FROM (
                SELECT DISTINCT {', '.join(f'src.{c}' for c in conflict_cols)}
                FROM {view_name} src
                WHERE NOT EXISTS (
                    SELECT 1 FROM {table_name} t WHERE {inner_join}
                )
            ) src
        """)
        con.execute(f"""
            CREATE OR REPLACE VIEW {output_view} AS
            SELECT t.{returning_col},
                   {', '.join(f't.{c}' for c in conflict_cols)}
            FROM {table_name} t
            INNER JOIN (SELECT DISTINCT {cols_str} FROM {view_name}) v
                ON {view_join}
        """)
        return con.execute(f"SELECT count(*) FROM {output_view}").fetchone()[0]

    def rotate_attributes(self, con, table_name: str, nk_col: str,
                          rotations_view: str,
                          column_pairs: list[tuple[str, str]]) -> int:
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
        return con.execute(
            f"SELECT count(*) FROM {rotations_view}"
        ).fetchone()[0]


def group_i_scd_type_variety(
    gen: BenchmarkDatasetGenerator,
    temp_dir: str,
    max_tier: str = "m",
    **_,
) -> list[BenchmarkResult]:
    """Group I — SCD Type3 and Type4 processor throughput at scale."""
    from sqldim.core.kimball.dimensions.scd.processors._lazy_type3_6 import (
        LazyType3Processor,
    )
    from sqldim.core.kimball.dimensions.scd.processors.scd_engine import (
        LazyType4Processor,
    )

    tier_order  = ["xs", "s", "m"]
    _max = max_tier if max_tier in tier_order else tier_order[-1]
    active_tiers = [t for t in tier_order
                    if t in _VARIETY_TIERS
                    and tier_order.index(t) <= tier_order.index(_max)]
    sink    = _BenchSink()
    results: list[BenchmarkResult] = []

    for tier in active_tiers:
        n = _VARIETY_TIERS[tier]

        # ── Type3: emp_id + region (current / previous) ────────────────────
        cid = f"I-type3-{tier}"
        result = BenchmarkResult(
            case_id=cid, group="I", profile="scd3-employees", tier=tier,
            processor="LazyType3Processor", sink="InMemory",
            source="synthetic", phase="batch", n_rows=n, n_changed=0,
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
            proc  = LazyType3Processor(
                natural_key="emp_id",
                column_pairs=[("region", "prev_region")],
                sink=sink, con=con,
            )
            probe = MemoryProbe(temp_dir=temp_dir, label=cid)
            with probe:
                t0  = time.perf_counter()
                scd = proc.process("src", "dim_scd3")
                result.wall_s = time.perf_counter() - t0
            m = probe.report
            result.peak_rss_gb      = m.peak_rss_gb
            result.min_sys_avail_gb = m.min_sys_avail_gb
            result.total_spill_gb   = m.total_spill_gb
            result.safety_breach    = m.safety_breach
            result.breach_detail    = m.breach_detail
            result.inserted         = scd.inserted
            result.versioned        = scd.versioned
            result.unchanged        = scd.unchanged
            result.rows_per_sec     = n / max(result.wall_s, 0.001)
            con.close()
        except RuntimeError as exc:
            result.ok = False; result.error = f"SKIPPED: {exc}"
        except Exception as exc:
            result.ok    = False
            result.error = (f"{type(exc).__name__}: {exc}\n"
                            + _traceback.format_exc()[-600:])
        results.append(result)

        # ── Type4: customer + mini-dimension (age/income) ──────────────────
        cid = f"I-type4-{tier}"
        result = BenchmarkResult(
            case_id=cid, group="I", profile="scd4-customers", tier=tier,
            processor="LazyType4Processor", sink="InMemory",
            source="synthetic", phase="batch", n_rows=n, n_changed=0,
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
            proc  = LazyType4Processor(
                natural_key="customer_id",
                base_columns=["name"],
                mini_dim_columns=["age_band", "income_band"],
                base_dim_table="dim_base",
                mini_dim_table="dim_mini",
                mini_dim_fk_col="profile_sk",
                mini_dim_id_col="id",
                sink=sink, con=con,
            )
            probe = MemoryProbe(temp_dir=temp_dir, label=cid)
            with probe:
                t0  = time.perf_counter()
                res = proc.process("src")
                result.wall_s = time.perf_counter() - t0
            m = probe.report
            result.peak_rss_gb      = m.peak_rss_gb
            result.min_sys_avail_gb = m.min_sys_avail_gb
            result.total_spill_gb   = m.total_spill_gb
            result.safety_breach    = m.safety_breach
            result.breach_detail    = m.breach_detail
            result.inserted         = res.get("inserted", 0)
            result.versioned        = res.get("versioned", 0)
            result.unchanged        = res.get("unchanged", 0)
            result.rows_per_sec     = n / max(result.wall_s, 0.001)
            con.close()
        except RuntimeError as exc:
            result.ok = False; result.error = f"SKIPPED: {exc}"
        except Exception as exc:
            result.ok    = False
            result.error = (f"{type(exc).__name__}: {exc}\n"
                            + _traceback.format_exc()[-600:])
        results.append(result)

    return results


# ═══════════════════════════════════════════════════════════════════════════
# ── Group J  Prebuilt dimension generation  (Date · Time) ────────────────
# ═══════════════════════════════════════════════════════════════════════════

_DATE_CASES: list[tuple[str, str, str]] = [
    ("J-date-1y",  "2024-01-01", "2024-12-31"),   #   366 rows
    ("J-date-5y",  "2020-01-01", "2024-12-31"),   # ~1 827 rows
    ("J-date-20y", "2005-01-01", "2024-12-31"),   # ~7 305 rows
    ("J-date-50y", "1975-01-01", "2024-12-31"),   # ~18 263 rows
]



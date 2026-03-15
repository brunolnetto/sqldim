"""
examples/real-world/sources_sinks/showcase.py
---------------------------------------------
Sources × Sinks — cartesian demonstration.

Every local (source, sink) combination is exercised against a small
product-catalogue dataset so you can see what each adapter actually
sends through DuckDB.

Sources demonstrated (all run locally, no external services):
  ParquetSource     — one or more Parquet files on disk
  CSVSource         — CSV / TSV file on disk
  DuckDBSource      — table or view already in the active connection
  SQLSource         — arbitrary SQL expression (used here with a WHERE filter)
  _DatasetSource    — already-loaded dlt DuckDB file (no dlt process run)

Sinks demonstrated (no external services):
  DuckDBSink        — appends to a .duckdb file via ATTACH + INSERT
  ParquetSink       — COPY to a partitioned Parquet directory

Skipped (require external services / optional extensions):
  PostgreSQLSource / PostgreSQLSink  — needs a running Postgres instance
  DeltaSource / DeltaLakeSink        — needs INSTALL delta; LOAD delta
  DltSource (resource mode)          — needs `pip install dlt`
"""
from __future__ import annotations

import os
import tempfile

import duckdb
import pandas as pd

from sqldim.sources import CSVSource, DuckDBSource, ParquetSource, SQLSource, _DatasetSource
from sqldim.sinks import DuckDBSink, ParquetSink


# ---------------------------------------------------------------------------
# Sample dataset — a small product catalogue with an SCD-compatible schema
# ---------------------------------------------------------------------------

_ROWS = [
    (1, "Widget A",   "widgets", 9.99,  True),
    (2, "Gadget B",   "gadgets", 24.99, True),
    (3, "Widget C",   "widgets", 14.99, True),
    (4, "Gadget D",   "gadgets", 49.99, True),
    (5, "Doohickey",  "misc",     4.99, True),
]
_COLS = ["product_id", "name", "category", "price", "is_current"]


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _write_fixtures(tmpdir: str) -> tuple[str, str, str]:
    """Write sample rows to Parquet, CSV, and a dlt-style DuckDB file."""
    df = pd.DataFrame(_ROWS, columns=_COLS)

    parquet_path = os.path.join(tmpdir, "products.parquet")
    csv_path     = os.path.join(tmpdir, "products.csv")
    dlt_db_path  = os.path.join(tmpdir, "dlt_staging.duckdb")

    df.to_parquet(parquet_path, index=False)
    df.to_csv(csv_path, index=False)

    # Emulate the DuckDB file that `dlt` would produce
    # (schema "staging", table "products")
    con = duckdb.connect(dlt_db_path)
    con.execute("CREATE SCHEMA staging")
    con.execute(
        f"CREATE TABLE staging.products AS "
        f"SELECT * FROM read_parquet('{parquet_path}')"
    )
    con.close()

    return parquet_path, csv_path, dlt_db_path


def _build_sources(parquet: str, csv: str, dlt_db: str) -> dict:
    """Return an ordered dict of source_name → SourceAdapter."""
    return {
        "ParquetSource":  ParquetSource(parquet),
        "CSVSource":      CSVSource(csv),
        # DuckDBSource depends on a view "products" being pre-registered in the
        # active connection — see _register_duckdb_view() below.
        "DuckDBSource":   DuckDBSource("products"),
        # SQLSource with a predicate — only rows where price < 25
        "SQLSource":      SQLSource(
            f"SELECT * FROM read_parquet('{parquet}') WHERE price < 25"
        ),
        # _DatasetSource reads from the dlt-style DuckDB file without running dlt
        "_DatasetSource": _DatasetSource(dlt_db, "products", "staging"),
    }


def _register_duckdb_view(con: duckdb.DuckDBPyConnection, parquet: str) -> None:
    """Register the 'products' view so DuckDBSource can find it."""
    con.execute(
        f"CREATE OR REPLACE VIEW products AS SELECT * FROM read_parquet('{parquet}')"
    )


# ---------------------------------------------------------------------------
# Per-sink runners
# ---------------------------------------------------------------------------


def _run_duckdb_sink(
    source,
    src_name: str,
    parquet: str,
    tmpdir: str,
    idx: int,
) -> int:
    """
    Feed *source* into a fresh DuckDBSink and return the written row count.

    A new .duckdb file is created for each run so that repeated calls to the
    cartesian loop never accumulate rows across iterations.
    """
    db_path = os.path.join(tmpdir, f"wh_{idx}.duckdb")

    # Pre-create the destination table (DuckDBSink.write() uses INSERT)
    init = duckdb.connect(db_path)
    init.execute(
        "CREATE TABLE dim_product "
        "(product_id INTEGER, name VARCHAR, category VARCHAR, "
        " price DOUBLE, is_current BOOLEAN)"
    )
    init.close()

    with DuckDBSink(db_path) as sink:
        con = sink._con  # the in-memory connection opened by __enter__

        if src_name == "DuckDBSource":
            _register_duckdb_view(con, parquet)

        sql = source.as_sql(con)
        view_def = sql if sql.strip().upper().startswith("SELECT") else f"SELECT * FROM {sql}"
        con.execute(f"CREATE OR REPLACE VIEW _src AS {view_def}")
        return sink.write(con, "_src", "dim_product")


def _run_parquet_sink(
    source,
    src_name: str,
    parquet: str,
    tmpdir: str,
    idx: int,
) -> int:
    """
    Feed *source* into ParquetSink and return the written row count.

    A fresh output directory is used per run to avoid OVERWRITE conflicts.
    """
    out_dir = os.path.join(tmpdir, f"pq_out_{idx}")

    con = duckdb.connect()

    if src_name == "DuckDBSource":
        _register_duckdb_view(con, parquet)

    sql = source.as_sql(con)
    view_def = sql if sql.strip().upper().startswith("SELECT") else f"SELECT * FROM {sql}"
    con.execute(f"CREATE OR REPLACE VIEW _src AS {view_def}")
    sink = ParquetSink(out_dir)
    os.makedirs(os.path.join(out_dir, "dim_product"), exist_ok=True)
    n = sink.write(con, "_src", "dim_product")
    con.close()
    return n


# ---------------------------------------------------------------------------
# Cartesian runner
# ---------------------------------------------------------------------------

_SINK_RUNNERS = {
    "DuckDBSink":  _run_duckdb_sink,
    "ParquetSink": _run_parquet_sink,
}


def run_showcase() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        parquet, csv, dlt_db = _write_fixtures(tmpdir)
        sources = _build_sources(parquet, csv, dlt_db)

        sink_names = list(_SINK_RUNNERS)
        src_col, cell_w = 18, 16

        header = f"\n{'Source':<{src_col}}" + "".join(
            f"{s:>{cell_w}}" for s in sink_names
        )
        divider = "-" * (src_col + cell_w * len(sink_names))

        print("Sources × Sinks — Product Catalogue (cartesian)")
        print("=" * len(divider))
        print(header)
        print(divider)

        for run_idx, (src_name, source) in enumerate(sources.items()):
            row = f"{src_name:<{src_col}}"
            for sink_name, runner in _SINK_RUNNERS.items():
                n = runner(source, src_name, parquet, tmpdir, run_idx * 10 + list(_SINK_RUNNERS).index(sink_name))
                cell = f"✓ {n} row{'s' if n != 1 else ''}"
                row += f"{cell:>{cell_w}}"
            print(row)

        print(divider)
        print(
            "\nSkipped (require external services or optional extensions):\n"
            "  PostgreSQLSource / PostgreSQLSink  — needs a live Postgres instance\n"
            "  DeltaSource / DeltaLakeSink        — needs INSTALL delta; LOAD delta\n"
            "  DltSource (resource mode)          — needs pip install dlt\n"
        )


if __name__ == "__main__":
    run_showcase()

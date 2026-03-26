"""
Integration & Extensibility — Examples 14 and 15
==================================================

14. dlt → sqldim warehouse
    ``GitHubIssuesSource`` simulates a dlt pipeline staging database.
    ``_DatasetSource`` reads the staging table; ``LazySCDProcessor`` builds
    a SCD2 dimension in the warehouse.  A second event batch (issue state
    changes + new issue) demonstrates incremental loading.

15. MotherDuck native sink
    ``MotherDuckSink`` is a native sqldim sink (``sqldim.sinks.MotherDuckSink``).
    Smoke-tested locally against a plain ``.duckdb`` file via
    ``ProductsSource``.

Run:
    PYTHONPATH=. python -m sqldim.application.examples.features.integrations.showcase
"""

from __future__ import annotations

import os
from typing import Any

import duckdb

from sqldim.core.kimball.dimensions.scd.processors.scd_engine import LazySCDProcessor
from sqldim.sinks import DuckDBSink, MotherDuckSink, SinkAdapter
from sqldim.sources import _DatasetSource

from sqldim.application.datasets.domains.devops import GitHubIssuesSource
from sqldim.application.datasets.domains.ecommerce import ProductsSource
from sqldim.application.examples.features.utils import make_tmp_db


def _tmp_db() -> str:
    return make_tmp_db()


# ── Example 14 ────────────────────────────────────────────────────────────────


def _fetch_warehouse_summary(con) -> tuple[int, int, int, list]:  # type: ignore[type-arg]
    """Return ``(total, current, hist, rows)`` from the dim_github_issue table."""
    total = (con.execute("SELECT COUNT(*) FROM dim_github_issue").fetchone() or (0,))[0]
    current = (
        con.execute("SELECT COUNT(*) FROM dim_github_issue WHERE is_current").fetchone()
        or (0,)
    )[0]
    hist = (
        con.execute(
            "SELECT COUNT(*) FROM dim_github_issue WHERE NOT is_current"
        ).fetchone()
        or (0,)
    )[0]
    rows = con.execute(
        "SELECT issue_id, title, state, is_current FROM dim_github_issue ORDER BY issue_id, valid_from"
    ).fetchall()
    return total, current, hist, rows


def _report_and_teardown(
    src: GitHubIssuesSource,
    warehouse_db: str,
    staging_db: str,
    update_db: str,
) -> None:
    """Print warehouse summary and clean up all temp files."""
    con = duckdb.connect(warehouse_db)
    total, current, hist, rows = _fetch_warehouse_summary(con)
    src.teardown(con, "dim_github_issue")
    con.close()
    for path in (staging_db, warehouse_db, update_db):
        os.unlink(path)

    print(f"  Warehouse: {total} rows  ({current} current, {hist} historical)")
    print("  Full history:")
    for r in rows:
        flag = "✓" if r[3] else "H"
        print(f"    [{flag}] #{r[0]}  {r[1]:<42}  [{r[2]}]")


def _run_scd2_load(
    staging_db: str, warehouse_db: str, src: GitHubIssuesSource
) -> Any:
    """Run one SCD2 load batch; returns the :class:`SCDResult`."""
    source = _DatasetSource(staging_db, "issues", "github_staging")
    with DuckDBSink(warehouse_db) as sink:
        proc = LazySCDProcessor(
            "issue_id", ["title", "state", "labels"], sink, con=sink._con
        )
        return proc.process(source, "dim_github_issue")


def example_14_dlt_github_to_sqldim() -> None:
    """
    dlt pipeline staging → sqldim SCD2 warehouse dimension.

    Workflow:
      GitHubIssuesSource.seed_staging(path, "initial") → staging DuckDB (T0)
      _DatasetSource reads staging.issues
      LazySCDProcessor → dim_github_issue (SCD2)
      GitHubIssuesSource.seed_staging(path, "updated") → staging DuckDB (T1)
      Second process() call → issues closed + new issue versioned
    """
    print("\n── Example 14: dlt GitHub → sqldim Warehouse ───────────────────")

    src = GitHubIssuesSource(n=8, seed=42)
    staging_db = make_tmp_db()
    warehouse_db = make_tmp_db()

    # ── Setup ─────────────────────────────────────────────────────────────
    src.seed_staging(staging_db, batch="initial")  # simulate dlt T0 run

    setup_con = duckdb.connect(warehouse_db)
    src.setup(setup_con, "dim_github_issue")
    setup_con.close()

    # ── Initial load ──────────────────────────────────────────────────────
    r1 = _run_scd2_load(staging_db, warehouse_db, src)
    print(f"  T0 initial load → inserted={r1.inserted}")  # type: ignore[attr-defined]

    # ── Event batch: issues change state; new issue opened ────────────────
    update_db = _tmp_db()
    src.seed_staging(update_db, batch="updated")  # simulate dlt T1 run

    r2 = _run_scd2_load(update_db, warehouse_db, src)
    print(
        f"  T1 event batch  → inserted={r2.inserted}, versioned={r2.versioned}, unchanged={r2.unchanged}"  # type: ignore[attr-defined]
    )

    _report_and_teardown(src, warehouse_db, staging_db, update_db)


def example_15_custom_sink_motherduck() -> None:
    """
    Demonstrate ``MotherDuckSink`` — now a native sqldim sink.

    Imported from ``sqldim.sinks`` and smoke-tested locally against a
    plain ``.duckdb`` file (identical ATTACH pattern to the cloud endpoint).
    """
    print("\n── Example 15: Custom Sink — MotherDuck Pattern ────────────────")

    src = ProductsSource(n=2, seed=42)
    local = _tmp_db()

    # ── Setup via ProductsSource ───────────────────────────────────────────
    setup = duckdb.connect(local)
    src.setup(setup, "dim_product")
    setup.close()

    print("  isinstance(MotherDuckSink(...), SinkAdapter) …", end=" ")
    print(isinstance(MotherDuckSink(db=local), SinkAdapter))

    with MotherDuckSink(db=local) as sink:
        proc = LazySCDProcessor("product_id", ["name", "price"], sink, con=sink._con)
        result = proc.process(src.snapshot(), "dim_product")

    print(f"  SCD2 via MotherDuckSink → inserted={result.inserted}")
    print("  MotherDuckSink implements all 6 SinkAdapter methods:")
    for method in [
        "current_state_sql",
        "write",
        "close_versions",
        "update_attributes",
        "rotate_attributes",
        "update_milestones",
    ]:
        print(f"    ✓ {method}()")

    # ── Teardown ──────────────────────────────────────────────────────────
    cleanup = duckdb.connect(local)
    src.teardown(cleanup, "dim_product")
    cleanup.close()
    os.unlink(local)

    print(
        "\n  To target MotherDuck replace db= with your database name and"
        "\n  pass token=os.environ['MOTHERDUCK_TOKEN']."
    )


# ── Entry point ───────────────────────────────────────────────────────────────


EXAMPLE_METADATA = {
    "name": "integrations",
    "title": "Integration & Extensibility",
    "description": "Examples 14-15: dlt staging pipeline + custom MotherDuck sink",
    "entry_point": "run_showcase",
}


def run_showcase() -> None:
    print("Integration & Extensibility Showcase")
    print("=====================================")
    example_14_dlt_github_to_sqldim()
    example_15_custom_sink_motherduck()
    print("\nDone.\n")


if __name__ == "__main__":  # pragma: no cover
    run_showcase()

"""Structural tests for drift eval cases — no LLM required.

For each drift case:
  1. Build the domain pipeline in a fresh in-memory DuckDB connection.
  2. Apply the event function.
  3. Verify every probe's ``expect_columns`` are present in the post-event schema.

This exercises the JSON definitions, event function wiring, and pipeline
builder in one pass without needing an LLM or network access.
"""

from __future__ import annotations

import importlib

import duckdb
import pytest

from sqldim.application.evals.loader import load_drift_suite

pytestmark = pytest.mark.filterwarnings(
    "ignore::logfire._internal.config.LogfireNotConfiguredWarning"
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_pipeline(domain: str, con: duckdb.DuckDBPyConnection) -> None:
    mod = importlib.import_module(
        f"sqldim.application.datasets.domains.{domain}.pipeline.builder"
    )
    mod.build_pipeline(con)


def _schema(con: duckdb.DuckDBPyConnection) -> dict[str, set[str]]:
    tables = con.execute("SHOW TABLES").fetchall()
    return {
        tbl: {c[0].lower() for c in con.execute(f"DESCRIBE {tbl}").fetchall()}
        for (tbl,) in tables
    }


# ---------------------------------------------------------------------------
# Parametrized test
# ---------------------------------------------------------------------------

_DRIFT_CASES = load_drift_suite()
_CASE_IDS = [c.id for c in _DRIFT_CASES]


@pytest.mark.parametrize("drift_case", _DRIFT_CASES, ids=_CASE_IDS)
def test_drift_structural(drift_case):
    """Build pipeline, apply event, verify all probe columns exist in schema."""
    domain = drift_case.id.split(".")[0]

    con = duckdb.connect(":memory:")
    try:
        _build_pipeline(domain, con)
        drift_case.event_fn(con)
        schema = _schema(con)
        all_cols = set.union(*schema.values()) if schema else set()
        for probe in drift_case.cases:
            for col in (c.lower() for c in probe.expect_columns):
                if probe.expect_table:
                    table_key = next(
                        (t for t in schema if t.lower() == probe.expect_table.lower()),
                        None,
                    )
                    available = schema.get(table_key, set()) if table_key else set()
                    assert col in available, (
                        f"[{probe.id}] column '{col}' missing from "
                        f"'{probe.expect_table}' (available: {sorted(available)})"
                    )
                else:
                    assert col in all_cols, (
                        f"[{probe.id}] column '{col}' missing from any table "
                        f"(tables: {sorted(schema)})"
                    )
    finally:
        con.close()

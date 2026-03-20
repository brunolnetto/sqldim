"""
Observability medallion pipeline showcase
==========================================
Demonstrates treating schema drift and quality violations as first-class
Kimball dimensional facts inside sqldim's own tooling.

Pipeline stages
---------------
Bronze  : capture raw ``EvolutionReport`` / ``ContractReport`` objects
Silver  : explode into ``obs_schema_evolution_fact`` / ``obs_quality_drift_fact``
Gold    : breaking_change_rate, worst_quality_datasets, drift_velocity, etc.

The observatory is backed by an in-memory DuckDB connection so the demo is
entirely self-contained.

Usage
-----
.. code-block:: bash

    python -m sqldim.examples.features.observability.showcase

Or from Python::

    from sqldim.examples.features.observability import run_showcase
    run_showcase()
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone


# ---------------------------------------------------------------------------
# Synthetic evolution / quality reports
# ---------------------------------------------------------------------------


def _make_evo_reports():
    """Simulate 3 weeks of schema changes across 3 datasets."""
    from sqldim.contracts.engine import EvolutionChange, EvolutionReport

    base = datetime(2026, 3, 1, tzinfo=timezone.utc)

    runs = []

    # Week 1 — safe additions to orders_dim
    r1 = EvolutionReport()
    r1.safe_changes = [
        EvolutionChange("added", "delivery_sla_days", "nullable INT added"),
        EvolutionChange("added", "geo_region", "nullable VARCHAR added"),
    ]
    runs.append(("run-2026-03-01", "orders_dim", "silver", r1, base))

    # Week 1 — widening on product_dim
    r2 = EvolutionReport()
    r2.additive_changes = [
        EvolutionChange("widened", "price", "FLOAT → DOUBLE (safe widening)"),
    ]
    runs.append(("run-2026-03-01", "product_dim", "silver", r2, base))

    # Week 2 — breaking rename on customer_dim
    r3 = EvolutionReport()
    r3.breaking_changes = [
        EvolutionChange("renamed", "cust_email", "cust_email → email_address"),
        EvolutionChange("removed", "legacy_segment", "column dropped"),
    ]
    runs.append(
        ("run-2026-03-08", "customer_dim", "gold", r3, base + timedelta(days=7))
    )

    # Week 2 — another safe addition to orders_dim
    r4 = EvolutionReport()
    r4.safe_changes = [
        EvolutionChange("added", "channel_code", "nullable VARCHAR added"),
    ]
    r4.additive_changes = [
        EvolutionChange("widened", "amount_usd", "INT → BIGINT"),
    ]
    runs.append(
        ("run-2026-03-08", "orders_dim", "silver", r4, base + timedelta(days=7))
    )

    # Week 3 — type narrowing on product_dim (dangerous!)
    r5 = EvolutionReport()
    r5.breaking_changes = [
        EvolutionChange(
            "type_changed", "stock_qty", "BIGINT → SMALLINT (data loss risk)"
        ),
        EvolutionChange("type_changed", "weight_kg", "DOUBLE → FLOAT"),
    ]
    runs.append(
        ("run-2026-03-15", "product_dim", "silver", r5, base + timedelta(days=14))
    )

    return runs


def _make_quality_reports():
    """Simulate quality gate failures across datasets and runs."""
    from sqldim.contracts.report import ContractReport, ContractViolation

    base = datetime(2026, 3, 1, tzinfo=timezone.utc)
    runs = []

    def _rpt(view, violations, elapsed=0.05):
        rpt = ContractReport(violations=violations, view=view, elapsed_s=elapsed)
        return rpt

    def _viol(rule, count, severity="error", detail=""):
        return ContractViolation(
            rule=rule, severity=severity, count=count, detail=detail
        )

    # Week 1
    runs.append(
        (
            "run-2026-03-01",
            "orders_dim",
            "silver",
            _rpt(
                "orders_dim",
                [_viol("not_null.order_id", 3, detail="3 rows null primary key")],
            ),
            base,
        )
    )
    runs.append(
        (
            "run-2026-03-01",
            "customer_dim",
            "gold",
            _rpt(
                "customer_dim",
                [
                    _viol(
                        "unique.email_address",
                        12,
                        severity="warning",
                        detail="12 duplicate emails",
                    ),
                    _viol("not_null.customer_id", 0),
                ],
            ),
            base,
        )
    )

    # Week 2
    runs.append(
        (
            "run-2026-03-08",
            "orders_dim",
            "silver",
            _rpt(
                "orders_dim",
                [
                    _viol("not_null.order_id", 0),
                    _viol("range.amount_usd", 47, detail="47 negative amounts"),
                ],
            ),
            base + timedelta(days=7),
        )
    )

    # Week 3 — product_dim quality degrades
    runs.append(
        (
            "run-2026-03-15",
            "product_dim",
            "silver",
            _rpt(
                "product_dim",
                [
                    _viol("not_null.sku", 0),
                    _viol(
                        "range.stock_qty",
                        112,
                        detail="112 stock values overflow SMALLINT",
                    ),
                    _viol("not_null.weight_kg", 8, severity="warning"),
                ],
                elapsed=0.12,
            ),
            base + timedelta(days=14),
        )
    )
    runs.append(
        (
            "run-2026-03-15",
            "customer_dim",
            "gold",
            _rpt(
                "customer_dim",
                [
                    _viol("unique.email_address", 29, severity="warning"),
                ],
            ),
            base + timedelta(days=14),
        )
    )

    return runs


# ---------------------------------------------------------------------------
# Showcase
# ---------------------------------------------------------------------------


def _ingest_evolution(obs) -> int:
    print("\n▶  Ingesting schema evolution events (silver layer)…")
    total = 0
    for run_id, dataset, layer, report, ts in _make_evo_reports():
        n = obs.ingest_evolution(
            report,
            dataset=dataset,
            run_id=run_id,
            layer=layer,
            pipeline_name="nightly_dim_refresh",
            detected_at=ts,
        )
        total += n
        if n:
            print(f"    {run_id} / {dataset}: {n} evolution fact(s) inserted")
    return total


def _ingest_quality(obs) -> int:
    print("\n▶  Ingesting quality drift events (silver layer)…")
    total = 0
    for run_id, dataset, layer, report, ts in _make_quality_reports():
        n = obs.ingest_quality(
            report,
            dataset=dataset,
            run_id=run_id,
            layer=layer,
            pipeline_name="nightly_dim_refresh",
            checked_at=ts,
        )
        total += n
        if n:
            print(f"    {run_id} / {dataset}: {n} quality fact(s) inserted")
    return total


# ---------------------------------------------------------------------------
# Gold-layer display helpers
# ---------------------------------------------------------------------------


def _show_breaking_rate(obs) -> None:
    print("\n▶  Gold — Breaking change rate per dataset:")
    try:
        df = obs.breaking_change_rate().fetchdf()
        print(df.to_string(index=False))
    except Exception as e:  # pragma: no cover
        _print_fallback(obs.breaking_change_rate(), e)


def _show_worst_quality(obs) -> None:
    print("\n▶  Gold — Worst quality datasets (most violations):")
    try:
        df = obs.worst_quality_datasets(top_n=5).fetchdf()
        print(df.to_string(index=False))
    except Exception as e:  # pragma: no cover
        _print_fallback(obs.worst_quality_datasets(top_n=5), e)


def _show_drift_velocity(obs) -> None:
    print("\n▶  Gold — Schema drift velocity (by week):")
    try:
        df = obs.drift_velocity(bucket="week").fetchdf()
        print(df.to_string(index=False))
    except Exception as e:  # pragma: no cover
        _print_fallback(obs.drift_velocity(bucket="week"), e)


def _show_migration_backlog(obs) -> None:
    print("\n▶  Gold — Migration / backfill backlog:")
    try:
        df = obs.migration_backlog().fetchdf()
        print(
            df[
                [
                    "dataset_name",
                    "column_name",
                    "change_type",
                    "tier",
                    "requires_migration",
                    "requires_backfill",
                ]
            ].to_string(index=False)
        )
    except Exception as e:  # pragma: no cover
        _print_fallback(obs.migration_backlog(), e)


def _show_rule_heatmap(obs) -> None:
    print("\n▶  Gold — Rule failure heatmap (dataset × rule):")
    try:
        df = obs.rule_failure_heatmap().fetchdf()
        print(df.to_string(index=False))
    except Exception as e:  # pragma: no cover
        _print_fallback(obs.rule_failure_heatmap(), e)


# ---------------------------------------------------------------------------
# Showcase
# ---------------------------------------------------------------------------


def run_showcase() -> None:
    from sqldim.observability.drift import DriftObservatory

    print("=" * 60)
    print("  sqldim Observability — Schema & Quality Drift Star Schema")
    print("=" * 60)

    obs = DriftObservatory.in_memory()
    total_evo = _ingest_evolution(obs)
    total_qa = _ingest_quality(obs)

    counts = obs.counts()
    print("\n  Observatory row counts:")
    for table, n in counts.items():
        print(f"    {table:<35} {n:>4} rows")

    _show_breaking_rate(obs)
    _show_worst_quality(obs)
    _show_drift_velocity(obs)
    _show_migration_backlog(obs)
    _show_rule_heatmap(obs)

    print("\n" + "=" * 60)
    print("  ✅  Observability pipeline complete.")
    print(f"      {total_evo} evolution facts + {total_qa} quality facts ingested.")
    print("=" * 60)


def _print_fallback(rel: object, err: Exception) -> None:
    """Print raw rows when pandas is unavailable."""
    try:
        rows = rel.fetchall()  # type: ignore[union-attr]
        for row in rows:
            print("  ", row)
    except Exception:
        print(f"  [query error: {err}]")


if __name__ == "__main__":  # pragma: no cover
    run_showcase()

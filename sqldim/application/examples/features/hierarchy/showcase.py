"""
Hierarchy dimension showcase
=============================
Demonstrates the three physical hierarchy strategies (adjacency list,
materialized path, closure table) on a classic org-chart use case.

Each strategy is exercised against the same DuckDB in-memory table so the
generated SQL differences are visible side-by-side.

Run directly:
    python -m sqldim.application.examples.features.hierarchy.showcase
"""

from __future__ import annotations

import duckdb

from sqldim.application.datasets.domains.hierarchy import OrgChartSource
from sqldim.application.examples.features.hierarchy.models import OrgDimModel


def _setup_db() -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection populated with the org-chart fixture."""
    con = duckdb.connect()
    OrgChartSource().setup(con)
    return con


# ---------------------------------------------------------------------------
# Per-strategy helpers
# ---------------------------------------------------------------------------


def _showcase_adjacency(con, adj) -> None:
    print("\n── AdjacencyListStrategy ──────────────────────────────────────")
    print("\n  Ancestors of Grace (id=7):")
    sql = adj.ancestors_sql("org_dim", id_col="id", node_id=7, parent_col="parent_id")
    for r in con.execute(sql).fetchall():
        print(f"    id={r[0]}  parent={r[1]}  depth={r[2]}")

    print("\n  Descendants of Bob (id=2):")
    sql = adj.descendants_sql("org_dim", id_col="id", node_id=2, parent_col="parent_id")
    for r in con.execute(sql).fetchall():
        print(f"    id={r[0]}  parent={r[1]}  depth={r[2]}")

    print("\n  Rollup revenue to VP level (depth=1):")
    sql = adj.rollup_sql(
        "sales_fact",
        "org_dim",
        "revenue",
        level=1,
        fact_fk="dim_id",
        id_col="id",
        parent_col="parent_id",
        level_col="hierarchy_level",
        agg="SUM",
    )
    for r in con.execute(sql).fetchall():
        print(f"    ancestor_id={r[0]}  revenue_sum={r[1]:,.1f}")


def _showcase_materialized_path(con, mat) -> None:
    print("\n── MaterializedPathStrategy ───────────────────────────────────")
    print("\n  Ancestors of Grace (id=7) via path LIKE:")
    for r in con.execute(
        mat.ancestors_sql("org_dim", id_col="id", node_id=7)
    ).fetchall():
        print(f"    {r[3]}  (path={r[5]})")

    print("\n  Descendants of Alice (id=1) via path LIKE:")
    for r in con.execute(
        mat.descendants_sql("org_dim", id_col="id", node_id=1)
    ).fetchall():
        print(f"    {r[3]}  (path={r[5]})")

    print(f"\n  build_path('/1/2/', 8) → {mat.build_path('/1/2/', 8)!r}")


def _showcase_closure(con, clo) -> None:
    print("\n── ClosureTableStrategy ───────────────────────────────────────")
    print("\n  Ancestors of Grace (id=7) via closure join:")
    for r in con.execute(
        clo.ancestors_sql("org_dim", id_col="id", node_id=7)
    ).fetchall():
        print(f"    id={r[0]}  {r[3]}")

    print("\n  Descendants of Bob (id=2) via closure join:")
    for r in con.execute(
        clo.descendants_sql("org_dim", id_col="id", node_id=2)
    ).fetchall():
        print(f"    id={r[0]}  {r[3]}")

    print("\n  Rollup revenue to VP level (depth=1) via closure:")
    sql = clo.rollup_sql(
        "sales_fact",
        "org_dim",
        "revenue",
        level=1,
        fact_fk="dim_id",
        id_col="id",
        level_col="hierarchy_level",
        agg="SUM",
    )
    for r in con.execute(sql).fetchall():
        print(f"    ancestor_id={r[0]}  revenue_sum={r[1]:,.1f}")


def _showcase_roller(con, adj) -> None:
    from sqldim.core.kimball.dimensions.hierarchy import HierarchyRoller

    print("\n── HierarchyRoller (strategy-agnostic) ────────────────────────")
    sql = HierarchyRoller.rollup_sql(
        "sales_fact",
        "org_dim",
        "revenue",
        level=1,
        strategy=adj,
        fact_fk="dim_id",
    )
    print("\n  Revenue rolled to VP level via HierarchyRoller:")
    for r in con.execute(sql).fetchall():
        name = con.execute("SELECT name FROM org_dim WHERE id=?", [r[0]]).fetchone()
        label = name[0] if name else f"id={r[0]}"
        print(f"    {label:<20}  ${r[1]:,.1f}")


def _showcase_mixin(con) -> None:
    from sqldim.core.kimball.dimensions.hierarchy import ClosureTableStrategy

    print("\n── HierarchyMixin class API ────────────────────────────────────")
    ancestors = con.execute(OrgDimModel.ancestors_sql(7)).fetchall()
    print(f"\n  OrgDimModel.ancestors_sql(7) → {len(ancestors)} ancestors found")
    descs = con.execute(OrgDimModel.descendants_sql(1)).fetchall()
    print(f"  OrgDimModel.descendants_sql(1) → {len(descs)} descendants of Alice")
    rebuild_sql = ClosureTableStrategy.build_closure_sql("org_dim")
    print(
        f"\n  ClosureTableStrategy.build_closure_sql: length={len(rebuild_sql)} chars"
    )


# ---------------------------------------------------------------------------
# Showcase
# ---------------------------------------------------------------------------


EXAMPLE_METADATA = {
    "name": "hierarchy",
    "title": "Hierarchy Dimensions",
    "description": "Adjacency list, materialized path, closure table strategies on an org-chart",
    "entry_point": "run_showcase",
}


def run_showcase() -> None:
    from sqldim.core.kimball.dimensions.hierarchy import (
        AdjacencyListStrategy,
        MaterializedPathStrategy,
        ClosureTableStrategy,
    )

    print("=" * 60)
    print("  sqldim Hierarchy Dimensions — Three Strategy Showcase")
    print("=" * 60)

    con = _setup_db()
    adj = AdjacencyListStrategy()
    mat = MaterializedPathStrategy()
    clo = ClosureTableStrategy()

    _showcase_adjacency(con, adj)
    _showcase_materialized_path(con, mat)
    _showcase_closure(con, clo)
    _showcase_roller(con, adj)
    _showcase_mixin(con)

    print("\n" + "=" * 60)
    print("  ✅  Hierarchy showcase complete.")
    print("=" * 60)


if __name__ == "__main__":  # pragma: no cover
    run_showcase()

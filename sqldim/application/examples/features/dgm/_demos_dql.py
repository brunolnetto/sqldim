"""DGM showcase — DQL demos (B1/B2/B3 queries, edge-kind, bridge path)."""
from __future__ import annotations

import duckdb

from sqldim import (
    DGMQuery,
    PropRef,
    AggRef,
    WinRef,
    ScalarPred,
    PathPred,
    AND,
    NOT,
    VerbHop,
    BridgeHop,
    Compose,
)
from sqldim.core.graph.schema_graph import SchemaGraph
from sqldim.application.examples.features.dgm.models import (
    CustomerDim,
    ProductDim,
    SegmentDim,
    SaleFact,
    ProductSegmentBridge,
)


def _section(title: str) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print("─" * 60)


def demo_edge_kind_classification() -> None:
    _section("τ_E edge-kind classification")
    sg = SchemaGraph(
        [CustomerDim, ProductDim, SegmentDim, SaleFact],
        bridge_models=[ProductSegmentBridge],
    )
    schema = sg.graph_schema()
    for edge in schema.edges:
        print(f"  {edge['name']:30s}  kind={edge['edge_kind']}")


def demo_b1_filter(con: duckdb.DuckDBPyConnection) -> None:
    _section("B1 — Context filter: retail customers only")
    hop_c = VerbHop(
        "s", "placed_by", "c", table="dgm_showcase_customer", on="c.id = s.customer_id"
    )
    q = (
        DGMQuery()
        .anchor("dgm_showcase_sale", "s")
        .path_join(hop_c)
        .where(ScalarPred(PropRef("c", "segment"), "=", "retail"))
    )
    print(q.to_sql())
    rows = q.execute(con)
    print(f"\n  → {len(rows)} rows (Alice 3 + Carol 2 = 5)")


def demo_b1_path_pred(con: duckdb.DuckDBPyConnection) -> None:
    _section("B1 — PathPred EXISTS: sales with electronics products")
    hop = VerbHop(
        "s", "includes", "d", table="dgm_showcase_product", on="d.id = s.product_id"
    )
    pp = PathPred(
        anchor="s",
        path=hop,
        sub_filter=ScalarPred(PropRef("d", "category"), "=", "electronics"),
    )
    q = DGMQuery().anchor("dgm_showcase_sale", "s").where(pp)
    print(q.to_sql())
    rows = q.execute(con)
    print(f"\n  → {len(rows)} rows (sales 1, 4, 5 — Widget only)")


def demo_b1_not(con: duckdb.DuckDBPyConnection) -> None:
    _section("B1 — NOT: exclude clearance products")
    hop_d = VerbHop(
        "s", "includes", "d", table="dgm_showcase_product", on="d.id = s.product_id"
    )
    q = (
        DGMQuery()
        .anchor("dgm_showcase_sale", "s")
        .path_join(hop_d)
        .where(NOT(ScalarPred(PropRef("d", "category"), "=", "clearance")))
    )
    rows = q.execute(con)
    print(f"  → {len(rows)} rows (sale 2/Gadget excluded; expect 5)")


def demo_b1_b2_having(con: duckdb.DuckDBPyConnection) -> None:
    _section("B1 ∘ B2 — Group revenue per customer, HAVING > 4000")
    hop = VerbHop(
        "s", "placed_by", "c", table="dgm_showcase_customer", on="c.id = s.customer_id"
    )
    q = (
        DGMQuery()
        .anchor("dgm_showcase_sale", "s")
        .path_join(hop)
        .group_by("c.id", "c.email")
        .agg(total_rev="SUM(s.revenue)", sale_cnt="COUNT(*)")
        .having(ScalarPred(AggRef("total_rev"), ">", 4000))
    )
    print(q.to_sql())
    rows = q.execute(con)
    print(f"\n  → {len(rows)} customer(s) (Alice 5200 > 4000; Bob 4000 not >)")
    for r in rows:
        print(f"     id={r[0]}  email={r[1]}  total_rev={r[2]}")


def demo_b1_b3_qualify(con: duckdb.DuckDBPyConnection) -> None:
    _section("B1 ∘ B3 — Top-1 sale per customer by revenue")
    q = (
        DGMQuery()
        .anchor("dgm_showcase_sale", "s")
        .window(
            rn="ROW_NUMBER() OVER (PARTITION BY s.customer_id ORDER BY s.revenue DESC)"
        )
        .qualify(ScalarPred(WinRef("rn"), "=", 1))
    )
    print(q.to_sql())
    rows = q.execute(con)
    print(f"\n  → {len(rows)} rows (one per customer)")
    for r in rows:
        print(f"     customer_id={r[1]}  revenue={r[3]}")


def demo_full_pipeline(con: duckdb.DuckDBPyConnection) -> None:
    _section("B1 ∘ B2 ∘ B3 — Full: retail, group revenue, rank top-1")
    hop = VerbHop(
        "s", "placed_by", "c", table="dgm_showcase_customer", on="c.id = s.customer_id"
    )
    q = (
        DGMQuery()
        .anchor("dgm_showcase_sale", "s")
        .path_join(hop)
        .where(ScalarPred(PropRef("c", "segment"), "=", "retail"))
        .group_by("c.id", "c.email")
        .agg(total_rev="SUM(s.revenue)")
        .having(ScalarPred(AggRef("total_rev"), ">", 1000))
        .window(rnk="RANK() OVER (ORDER BY SUM(s.revenue) DESC)")
        .qualify(ScalarPred(WinRef("rnk"), "=", 1))
    )
    print(q.to_sql())
    rows = q.execute(con)
    print(f"\n  → {len(rows)} winner(s) (Alice ranks 1st among retail customers)")
    for r in rows:
        print(f"     id={r[0]}  email={r[1]}  total_rev={r[2]}")


def demo_bridge_path(con: duckdb.DuckDBPyConnection) -> None:
    _section("B1 — Composed path: sale → product → segment (bridge)")
    hop_verb = VerbHop(
        "s", "includes", "d", table="dgm_showcase_product", on="d.id = s.product_id"
    )
    hop_bridge = BridgeHop(
        "d",
        "belongs_to",
        "seg",
        table="dgm_showcase_prod_seg",
        on="seg.product_id = d.id",
    )
    path = Compose(hop_verb, hop_bridge)
    pp = PathPred(
        anchor="s",
        path=path,
        sub_filter=ScalarPred(PropRef("seg", "segment_id"), "=", 1),
    )
    q = DGMQuery().anchor("dgm_showcase_sale", "s").where(pp)
    print(q.to_sql())
    rows = q.execute(con)
    print(f"\n  → {len(rows)} rows (sales of electronics-tier products)")

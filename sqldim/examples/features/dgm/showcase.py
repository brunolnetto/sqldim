"""
DGM — Example 17
==================

17. Dimensional Graph Model in action — three-band queries over a star schema.

Demonstrates the four valid query forms from DGM §3:
  B1 only         — context filter (WHERE)
  B1 ∘ B2         — aggregation + HAVING (group revenue per customer)
  B1 ∘ B3         — window ranking + QUALIFY (top-1 sale per customer)
  B1 ∘ B2 ∘ B3    — full pipeline (ranked customer segments with HAVING)

Also shows:
  • τ_E edge-kind classification (verb vs. bridge) via GraphSchema
  • PathPred EXISTS subquery for existence-based B1 filters
  • NOT / AND / OR predicate algebra
  • Temporal joins (SCD-2) via temporal_join()

Run:
    PYTHONPATH=. python -m sqldim.examples.features.dgm.showcase
"""

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
    OR,
    NOT,
    VerbHop,
    BridgeHop,
    Compose,
)
from sqldim import DimensionModel, FactModel, BridgeModel, Field
from sqldim.core.graph.schema_graph import SchemaGraph


# ---------------------------------------------------------------------------
# Minimal star schema used for the showcase
# ---------------------------------------------------------------------------


class CustomerDim(DimensionModel, table=True):
    __tablename__ = "dgm_showcase_customer"
    __natural_key__ = ["email"]
    id: int = Field(primary_key=True, surrogate_key=True)
    email: str
    segment: str
    region: str
    valid_from: str | None = None
    valid_to: str | None = None


class ProductDim(DimensionModel, table=True):
    __tablename__ = "dgm_showcase_product"
    __natural_key__ = ["sku"]
    id: int = Field(primary_key=True, surrogate_key=True)
    sku: str
    category: str


class SegmentDim(DimensionModel, table=True):
    __tablename__ = "dgm_showcase_segment"
    __natural_key__ = ["code"]
    id: int = Field(primary_key=True, surrogate_key=True)
    code: str
    tier: str


class SaleFact(FactModel, table=True):
    __tablename__ = "dgm_showcase_sale"
    __grain__ = "one row per transaction"
    id: int = Field(primary_key=True)
    customer_id: int = Field(
        foreign_key="dgm_showcase_customer.id", dimension=CustomerDim
    )
    product_id: int = Field(foreign_key="dgm_showcase_product.id", dimension=ProductDim)
    revenue: float
    quantity: int
    sale_year: int


class ProductSegmentBridge(BridgeModel, table=True):
    __tablename__ = "dgm_showcase_prod_seg"
    __bridge_keys__ = ["product_id", "segment_id"]
    id: int = Field(default=None, primary_key=True)
    product_id: int
    segment_id: int
    weight: float = Field(default=1.0)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _setup(con: duckdb.DuckDBPyConnection) -> None:
    """Create and populate the in-memory star schema."""
    con.execute("""
        CREATE TABLE dgm_showcase_customer (
            id INTEGER PRIMARY KEY, email VARCHAR, segment VARCHAR,
            region VARCHAR, valid_from DATE, valid_to DATE
        )
    """)
    con.execute("""
        CREATE TABLE dgm_showcase_product (
            id INTEGER PRIMARY KEY, sku VARCHAR, category VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE dgm_showcase_segment (
            id INTEGER PRIMARY KEY, code VARCHAR, tier VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE dgm_showcase_sale (
            id INTEGER PRIMARY KEY, customer_id INTEGER, product_id INTEGER,
            revenue DOUBLE, quantity INTEGER, sale_year INTEGER
        )
    """)
    con.execute("""
        CREATE TABLE dgm_showcase_prod_seg (
            id INTEGER PRIMARY KEY, product_id INTEGER,
            segment_id INTEGER, weight DOUBLE
        )
    """)

    con.execute("""INSERT INTO dgm_showcase_customer VALUES
        (1,'alice@x','retail','US','2020-01-01',NULL),
        (2,'bob@x','wholesale','EU','2020-01-01',NULL),
        (3,'carol@x','retail','US','2020-01-01',NULL)
    """)
    con.execute("""INSERT INTO dgm_showcase_product VALUES
        (1,'W-001','electronics'), (2,'G-002','clearance'), (3,'D-003','food')
    """)
    con.execute("""INSERT INTO dgm_showcase_segment VALUES
        (1,'elec','premium'), (2,'food','standard')
    """)
    con.execute("""INSERT INTO dgm_showcase_sale VALUES
        (1,1,1,1500.0,3,2024),(2,1,2,200.0,1,2024),(3,1,3,3500.0,5,2024),
        (4,2,1,4000.0,8,2024),(5,3,1,2000.0,4,2024),(6,3,3,600.0,2,2024)
    """)
    con.execute("""INSERT INTO dgm_showcase_prod_seg VALUES
        (1,1,1,1.0),(2,3,2,1.0)
    """)


def _section(title: str) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print("─" * 60)


# ---------------------------------------------------------------------------
# τ_E edge-kind classification (DGM §4)
# ---------------------------------------------------------------------------


def demo_edge_kind_classification() -> None:
    _section("τ_E edge-kind classification")
    sg = SchemaGraph(
        [CustomerDim, ProductDim, SegmentDim, SaleFact],
        bridge_models=[ProductSegmentBridge],
    )
    schema = sg.graph_schema()
    for edge in schema.edges:
        print(f"  {edge['name']:30s}  kind={edge['edge_kind']}")


# ---------------------------------------------------------------------------
# B1 — Context filter
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# B1 — PathPred EXISTS filter
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# B1 — NOT predicate
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# B1 ∘ B2 — Aggregation + HAVING
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# B1 ∘ B3 — Window ranking (top-1 sale per customer)
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# B1 ∘ B2 ∘ B3 — Full pipeline
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Bridge path traversal (B1 with BridgeHop + VerbHop composed)
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# BDD predicate compilation
# ---------------------------------------------------------------------------


def demo_bdd_predicate() -> None:
    """Demonstrate BDD compile, satisfiability, implication, and to_sql."""
    from sqldim.core.query._dgm_bdd import BDDManager, DGMPredicateBDD

    _section("BDD predicate compilation and canonical form")

    mgr = BDDManager()
    bdd = DGMPredicateBDD(mgr)

    p1 = ScalarPred(PropRef("s", "revenue"), ">", 1000)
    p2 = ScalarPred(PropRef("s", "sale_year"), "=", 2024)
    pred = AND(p1, p2)

    uid = bdd.compile(pred)
    print(f"  BDD node ID for AND(p1,p2): {uid}")
    print(f"  Satisfiable: {bdd.is_satisfiable(uid)}")
    print(f"  To SQL:\n    {bdd.to_sql(uid)}")

    # Implication: (p1 AND p2) → p1
    uid_p1 = bdd.compile(p1)
    print(f"  AND(p1,p2) implies p1: {bdd.implies(uid, uid_p1)}")
    print(f"  p1 implies AND(p1,p2): {bdd.implies(uid_p1, uid)}")


# ---------------------------------------------------------------------------
# Schema annotation layer (Σ) + recommender
# ---------------------------------------------------------------------------


def demo_annotation_sigma() -> None:
    """Build an AnnotationSigma and run the DGMRecommender over it."""
    from sqldim.core.query._dgm_annotations import (
        AnnotationSigma,
        Grain,
        GrainKind,
        SCDType,
        SCDKind,
        Conformed,
        BridgeSemantics,
        BridgeSemanticsKind,
    )
    from sqldim.core.query._dgm_recommender import DGMRecommender

    _section("Schema annotation layer Σ and recommender")

    sigma = AnnotationSigma(
        annotations=[
            Grain(fact="dgm_showcase_sale", grain=GrainKind.PERIOD),
            SCDType(dim="dgm_showcase_customer", scd=SCDKind.SCD2),
            Conformed(dim="dgm_showcase_customer", fact_types=frozenset({"Sale"})),
            BridgeSemantics(bridge="dgm_showcase_prod_seg", sem=BridgeSemanticsKind.STRUCTURAL),
        ]
    )
    print(f"  Annotations loaded: {len(sigma)}")
    print(f"  scd_of('dgm_showcase_customer'): {sigma.scd_of('dgm_showcase_customer').value}")
    print(f"  is_conformed('dgm_showcase_customer', 'Sale'): "
          f"{sigma.is_conformed('dgm_showcase_customer', 'Sale')}")
    print(f"  grain_of('dgm_showcase_sale'): {sigma.grain_of('dgm_showcase_sale').value}")

    rec = DGMRecommender(sigma)
    suggestions = rec.run_annotation_rules()
    print(f"  Recommender suggestions: {len(suggestions)}")
    for s in suggestions[:3]:
        print(f"    • {s.text}")

    # Routing: high entropy → stage1 (Free-endpoint characterisation)
    route = rec.route(entropy=0.9)
    print(f"  Route (entropy=0.9): {route}")
    route_low = rec.route(entropy=0.1)
    print(f"  Route (entropy=0.1): {route_low}")


# ---------------------------------------------------------------------------
# DGM planner
# ---------------------------------------------------------------------------


def demo_planner() -> None:
    """DGMPlanner: build an ExportPlan with rules 1a–9."""
    from sqldim.core.query._dgm_annotations import AnnotationSigma
    from sqldim.core.query._dgm_planner import DGMPlanner, QueryTarget, SinkTarget
    from sqldim.core.query._dgm_graph import GraphStatistics
    from sqldim.core.query._dgm_exporters import (
        DGMJSONExporter,
        DGMYAMLExporter,
    )

    _section("DGM query planner and exporters")

    sigma = AnnotationSigma(annotations=[])
    stats = GraphStatistics(node_count=3, edge_count=6)
    planner = DGMPlanner(
        cost_model=None,
        statistics=stats,
        annotations=sigma,
        rules=None,
        query_target=QueryTarget.SQL_DUCKDB,
        sink_target=SinkTarget.DUCKDB,
    )

    sql = (
        DGMQuery()
        .anchor("dgm_showcase_sale", "s")
        .where(ScalarPred(PropRef("s", "sale_year"), "=", 2024))
        .to_sql()
    )
    plan = planner.build_plan(sql)
    print(f"  Query target : {plan.query_target.value}")
    print(f"  Sink target  : {plan.sink_target.value if plan.sink_target else 'none'}")
    print(f"  Cost estimate: {plan.cost_estimate}")

    json_out = DGMJSONExporter().export(plan)
    yaml_out = DGMYAMLExporter().export(plan)
    print(f"  JSON export  : {len(json_out)} chars")
    print(f"  YAML export  : {len(yaml_out)} chars")
    print(f"  YAML snippet :\n    {yaml_out.splitlines()[0]}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def run_all() -> None:
    con = duckdb.connect()
    _setup(con)

    demo_edge_kind_classification()
    demo_b1_filter(con)
    demo_b1_path_pred(con)
    demo_b1_not(con)
    demo_b1_b2_having(con)
    demo_b1_b3_qualify(con)
    demo_full_pipeline(con)
    demo_bridge_path(con)
    demo_bdd_predicate()
    demo_annotation_sigma()
    demo_planner()

    con.close()
    print("\nDGM showcase complete.")


if __name__ == "__main__":
    run_all()

"""
sqldim Showcase: Supply Chain Inventory Intelligence Pipeline
-------------------------------------------------------------
Pillar 1: SCD Type 2  — Warehouse capacity history
Pillar 2: Graph       — Shipment routing traversal (Warehouse → Warehouse)
Pillar 3: Cumulative  — Running stock-level arrays per SKU per warehouse
Pillar 4: Schema Graph — Dimensional lineage visualisation (Mermaid)
Pillar 5: DGM Analytics — TARJAN_SCC cycle detection, TrimJoin, ArithExpr
"""

import asyncio
from datetime import date
from sqlmodel import Session, SQLModel, create_engine, select
from sqlalchemy.pool import StaticPool

from sqldim.examples.real_world.supply_chain.models import (
    Supplier,
    Warehouse,
    SKU,
    ShipmentEdge,
    ReceiptFact,
    InventoryFact,
)
from sqldim.examples.datasets.domains.supply_chain import WarehousesSource, SKUsSource
from sqldim.core.kimball.dimensions.scd.handler import SCDHandler
from sqldim.core.graph import GraphModel
from sqldim import SchemaGraph

_WAREHOUSE_FACTORY_KEYS = frozenset({"warehouse_id"})
_SKU_FACTORY_KEYS = frozenset({"sku_id"})


def _strip(row: dict, drop: frozenset) -> dict:
    return {k: v for k, v in row.items() if k not in drop}


def _version_tag(is_current: bool) -> str:
    return "current" if is_current else "expanded"


async def _sc_pillar1_scd_warehouses(engine) -> None:
    """SCD Type 2 — Warehouse Capacity History (WarehousesSource factory)."""
    src = WarehousesSource(n_entities=3, seed=42)
    with Session(engine) as session:
        handler = SCDHandler(
            Warehouse, session, track_columns=["max_capacity_units", "region"]
        )
        await handler.process([_strip(r, _WAREHOUSE_FACTORY_KEYS) for r in src.initial])
        await handler.process([_strip(r, _WAREHOUSE_FACTORY_KEYS) for r in src.events])
        versions = session.exec(
            select(Warehouse).order_by(Warehouse.warehouse_code, Warehouse.valid_from)
        ).all()

    upgraded = sum(not v.is_current for v in versions)
    print("✅ Pillar 1: SCD Type 2 — Warehouse Capacity History")
    print(f"   Loaded {len(src.initial)} warehouses via WarehousesSource factory.")
    print(
        f"   Total version rows in dim_warehouse: {len(versions)} "
        f"({upgraded} expired / capacity-changed)."
    )
    for v in versions:
        print(
            f"     {v.warehouse_name:<20}  "
            f"capacity={v.max_capacity_units:>7,}  ({_version_tag(v.is_current)})"
        )
    print()


async def _sc_pillar2_shipment_graph(engine) -> tuple:
    """Shipment Graph — Hub-and-Spoke Routing."""
    sku_src = SKUsSource(n_entities=1, seed=0)
    with Session(engine) as session:
        # Use first 3 current warehouses from SCD dimension as hub/spokes
        current_wh = session.exec(
            select(Warehouse)
            .where(Warehouse.is_current == True)  # noqa: E712
            .order_by(Warehouse.id)
            .limit(3)
        ).all()
        hub, spoke_a, spoke_b = current_wh[0], current_wh[1], current_wh[2]

        # SKU from factory (strip seq id)
        sku_row = _strip(sku_src.initial[0], _SKU_FACTORY_KEYS)
        sku = SKU(**sku_row)
        session.add(sku)
        session.commit()
        shipments = [
            ShipmentEdge(
                subject_id=hub.id,
                object_id=spoke_a.id,
                sku_id=sku.id,
                ship_date=date(2024, 7, 10),
                quantity=5_000,
                carrier="freight",
            ),
            ShipmentEdge(
                subject_id=hub.id,
                object_id=spoke_b.id,
                sku_id=sku.id,
                ship_date=date(2024, 7, 12),
                quantity=8_000,
                carrier="air",
            ),
        ]
        session.add_all(shipments)
        session.commit()
        graph = GraphModel(Warehouse, ShipmentEdge, session=session)
        hub_vertex = await graph.get_vertex(Warehouse, hub.id)
        destinations = await graph.neighbors(hub_vertex, edge_type=ShipmentEdge)
        total_shipped = sum(s.quantity for s in shipments if s.subject_id == hub.id)
        hub_id, spoke_a_id, spoke_b_id, sku_id = (
            hub.id,
            spoke_a.id,
            spoke_b.id,
            sku.id,
        )
        hub_name = hub.warehouse_name

    print("✅ Pillar 2: Shipment Graph — Hub-and-Spoke Routing")
    print(f"   {hub_name} is the consolidation hub (from WarehousesSource factory).")
    print(f"   Outbound destinations: {[v.warehouse_name for v in destinations]}")
    print(f"   Total units shipped: {total_shipped:,}")
    print()
    return hub_id, spoke_a_id, spoke_b_id, sku_id


async def _sc_pillar3_cumulative_stock(
    engine, hub_id: int, spoke_id: int, sku_id: int
) -> None:
    """Cumulative Fact — Running Stock Arrays."""
    with Session(engine) as session:
        supplier = Supplier(
            supplier_code="SUP-001",
            supplier_name="Precision Parts GmbH",
            country_code="DE",
            lead_time_days=14,
            reliability_score=0.92,
        )
        session.add(supplier)
        session.commit()
        receipts = [
            ReceiptFact(
                warehouse_id=hub_id,
                supplier_id=supplier.id,
                sku_id=sku_id,
                receipt_date=date(2024, 7, 1),
                quantity_received=20_000,
                unit_cost_usd=4.50,
            ),
            ReceiptFact(
                warehouse_id=spoke_id,
                supplier_id=supplier.id,
                sku_id=sku_id,
                receipt_date=date(2024, 7, 5),
                quantity_received=12_000,
                unit_cost_usd=4.75,
            ),
        ]
        session.add_all(receipts)
        session.commit()
        inventory = [
            InventoryFact(
                sku_id=sku_id,
                warehouse_id=hub_id,
                snapshot_date=date(2024, 7, 1),
                current_season="2024-Q3",
                stock_units=20_000,
                reserved_units=5_000,
                stock_history=[20_000],
            ),
            InventoryFact(
                sku_id=sku_id,
                warehouse_id=spoke_id,
                snapshot_date=date(2024, 7, 5),
                current_season="2024-Q3",
                stock_units=12_000,
                reserved_units=3_000,
                stock_history=[12_000],
            ),
        ]
        session.add_all(inventory)
        session.commit()
        inv_rows = session.exec(select(InventoryFact)).all()

    print("✅ Pillar 3: Cumulative Fact — Running Stock-Level Arrays")
    total_available = sum(r.stock_units - r.reserved_units for r in inv_rows)
    for r in inv_rows:
        available = r.stock_units - r.reserved_units
        print(
            f"     warehouse_id={r.warehouse_id}  "
            f"stock={r.stock_units:>6,}  "
            f"reserved={r.reserved_units:>5,}  "
            f"available={available:>6,}"
        )
    print(f"   Total available across network: {total_available:,} units")
    print()


async def _sc_pillar4_schema_graph() -> None:
    """Schema Graph — Dimensional Lineage."""
    sg = SchemaGraph.from_models(
        [Supplier, Warehouse, SKU, ShipmentEdge, ReceiptFact, InventoryFact]
    )
    graph_dict = sg.to_dict()
    mermaid = sg.to_mermaid()
    dim_count = len(graph_dict.get("dimensions", []))
    fact_count = len(graph_dict.get("facts", []))
    mermaid_lines = len(mermaid.strip().splitlines())
    print("✅ Pillar 4: Schema Graph — Dimensional Lineage Diagram")
    print(
        f"   SchemaGraph built: {dim_count} dimensions, {fact_count} fact/edge tables."
    )
    print(f"   Mermaid diagram: {mermaid_lines} lines")
    print("   (Use SchemaGraph.to_mermaid() to render in any Markdown viewer)\n")


# ---------------------------------------------------------------------------
# DGM helper functions — demonstrate new spec features (DGM v0.11)
# ---------------------------------------------------------------------------


def _scq_detect_shipment_cycles() -> tuple[str, str]:
    """Build DGM expressions for TARJAN_SCC cycle detection over a shipment graph.

    Returns ``(trim_sql, scc_expr_sql)``:

    * ``trim_sql``     — :class:`~sqldim.core.query.dgm.TrimJoin` with
      :class:`~sqldim.core.query.dgm.SINK_FREE` (prune warehouses with no
      outbound shipments before SCC analysis).
    * ``scc_expr_sql`` — :class:`~sqldim.core.query.dgm.GraphExpr` wrapping
      ``COMMUNITY_LABEL(TARJAN_SCC())`` on the ``w`` alias.  Each warehouse is
      labelled with its strongly-connected component — warehouses in the same
      non-trivial SCC form a directed routing cycle.
    """
    from sqldim.core.query.dgm import (
        GraphExpr,
        NodeExpr,
        COMMUNITY_LABEL,
        TARJAN_SCC,
        SINK_FREE,
        TrimJoin,
    )

    placeholder_join = object()  # formal grammar arg; not executed here
    trim_spec = TrimJoin(placeholder_join, SINK_FREE())
    scc_expr = GraphExpr(NodeExpr(COMMUNITY_LABEL(TARJAN_SCC()), "w"))
    return trim_spec.to_sql(), scc_expr.to_sql()


def _scq_weighted_shipment_cost() -> str:
    """Return the SQL for quantity * unit_cost via :class:`~sqldim.core.query.dgm.ArithExpr`."""
    from sqldim.core.query.dgm import ArithExpr, PropRef

    return ArithExpr(
        PropRef("sh", "quantity"), "*", PropRef("sh", "unit_cost")
    ).to_sql()


def _scq_hub_degree_out() -> str:
    """Return the SQL for DEGREE(direction='OUT') — identifies consolidation hubs."""
    from sqldim.core.query.dgm import GraphExpr, NodeExpr, DEGREE

    return GraphExpr(NodeExpr(DEGREE(direction="OUT"), "w")).to_sql()


def _scq_regional_density() -> str:
    """Return the SQL for SubgraphExpr(DENSITY, partition=[w.region])."""
    from sqldim.core.query.dgm import SubgraphExpr, DENSITY, PropRef

    return SubgraphExpr(DENSITY(), partition=[PropRef("w", "region")]).to_sql()


async def _sc_pillar5_dgm_graph_analytics() -> None:
    """DGM Graph Analytics — new v0.11 features on a supply-chain topology.

    Demonstrates (without executing DuckDB graph extensions):

    * **TrimJoin(SINK_FREE)** — prune warehouse nodes that have no outbound
      shipments before applying SCC analysis.
    * **TARJAN_SCC** — label each warehouse with its directed strongly-connected
      component; non-trivial SCCs (size > 1) identify routing cycles.
    * **DEGREE(direction='OUT')** — measure outbound fan-out to rank consolidation
      hubs.
    * **ArithExpr(quantity \\* unit_cost)** — weighted shipment cost expression
      combining two ``PropRef`` operands.
    * **SubgraphExpr(DENSITY, partition=[w.region])** — per-region subgraph
      density broadcast to each tuple in the group.
    """
    trim_sql, scc_sql = _scq_detect_shipment_cycles()
    weighted_cost_sql = _scq_weighted_shipment_cost()
    hub_degree_sql = _scq_hub_degree_out()
    regional_density_sql = _scq_regional_density()

    print("✅ Pillar 5: DGM Graph Analytics (v0.11 features)")
    print(f"   TrimJoin(SINK_FREE): {trim_sql}")
    print(f"   TARJAN_SCC node expr: {scc_sql}")
    print(f"   DEGREE(OUT) hub expr: {hub_degree_sql}")
    print(f"   ArithExpr quantity×cost: {weighted_cost_sql}")
    print(f"   SubgraphExpr(DENSITY, partition=[w.region]): {regional_density_sql}")
    print()


async def run_supply_chain_showcase():
    """End-to-end supply chain inventory intelligence showcase.

    Demonstrates SCD Type 2 warehouse history, directed shipment-graph
    traversal, cumulative running stock arrays, dimensional schema-graph
    visualisation, and DGM v0.11 graph analytics expressions.
    """
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    SQLModel.metadata.create_all(engine)
    print("📦 Supply Chain Inventory Intelligence Pipeline\n")
    await _sc_pillar1_scd_warehouses(engine)
    hub_id, _spoke_a_id, spoke_b_id, sku_id = await _sc_pillar2_shipment_graph(engine)
    await _sc_pillar3_cumulative_stock(engine, hub_id, spoke_b_id, sku_id)
    await _sc_pillar4_schema_graph()
    await _sc_pillar5_dgm_graph_analytics()
    print(
        "🚀 Supply Chain Pipeline complete. "
        "SCD2 + Shipment Graph + Cumulative Inventory + Schema Graph + DGM Analytics."
    )
    engine.dispose()


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(run_supply_chain_showcase())

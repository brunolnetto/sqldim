"""
sqldim/examples/real_world/supply_chain/models.py
==================================================
Kimball models for the supply chain real-world example.

Demonstrates:
* :class:`Supplier`        — SCD Type 1 dimension (contact details overwrite)
* :class:`Warehouse`       — SCD Type 2 dimension (location / capacity history)
* :class:`Product`         — SCD Type 1 dimension (weight, HS code)
* :class:`ShipmentEdge`    — Edge model (product-movement graph, supplier →
                               warehouse)
* :class:`InventoryFact`   — Cumulative fact (running stock levels with a
                               dense array of daily quantities, loaded via
                               :class:`~sqldim.LazyCumulativeLoader`)
* :class:`ReceiptFact`     — Transaction fact (individual goods-receipt events)
"""

from __future__ import annotations

from datetime import date
from typing import List, Optional
from sqlmodel import Field, Column, JSON

from sqldim import (
    DimensionModel,
    FactModel,
    SCD2Mixin,
    VertexModel,
    EdgeModel,
)
from sqldim.core.kimball.mixins import CumulativeMixin


# ---------------------------------------------------------------------------
# Dimensions
# ---------------------------------------------------------------------------


class Supplier(DimensionModel, table=True):
    """Supplier dimension — SCD Type 1 (contacts overwritten in place).

    Natural key is ``supplier_code``.  No version history: supplier name /
    contact changes are not analytically significant.
    """

    __natural_key__ = ["supplier_code"]
    __scd_type__ = 1
    __tablename__ = "dim_supplier"

    id: Optional[int] = Field(default=None, primary_key=True)
    supplier_code: str
    supplier_name: str
    country_code: str
    lead_time_days: int = 7
    reliability_score: float = 0.8  # 0.0 → 1.0


class Warehouse(VertexModel, SCD2Mixin, table=True):
    """Warehouse dimension — capacity and region tracked historically (SCD 2).

    Also projects as a :class:`~sqldim.VertexModel` so :class:`ShipmentEdge`
    can form a directed graph between supplier and warehouse nodes.
    """

    __natural_key__ = ["warehouse_code"]
    __vertex_type__ = "warehouse"
    __vertex_properties__ = ["warehouse_code", "region", "max_capacity_units"]
    __tablename__ = "dim_warehouse"

    id: Optional[int] = Field(default=None, primary_key=True)
    warehouse_code: str
    warehouse_name: str
    region: str  # "EMEA" | "APAC" | "AMER"
    max_capacity_units: int = 100_000
    is_active: bool = True


class SKU(DimensionModel, table=True):
    """SKU (Stock Keeping Unit) dimension — SCD Type 1.

    Physical attributes (weight, HS tariff code) are overwritten; price
    history is captured on the fact table.
    """

    __natural_key__ = ["sku_code"]
    __scd_type__ = 1
    __tablename__ = "dim_sku"

    id: Optional[int] = Field(default=None, primary_key=True)
    sku_code: str
    product_name: str
    category: str
    weight_kg: float = 0.0
    hs_code: str = ""  # HS-6 tariff classification


# ---------------------------------------------------------------------------
# Edge (shipment graph)
# ---------------------------------------------------------------------------


class ShipmentEdge(EdgeModel, table=True):
    """Directed edge from :class:`Warehouse` origin to :class:`Warehouse` dest.

    Models the physical movement of goods between warehouse nodes.
    ``sku_id`` links to the SKU being shipped so traversal queries can
    filter by product category.
    """

    __edge_type__ = "shipment"
    __subject__ = Warehouse  # origin warehouse
    __object__ = Warehouse  # destination warehouse
    __directed__ = True
    __tablename__ = "edge_shipment"

    id: Optional[int] = Field(default=None, primary_key=True)
    subject_id: int = Field(foreign_key="dim_warehouse.id")
    object_id: int = Field(foreign_key="dim_warehouse.id")
    sku_id: int = Field(foreign_key="dim_sku.id")
    ship_date: date
    quantity: int
    carrier: str = "freight"


# ---------------------------------------------------------------------------
# Facts
# ---------------------------------------------------------------------------


class ReceiptFact(FactModel, table=True):
    """Goods-receipt transaction fact — one row per inbound delivery.

    Append-only (``strategy="bulk"``); each row records a supplier delivering
    a batch of SKUs to a warehouse on a given date.
    """

    __grain__ = "one row per goods receipt"
    __tablename__ = "fact_receipt"

    id: Optional[int] = Field(default=None, primary_key=True)
    warehouse_id: int = Field(foreign_key="dim_warehouse.id")
    supplier_id: int = Field(foreign_key="dim_supplier.id")
    sku_id: int = Field(foreign_key="dim_sku.id")
    receipt_date: date
    quantity_received: int
    unit_cost_usd: float


class InventoryFact(FactModel, CumulativeMixin, table=True):
    """Cumulative inventory fact — one row per SKU per warehouse per day.

    ``stock_history`` stores a running JSON array of end-of-day stock levels.
    Loaded via :class:`~sqldim.LazyCumulativeLoader` which appends new
    observations to the array rather than replacing the row.
    """

    __grain__ = "one row per sku per warehouse per day"
    __strategy__ = "cumulative"
    __natural_key__ = ["sku_id", "warehouse_id"]
    __partition_key__ = "sku_id"
    __cumulative_column__ = "stock_history"
    __metric_columns__ = ["stock_units", "reserved_units"]
    __tablename__ = "fact_inventory"

    sku_id: int = Field(primary_key=True, foreign_key="dim_sku.id")
    warehouse_id: int = Field(primary_key=True, foreign_key="dim_warehouse.id")
    snapshot_date: date = Field(primary_key=True)
    current_season: str = ""  # required by CumulativeMixin

    stock_units: int = 0
    reserved_units: int = 0
    stock_history: List[int] = Field(default_factory=list, sa_column=Column(JSON))

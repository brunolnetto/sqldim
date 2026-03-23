"""
sqldim/examples/features/dgm/models.py
=======================================
SQLModel declarations for the DGM showcase star schema.

Five tables form a small analytic star:

* :class:`CustomerDim`         — SCD-2–ready customer dimension (email, segment, region)
* :class:`ProductDim`          — Product dimension (sku, category)
* :class:`SegmentDim`          — Segment lookup (code, tier)
* :class:`SaleFact`            — Transactional fact (revenue, quantity, sale_year)
* :class:`ProductSegmentBridge`— Bridge between products and segments (weight)

These models are intentionally minimal — just enough to demonstrate the four
DGM query forms (B1, B1∘B2, B1∘B3, B1∘B2∘B3) and the τ_E edge classifier.
The matching synthetic fixture data lives in
:mod:`sqldim.application.datasets.domains.dgm`.
"""

from __future__ import annotations

from sqldim import DimensionModel, FactModel, BridgeModel, Field


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

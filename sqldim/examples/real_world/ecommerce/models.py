"""
sqldim/examples/real_world/ecommerce/models.py
===============================================
Kimball models for the e-commerce real-world example.

Demonstrates:
* :class:`Customer` — SCD Type 2 dimension (tier upgrades tracked historically)
* :class:`Product`  — SCD Type 1 dimension (price & stock overwritten in place)
* :class:`Campaign` — SCD Type 1 dimension (budget and channel assignments)
* :class:`OrderFact` — Accumulating snapshot fact (five milestone timestamps)
* :class:`ProductCampaignBridge` — Bridge table (multi-valued dimension — a
  product can appear in multiple active campaigns at once)
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional
from sqlmodel import Field

from sqldim import FactModel, BridgeModel, SCD2Mixin
from sqldim import DimensionModel


# ---------------------------------------------------------------------------
# Dimensions
# ---------------------------------------------------------------------------


class Customer(DimensionModel, SCD2Mixin, table=True):
    """Customer dimension — loyalty tier tracked historically via SCD Type 2.

    When a customer upgrades from "bronze" to "gold", a new version row is
    written and the previous row receives a ``valid_to`` end-date.
    """

    __natural_key__ = ["email"]
    __tablename__ = "dim_customer"

    id: Optional[int] = Field(default=None, primary_key=True)
    email: str
    full_name: str
    loyalty_tier: str = "bronze"  # "bronze" | "silver" | "gold" | "platinum"
    country_code: str = "US"
    acquisition_channel: str = "organic"


class Product(DimensionModel, table=True):
    """Product dimension — price and stock are overwritten (SCD Type 1).

    The natural key is ``sku``.  No version history is needed because price
    changes are tracked on the fact table via ``unit_price_at_order``.
    """

    __natural_key__ = ["sku"]
    __scd_type__ = 1
    __tablename__ = "dim_product"

    id: Optional[int] = Field(default=None, primary_key=True)
    sku: str
    product_name: str
    category: str
    unit_price: float
    stock_units: int = 0


class Campaign(DimensionModel, table=True):
    """Marketing campaign dimension (SCD Type 1).

    Budget and channel can change; historical campaign states are not tracked
    because campaign performance is joined via the bridge table and the fact.
    """

    __natural_key__ = ["campaign_code"]
    __scd_type__ = 1
    __tablename__ = "dim_campaign"

    id: Optional[int] = Field(default=None, primary_key=True)
    campaign_code: str
    campaign_name: str
    channel: str  # "email" | "social" | "search" | "display"
    budget_usd: float
    is_active: bool = True


# ---------------------------------------------------------------------------
# Bridge
# ---------------------------------------------------------------------------


class ProductCampaignBridge(BridgeModel, table=True):
    """Bridge between :class:`Product` and :class:`Campaign`.

    A product can be promoted by several campaigns simultaneously; *weight*
    (inherited from ``BridgeModel``) is the fractional attribution share so
    revenue metric queries avoid double-counting.
    """

    __bridge_keys__ = ["product_id", "campaign_id"]
    __tablename__ = "bridge_product_campaign"

    id: Optional[int] = Field(default=None, primary_key=True)
    product_id: int = Field(foreign_key="dim_product.id")
    campaign_id: int = Field(foreign_key="dim_campaign.id")


# ---------------------------------------------------------------------------
# Accumulating snapshot fact
# ---------------------------------------------------------------------------


class OrderFact(FactModel, table=True):
    """Accumulating snapshot fact — one row per order, updated at each stage.

    Five milestone columns record the exact timestamp each order reached that
    stage.  Nulls mean the stage has not been reached yet.  The
    :class:`~sqldim.LazyAccumulatingLoader` matches on ``order_id`` and
    overwrites each milestone as new events arrive.
    """

    __grain__ = "one row per order"
    __strategy__ = "accumulating"
    __natural_key__ = ["order_id"]
    __match_column__ = "order_id"
    __milestones__ = [
        "placed_at",
        "paid_at",
        "shipped_at",
        "delivered_at",
        "returned_at",
    ]
    __tablename__ = "fact_order"

    id: Optional[int] = Field(default=None, primary_key=True)
    order_id: str
    customer_id: int = Field(foreign_key="dim_customer.id")
    product_id: int = Field(foreign_key="dim_product.id")
    campaign_id: Optional[int] = Field(default=None, foreign_key="dim_campaign.id")

    quantity: int = 1
    unit_price_at_order: float = 0.0  # snapshot of price at order time

    # Milestone timestamps — NULL until the stage is reached
    placed_at: Optional[datetime] = None
    paid_at: Optional[datetime] = None
    shipped_at: Optional[datetime] = None
    delivered_at: Optional[datetime] = None
    returned_at: Optional[datetime] = None

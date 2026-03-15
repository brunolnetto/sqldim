from __future__ import annotations
from datetime import date, datetime
from typing import List, Optional, Dict, Any
from sqlmodel import Field, Column, JSON, select
from sqldim import DimensionModel, FactModel, SCD2Mixin, VertexModel, EdgeModel, DatelistMixin

# 1. THE HYBRID DIMENSION + GRAPH VERTEX
class User(VertexModel, SCD2Mixin, table=True):
    """
    Tracks User identity, subscription status (SCD2), and metadata (JSONB).
    Also projects as a Vertex in the referral graph.
    """
    __vertex_type__ = "user"
    __natural_key__ = ["email"]
    
    id: Optional[int] = Field(default=None, primary_key=True)
    email: str
    plan_tier: str = "free"  # "free", "pro", "enterprise"
    
    # Hybrid SCD: Versioned property bag for marketing/device data
    meta: Dict[str, Any] = Field(
        default_factory=dict,
        sa_column=Column(JSON, info={"scd": 2})
    )

# 2. THE DUAL-PARADIGM RELATIONSHIP
class ReferralEdge(EdgeModel, table=True):
    """
    Projects the relationship between a referrer and a new user.
    """
    __edge_type__ = "referred"
    __subject__ = User # The referrer
    __object__ = User  # The new user
    __directed__ = True

    id: Optional[int] = Field(default=None, primary_key=True)
    subject_id: int = Field(foreign_key="user.id")
    object_id: int = Field(foreign_key="user.id")
    referral_code: str
    discount_applied: float = 0.0

# 3. THE RETENTION ENGINE
class UserActivity(FactModel, DatelistMixin, table=True):
    """
    Tracks daily engagement using bitmask encoding.
    Replaces massive tables with a single row per user per month.
    """
    __grain__ = "one row per user per month"

    user_id: int = Field(primary_key=True, foreign_key="user.id")
    month_partition: date = Field(primary_key=True)
    
    # DatelistMixin provides .l7(), .l28() via this column
    dates_active: List[date] = Field(
        default_factory=list,
        sa_column=Column(JSON)
    )

"""
sqldim/examples/real_world/fintech/models.py
============================================
Kimball models for the fintech real-world example.

Demonstrates:
* :class:`Account`          — SCD Type 2 dimension (risk tier history)
* :class:`Counterparty`     — SCD Type 1 dimension (name / jurisdiction)
* :class:`TransferEdge`     — Edge model (money-movement graph)
* :class:`MonthlyBalanceFact` — Array-metric fact (per-account balance array
  partitioned by month, loaded via :class:`~sqldim.LazyArrayMetricLoader`)
* :class:`TxnFact`          — Transaction fact (raw ledger entries)
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


# ---------------------------------------------------------------------------
# Dimensions / vertices
# ---------------------------------------------------------------------------


class Account(VertexModel, SCD2Mixin, table=True):
    """Bank account dimension — risk tier tracked historically via SCD Type 2.

    Also projects as a :class:`~sqldim.VertexModel` so the payment graph can
    traverse ``TransferEdge`` connections.
    """

    __natural_key__ = ["account_number"]
    __vertex_type__ = "account"
    __vertex_properties__ = ["account_number", "account_type", "risk_tier"]
    __tablename__ = "dim_account"

    id: Optional[int] = Field(default=None, primary_key=True)
    account_number: str
    account_type: str = "checking"  # "checking" | "savings" | "investment"
    risk_tier: str = "low"  # "low" | "medium" | "high" | "blocked"
    currency: str = "USD"
    owner_name: str = ""


class Counterparty(DimensionModel, table=True):
    """External counterparty dimension (banks, merchants, payment processors).

    SCD Type 1 — only current name and jurisdiction are relevant; historical
    states are not needed for AML analytics.
    """

    __natural_key__ = ["bic_code"]
    __scd_type__ = 1
    __tablename__ = "dim_counterparty"

    id: Optional[int] = Field(default=None, primary_key=True)
    bic_code: str
    institution_name: str
    country_code: str
    is_sanctioned: bool = False


# ---------------------------------------------------------------------------
# Edge (payment graph)
# ---------------------------------------------------------------------------


class TransferEdge(EdgeModel, table=True):
    """Directed money-movement edge between two :class:`Account` vertices.

    Each transfer is a directed edge from sender → receiver.  The edge carries
    the amount and currency so graph traversal queries can aggregate total flow
    between node pairs.
    """

    __edge_type__ = "transfer"
    __subject__ = Account
    __object__ = Account
    __directed__ = True
    __tablename__ = "edge_transfer"

    id: Optional[int] = Field(default=None, primary_key=True)
    subject_id: int = Field(foreign_key="dim_account.id")
    object_id: int = Field(foreign_key="dim_account.id")
    transfer_date: date
    amount_usd: float
    reference: str = ""


# ---------------------------------------------------------------------------
# Facts
# ---------------------------------------------------------------------------


class TxnFact(FactModel, table=True):
    """Raw ledger transaction fact — one row per debit/credit entry.

    The ``__strategy__`` is ``"bulk"`` (default) — entries are append-only and
    never updated.  Loaded via :class:`~sqldim.LazyTransactionLoader`.
    """

    __grain__ = "one row per ledger entry"
    __tablename__ = "fact_txn"

    id: Optional[int] = Field(default=None, primary_key=True)
    account_id: int = Field(foreign_key="dim_account.id")
    counterparty_id: Optional[int] = Field(
        default=None, foreign_key="dim_counterparty.id"
    )
    txn_date: date
    amount_usd: float  # positive = credit, negative = debit
    txn_type: str = "payment"  # "payment" | "refund" | "fee" | "interest"
    channel: str = "online"


class MonthlyBalanceFact(FactModel, table=True):
    """Month-partitioned balance array — one row per account per month.

    ``balance_history`` stores a JSON array of end-of-day balances for each
    calendar day of the month.  Loaded via
    :class:`~sqldim.LazyArrayMetricLoader` which appends new values into the
    existing array rather than overwriting the row.
    """

    __grain__ = "one row per account per month"
    __strategy__ = "array_metric"
    __natural_key__ = ["account_id", "month_start"]
    __partition_key__ = "account_id"
    __value_column__ = "balance_usd"
    __metric_name__ = "end_of_day_balance"
    __tablename__ = "fact_monthly_balance"

    account_id: int = Field(primary_key=True, foreign_key="dim_account.id")
    month_start: date = Field(primary_key=True)
    balance_usd: float = 0.0
    balance_history: List[float] = Field(default_factory=list, sa_column=Column(JSON))

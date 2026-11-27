"""The DGM model the fintech dataset declares (§2.6).

Written from ``sources/transactions.py``: ``accounts`` and ``counterparties``
are dimensions; each ``transactions`` row is one payment event that names its
account (``account_id``) and its counterparty (``counterparty_id``).

Grain (§2.4): a transaction is an event.  No dimension holds verb edge types
into two fact types, so nothing is Conformed; there is no bridge edge type.
"""

from __future__ import annotations

from sqldim.application.datasets.model_decl import (
    ASSESSED_KINDS,
    BOOLEAN,
    REAL,
    STRING,
    table_node,
    verb_fk,
)
from sqldim.core.query.dgm.annotations import Grain, GrainKind
from sqldim.core.query.dgm.model import DGMModel, NodeKind

__all__ = ["build_model"]


def build_model() -> DGMModel:
    return DGMModel(
        node_types=(
            table_node(
                "Account",
                NodeKind.DIM,
                "accounts",
                ("account_id",),
                {
                    "account_number": STRING,
                    "account_type": STRING,
                    "risk_tier": STRING,
                    "currency": STRING,
                    "owner_name": STRING,
                },
            ),
            table_node(
                "Counterparty",
                NodeKind.DIM,
                "counterparties",
                ("cp_id",),
                {
                    "bic_code": STRING,
                    "institution_name": STRING,
                    "country_code": STRING,
                    "is_sanctioned": BOOLEAN,
                },
            ),
            table_node(
                "Transaction",
                NodeKind.FACT,
                "transactions",
                ("txn_id",),
                {
                    "txn_date": STRING,  # an ISO date stored as VARCHAR
                    "amount_usd": REAL,
                    "txn_type": STRING,
                    "channel": STRING,
                },
            ),
        ),
        edge_types=(
            verb_fk("Account", "Transaction", "holds", "account_id"),
            verb_fk(
                "Counterparty", "Transaction", "counterparty_of", "counterparty_id"
            ),
        ),
        sigma=(Grain("Transaction", GrainKind.EVENT),),
        sigma_kinds=ASSESSED_KINDS,
    )

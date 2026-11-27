"""The DGM model the growth dataset declares (§2.6).

Written from ``sources/``: ``employees`` and ``saas_users`` are dimensions;
``saas_sessions`` records one activity event per row and names its user
(``user_id``, "FK into saas_users"); it carries no measure, so it is a
factless fact.  ``accounts`` is a periodic balance snapshot: one row per
account per ``snapshot_date`` (the bundled data holds the one date
``INITIAL_SNAPSHOT_DATE``), so its entity key is ``(account_id,
snapshot_date)``.  No account dimension table exists, so ``account_id`` is a
property of that fact, not an edge.

Grain (§2.4): a session is an event; the balance snapshot is PERIOD, so a SUM
of balances is valid only within one ``snapshot_date`` (AD2).  No dimension
holds verb edge types into two fact types, so nothing is Conformed; there is
no bridge edge type.  ``balance`` is a state, SEMI_ADDITIVE over TIME
(Additivity, §2.4); ``account_id`` identifies an account with no node type,
Degenerate.
"""

from __future__ import annotations

from sqldim.application.datasets.model_decl import (
    ASSESSED_KINDS,
    measures,
    DATE,
    INTEGER,
    REAL,
    STRING,
    table_node,
    verb_fk,
)
from sqldim.core.query.dgm.annotations import FactlessFact, Grain, GrainKind
from sqldim.core.query.dgm.model import DGMModel, NodeKind

__all__ = ["build_model"]


def build_model() -> DGMModel:
    return DGMModel(
        node_types=(
            table_node(
                "Employee",
                NodeKind.DIM,
                "employees",
                ("employee_id",),
                {"full_name": STRING, "title": STRING, "department": STRING},
            ),
            table_node(
                "AccountBalance",
                NodeKind.FACT,
                "accounts",
                ("account_id", "snapshot_date"),
                {
                    "account_id": INTEGER,
                    "account_type": STRING,
                    "balance": REAL,
                    "snapshot_date": DATE,
                },
            ),
            table_node(
                "SaaSUser",
                NodeKind.DIM,
                "saas_users",
                ("user_id",),
                {
                    "email": STRING,
                    "plan_tier": STRING,
                    "acq_source": STRING,
                    "device": STRING,
                    "signup_date": STRING,  # an ISO date stored as VARCHAR
                },
            ),
            table_node(
                "Session",
                NodeKind.FACT,
                "saas_sessions",
                ("session_id",),
                {"event_date": STRING, "event_type": STRING},
            ),
        ),
        edge_types=(verb_fk("SaaSUser", "Session", "started", "user_id"),),
        sigma=(
            FactlessFact("Session"),
            Grain("Session", GrainKind.EVENT),
            Grain("AccountBalance", GrainKind.PERIOD),
            *measures(
                "AccountBalance",
                semi_over_time=("balance",),
                degenerate=("account_id",),
            ),
        ),
        sigma_kinds=ASSESSED_KINDS,
    )

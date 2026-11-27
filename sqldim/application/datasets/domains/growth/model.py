"""The DGM model the growth dataset declares (§2.6).

Written from ``sources/``: ``employees`` and ``saas_users`` are dimensions;
``saas_sessions`` records one activity event per row and names its user
(``user_id``, "FK into saas_users"); it carries no measure, so it is a
factless fact.  ``accounts`` is a balance snapshot fact.  The bundled
snapshot holds one row per account and leaves ``snapshot_date`` NULL (the
source emits ``as_of_date``), so its entity key is ``account_id`` alone and
``snapshot_date`` is a property.  No account dimension table exists, so
``account_id`` is a property of that fact, not an edge.
"""

from __future__ import annotations

from sqldim.application.datasets.model_decl import (
    ASSESSED_KINDS,
    DATE,
    INTEGER,
    REAL,
    STRING,
    table_node,
    verb_fk,
)
from sqldim.core.query.dgm.annotations import FactlessFact
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
                ("account_id",),
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
        sigma=(FactlessFact("Session"),),
        sigma_kinds=ASSESSED_KINDS,
    )

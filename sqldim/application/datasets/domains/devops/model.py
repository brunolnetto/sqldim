"""The DGM model the devops dataset declares (§2.6).

Written from ``sources/github_issues.py``: the dataset holds one table,
``github_issues``, one row per issue and no foreign key, so ``G_S`` is a
single dimension type with no edge type.  With no fact type and no bridge
edge type, Grain, Conformed and BridgeSemantics hold no annotation.
"""

from __future__ import annotations

from sqldim.application.datasets.model_decl import ASSESSED_KINDS, STRING, table_node
from sqldim.core.query.dgm.model import DGMModel, NodeKind

__all__ = ["build_model"]


def build_model() -> DGMModel:
    return DGMModel(
        node_types=(
            table_node(
                "Issue",
                NodeKind.DIM,
                "github_issues",
                ("issue_id",),
                {"title": STRING, "state": STRING, "author": STRING, "labels": STRING},
            ),
        ),
        sigma_kinds=ASSESSED_KINDS,
    )

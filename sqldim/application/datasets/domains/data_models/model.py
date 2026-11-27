"""The DGM model the data_models dataset declares (§2.6).

Written from ``sources/``.  Two static fixtures, unrelated to each other:

* the DGM showcase (``dgm_showcase_*``): ``customer``, ``product`` and
  ``segment`` are dimensions; each ``sale`` row is one sale naming its
  customer and product.  ``prod_seg`` is a bridge table relating products to
  segments, with a weight per pair, so ``(Product, Segment)`` is an N:N bridge
  edge type held by a RelationTable (M4); its surrogate ``id`` is not mapped.
  ``customer.valid_from``/``valid_to`` are SCD audit columns of a table with
  one row per customer (``id`` is its primary key and there is no version
  key), so they are not properties and ``Customer`` is not versioned;
* the org chart: ``org_dim`` is a dimension of employees whose ``parent_id``
  names the manager, an N:1 bridge edge type held by the employee row.
  ``org_dim_closure`` holds one row per (ancestor, descendant) pair with its
  depth, including each employee with itself at depth 0: an N:N bridge edge
  type with the property ``depth``.  Each ``sales_fact`` row is one sale
  credited to an employee (``dim_id``).

Grain (§2.4): both sale tables hold one row per sale, an event.  The three
bridge edge types relate a member to the group or the superior it belongs to,
with no order in time between them: STRUCTURAL (§2.3).  No dimension holds verb
edge types into two fact types, so nothing is Conformed.
"""

from __future__ import annotations

from sqldim.application.datasets.model_decl import (
    ASSESSED_KINDS,
    INTEGER,
    REAL,
    STRING,
    bridge_fk,
    bridge_table,
    table_node,
    verb_fk,
)
from sqldim.core.query.dgm.annotations import (
    BridgeSemantics,
    BridgeSemanticsKind,
    Grain,
    GrainKind,
)
from sqldim.core.query.dgm.model import DGMModel, EdgeType, NodeKind, NodeType

__all__ = ["build_model"]

_STRUCTURAL = BridgeSemanticsKind.STRUCTURAL


def _showcase() -> tuple[tuple[NodeType, ...], tuple[EdgeType, ...]]:
    nodes = (
        table_node(
            "Customer",
            NodeKind.DIM,
            "dgm_showcase_customer",
            ("id",),
            {"email": STRING, "segment": STRING, "region": STRING},
        ),
        table_node(
            "Product",
            NodeKind.DIM,
            "dgm_showcase_product",
            ("id",),
            {"sku": STRING, "category": STRING},
        ),
        table_node(
            "Segment",
            NodeKind.DIM,
            "dgm_showcase_segment",
            ("id",),
            {"code": STRING, "tier": STRING},
        ),
        table_node(
            "Sale",
            NodeKind.FACT,
            "dgm_showcase_sale",
            ("id",),
            {"revenue": REAL, "quantity": INTEGER, "sale_year": INTEGER},
        ),
    )
    edges = (
        verb_fk("Customer", "Sale", "placed", "customer_id"),
        verb_fk("Product", "Sale", "sold", "product_id"),
        bridge_table(
            ("Product", "product_id"),
            ("Segment", "segment_id"),
            "in_segment",
            "dgm_showcase_prod_seg",
            weight="weight",
        ),
    )
    return nodes, edges


def _org_chart() -> tuple[tuple[NodeType, ...], tuple[EdgeType, ...]]:
    nodes = (
        table_node(
            "OrgMember",
            NodeKind.DIM,
            "org_dim",
            ("id",),
            {
                "employee_code": STRING,
                "name": STRING,
                "hierarchy_level": INTEGER,
                "hierarchy_path": STRING,
            },
        ),
        table_node(
            "OrgSale",
            NodeKind.FACT,
            "sales_fact",
            ("sale_id",),
            {"revenue": REAL},
        ),
    )
    edges = (
        bridge_fk("OrgMember", "OrgMember", "reports_to", "parent_id"),
        bridge_table(
            ("OrgMember", "ancestor_id"),
            ("OrgMember", "descendant_id"),
            "ancestor_of",
            "org_dim_closure",
            {"depth": INTEGER},
        ),
        verb_fk("OrgMember", "OrgSale", "credited_with", "dim_id"),
    )
    return nodes, edges


def build_model() -> DGMModel:
    showcase_nodes, showcase_edges = _showcase()
    org_nodes, org_edges = _org_chart()
    return DGMModel(
        node_types=(*showcase_nodes, *org_nodes),
        edge_types=(*showcase_edges, *org_edges),
        sigma=(
            Grain("Sale", GrainKind.EVENT),
            Grain("OrgSale", GrainKind.EVENT),
            BridgeSemantics("in_segment", _STRUCTURAL),
            BridgeSemantics("reports_to", _STRUCTURAL),
            BridgeSemantics("ancestor_of", _STRUCTURAL),
        ),
        sigma_kinds=ASSESSED_KINDS,
    )

"""
sqldim/examples/features/hierarchy/models.py
=============================================
Kimball dimensional models for the org-chart hierarchy showcase.

These SQLModel table classes define the *warehouse* (analytical) layer:

* :class:`OrgDimModel`        — adjacency-list + materialized-path dimension
* :class:`OrgSalesFact`       — revenue transaction fact keyed to org_dim
* :class:`OrgDimClosureModel` — pre-computed closure bridge for
                                 :class:`~sqldim.core.kimball.dimensions.hierarchy.ClosureTableStrategy`

They are imported by :mod:`sqldim.application.examples.features.hierarchy.showcase`
to call the ``HierarchyMixin`` class-level SQL helpers (``ancestors_sql``,
``descendants_sql``) and to verify the dimensional schema via DDL.

The accompanying OLTP fixture is provided by
:class:`~sqldim.application.datasets.hierarchy.OrgChartSource`.
"""

from __future__ import annotations

from typing import Optional

from sqldim import DimensionModel, FactModel
from sqldim.core.kimball.facts import TransactionFact
from sqldim.core.kimball.fields import Field
from sqldim.core.kimball.dimensions.hierarchy import HierarchyMixin


class OrgDimModel(HierarchyMixin, DimensionModel, table=True):
    """Org-chart dimension with adjacency + materialized-path columns."""

    __tablename__ = "org_dim"
    __natural_key__ = ["employee_code"]
    __scd_type__ = 1
    __hierarchy_strategy__ = "adjacency"
    __table_args__ = ({"extend_existing": True},)

    id: Optional[int] = Field(default=None, primary_key=True, surrogate_key=True)
    employee_code: str = Field(natural_key=True)
    name: str = Field()
    # parent_id, hierarchy_level, hierarchy_path — inherited from HierarchyMixin


class OrgSalesFact(TransactionFact, table=True):
    """Revenue transaction fact keyed to org_dim."""

    __tablename__ = "sales_fact"
    __grain__ = "one revenue event per employee"
    __table_args__ = ({"extend_existing": True},)

    sale_id: Optional[int] = Field(default=None, primary_key=True, surrogate_key=True)
    dim_id: int = Field(foreign_key="org_dim.id", measure=False)
    revenue: float = Field(measure=True, additive=True)


class OrgDimClosureModel(FactModel, table=True):
    """Closure bridge table for ClosureTableStrategy."""

    __tablename__ = "org_dim_closure"
    __table_args__ = ({"extend_existing": True},)

    ancestor_id: int = Field(primary_key=True, foreign_key="org_dim.id")
    descendant_id: int = Field(primary_key=True, foreign_key="org_dim.id")
    depth: int = Field(default=0)

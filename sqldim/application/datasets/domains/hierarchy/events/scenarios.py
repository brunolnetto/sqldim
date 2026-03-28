"""Hierarchy domain — drift event façade.

Re-exports source-specific OLTP mutation functions from their
respective source modules so that ``artifacts/drift.json`` references
resolve unchanged.
"""

from sqldim.application.datasets.domains.hierarchy.events.sales_fact import (
    apply_revenue_surge,
)

__all__ = ["apply_revenue_surge"]

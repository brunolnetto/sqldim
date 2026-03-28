"""DGM domain — drift event façade.

Re-exports source-specific OLTP mutation functions from their
respective source modules so that ``artifacts/drift.json`` references
resolve unchanged.
"""

from sqldim.application.datasets.domains.dgm.events.customers import (
    apply_segment_upgrade,
)

__all__ = ["apply_segment_upgrade"]

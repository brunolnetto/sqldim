"""User-activity domain — drift event façade.

Re-exports source-specific OLTP mutation functions from their
respective source modules so that ``artifacts/drift.json`` references
resolve unchanged.
"""

from sqldim.application.datasets.domains.user_activity.events.devices import (
    apply_mobile_migration,
    apply_os_migration,
)

__all__ = ["apply_mobile_migration", "apply_os_migration"]

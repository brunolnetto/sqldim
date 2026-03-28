"""Media domain — drift event façade.

Re-exports source-specific OLTP mutation functions from their
respective source modules so that ``artifacts/drift.json`` references
resolve unchanged.
"""

from sqldim.application.datasets.domains.media.events.movies import (
    apply_new_release,
)

__all__ = ["apply_new_release"]

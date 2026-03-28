"""DevOps domain — drift event façade.

Re-exports source-specific OLTP mutation functions from their
respective source modules so that ``artifacts/drift.json`` references
resolve unchanged.
"""

from sqldim.application.datasets.domains.devops.events.github_issues import (
    apply_issue_resolution,
)

__all__ = ["apply_issue_resolution"]

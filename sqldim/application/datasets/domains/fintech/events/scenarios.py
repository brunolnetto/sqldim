"""Fintech domain — drift event façade.

Re-exports source-specific OLTP mutation functions from their
respective source modules so that ``artifacts/drift.json`` references
resolve unchanged.
"""

from sqldim.application.datasets.domains.fintech.events.transactions import (
    apply_rate_surge,
)
from sqldim.application.datasets.domains.fintech.events.counterparties import (
    apply_sanctions_wave,
)
from sqldim.application.datasets.domains.fintech.events.accounts import (
    apply_risk_downgrade,
)

__all__ = ["apply_rate_surge", "apply_sanctions_wave", "apply_risk_downgrade"]

"""ContractViolationError — raised when an ERROR-severity rule fires."""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqldim.contracts.report import ContractReport


class ContractViolationError(Exception):
    """Raised by :class:`ContractEngine` (or callers) on ERROR violations.

    Carries the full :class:`ContractReport` so upstream code can inspect
    every violation or log it to an audit trail.
    """

    def __init__(self, report: "ContractReport") -> None:
        self.report = report
        super().__init__(report.summary())

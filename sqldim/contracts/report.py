"""Contract violation report dataclasses."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum


class Severity(str, Enum):
    ERROR   = "error"
    WARNING = "warning"
    INFO    = "info"


@dataclass
class ContractViolation:
    """A single rule violation recorded by :class:`ContractEngine`."""

    rule: str
    severity: str
    count: int
    detail: str


@dataclass
class ContractReport:
    """All violations produced by one :meth:`ContractEngine.validate` call."""

    violations: list[ContractViolation]
    view: str
    elapsed_s: float = 0.0

    @classmethod
    def empty(cls) -> "ContractReport":
        return cls(violations=[], view="", elapsed_s=0.0)

    def has_errors(self) -> bool:
        return any(v.severity == Severity.ERROR.value for v in self.violations)

    def has_warnings(self) -> bool:
        return any(v.severity == Severity.WARNING.value for v in self.violations)

    def summary(self) -> str:
        lines = [
            f"ContractReport({self.view!r}) — {len(self.violations)} violation(s)"
        ]
        for v in self.violations:
            icon = "🔴" if v.severity == Severity.ERROR.value else "⚠️"
            lines.append(f"  {icon} [{v.rule}] {v.count:,} rows: {v.detail}")
        return "\n".join(lines)

    def log(self, logger: logging.Logger | None = None) -> None:
        _log = logger or logging.getLogger("sqldim.contracts")
        for v in self.violations:
            msg = f"[{v.rule}] {v.count:,} violations — {v.detail}"
            if v.severity == Severity.ERROR.value:
                _log.error(msg)
            elif v.severity == Severity.WARNING.value:
                _log.warning(msg)
            else:
                _log.info(msg)

    def to_dict(self) -> dict:
        return {
            "view": self.view,
            "elapsed_s": self.elapsed_s,
            "violations": [
                {"rule": v.rule, "severity": v.severity, "count": v.count, "detail": v.detail}
                for v in self.violations
            ],
        }

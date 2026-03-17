"""sqldim/contracts/schema.py — Column-level schema specification."""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ColumnSpec:
    """Specification for a single column in a data contract schema."""

    name:        str
    dtype:       str
    nullable:    bool       = True
    primary_key: bool       = False
    foreign_key: str | None = None   # "table.column"

    def is_breaking_change(self, new: "ColumnSpec") -> bool:
        """Return True when changing *self* → *new* is a breaking change.

        Breaking changes:
        - dtype changed
        - nullable widened to required (True → False)
        """
        if self.dtype != new.dtype:
            return True
        if self.nullable and not new.nullable:
            return True
        return False

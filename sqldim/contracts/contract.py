"""sqldim/contracts/contract.py — DataContract dataclass."""
from __future__ import annotations

from dataclasses import dataclass, field

from sqldim.medallion import Layer
from sqldim.contracts.version import ContractVersion
from sqldim.contracts.schema import ColumnSpec
from sqldim.contracts.sla import SLASpec


@dataclass
class DataContract:
    """Full data contract for a named dataset."""

    name:    str
    version: ContractVersion
    owner:   str
    layer:   Layer
    columns: list[ColumnSpec] = field(default_factory=list)
    sla:     SLASpec | None   = None

    # ── column lookup ─────────────────────────────────────────────────────────

    def column(self, name: str) -> ColumnSpec | None:
        for col in self.columns:
            if col.name == name:
                return col
        return None

    # ── breaking change detection ─────────────────────────────────────────────

    def is_breaking_change_from(self, old: "DataContract") -> bool:
        """Return True when *self* (new) introduces a breaking change vs *old*.

        Breaking if:
        - a column present in *old* is absent in *new*
        - a shared column is a breaking change per ``ColumnSpec.is_breaking_change``
        """
        old_by_name = {c.name: c for c in old.columns}
        new_by_name = {c.name: c for c in self.columns}

        for col_name, old_col in old_by_name.items():
            if col_name not in new_by_name:
                return True  # column removed
            if old_col.is_breaking_change(new_by_name[col_name]):
                return True

        return False

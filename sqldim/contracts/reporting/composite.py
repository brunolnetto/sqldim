"""Composite contract labels — thin dataclass wrappers that tag which
pipeline boundary each contract applies to (source, state, or output).
"""

from __future__ import annotations

from dataclasses import dataclass, field

from sqldim.contracts.validation.rules import Rule


@dataclass
class Contract:
    """Base for all typed pipeline-boundary contracts."""

    rules: list[Rule] = field(default_factory=list)
    label: str = ""


@dataclass
class SourceContract(Contract):
    """Validates the incoming batch before any processing occurs."""

    label: str = "source"


@dataclass
class StateContract(Contract):
    """Validates the current-state dimension after checksums are built."""

    label: str = "state"


@dataclass
class OutputContract(Contract):
    """Validates the rows written by the processor (new_rows VIEW)."""

    label: str = "output"

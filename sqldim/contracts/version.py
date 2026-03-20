"""sqldim/contracts/version.py — Semantic version model for data contracts."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class ChangeKind(Enum):
    PATCH = "patch"
    MINOR = "minor"
    MAJOR = "major"


@dataclass(frozen=True, order=True)
class ContractVersion:
    major: int
    minor: int
    patch: int

    # ── construction ──────────────────────────────────────────────────────────

    @classmethod
    def parse(cls, s: str) -> "ContractVersion":
        parts = s.split(".")
        if len(parts) != 3:
            raise ValueError(f"Version must be 'major.minor.patch', got {s!r}")
        try:
            return cls(int(parts[0]), int(parts[1]), int(parts[2]))
        except ValueError:
            raise ValueError(f"Version parts must be integers, got {s!r}")

    # ── display ───────────────────────────────────────────────────────────────

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"

    # ── comparison helpers ────────────────────────────────────────────────────

    def is_compatible_with(self, other: "ContractVersion") -> bool:
        """Two versions are compatible when they share the same major number."""
        return self.major == other.major

    def classify_change(self, new: "ContractVersion") -> ChangeKind:
        """Classify the change kind from *self* (old) to *new*."""
        if new.major != self.major:
            return ChangeKind.MAJOR
        if new.minor != self.minor:
            return ChangeKind.MINOR
        return ChangeKind.PATCH

"""DGM parameter space Π(q) — §8.15.

Models the six parameter families, parameter type stratification, and the
ParameterSpace product for a given question.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

__all__ = [
    "ParameterFamily",
    "ParameterTypeKind",
    "ParameterSlot",
    "ParameterSpace",
]


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class ParameterFamily(Enum):
    """Six parameter families from §8.15."""

    TEMPORAL = "TEMPORAL"
    STRATEGY = "STRATEGY"
    FILTER = "FILTER"
    AGGREGATION = "AGGREGATION"
    WINDOW = "WINDOW"
    ALGORITHM = "ALGORITHM"


class ParameterTypeKind(Enum):
    """Three computational types from §8.15 stratification."""

    DISCRETE_FINITE = "DISCRETE_FINITE"
    DISCRETE_INFINITE = "DISCRETE_INFINITE"
    CONTINUOUS = "CONTINUOUS"


# ---------------------------------------------------------------------------
# ParameterSlot
# ---------------------------------------------------------------------------


@dataclass(frozen=True, eq=True)
class ParameterSlot:
    """A single parameterisable dimension within Π(q).

    Parameters
    ----------
    name:
        Human-readable identifier (e.g. ``"as_of"``, ``"agg_fn"``).
    family:
        Which of the six parameter families this slot belongs to.
    type_kind:
        Computational type — discrete finite, discrete infinite, or continuous.
    allowed_values:
        Optional finite set of allowed values (only meaningful for DISCRETE_FINITE).
    """

    name: str
    family: ParameterFamily
    type_kind: ParameterTypeKind
    allowed_values: frozenset[str] | None = None

    @property
    def is_discrete(self) -> bool:
        """True when the slot is discrete (finite or infinite)."""
        return self.type_kind is not ParameterTypeKind.CONTINUOUS


# ---------------------------------------------------------------------------
# ParameterSpace
# ---------------------------------------------------------------------------


@dataclass(frozen=True, eq=True)
class ParameterSpace:
    """Product of all ParameterSlots for a given question q.

    ``Π(q) = Π_temporal(q) × Π_strategy(q) × Π_filter(q) × Π_agg(q) × Π_window(q) × Π_algo(q)``

    Parameters
    ----------
    question_id:
        Identifier of the question this parameter space belongs to.
    slots:
        Tuple of ParameterSlot instances comprising this space.
    """

    question_id: str
    slots: tuple[ParameterSlot, ...]

    @property
    def slot_count(self) -> int:
        """Number of parameter slots."""
        return len(self.slots)

    @property
    def families(self) -> frozenset[ParameterFamily]:
        """Set of parameter families present in this space."""
        return frozenset(s.family for s in self.slots)

    def has_family(self, family: ParameterFamily) -> bool:
        """True when at least one slot belongs to *family*."""
        return family in self.families

    def slots_by_family(self, family: ParameterFamily) -> tuple[ParameterSlot, ...]:
        """Return all slots belonging to *family*."""
        return tuple(s for s in self.slots if s.family is family)

    @property
    def discrete_skeleton(self) -> tuple[ParameterSlot, ...]:
        """The finite set of DISCRETE_FINITE slots — the parametric skeleton."""
        return tuple(
            s for s in self.slots if s.type_kind is ParameterTypeKind.DISCRETE_FINITE
        )

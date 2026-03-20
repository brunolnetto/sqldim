"""Kimball fact table base classes.

Provides :class:`FactType` enumeration and five typed base classes:
:class:`TransactionFact`, :class:`PeriodicSnapshotFact`,
:class:`AccumulatingFact`, :class:`CumulativeFact`, and
:class:`ActivityFact`.  Sub-class any of these to obtain automatic
``__fact_type__`` tagging used by loaders and the schema registry.
"""

from enum import Enum
from sqldim.core.kimball.models import FactModel


class FactType(str, Enum):
    """Enumeration of supported Kimball fact table grain types."""

    TRANSACTION = "transaction"
    PERIODIC_SNAPSHOT = "periodic_snapshot"
    ACCUMULATING_SNAPSHOT = "accumulating_snapshot"
    CUMULATIVE = "cumulative"
    ACTIVITY = "activity"


class TransactionFact(FactModel):
    """Base for transaction-grain fact tables (one row per business event)."""

    __fact_type__ = FactType.TRANSACTION


class PeriodicSnapshotFact(FactModel):
    """Base for periodic-snapshot fact tables (one row per grain per period)."""

    __fact_type__ = FactType.PERIODIC_SNAPSHOT


class AccumulatingFact(FactModel):
    """Base for accumulating-snapshot fact tables (one row per pipeline instance)."""

    __fact_type__ = FactType.ACCUMULATING_SNAPSHOT


class CumulativeFact(FactModel):
    """Base for cumulative fact tables (running totals aggregated over time)."""

    __fact_type__ = FactType.CUMULATIVE


class ActivityFact(FactModel):
    """Base for activity-schema fact tables (sparse, entity-centric event log)."""

    __fact_type__ = FactType.ACTIVITY

from enum import Enum
from sqlmodel import SQLModel
from typing import ClassVar, List, Optional
from sqldim.core.models import FactModel

class FactType(str, Enum):
    TRANSACTION = "transaction"
    PERIODIC_SNAPSHOT = "periodic_snapshot"
    ACCUMULATING_SNAPSHOT = "accumulating_snapshot"
    CUMULATIVE = "cumulative"
    ACTIVITY = "activity"

class TransactionFact(FactModel):
    __fact_type__ = FactType.TRANSACTION

class PeriodicSnapshotFact(FactModel):
    __fact_type__ = FactType.PERIODIC_SNAPSHOT

class AccumulatingFact(FactModel):
    __fact_type__ = FactType.ACCUMULATING_SNAPSHOT

class CumulativeFact(FactModel):
    __fact_type__ = FactType.CUMULATIVE

class ActivityFact(FactModel):
    __fact_type__ = FactType.ACTIVITY

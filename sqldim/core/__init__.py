"""sqldim.core — base models, fields, mixins, SCD foundations, and prebuilt dimensions."""
from sqldim.core.models import DimensionModel, FactModel, BridgeModel
from sqldim.core.fields import Field
from sqldim.core.mixins import SCD2Mixin, SCD3Mixin, DatelistMixin, CumulativeMixin
from sqldim.core.facts import (
    TransactionFact, PeriodicSnapshotFact, AccumulatingFact,
    CumulativeFact, ActivityFact,
)

__all__ = [
    "DimensionModel", "FactModel", "BridgeModel",
    "Field",
    "SCD2Mixin", "SCD3Mixin", "DatelistMixin", "CumulativeMixin",
    "TransactionFact", "PeriodicSnapshotFact", "AccumulatingFact",
    "CumulativeFact", "ActivityFact",
]

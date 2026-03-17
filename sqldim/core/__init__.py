"""sqldim.core — base models, fields, mixins, SCD foundations, and prebuilt dimensions."""
from sqldim.core.kimball.models import DimensionModel, FactModel, BridgeModel
from sqldim.core.kimball.fields import Field
from sqldim.core.kimball.mixins import SCD2Mixin, SCD3Mixin, DatelistMixin, CumulativeMixin
from sqldim.core.kimball.facts import (
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

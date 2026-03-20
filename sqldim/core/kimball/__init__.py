"""sqldim.core.kimball — Kimball-style dimensional modelling primitives.

Exports models, fields, mixins, fact types, and the base SchemaGraph used for
star/snowflake schema resolution.
"""

from sqldim.core.kimball.fields import Field
from sqldim.core.kimball.models import DimensionModel, FactModel, BridgeModel
from sqldim.core.kimball.mixins import (
    SCD2Mixin,
    SCD3Mixin,
    DatelistMixin,
    CumulativeMixin,
)
from sqldim.core.kimball.facts import (
    TransactionFact,
    PeriodicSnapshotFact,
    AccumulatingFact,
    CumulativeFact,
    ActivityFact,
)
from sqldim.core.kimball.schema_graph import SchemaGraph, RolePlayingRef

__all__ = [
    "Field",
    "DimensionModel",
    "FactModel",
    "BridgeModel",
    "SCD2Mixin",
    "SCD3Mixin",
    "DatelistMixin",
    "CumulativeMixin",
    "TransactionFact",
    "PeriodicSnapshotFact",
    "AccumulatingFact",
    "CumulativeFact",
    "ActivityFact",
    "SchemaGraph",
    "RolePlayingRef",
]

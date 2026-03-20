"""sqldim.lineage — pipeline lineage event model and emitters."""

from sqldim.lineage.events import (
    LineageEvent,
    RunState,
    InferredMemberEvent,
    InferredMemberEventType,
)
from sqldim.lineage.emitter import (
    LineageEmitter,
    ConsoleLineageEmitter,
    OpenLineageEmitter,
)
from sqldim.lineage.column import (
    ColumnLineageEntry,
    ColumnLineageFacet,
    extract_declared_lineage,
    extract_structural_lineage,
    extract_sql_lineage,
)

__all__ = [
    "LineageEvent",
    "RunState",
    "InferredMemberEvent",
    "InferredMemberEventType",
    "LineageEmitter",
    "ConsoleLineageEmitter",
    "OpenLineageEmitter",
    "ColumnLineageEntry",
    "ColumnLineageFacet",
    "extract_declared_lineage",
    "extract_structural_lineage",
    "extract_sql_lineage",
]

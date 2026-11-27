"""Helpers for writing the DGM model a bundled dataset declares (§2.6).

Each ``domains/<name>/model.py`` writes its model by hand from the source
definitions of that domain — the tables, columns and foreign keys the sources
create — and is reviewed per dataset.  Nothing here reads a database: a model
is never inferred from table or column names.

Conventions of the bundled declarations:

* a property key is the name of the column that holds it;
* the SCD audit columns every OLTP source carries (``valid_from``,
  ``valid_to``, ``is_current``, ``checksum``; NULL in a snapshot) are not
  properties, and no table holds one row per version, so no type is versioned;
* a foreign-key column is an edge type, not a property;
* :data:`ASSESSED_KINDS` are the annotation kinds each declaration states
  completely: for these, a type without the annotation does not have it.
  Other kinds are left undeclared, and a tool that needs one fails.
"""

from __future__ import annotations

from collections.abc import Mapping

from sqldim.core.query.dgm.model import (
    EdgeKind,
    EdgeType,
    ForeignKey,
    Holder,
    NodeKind,
    NodeTable,
    NodeType,
    PropType,
    Reference,
)

__all__ = [
    "ASSESSED_KINDS",
    "BOOLEAN",
    "DATE",
    "INTEGER",
    "REAL",
    "STRING",
    "TIMESTAMP",
    "bridge_fk",
    "table_node",
    "verb_fk",
]

ASSESSED_KINDS = frozenset(
    {"FactlessFact", "MedallionLayer", "PipelineArtifact", "RolePlaying", "SCDType"}
)

STRING = PropType("String")
INTEGER = PropType("Integer")
REAL = PropType("Real")
BOOLEAN = PropType("Boolean")
DATE = PropType("Date")
TIMESTAMP = PropType("Timestamp")


def table_node(
    name: str,
    kind: NodeKind,
    relation: str,
    entity_key: tuple[str, ...],
    props: Mapping[str, PropType],
    *,
    event_time: str | None = None,
) -> NodeType:
    """A node type held one row per entity in *relation*; props by column name."""
    return NodeType(
        name,
        kind,
        dict(props),
        NodeTable(
            relation,
            entity_key,
            {key: key for key in props},
            event_time=event_time,
        ),
    )


def verb_fk(dim: str, fact: str, label: str, column: str) -> EdgeType:
    """A verb edge type held as a fact-table column referencing the dimension."""
    return EdgeType(
        dim,
        fact,
        label,
        EdgeKind.VERB,
        mapping=ForeignKey(Holder.TARGET, (column,), Reference.ENTITY_KEY),
    )


def bridge_fk(source: str, target: str, label: str, column: str) -> EdgeType:
    """An N:1 bridge edge type held as a source-table column (§2.6)."""
    return EdgeType(
        source,
        target,
        label,
        EdgeKind.BRIDGE,
        mapping=ForeignKey(Holder.SOURCE, (column,), Reference.ENTITY_KEY),
    )

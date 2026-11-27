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
  Other kinds are left undeclared, and a tool that needs one fails;
* every fact type carries one ``Grain`` and every bridge edge type one
  ``BridgeSemantics``; ``Conformed(d, Fs)`` is declared where a dimension
  holds verb edge types into two or more fact types ``Fs`` (§2.4);
* every numeric property of a fact type is either a measure with an explicit
  ``Additivity`` — flows ADDITIVE; rates, ratios, percentages, unit prices,
  scores and gauges NON_ADDITIVE; balances and other states SEMI_ADDITIVE
  over TIME — or a ``Degenerate`` attribute: an identifier with no node type
  behind it, or a dimensional attribute (a season, a year, an age) carried by
  the fact (:func:`measures`).
"""

from __future__ import annotations

from collections.abc import Mapping

from sqldim.core.query.dgm.annotations import (
    TIME,
    Additivity,
    AdditivityKind,
    Degenerate,
    SchemaAnnotation,
)
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
    RelationTable,
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
    "bridge_table",
    "measures",
    "table_node",
    "verb_fk",
]

ASSESSED_KINDS = frozenset(
    {
        "Additivity",
        "BridgeSemantics",
        "Conformed",
        "Degenerate",
        "FactlessFact",
        "Grain",
        "MedallionLayer",
        "PipelineArtifact",
        "RolePlaying",
        "SCDType",
    }
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


def bridge_table(
    source: tuple[str, str],
    target: tuple[str, str],
    label: str,
    relation: str,
    props: Mapping[str, PropType] | None = None,
    *,
    weight: str | None = None,
) -> EdgeType:
    """An N:N bridge edge type held as a bridge table (§2.6, M4).

    *source* and *target* are ``(node type, column)`` pairs: the column of
    *relation* that holds the entity key of that endpoint.  *props* are edge
    properties by column name; *weight* names the column holding the weight.
    """
    declared = dict(props or {})
    columns = {key: key for key in declared}
    if weight is not None:
        declared["weight"] = PropType("Real")
        columns["weight"] = weight
    return EdgeType(
        source[0],
        target[0],
        label,
        EdgeKind.BRIDGE,
        properties=declared,
        weighted=weight is not None,
        many_to_many=True,
        mapping=RelationTable(
            relation,
            (source[1],),
            (target[1],),
            Reference.ENTITY_KEY,
            props=columns,
        ),
    )


def measures(
    fact: str,
    *,
    additive: tuple[str, ...] = (),
    non_additive: tuple[str, ...] = (),
    semi_over_time: tuple[str, ...] = (),
    degenerate: tuple[str, ...] = (),
) -> tuple[SchemaAnnotation, ...]:
    """The Additivity and Degenerate annotations of *fact*'s numeric properties."""
    out: list[SchemaAnnotation] = []
    out += [Additivity(fact, k, AdditivityKind.ADDITIVE) for k in additive]
    out += [Additivity(fact, k, AdditivityKind.NON_ADDITIVE) for k in non_additive]
    out += [
        Additivity(fact, k, AdditivityKind.SEMI_ADDITIVE, {TIME})
        for k in semi_over_time
    ]
    out += [Degenerate(k, fact=fact) for k in degenerate]
    return tuple(out)

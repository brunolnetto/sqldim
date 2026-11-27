"""The artifact and transition tables of one PipelineArtifact, as the model maps them.

:func:`resolve_layout` reads every table, column and edge mapping from the
declared :class:`DGMModel` (§2.4, §2.6); nothing is inferred.  The runtime
appends transitions, so it supports the mappings an append can satisfy:

* the transition type's NodeTable has a one-column entity key, which holds
  the transition's ``idempotency_key`` — appending the same key twice is
  detected, never duplicated;
* ``transition_of`` is a ``ForeignKey`` held by the transition row
  (``Holder.SOURCE``) referencing the artifact's entity key;
* ``followed_by`` is a ``ForeignKey`` held by the later transition
  (``Holder.TARGET``), one column referencing the earlier one's key.

Any other mapping is :class:`UnsupportedOperation`: the runtime does not
rewrite earlier rows to link a chain.  :func:`read_history` reads one
artifact's row and transition chain and refuses a history that breaks PA2 or
PA4, since appending to it could not restore them.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqldim.core.query.dgm.annotations import PipelineArtifact
from sqldim.core.query.dgm.model import (
    AnnotationNotDeclared,
    DGMModel,
    EdgeType,
    ForeignKey,
    Holder,
    NodeTable,
    Reference,
)
from sqldim.core.query.dgm.nl.supervisor._invariant_core import col, ident
from sqldim.core.query.dgm.runtime._artifact_plan import (
    ArtifactConflict,
    ArtifactKey,
    ArtifactState,
    TransitionPlan,
)
from sqldim.core.query.dgm.runtime._errors import (
    UnknownIdentifier,
    UnsupportedOperation,
)

__all__ = ["ArtifactLayout", "History", "Recorded", "resolve_layout", "read_history"]


@dataclass(frozen=True)
class ArtifactLayout:
    name: str
    annotation: PipelineArtifact
    artifact: NodeTable
    transitions: NodeTable
    state: str
    window_start: str
    window_end: str
    tid: str
    owner: tuple[str, ...]
    prev: str
    from_state: str
    kind: str
    to_state: str
    occurred_at: str


def _prop(table: NodeTable, key: str, owner: str) -> str:
    column = table.props.get(key)
    if column is None:
        raise UnsupportedOperation(f"{owner} does not map the property {key!r} (§2.4)")
    return column


def _fk(
    model: DGMModel, source: str, target: str, label: str, holder: Holder
) -> ForeignKey:
    edge: EdgeType | None = model.edge(source, target, label)
    fk = edge.mapping if edge is not None else None
    if (
        not isinstance(fk, ForeignKey)
        or fk.holder is not holder
        or fk.references is not Reference.ENTITY_KEY
    ):
        raise UnsupportedOperation(
            f"({source}, {target}, {label!r}) must be a ForeignKey held by the "
            f"{holder.value.lower()} referencing the entity key for the runtime to "
            "append transitions"
        )
    return fk


def _annotation(model: DGMModel, artifact_type: str) -> PipelineArtifact:
    try:
        anns = model.annotations(PipelineArtifact)
    except AnnotationNotDeclared as exc:
        raise UnsupportedOperation(str(exc)) from exc
    ann = next((a for a in anns if a.fact == artifact_type), None)
    if ann is None:
        raise UnknownIdentifier(f"{artifact_type!r} is not a PipelineArtifact type")
    if ann.transitions is None:
        raise UnsupportedOperation(
            f"PipelineArtifact {artifact_type!r} declares no transitions fact type"
        )
    return ann


def _table(model: DGMModel, name: str) -> NodeTable:
    node = model.node(name)
    if node is None or node.mapping is None:
        raise UnsupportedOperation(f"{name!r} has no NodeTable mapping (§2.6)")
    return node.mapping


def resolve_layout(model: DGMModel, artifact_type: str) -> ArtifactLayout:
    """The tables and columns of *artifact_type*, read from *model*."""
    ann = _annotation(model, artifact_type)
    tname = ann.transitions
    assert tname is not None
    art, tr = _table(model, artifact_type), _table(model, tname)
    if len(tr.entity_key) != 1:
        raise UnsupportedOperation(
            f"{tname!r} needs a one-column entity key to hold the idempotency_key"
        )
    owner = _fk(model, tname, artifact_type, "transition_of", Holder.SOURCE)
    if len(owner.columns) != len(art.entity_key):
        raise UnsupportedOperation("transition_of does not match the artifact's key")
    prev = _fk(model, tname, tname, "followed_by", Holder.TARGET)
    if len(prev.columns) != 1:
        raise UnsupportedOperation("followed_by must be one column")
    a_owner, t_owner = f"artifact type {artifact_type!r}", f"transitions {tname!r}"
    return ArtifactLayout(
        name=artifact_type,
        annotation=ann,
        artifact=art,
        transitions=tr,
        state=_prop(art, "state", a_owner),
        window_start=_prop(art, "window_start", a_owner),
        window_end=_prop(art, "window_end", a_owner),
        tid=tr.entity_key[0],
        owner=owner.columns,
        prev=prev.columns[0],
        from_state=_prop(tr, "from_state", t_owner),
        kind=_prop(tr, "kind", t_owner),
        to_state=_prop(tr, "to_state", t_owner),
        occurred_at=_prop(tr, "occurred_at", t_owner),
    )


# ---------------------------------------------------------------------------
# Reading one artifact
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Recorded:
    key: str
    prev: str | None
    from_state: str
    kind: str
    to_state: str
    occurred_at: datetime


@dataclass(frozen=True)
class History:
    """One artifact's row (``state``, window) and its ordered transition chain."""

    row: tuple[Any, Any, Any] | None
    chain: tuple[Recorded, ...]

    @property
    def state(self) -> ArtifactState:
        """``state_at(a, now)`` of PA3; the row agrees with it (PA4)."""
        return (
            ArtifactState(self.chain[-1].to_state)
            if self.chain
            else ArtifactState.MISSING
        )

    @property
    def last(self) -> Recorded | None:
        return self.chain[-1] if self.chain else None

    @property
    def reached_complete(self) -> bool:
        return any(t.to_state == ArtifactState.COMPLETE.value for t in self.chain)

    def get(self, key: str) -> Recorded | None:
        return next((t for t in self.chain if t.key == key), None)


def _where(columns: Sequence[str], alias: str = "x") -> str:
    return " AND ".join(f"{col(alias, c)} = ?" for c in columns)


def _order(rows: list[Recorded]) -> tuple[Recorded, ...]:
    """The rows as one followed_by chain (PA2), or :class:`ArtifactConflict`."""
    if not rows:
        return ()
    by_prev: dict[str | None, list[Recorded]] = {}
    for r in rows:
        by_prev.setdefault(r.prev, []).append(r)
    chain: list[Recorded] = []
    cursor: str | None = None
    while len(by_prev.get(cursor, [])) == 1 and len(chain) < len(rows):
        chain.append(by_prev[cursor][0])
        cursor = chain[-1].key
    if len(chain) != len(rows) or by_prev.get(cursor):
        raise ArtifactConflict("the artifact's transitions are not one chain (PA2)")
    return tuple(chain)


def _check_chain(chain: tuple[Recorded, ...]) -> None:
    if chain and chain[0].from_state != ArtifactState.MISSING.value:
        raise ArtifactConflict("the first transition does not start at Missing (PA2)")
    for a, b in zip(chain, chain[1:]):
        if b.from_state != a.to_state or b.occurred_at < a.occurred_at:
            raise ArtifactConflict("the transition chain is not consistent (PA2)")


def read_history(con: Any, lay: ArtifactLayout, key: ArtifactKey) -> History:
    art, tr = lay.artifact, lay.transitions
    rows = con.execute(
        f"SELECT CAST({col('x', lay.state)} AS VARCHAR), {col('x', lay.window_start)}, "
        f"{col('x', lay.window_end)} FROM {ident(art.relation)} x "
        f"WHERE {_where(art.entity_key)}",
        list(key),
    ).fetchall()
    if len(rows) > 1:
        raise ArtifactConflict("the artifact key names more than one row")
    found = con.execute(
        f"SELECT CAST({col('x', lay.tid)} AS VARCHAR), CAST({col('x', lay.prev)} AS VARCHAR), "
        f"CAST({col('x', lay.from_state)} AS VARCHAR), CAST({col('x', lay.kind)} AS VARCHAR), "
        f"CAST({col('x', lay.to_state)} AS VARCHAR), "
        f"CAST({col('x', lay.occurred_at)} AS TIMESTAMP) "
        f"FROM {ident(tr.relation)} x WHERE {_where(lay.owner)}",
        list(key),
    ).fetchall()
    history = History(rows[0] if rows else None, _order([Recorded(*r) for r in found]))
    _check_chain(history.chain)
    stored = history.row[0] if history.row else ArtifactState.MISSING.value
    if stored != history.state.value:
        raise ArtifactConflict(
            f"P(a).state is {stored!r} but its transitions say "
            f"{history.state.value!r} (PA4)"
        )
    return history


# ---------------------------------------------------------------------------
# Appending
# ---------------------------------------------------------------------------


def append_transition(
    con: Any,
    lay: ArtifactLayout,
    tp: TransitionPlan,
    prev: Recorded | None,
    at: datetime,
) -> None:
    """Append *tp* after *prev* and set ``P(a).state`` to its ``to_state``."""
    if prev is not None and at < prev.occurred_at:
        raise ArtifactConflict(
            "the clock is behind the artifact's last transition; occurred_at "
            "would break the chain order (PA2)"
        )
    cols = (
        lay.tid,
        *lay.owner,
        lay.prev,
        lay.from_state,
        lay.kind,
        lay.to_state,
        lay.occurred_at,
    )
    values = [
        tp.idempotency_key,
        *tp.artifact_key,
        prev.key if prev else None,
        tp.from_state.value,
        tp.kind.value,
        tp.to_state.value,
        at,
    ]
    names = ", ".join(ident(c) for c in cols)
    marks = ", ".join("?" for _ in cols)
    con.execute(
        f"INSERT INTO {ident(lay.transitions.relation)} ({names}) VALUES ({marks})",
        values,
    )
    con.execute(
        f"UPDATE {ident(lay.artifact.relation)} AS x SET {ident(lay.state)} = ? "
        f"WHERE {_where(lay.artifact.entity_key)}",
        [tp.to_state.value, *tp.artifact_key],
    )


def insert_artifact(
    con: Any, lay: ArtifactLayout, key: ArtifactKey, start: datetime, end: datetime
) -> None:
    """Create the artifact row of a window seen for the first time (state Missing)."""
    cols = (*lay.artifact.entity_key, lay.state, lay.window_start, lay.window_end)
    names = ", ".join(ident(c) for c in cols)
    marks = ", ".join("?" for _ in cols)
    con.execute(
        f"INSERT INTO {ident(lay.artifact.relation)} ({names}) VALUES ({marks})",
        [*key, ArtifactState.MISSING.value, start, end],
    )

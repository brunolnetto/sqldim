"""DGM Supervisor — evaluators of PA1–PA4 (§2.4, PipelineArtifact).

The scope is every ``PipelineArtifact`` of the declared model; a model with
none makes a ``PA*`` request ``INVALID_ARGUMENTS`` (§12.3).  The artifact and
transition fact types, their ``state`` / ``from_state`` / ``kind`` /
``to_state`` / ``occurred_at`` properties and the bridge edge types
``transition_of`` and ``followed_by`` are read from the model; a missing one
makes the evaluation UNSUPPORTED, never vacuous.

* PA1 examines every transition row.
* PA2 examines every artifact: its transitions (via ``transition_of``) must
  form one ``followed_by`` chain, non-decreasing in ``occurred_at``, starting
  at ``Missing``, with ``next.from_state = previous.to_state``.
* PA3 defines ``state_at``; it constrains nothing, so it is UNSUPPORTED.
* PA4 examines every artifact row of an unversioned artifact type.  The
  versioned clause is NOT_IMPLEMENTED in this build.  ``SCDType`` names a
  dimension type (§2.4), so an artifact — a fact type — is versioned only
  through a NodeTable that declares a ``version_key``.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Hashable
from dataclasses import dataclass
from typing import Any

from sqldim.core.query.dgm.nl.supervisor._invariant_core import (
    Evaluator,
    NotEvaluable,
    col,
    ident,
    lit,
    plain,
)
from sqldim.core.query.dgm.annotations import PipelineArtifact
from sqldim.core.query.dgm.model import EdgeType, NodeTable
from sqldim.core.query.dgm.nl.supervisor._types import ToolErrorKind

__all__ = ["ARTIFACT_STATES", "DELTA", "PA_EVALUATORS"]

ARTIFACT_STATES = ("Missing", "In-flight", "Complete", "Stale", "Failed")

DELTA: frozenset[tuple[str, str, str]] = frozenset(
    {
        ("Missing", "scheduled", "In-flight"),
        ("In-flight", "succeeded", "Complete"),
        ("In-flight", "failed", "Failed"),
        ("In-flight", "failed", "Stale"),
        ("Complete", "expired", "Stale"),
        ("Stale", "rescheduled", "In-flight"),
        ("Failed", "retried", "In-flight"),
    }
)
"""The admissible ``(from_state, kind, to_state)`` triples δ of §2.4."""

Key = tuple[Hashable, ...]


@dataclass(frozen=True)
class _Artifact:
    name: str
    table: NodeTable
    transitions: NodeTable
    transition_of: EdgeType
    followed_by: EdgeType


def _prop(table: NodeTable, key: str, owner: str) -> str:
    column = table.props.get(key)
    if column is None:
        raise NotEvaluable(
            ToolErrorKind.UNSUPPORTED,
            f"{owner} does not map the property {key!r} (§2.4)",
            property=key,
        )
    return column


def _edge(
    ev: Evaluator, fact: str, transitions: str, target: str, label: str
) -> EdgeType:
    edge = ev.model.edge(transitions, target, label)
    if edge is None:
        raise NotEvaluable(
            ToolErrorKind.UNSUPPORTED,
            f"PipelineArtifact {fact!r}: the model declares no edge type "
            f"({transitions}, {target}, {label!r}) (§2.4)",
            artifact=fact,
        )
    return edge


def _transition_type(ann: PipelineArtifact) -> str:
    if ann.transitions is None:
        raise NotEvaluable(
            ToolErrorKind.UNSUPPORTED,
            f"PipelineArtifact {ann.fact!r} declares no transitions fact type (§2.4)",
            artifact=ann.fact,
        )
    return ann.transitions


def _artifacts(ev: Evaluator) -> list[_Artifact]:
    anns = ev.model.annotations(PipelineArtifact)
    if not anns:
        raise NotEvaluable(
            ToolErrorKind.INVALID_ARGUMENTS,
            "the dataset has no PipelineArtifact type (§12.3)",
        )
    out = []
    for ann in anns:
        owner = f"PipelineArtifact {ann.fact!r}"
        transitions = _transition_type(ann)
        out.append(
            _Artifact(
                name=ann.fact,
                table=ev.node_table(ann.fact, owner),
                transitions=ev.node_table(transitions, owner),
                transition_of=_edge(
                    ev, ann.fact, transitions, ann.fact, "transition_of"
                ),
                followed_by=_edge(
                    ev, ann.fact, transitions, transitions, "followed_by"
                ),
            )
        )
    return out


def _keys(alias: str, table: NodeTable) -> str:
    return ", ".join(col(alias, k) for k in table.entity_key)


# ---------------------------------------------------------------------------
# PA1
# ---------------------------------------------------------------------------


def pa1(ev: Evaluator) -> None:
    """PA1: (from_state, kind, to_state) ∈ δ for every transition."""
    delta = ", ".join(f"({lit(f)}, {lit(k)}, {lit(t)})" for f, k, t in sorted(DELTA))
    for art in _artifacts(ev):
        tr = art.transitions
        owner = f"transitions of {art.name!r}"
        fs, kd, ts = (
            col("x", _prop(tr, p, owner)) for p in ("from_state", "kind", "to_state")
        )
        base = f"FROM {ident(tr.relation)} x"
        ev.examine(ev.count(base))
        where = (
            f"{base} WHERE NOT EXISTS (SELECT 1 FROM (VALUES {delta}) d(f, k, t) "
            f"WHERE d.f = CAST({fs} AS VARCHAR) AND d.k = CAST({kd} AS VARCHAR) "
            f"AND d.t = CAST({ts} AS VARCHAR))"
        )
        count = ev.count(where)
        if not count:
            continue
        width = len(tr.entity_key)
        rows = ev.sample(f"{_keys('x', tr)}, {fs}, {kd}, {ts}", where)
        ev.violate_many(
            count,
            [
                {
                    "artifact_type": art.name,
                    "transition": plain(list(r[:width])),
                    "from_state": plain(r[width]),
                    "kind": plain(r[width + 1]),
                    "to_state": plain(r[width + 2]),
                    "problem": "(from_state, kind, to_state) is not in δ",
                }
                for r in rows
            ],
        )


# ---------------------------------------------------------------------------
# Shared reads for PA2 and PA4
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Transition:
    from_state: Any
    to_state: Any
    occurred_at: Any


def _transitions(ev: Evaluator, art: _Artifact) -> dict[Key, _Transition]:
    tr = art.transitions
    owner = f"transitions of {art.name!r}"
    fs, ts, oc = (
        col("x", _prop(tr, p, owner)) for p in ("from_state", "to_state", "occurred_at")
    )
    width = len(tr.entity_key)
    rows = ev.rows(
        f"SELECT {_keys('x', tr)}, CAST({fs} AS VARCHAR), CAST({ts} AS VARCHAR), "
        f"CAST({oc} AS TIMESTAMP) FROM {ident(tr.relation)} x"
    )
    return {tuple(r[:width]): _Transition(*r[width:]) for r in rows}


def _pairs(ev: Evaluator, edge: EdgeType) -> list[tuple[Key, Key]]:
    es = ev.edge_sql(edge)
    rows = ev.rows(es.sql)
    n = es.n_source
    return [(tuple(r[:n]), tuple(r[n : n + es.n_target])) for r in rows]


def _owned(ev: Evaluator, art: _Artifact) -> dict[Key, set[Key]]:
    """Artifact key → keys of its transitions (``transition_of``)."""
    owned: dict[Key, set[Key]] = defaultdict(set)
    for transition, artifact in _pairs(ev, art.transition_of):
        owned[artifact].add(transition)
    return owned


def _artifact_keys(ev: Evaluator, art: _Artifact) -> list[Key]:
    return [
        tuple(r)
        for r in ev.rows(
            f"SELECT DISTINCT {_keys('x', art.table)} FROM {ident(art.table.relation)} x"
        )
    ]


# ---------------------------------------------------------------------------
# PA2
# ---------------------------------------------------------------------------


def _link_problems(
    owned: set[Key], succ: dict[Key, list[Key]], pred: dict[Key, list[Key]]
) -> list[str]:
    problems = []
    if any(t not in owned for s in owned for t in succ.get(s, [])) or any(
        p not in owned for t in owned for p in pred.get(t, [])
    ):
        problems.append("followed_by links a transition of another artifact")
    if any(len(succ.get(t, [])) > 1 or len(pred.get(t, [])) > 1 for t in owned):
        problems.append("a transition has more than one successor or predecessor")
    return problems


def _walk(
    owned: set[Key], succ: dict[Key, list[Key]], pred: dict[Key, list[Key]]
) -> list[Key] | str:
    """The chain from its head; called once every degree is ≤ 1 within *owned*.

    In-degree ≤ 1 means the walk never revisits a transition, so it ends; a
    transition it does not reach lies on a separate path or cycle.
    """
    heads = [t for t in owned if not pred.get(t)]
    if len(heads) != 1:
        return f"the followed_by chain has {len(heads)} heads"
    order = [heads[0]]
    while succ.get(order[-1]) and len(order) <= len(owned):
        order.append(succ[order[-1]][0])
    if len(order) != len(owned):
        return "the followed_by chain does not reach every transition"
    return order


def _order_problems(order: list[Key], tr: dict[Key, _Transition]) -> list[str]:
    missing = [t for t in order if t not in tr]
    if missing:
        return ["a transition_of edge names a transition with no row"]
    problems = []
    if tr[order[0]].from_state != "Missing":
        problems.append("the first transition does not start at Missing")
    for prev, nxt in zip(order, order[1:]):
        a, b = tr[prev], tr[nxt]
        if (
            a.occurred_at is None
            or b.occurred_at is None
            or b.occurred_at < a.occurred_at
        ):
            problems.append("the chain is not ordered by occurred_at")
        if b.from_state != a.to_state:
            problems.append("next.from_state differs from previous.to_state")
    return sorted(set(problems))


def _chain_problems(
    owned: set[Key],
    succ: dict[Key, list[Key]],
    pred: dict[Key, list[Key]],
    tr: dict[Key, _Transition],
) -> list[str]:
    if not owned:
        return []
    problems = _link_problems(owned, succ, pred)
    if problems:
        return problems
    order = _walk(owned, succ, pred)
    if isinstance(order, str):
        return [order]
    return _order_problems(order, tr)


def pa2(ev: Evaluator) -> None:
    """PA2: the transitions of one artifact form a single ordered chain."""
    for art in _artifacts(ev):
        tr = _transitions(ev, art)
        owned = _owned(ev, art)
        succ: dict[Key, list[Key]] = defaultdict(list)
        pred: dict[Key, list[Key]] = defaultdict(list)
        for s, t in _pairs(ev, art.followed_by):
            succ[s].append(t)
            pred[t].append(s)
        for key in _artifact_keys(ev, art):
            ev.examine()
            problems = _chain_problems(owned.get(key, set()), succ, pred, tr)
            if problems:
                ev.violate(
                    {
                        "artifact_type": art.name,
                        "artifact": plain(list(key)),
                        "transitions": len(owned.get(key, set())),
                        "problems": problems,
                    }
                )


# ---------------------------------------------------------------------------
# PA4
# ---------------------------------------------------------------------------


def _state_at(
    art: str, key: Key, owned: set[Key], tr: dict[Key, _Transition], now: Any
) -> Any:
    """PA3: to_state of the last transition with occurred_at ≤ now, or Missing."""
    past = [
        tr[t]
        for t in owned
        if t in tr and tr[t].occurred_at is not None and tr[t].occurred_at <= now
    ]
    if not past:
        return "Missing"
    latest = max(t.occurred_at for t in past)
    states = {t.to_state for t in past if t.occurred_at == latest}
    if len(states) != 1:
        raise NotEvaluable(
            ToolErrorKind.UNSUPPORTED,
            "state_at is undefined: transitions of one artifact share the latest "
            "occurred_at with different to_state (PA3)",
            artifact_type=art,
            artifact=plain(list(key)),
        )
    return states.pop()


def pa4(ev: Evaluator) -> None:
    """PA4: P(a).state = state_at(a, now) for every artifact row."""
    for art in _artifacts(ev):
        if art.table.version_key is not None:
            raise NotEvaluable(
                ToolErrorKind.NOT_IMPLEMENTED,
                "PA4 for a versioned artifact type (state in force per version) is "
                "not implemented in this build",
                artifact_type=art.name,
            )
        state = col("x", _prop(art.table, "state", f"artifact type {art.name!r}"))
        now = (
            ev.now
            if ev.now is not None
            else ev.scalar("SELECT CAST(current_timestamp AS TIMESTAMP)")
        )
        tr = _transitions(ev, art)
        owned = _owned(ev, art)
        width = len(art.table.entity_key)
        rows = ev.rows(
            f"SELECT {_keys('x', art.table)}, CAST({state} AS VARCHAR) FROM {ident(art.table.relation)} x"
        )
        for r in rows:
            ev.examine()
            key = tuple(r[:width])
            expected = _state_at(art.name, key, owned.get(key, set()), tr, now)
            if r[width] != expected:
                ev.violate(
                    {
                        "artifact_type": art.name,
                        "artifact": plain(list(key)),
                        "state": r[width],
                        "state_at_now": expected,
                        "now": plain(now),
                    }
                )


def _pa3(ev: Evaluator) -> None:
    _artifacts(ev)  # no PipelineArtifact type: INVALID_ARGUMENTS first (§12.3)
    raise NotEvaluable(
        ToolErrorKind.UNSUPPORTED,
        "PA3 defines state_at and constrains no instance; PA4 checks the state "
        "against it",
        invariant="PA3",
    )


PA_EVALUATORS: dict[str, Callable[[Evaluator], None]] = {
    "PA1": pa1,
    "PA2": pa2,
    "PA3": _pa3,
    "PA4": pa4,
}

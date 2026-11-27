"""The recommender runtime of §11.10 over the three layers of §7.1.

:class:`RuntimeRecommender` is the ``recommender`` of ``DGMRuntime``:

* :meth:`~RuntimeRecommender.top_k_candidates` — questions from the
  :class:`QuestionSpace` (Layer 1: drawn from ``Q_valid`` over the schema),
  each decoded to a plan IR, kept only if its filters are BDD-feasible
  (Layer 2), priced by the :class:`CostModel` and ordered by the space's rank
  then cost (Layer 3).  Candidates are plan IR, never SQL templates.
* :meth:`~RuntimeRecommender.refinements` — the gate's
  :class:`RefinementProvider` (§6.3 C1): single descent steps that add one
  conjunct to the B1 filter of a single-leaf question and are BDD-valid.
* :meth:`~RuntimeRecommender.clarification_options` — the options of the
  gate's ``CLARIFY`` decision for a plan, as candidates.  They are exactly the
  options the gate admitted under C1–C3; the recommender adds none.

:class:`DeclaredQuestionSpace` is a :class:`QuestionSpace` over questions
and refinement atoms the host declares.  The space generated from the
question lattice ``L(G_AB)`` is the subject of #131.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from sqldim.core.query.dgm.planner._gate import (
    ClarifyDecision,
    ClarifyOption,
    ExecutionBudget,
    ExecutionGate,
    Refinement,
    cost_total,
)
from sqldim.core.query.dgm.planner._ir import (
    BandQuery,
    Compose,
    Conj,
    Join,
    PlanIdentityUnavailable,
    PlanIR,
    PlanNode,
    Pred,
)
from sqldim.core.query.dgm.planner._ir_identity import dag_hash
from sqldim.core.query.dgm.planner._targets import ExportPlan
from sqldim.core.query.dgm.runtime._bdd import PredicateBDD
from sqldim.core.query.dgm.runtime._dag import CostModel, PlanRefLike, QueryDAG
from sqldim.core.query.dgm.runtime._errors import (
    InvalidRuntimeArgument,
    NoCandidates,
    NotConfigured,
    UnsupportedOperation,
)
from sqldim.core.query.dgm.runtime._spec import (
    band_coverage,
    decode_pred,
    decode_spec,
    encode_spec,
    spec_hash,
)

__all__ = [
    "Grounding",
    "ProposedQuestion",
    "QuestionSpace",
    "QuestionCandidate",
    "RefinementAtom",
    "DeclaredQuestion",
    "DeclaredQuestionSpace",
    "RuntimeRecommender",
]


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Grounding:
    """What candidate generation is grounded in (§11.10 agents 1–3)."""

    utterance_version: int
    resolved: tuple[tuple[str, str], ...]  # (alias, prop)
    node_aliases: Mapping[str, str]  # NL term → alias
    temporal: Any = None
    compositional: Any = None

    @property
    def aliases(self) -> frozenset[str]:
        return frozenset(a for a, _ in self.resolved) | frozenset(
            self.node_aliases.values()
        )


@dataclass(frozen=True)
class ProposedQuestion:
    """A question proposed by a :class:`QuestionSpace`; lower rank first."""

    spec: Mapping[str, object]
    description: str
    rank: int = 0


class QuestionSpace(Protocol):
    def questions(self, grounding: Grounding) -> Sequence[ProposedQuestion]:
        """Questions of ``Q_valid`` that use the grounded aliases."""
        ...

    def refinements(self, ir: PlanIR) -> Sequence[ProposedQuestion]:
        """Single descent steps from the question *ir* in ``L(G_AB)``."""
        ...


@dataclass(frozen=True)
class QuestionCandidate:
    """§11.10 ``QueryCandidate`` plus what a CLARIFY option adds."""

    candidate_id: str
    spec: dict
    description: str
    band_coverage: list[str]
    cost_estimate: float
    kind: str = "QUESTION"  # QUESTION | REFINEMENT | SUBSTITUTION
    substitution: str | None = None
    warning: str | None = None

    def to_dict(self) -> dict:
        return {
            "candidate_id": self.candidate_id,
            "spec": self.spec,
            "description": self.description,
            "band_coverage": list(self.band_coverage),
            "cost_estimate": self.cost_estimate,
            "kind": self.kind,
            "substitution": self.substitution,
            "warning": self.warning,
        }


# ---------------------------------------------------------------------------
# Grounding from agent results
# ---------------------------------------------------------------------------


def _field(obj: object, name: str) -> Any:
    if isinstance(obj, Mapping):
        if name not in obj:
            raise InvalidRuntimeArgument(f"grounding result has no {name!r}")
        return obj[name]
    if not hasattr(obj, name):
        raise InvalidRuntimeArgument(f"grounding result has no {name!r}")
    return getattr(obj, name)


def _version(obj: object, what: str) -> int:
    v = _field(obj, "utterance_version")
    if isinstance(v, bool) or not isinstance(v, int):
        raise InvalidRuntimeArgument(f"{what}.utterance_version must be an int")
    return v


def _grounding(entity: object, temporal: object, compositional: object) -> Grounding:
    if entity is None or temporal is None or compositional is None:
        raise InvalidRuntimeArgument(
            "candidate generation needs the entity, temporal and compositional results"
        )
    versions = {
        _version(entity, "entity_result"),
        _version(temporal, "temporal_result"),
        _version(compositional, "compositional"),
    }
    if len(versions) != 1:
        raise InvalidRuntimeArgument(
            f"grounding results are of different utterance versions {sorted(versions)}"
        )
    unresolved = list(_field(entity, "unresolved"))
    if unresolved:
        raise NoCandidates(
            f"terms {unresolved} are unresolved; they are asked back, not guessed"
        )
    resolved = tuple(
        (str(_field(r, "alias")), str(_field(r, "prop")))
        for r in _field(entity, "resolved")
    )
    aliases = dict(_field(entity, "node_aliases"))
    return Grounding(versions.pop(), resolved, aliases, temporal, compositional)


# ---------------------------------------------------------------------------
# Descent steps
# ---------------------------------------------------------------------------


def _conjuncts(p: Pred | None) -> tuple[Pred, ...]:
    if p is None:
        return ()
    return p.preds if isinstance(p, Conj) else (p,)


def _is_descent(base: PlanNode, refined: PlanNode, bdd: PredicateBDD) -> bool:
    """*refined* adds exactly one conjunct to the B1 filter of *base* (C1)."""
    if not isinstance(base, BandQuery) or not isinstance(refined, BandQuery):
        return False
    old, new = list(_conjuncts(base.where)), list(_conjuncts(refined.where))
    if len(new) != len(old) + 1:
        return False
    rest = list(new)
    for p in old:
        if p not in rest:
            return False
        rest.remove(p)
    same_shape = BandQuery(**{**refined.__dict__, "where": base.where}) == base
    return (
        same_shape
        and bdd.satisfiable(refined.where)
        and not bdd.equivalent(refined.where, base.where)
    )


def _feasible(ir: PlanIR, bdd: PredicateBDD) -> bool:
    """No leaf filter is contradictory by Boolean structure (§7.1 Layer 2)."""
    stack: list[PlanNode] = [ir.root]
    while stack:
        n = stack.pop()
        if isinstance(n, Compose):
            stack += [n.left, n.right]
        elif not all(bdd.satisfiable(p) for p in (n.where, n.having, n.qualify)):
            return False
    return True


# ---------------------------------------------------------------------------
# RuntimeRecommender
# ---------------------------------------------------------------------------


class RuntimeRecommender:
    """``recommender`` of §11.10; also the gate's :class:`RefinementProvider`.

    The gate that :meth:`clarification_options` consults is attached after
    construction (:meth:`attach_gate`), because the gate itself is built with
    this recommender as its refinement provider.
    """

    def __init__(
        self, *, dag: QueryDAG, space: QuestionSpace, cost_model: CostModel
    ) -> None:
        for name, dep in (("dag", dag), ("space", space), ("cost_model", cost_model)):
            if dep is None:
                raise NotConfigured(f"RuntimeRecommender needs a {name}")
        self._dag = dag
        self._space = space
        self._cost_model = cost_model
        self._gate: ExecutionGate | None = None
        self._described: dict[str, str] = {}  # dag_hash → refinement description

    def attach_gate(self, gate: ExecutionGate) -> None:
        if self._gate is not None:
            raise InvalidRuntimeArgument("a gate is already attached")
        self._gate = gate

    # -- pricing -----------------------------------------------------------

    def _priced(self, plan: ExportPlan) -> ExportPlan:
        est = self._cost_model.estimate(plan)
        if est.cost is None:
            raise UnsupportedOperation(
                "the cost model gives no estimate for a candidate; an unpriced "
                "candidate is not offered"
            )
        plan.cost_estimate = est.cost
        plan.estimated_rows = est.rows
        return plan

    def _candidate(
        self, spec: Mapping[str, object], description: str, bdd: PredicateBDD
    ) -> tuple[str, QuestionCandidate] | None:
        qs = decode_spec(spec)
        if not _feasible(qs.ir, bdd):
            return None
        key = self._dag.key_for_spec(spec)
        plan = self._priced(ExportPlan.from_ir(qs.ir, self._dag.query_target))
        assert plan.cost_estimate is not None
        canon = encode_spec(qs.ir, qs.params)
        return key, QuestionCandidate(
            spec_hash(canon),
            canon,
            description,
            band_coverage(qs.ir),
            cost_total(plan.cost_estimate),
        )

    # -- top_k_candidates ----------------------------------------------------

    def top_k_candidates(
        self,
        *,
        entity_result: object,
        temporal_result: object,
        compositional: object,
        k: int,
        page: int = 0,
    ) -> list[QuestionCandidate]:
        """Page *page* of the *k* best feasible questions for the grounding."""
        if isinstance(k, bool) or not isinstance(k, int) or k < 1:
            raise InvalidRuntimeArgument(f"k must be a positive int, got {k!r}")
        if isinstance(page, bool) or not isinstance(page, int) or page < 0:
            raise InvalidRuntimeArgument(
                f"page must be a non-negative int, got {page!r}"
            )
        g = _grounding(entity_result, temporal_result, compositional)
        if not g.aliases:
            raise NoCandidates("nothing was resolved; there is no question to propose")
        ranked = self._ranked(self._space.questions(g))
        items = ranked[page * k : (page + 1) * k]
        if not items:
            raise NoCandidates(
                "no feasible question for this grounding"
                if page == 0
                else f"no candidates beyond page {page - 1}"
            )
        return items

    def _ranked(self, proposals: Sequence[ProposedQuestion]) -> list[QuestionCandidate]:
        bdd = PredicateBDD()
        best: dict[str, tuple[tuple[int, float, int], QuestionCandidate]] = {}
        for i, p in enumerate(proposals):
            got = self._candidate(p.spec, p.description, bdd)
            if got is None:
                continue
            key, cand = got
            order = (p.rank, cand.cost_estimate, i)
            if key not in best or order < best[key][0]:  # one per question
                best[key] = (order, cand)
        return [c for _, c in sorted(best.values(), key=lambda pair: pair[0])]

    # -- RefinementProvider --------------------------------------------------

    def refinements(
        self, plan: ExportPlan, budget: ExecutionBudget
    ) -> list[Refinement]:
        """Single BDD-valid descent steps from *plan*, priced (§6.3 C1)."""
        if plan.ir is None:
            raise PlanIdentityUnavailable("a plan without IR has no refinements")
        proposals = self._space.refinements(plan.ir)
        if not proposals:
            return []
        if not isinstance(plan.ir.root, BandQuery) or plan.ir.ctes:
            raise UnsupportedOperation(
                "refinement of a composed question is not validated in this version"
            )
        bdd = PredicateBDD()
        out = []
        for i, p in enumerate(proposals):
            refined = decode_spec(p.spec).ir
            if refined.ctes or not _is_descent(plan.ir.root, refined.root, bdd):
                raise UnsupportedOperation(
                    f"proposal {p.description!r} is not a single BDD-valid descent step"
                )
            priced = self._priced(ExportPlan.from_ir(refined, plan.query_target))
            self._described[dag_hash(refined)] = p.description
            out.append(Refinement(plan=priced, rank=p.rank * len(proposals) + i))
        return out

    # -- clarification_options -----------------------------------------------

    def _option(self, o: ClarifyOption, params: dict) -> QuestionCandidate:
        spec = self._dag.spec_for_plan(o.plan, params)
        assert o.plan.ir is not None
        if o.substitution is not None:
            description = o.warning or ""
            sub: str | None = o.substitution.value
        else:
            description = self._described.get(dag_hash(o.plan.ir), "")
            sub = None
        if not description:
            raise UnsupportedOperation("a clarification option has no description")
        return QuestionCandidate(
            spec_hash(spec),
            spec,
            description,
            band_coverage(o.plan.ir),
            cost_total(o.cost),
            o.kind.value,
            sub,
            o.warning,
        )

    def clarification_options(
        self, plan: PlanRefLike, *, budget: ExecutionBudget
    ) -> list[QuestionCandidate]:
        """The gate's CLARIFY options for *plan* (C1–C3 of §6.3), as candidates."""
        if self._gate is None:
            raise NotConfigured("no gate is attached to the recommender")
        decision = self._gate.gate(self._dag.plan_for(plan), budget)
        if not isinstance(decision, ClarifyDecision):
            raise NoCandidates(
                f"the gate returned {decision.kind.value}, not CLARIFY; there "
                "are no clarification options"
            )
        params = self._dag.params_for(plan.question_key)
        return [self._option(o, params) for o in decision.options]


# ---------------------------------------------------------------------------
# DeclaredQuestionSpace
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DeclaredQuestion:
    spec: Mapping[str, object]
    description: str


@dataclass(frozen=True)
class RefinementAtom:
    """A predicate (spec form) that narrows a question that binds ``alias``."""

    alias: str
    pred: Mapping[str, object]
    description: str


def _aliases(ir: PlanIR) -> frozenset[str]:
    out: set[str] = set()
    stack: list[PlanNode] = [ir.root]
    while stack:
        n = stack.pop()
        if isinstance(n, Compose):
            stack += [n.left, n.right]
        else:
            out |= {s.alias for s in n.scans()}
    return frozenset(out)


@dataclass
class DeclaredQuestionSpace:
    """A :class:`QuestionSpace` over host-declared questions and atoms.

    ``questions`` returns, in declaration order, every declared question that
    binds all grounded aliases.  ``refinements`` conjoins each declared atom
    whose alias the question's anchor or joins bind, and that is not already a
    conjunct, to the question's B1 filter.  Every declaration is decoded at
    construction, so a malformed one fails there.
    """

    questions_declared: Sequence[DeclaredQuestion] = ()
    atoms: Sequence[RefinementAtom] = ()
    _decoded: list[tuple[DeclaredQuestion, frozenset[str]]] = field(
        init=False, default_factory=list
    )

    def __post_init__(self) -> None:
        for q in self.questions_declared:
            self._decoded.append((q, _aliases(decode_spec(q.spec).ir)))
        for a in self.atoms:
            decode_pred(a.pred, frozenset({a.alias}))

    def questions(self, grounding: Grounding) -> list[ProposedQuestion]:
        need = grounding.aliases
        return [
            ProposedQuestion(q.spec, q.description, i)
            for i, (q, bound) in enumerate(self._decoded)
            if need <= bound
        ]

    def refinements(self, ir: PlanIR) -> list[ProposedQuestion]:
        root = ir.root
        if not isinstance(root, BandQuery) or ir.ctes:
            return []
        joins = [j for j in root.joins if isinstance(j, Join)]
        if len(joins) != len(root.joins):  # a Repeat join has no spec form yet
            return []
        scope = frozenset([root.anchor.alias, *(j.scan.alias for j in joins)])
        base = _conjuncts(root.where)
        out = []
        for i, a in enumerate(self.atoms):
            if a.alias not in scope:
                continue
            atom = decode_pred(a.pred, scope)
            if atom in base:
                continue
            refined = BandQuery(**{**root.__dict__, "where": Conj((*base, atom))})
            out.append(
                ProposedQuestion(encode_spec(PlanIR(root=refined)), a.description, i)
            )
        return out

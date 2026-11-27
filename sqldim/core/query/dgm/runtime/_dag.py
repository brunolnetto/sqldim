"""The query DAG manager of §11.10 over the canonical identity of §8.14.

:class:`QueryDAG` is the ``query_dag`` of ``DGMRuntime``.  A question enters
it as a spec (:mod:`._spec`) and is identified from then on by its
``question_key`` — ``H(canon_version, schema_version, dag_hash(root),
params)`` of #148.  A :data:`DagNodeId` is a handle local to one manager: it
is issued by :meth:`QueryDAG.make_from_spec`, never stored in state, and means
nothing to another manager.  Everything a later step needs is rebuilt from a
:class:`PlanRef` — the ``question_key`` plus the ordered rewrites — through
the :class:`QuestionStore`, so a resume in a new process works with a new
manager.

Fail closed:

* a spec that does not decode, or that the IR cannot emit, is an error;
* a ``question_key`` the store does not hold is :class:`UnknownIdentifier`;
* a stored question whose key no longer matches — the schema version
  changed — is refused, not re-keyed;
* a recorded rewrite that the configured :class:`RewriteProvider` does not
  reproduce is refused; with no provider, any recorded rewrite is refused;
* ``result_key`` needs ``deps(plan)`` and a watermark reader; a relation the
  catalog does not declare is an error, and a source without a watermark is
  reported as *unversioned* on the key (D41), never given a made-up version.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import NewType, Protocol

from sqldim.core.query.dgm.algebra import ComposeOp
from sqldim.core.query.dgm.planner._canon import canonical_hash
from sqldim.core.query.dgm.planner._gate import RewriteProvider
from sqldim.core.query.dgm.planner._ir import (
    BandQuery,
    Compose,
    PlanIdentityUnavailable,
    PlanIR,
    PlanNode,
)
from sqldim.core.query.dgm.planner._ir_identity import (
    Dependency,
    RelationCatalog,
    dag_hash,
)
from sqldim.core.query.dgm.planner._targets import (
    CostEstimate,
    ExportPlan,
    QueryTarget,
)
from sqldim.core.query.dgm.runtime._errors import (
    InvalidRuntimeArgument,
    NotConfigured,
    UnknownIdentifier,
    UnsupportedOperation,
)
from sqldim.core.query.dgm.runtime._spec import (
    QuestionSpec,
    decode_spec,
    encode_spec,
)

__all__ = [
    "DagNodeId",
    "QuestionKey",
    "PlanRef",
    "PlanRefLike",
    "ResultKey",
    "PlanEstimate",
    "CostModel",
    "QuestionStore",
    "InMemoryQuestionStore",
    "WatermarkReader",
    "DecompositionStep",
    "Decomposition",
    "QueryDAG",
]

DagNodeId = NewType("DagNodeId", int)
QuestionKey = NewType("QuestionKey", str)


# ---------------------------------------------------------------------------
# Value types
# ---------------------------------------------------------------------------


class PlanRefLike(Protocol):
    """What :meth:`QueryDAG.plan_for` reads from a plan reference (§11.10)."""

    @property
    def question_key(self) -> str: ...

    @property
    def plan_version(self) -> int: ...

    @property
    def applied_rewrites(self) -> Sequence[str]: ...


@dataclass(frozen=True)
class PlanRef:
    """Serialisable reference to a plan: the question and its applied rewrites."""

    question_key: str
    plan_version: int = 0
    applied_rewrites: tuple[str, ...] = ()


@dataclass(frozen=True)
class ResultKey:
    """``result_key`` of §8.14 and what it was computed from.

    ``unversioned`` lists the dependencies without a watermark (D41): no key
    change can reveal that they changed, so a result or a job under this key
    is not reused by default.
    """

    value: str
    question_key: str
    dag_hash: str
    watermarks: tuple[tuple[str, tuple[str, ...] | None, str | int | None], ...]
    unversioned: tuple[str, ...] = ()

    @property
    def versioned(self) -> bool:
        return not self.unversioned


@dataclass(frozen=True)
class PlanEstimate:
    """A cost model's answer; ``None`` fields are unknown (never guessed)."""

    cost: CostEstimate | None
    rows: int | None = None


class CostModel(Protocol):
    def estimate(self, plan: ExportPlan) -> PlanEstimate:
        """Cost and row estimate of *plan* (§6.3 needs a cost to admit it)."""
        ...


class QuestionStore(Protocol):
    """``saved[question_key]`` of §8.14: specs, independent of data and time."""

    def get(self, key: str) -> Mapping[str, object] | None: ...

    def put(self, key: str, spec: Mapping[str, object]) -> None: ...


@dataclass
class InMemoryQuestionStore:
    """A :class:`QuestionStore` held in this process (lost on restart)."""

    _specs: dict[str, dict] = field(default_factory=dict)

    def get(self, key: str) -> Mapping[str, object] | None:
        spec = self._specs.get(key)
        return None if spec is None else json.loads(json.dumps(spec))

    def put(self, key: str, spec: Mapping[str, object]) -> None:
        self._specs.setdefault(key, json.loads(json.dumps(dict(spec))))


class WatermarkReader(Protocol):
    def watermark(self, dep: Dependency) -> str | int | None:
        """``w(src)`` of §8.14, or ``None`` when the source has no watermark."""
        ...


@dataclass(frozen=True)
class DecompositionStep:
    index: int
    kind: str  # QUESTION | UNION | INTERSECT | JOIN
    label: str
    text: str
    children: tuple[int, ...] = ()


@dataclass(frozen=True)
class Decomposition:
    """The mechanical explanation of §8.14 *Query explanation*."""

    question_key: str
    steps: tuple[DecompositionStep, ...]

    def to_dict(self) -> dict:
        return {
            "question_key": self.question_key,
            "steps": [
                {
                    "index": s.index,
                    "kind": s.kind,
                    "label": s.label,
                    "text": s.text,
                    "children": list(s.children),
                }
                for s in self.steps
            ],
        }


# ---------------------------------------------------------------------------
# Decomposition
# ---------------------------------------------------------------------------

_EXPLAINED = {
    ComposeOp.UNION: "either {l} or {r}",
    ComposeOp.INTERSECT: "both {l} and {r} simultaneously",
    ComposeOp.JOIN: "{l} correlated with {r} on {on}",
}


class _Decomposer:
    def __init__(self) -> None:
        self.steps: list[DecompositionStep] = []
        self.index: dict[PlanNode, int] = {}
        self.questions = 0

    def visit(self, n: PlanNode) -> int:
        if n in self.index:
            return self.index[n]
        step = self._leaf(n) if isinstance(n, BandQuery) else self._compose(n)
        self.index[n] = step.index
        self.steps.append(step)
        return step.index

    def _leaf(self, n: BandQuery) -> DecompositionStep:
        self.questions += 1
        label = f"question {self.questions}"
        body = json.dumps(encode_spec(PlanIR(root=n))["nodes"][0], sort_keys=True)
        return DecompositionStep(len(self.steps), "QUESTION", label, f"{label}: {body}")

    def _compose(self, n: Compose) -> DecompositionStep:
        if n.op not in _EXPLAINED:
            raise UnsupportedOperation(
                f"§8.14 defines no explanation for a {n.op.value} node"
            )
        left, right = self.visit(n.left), self.visit(n.right)
        on = "" if n.on is None else n.on.sql
        text = _EXPLAINED[n.op].format(
            l=self.steps[left].label, r=self.steps[right].label, on=on
        )
        index = len(self.steps)
        return DecompositionStep(
            index, n.op.value, f"result {index}", text, (left, right)
        )


# ---------------------------------------------------------------------------
# QueryDAG
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Entry:
    key: str
    spec: QuestionSpec


class QueryDAG:
    """``query_dag`` of §11.10.

    Every dependency is explicit: *schema_version* (the hash of ``G_S``, Σ
    and M, §2.6), the *store* of questions, the *catalog* that states
    ``deps(plan)``, the *watermarks* reader, and the *cost_model* that prices
    plans for the gate.  *rewrites* reproduces recorded rewrites; without it a
    :class:`PlanRef` with rewrites is refused.
    """

    def __init__(
        self,
        *,
        schema_version: str,
        store: QuestionStore,
        catalog: RelationCatalog,
        watermarks: WatermarkReader,
        cost_model: CostModel,
        rewrites: RewriteProvider | None = None,
        query_target: QueryTarget = QueryTarget.SQL_DUCKDB,
    ) -> None:
        if not isinstance(schema_version, str) or not schema_version:
            raise NotConfigured("QueryDAG needs a non-empty schema_version")
        for name, dep in (
            ("store", store),
            ("catalog", catalog),
            ("watermarks", watermarks),
            ("cost_model", cost_model),
        ):
            if dep is None:
                raise NotConfigured(f"QueryDAG needs a {name}")
        self.schema_version = schema_version
        self._store = store
        self._catalog = catalog
        self._watermarks = watermarks
        self._cost_model = cost_model
        self._rewrites = rewrites
        self._target = query_target
        self._entries: list[_Entry] = []
        self._unique: dict[str, DagNodeId] = {}

    @property
    def query_target(self) -> QueryTarget:
        return self._target

    # -- construction ------------------------------------------------------

    def _plan(self, qs: QuestionSpec) -> ExportPlan:
        return ExportPlan.from_ir(qs.ir, self._target)

    def key_for_spec(self, spec: object) -> QuestionKey:
        """The ``question_key`` of *spec*, without adding it to the DAG."""
        qs = decode_spec(spec)
        return self._key(qs)

    def _key(self, qs: QuestionSpec) -> QuestionKey:
        plan = self._plan(qs)
        return QuestionKey(
            plan.question_key(schema_version=self.schema_version, params=qs.params)
        )

    def make_from_spec(self, spec: object) -> DagNodeId:
        """Add the question *spec* describes; equal questions share one node."""
        qs = decode_spec(spec)
        key = self._key(qs)
        if key in self._unique:
            return self._unique[key]
        self._store.put(key, encode_spec(qs.ir, qs.params))
        node = DagNodeId(len(self._entries))
        self._entries.append(_Entry(key, qs))
        self._unique[key] = node
        return node

    def question_key(self, node: DagNodeId) -> QuestionKey:
        if isinstance(node, bool) or not isinstance(node, int):
            raise InvalidRuntimeArgument(f"a DagNodeId is an int, got {node!r}")
        if not 0 <= node < len(self._entries):
            raise UnknownIdentifier(f"DAG node {node} is not held by this manager")
        return QuestionKey(self._entries[node].key)

    def params_for(self, key: str) -> dict[str, object]:
        """The resolved parameters π of the question stored under *key*."""
        return dict(self._load(key).params)

    def spec_for_plan(self, plan: ExportPlan, params: Mapping[str, object]) -> dict:
        """The spec of an IR plan (a candidate offered from a gate option)."""
        if plan.ir is None or plan.pre_compute:
            raise PlanIdentityUnavailable(
                "only a plan with an IR and no PreComputation has a spec"
            )
        return encode_spec(plan.ir, params)

    # -- lookup ------------------------------------------------------------

    def _load(self, key: str) -> QuestionSpec:
        if not isinstance(key, str) or not key:
            raise InvalidRuntimeArgument("a question_key is a non-empty string")
        spec = self._store.get(key)
        if spec is None:
            raise UnknownIdentifier(f"question {key!r} is not in the question store")
        qs = decode_spec(spec)
        if self._key(qs) != key:
            raise InvalidRuntimeArgument(
                f"question {key!r} was stored under another schema version or "
                "canonical form; it is not re-keyed"
            )
        return qs

    @staticmethod
    def _check_ref(ref: PlanRefLike) -> tuple[str, ...]:
        rewrites = tuple(ref.applied_rewrites)
        if ref.plan_version != len(rewrites):
            raise InvalidRuntimeArgument(
                f"plan_version {ref.plan_version} does not match "
                f"{len(rewrites)} applied rewrites"
            )
        if len(set(rewrites)) != len(rewrites):
            raise InvalidRuntimeArgument("a rewrite is never applied twice (§11.10)")
        return rewrites

    def _rewrite(self, plan: ExportPlan, rule: str) -> ExportPlan:
        if self._rewrites is None:
            raise NotConfigured(
                f"the plan records rewrite {rule!r} but no RewriteProvider is configured"
            )
        matches = [
            c for c in self._rewrites.candidates(plan) if c.substitution.value == rule
        ]
        if len(matches) != 1:
            raise UnsupportedOperation(
                f"rewrite {rule!r} is not reproduced by the RewriteProvider "
                f"({len(matches)} candidates)"
            )
        return matches[0].plan

    def _priced(self, plan: ExportPlan) -> ExportPlan:
        if plan.cost_estimate is not None:
            return plan
        est = self._cost_model.estimate(plan)
        return replace(plan, cost_estimate=est.cost, estimated_rows=est.rows)

    def _resolve(self, ref: PlanRefLike) -> tuple[QuestionSpec, ExportPlan]:
        rewrites = self._check_ref(ref)
        qs = self._load(ref.question_key)
        plan = self._plan(qs)
        for rule in rewrites:
            plan = self._rewrite(plan, rule)
        return qs, plan

    def plan_for(self, ref: PlanRefLike) -> ExportPlan:
        """The plan *ref* denotes, rebuilt from the store and priced (§11.10)."""
        _, plan = self._resolve(ref)
        return self._priced(plan)

    # -- result_key ----------------------------------------------------------

    def _watermark(self, dep: Dependency) -> str | int | None:
        w = self._watermarks.watermark(dep)
        if w is not None and (isinstance(w, bool) or not isinstance(w, (str, int))):
            raise InvalidRuntimeArgument(
                f"watermark of {dep.relation!r} must be a str or int, got {w!r}"
            )
        return w

    def result_key(self, ref: PlanRefLike) -> ResultKey:
        """``result_key`` of §8.14 for the plan *ref* denotes.

        The plan IR has no τ-dependent construct (``Now``) in this version,
        so τ and the time zone do not enter the key.
        """
        qs, plan = self._resolve(ref)
        if plan.ir is None:
            raise PlanIdentityUnavailable(
                "the rewritten plan has no IR, so it has no result_key"
            )
        qkey = plan.question_key(schema_version=self.schema_version, params=qs.params)
        w = tuple(
            (d.relation, d.partitions, self._watermark(d))
            for d in plan.deps(self._catalog)
        )
        unversioned = tuple(r for r, _, wm in w if wm is None)
        value = canonical_hash("result_key", qkey, w)
        return ResultKey(value, qkey, dag_hash(plan.ir), w, unversioned)

    # -- explanation ---------------------------------------------------------

    def decompose_by_key(self, key: str) -> Decomposition:
        """The §8.14 decomposition of the question stored under *key*."""
        qs = self._load(key)
        d = _Decomposer()
        d.visit(qs.ir.root)
        return Decomposition(key, tuple(d.steps))

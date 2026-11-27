"""DGM pre-execution gate — ExecutionBudget, BudgetDecision, ExecutionGate (§6.3).

Sits between the planner and the SQL emitter.  A plan the budget admits is
delivered exactly as submitted.  A reduction that changes the answer — a
rewrite, a sample or an approximate algorithm — is a :class:`Substitution`
and is applied only when the caller lists it in ``budget.substitutions``;
otherwise it is offered as a ``CLARIFY`` option and nothing runs (D40,
ADR 0010).

Fail closed:

* a budget that violates I1–I3 is rejected at construction;
* a plan without a cost estimate, or with a PreComputation whose time cannot
  be estimated, is rejected — an unknown cost is never admitted;
* a plan in the OVER zone is never executed as submitted;
* ``ASYNC`` names a sink and a callback the host configured; there is no
  default (:class:`AsyncNotConfigured` otherwise).

Rewrites, samples, refinements and approximations come from injected
providers (:class:`RewriteProvider`, :class:`SamplingProvider`,
:class:`RefinementProvider`, :class:`PrecomputeProvider`).  A gate without a
provider simply knows no such reduction: the OVER zone then ends in
``REJECT``, never in an unrequested execution.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum, IntEnum
from typing import TYPE_CHECKING, Protocol, Union

if TYPE_CHECKING:
    from sqldim.core.query.dgm.planner._targets import (
        CostEstimate,
        ExportPlan,
        PreComputation,
        SinkTarget,
    )

__all__ = [
    "HARD_CEILING_FACTOR",
    "SAMPLE_DELTA",
    "SAMPLE_MAX_ERROR",
    "MAX_CLARIFY_OPTIONS",
    "DEFAULT_PAGE_SIZE",
    "Substitution",
    "Zone",
    "ClarifyKind",
    "ClarifyOption",
    "BudgetDecisionKind",
    "SampleMethod",
    "BudgetDecision",
    "ProceedDecision",
    "StreamDecision",
    "AsyncDecision",
    "RewriteDecision",
    "SampleDecision",
    "PaginateDecision",
    "ClarifyDecision",
    "RejectDecision",
    "Delivery",
    "InvalidBudget",
    "AsyncNotConfigured",
    "AsyncTarget",
    "RewriteCandidate",
    "SamplingPlan",
    "Refinement",
    "Approximation",
    "RewriteProvider",
    "SamplingProvider",
    "RefinementProvider",
    "PrecomputeProvider",
    "ExecutionBudget",
    "ExecutionGate",
    "cost_total",
    "zone",
]

# ---------------------------------------------------------------------------
# Policy constants (§6.3)
# ---------------------------------------------------------------------------

#: hard_ceiling(b) = b.max_estimated_cost × HARD_CEILING_FACTOR; finite, ≥ 1.
HARD_CEILING_FACTOR: float = 2.0
#: Failure probability of a SAMPLE guarantee.
SAMPLE_DELTA: float = 0.05
#: Largest error half-width a SAMPLE may report.
SAMPLE_MAX_ERROR: float = 0.05
#: Options returned by one CLARIFY.
MAX_CLARIFY_OPTIONS: int = 3
#: Page size of PAGINATE.
DEFAULT_PAGE_SIZE: int = 1_000

_NARROWING_HINT = "Narrow the query scope to reduce cost."


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class Substitution(Enum):
    """An answer-changing reduction the caller may allow (D40)."""

    STRATEGY_SHORTEST = "STRATEGY_SHORTEST"
    DEPTH_LIMIT = "DEPTH_LIMIT"
    CONE_SCOPE = "CONE_SCOPE"
    APPROX_ALGORITHM = "APPROX_ALGORITHM"
    SAMPLE = "SAMPLE"


#: Rule order of Step 2 — the tie-break of ``best(allowed)``.
_REWRITE_ORDER: tuple[Substitution, ...] = (
    Substitution.STRATEGY_SHORTEST,
    Substitution.DEPTH_LIMIT,
    Substitution.CONE_SCOPE,
)


class Zone(IntEnum):
    """Cost zones, ordered SYNC < … < FORBIDDEN (§6.3)."""

    SYNC = 0
    STREAMING = 1
    DEFERRED = 2
    OVER = 3
    FORBIDDEN = 4


class ClarifyKind(Enum):
    REFINEMENT = "REFINEMENT"
    SUBSTITUTION = "SUBSTITUTION"


class BudgetDecisionKind(Enum):
    """The eight possible outcomes of the pre-execution gate (§6.3)."""

    PROCEED = "PROCEED"
    STREAM = "STREAM"
    ASYNC = "ASYNC"
    REWRITE = "REWRITE"
    SAMPLE = "SAMPLE"
    PAGINATE = "PAGINATE"
    CLARIFY = "CLARIFY"
    REJECT = "REJECT"


class SampleMethod(Enum):
    """Sampling strategy for :class:`SampleDecision` (§6.3 Strategy 3)."""

    RANDOM = "RANDOM"
    STRATIFIED = "STRATIFIED"
    RESERVOIR = "RESERVOIR"


class InvalidBudget(ValueError):
    """An ExecutionBudget that violates I1–I3 (configuration error)."""


class AsyncNotConfigured(RuntimeError):
    """A DEFERRED plan, but the host configured no async sink and callback."""


# ---------------------------------------------------------------------------
# Cost arithmetic and zones
# ---------------------------------------------------------------------------


def cost_total(cost: "CostEstimate") -> float:
    """The scalar total by which costs are compared (§6.3)."""
    return float(cost.cpu_ops) + float(cost.io_ops)


def zone(cost: "CostEstimate", budget: "ExecutionBudget") -> Zone:
    """``zone(c, b)`` — every threshold is inclusive on the cheaper side."""
    c = cost_total(cost)
    if c <= cost_total(budget.streaming_threshold):
        return Zone.SYNC
    if c <= cost_total(budget.async_threshold):
        return Zone.STREAMING
    if c <= cost_total(budget.max_estimated_cost):
        return Zone.DEFERRED
    if c <= budget.hard_ceiling:
        return Zone.OVER
    return Zone.FORBIDDEN


# ---------------------------------------------------------------------------
# ExecutionBudget
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ExecutionBudget:
    """Declared execution constraints for the pre-execution gate (§6.3).

    ``substitutions`` lists the answer-changing reductions the caller allows
    (default ∅).  The budget is immutable: a child budget (§12.5) is derived
    with :func:`dataclasses.replace`, which re-checks the invariants and
    keeps the set unchanged.
    """

    max_estimated_cost: "CostEstimate"
    max_result_rows: int
    max_wall_time: timedelta
    max_precompute_time: timedelta
    streaming_threshold: "CostEstimate"
    async_threshold: "CostEstimate"
    substitutions: frozenset[Substitution] = frozenset()

    def __post_init__(self) -> None:
        if not isinstance(self.substitutions, frozenset) or not all(
            isinstance(s, Substitution) for s in self.substitutions
        ):
            raise InvalidBudget("substitutions must be a frozenset of Substitution")
        totals = [
            cost_total(self.streaming_threshold),
            cost_total(self.async_threshold),
            cost_total(self.max_estimated_cost),
        ]
        if not all(math.isfinite(t) for t in totals):  # I2
            raise InvalidBudget("I2: cost thresholds must be finite")
        stream, async_, ceiling = totals
        if not 0 <= stream <= async_ <= ceiling:  # I1
            raise InvalidBudget(
                "I1: 0 ≤ streaming_threshold ≤ async_threshold ≤ max_estimated_cost "
                f"does not hold ({stream}, {async_}, {ceiling})"
            )
        if not ceiling <= self.hard_ceiling:  # I3
            raise InvalidBudget("I3: max_estimated_cost exceeds the hard ceiling")
        if self.max_result_rows < 0:
            raise InvalidBudget("max_result_rows must be non-negative")

    @property
    def hard_ceiling(self) -> float:
        return cost_total(self.max_estimated_cost) * HARD_CEILING_FACTOR


# ---------------------------------------------------------------------------
# Decisions
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BudgetDecision:
    """Abstract base for all pre-execution gate decisions."""

    kind: BudgetDecisionKind


@dataclass(frozen=True)
class ProceedDecision(BudgetDecision):
    """SYNC zone — execute synchronously."""

    kind: BudgetDecisionKind = field(default=BudgetDecisionKind.PROCEED, init=False)


@dataclass(frozen=True)
class StreamDecision(BudgetDecision):
    """STREAMING zone — execute now, deliver progressively."""

    kind: BudgetDecisionKind = field(default=BudgetDecisionKind.STREAM, init=False)


@dataclass(frozen=True)
class AsyncDecision(BudgetDecision):
    """DEFERRED zone — execute asynchronously, deliver to a configured sink."""

    kind: BudgetDecisionKind = field(default=BudgetDecisionKind.ASYNC, init=False)
    sink: "SinkTarget"
    callback: str
    ttl: timedelta

    def __post_init__(self) -> None:
        if self.sink is None or not self.callback:
            raise AsyncNotConfigured("ASYNC needs a sink and a callback endpoint")


@dataclass(frozen=True)
class PaginateDecision(BudgetDecision):
    """Deliver the exact result page by page."""

    kind: BudgetDecisionKind = field(default=BudgetDecisionKind.PAGINATE, init=False)
    page_size: int


Delivery = Union[ProceedDecision, StreamDecision, AsyncDecision, PaginateDecision]
"""The decisions that execute a plan as given."""

_DELIVERIES = (ProceedDecision, StreamDecision, AsyncDecision, PaginateDecision)


def _check_then(then: object) -> None:
    if not isinstance(then, _DELIVERIES):
        raise TypeError("`then` must be a Delivery (PROCEED, STREAM, ASYNC, PAGINATE)")


@dataclass(frozen=True)
class RewriteDecision(BudgetDecision):
    """An allowed substitution, delivered under ``then`` without re-gating."""

    kind: BudgetDecisionKind = field(default=BudgetDecisionKind.REWRITE, init=False)
    plan: "ExportPlan"
    substitution: Substitution
    warning: str
    then: Delivery

    def __post_init__(self) -> None:
        _check_then(self.then)


@dataclass(frozen=True)
class SampleDecision(BudgetDecision):
    """An allowed sample with a stated Hoeffding guarantee.

    ``plan`` is the sampled plan the caller executes under ``then``.
    """

    kind: BudgetDecisionKind = field(default=BudgetDecisionKind.SAMPLE, init=False)
    plan: "ExportPlan"
    fraction: float
    method: SampleMethod
    error_bound: float
    confidence: float
    then: Delivery

    def __post_init__(self) -> None:
        _check_then(self.then)
        if not 0.0 < self.fraction < 1.0:
            raise ValueError("a sample fraction lies in (0, 1)")


@dataclass(frozen=True)
class ClarifyOption:
    """One choice put to the caller (§6.3, C1–C3)."""

    plan: "ExportPlan"
    cost: "CostEstimate"
    kind: ClarifyKind
    substitution: Substitution | None = None
    warning: str | None = None

    def __post_init__(self) -> None:
        is_sub = self.kind is ClarifyKind.SUBSTITUTION
        if is_sub != (self.substitution is not None) or is_sub != (
            self.warning is not None
        ):
            raise ValueError(
                "a SUBSTITUTION option names its substitution and warning; "
                "a REFINEMENT option has neither"
            )


@dataclass(frozen=True)
class ClarifyDecision(BudgetDecision):
    """Ask the caller to choose; nothing is executed."""

    kind: BudgetDecisionKind = field(default=BudgetDecisionKind.CLARIFY, init=False)
    options: tuple[ClarifyOption, ...]

    def __post_init__(self) -> None:
        if not self.options:
            raise ValueError("CLARIFY carries at least one option")


@dataclass(frozen=True)
class RejectDecision(BudgetDecision):
    """Refuse; nothing is executed."""

    kind: BudgetDecisionKind = field(default=BudgetDecisionKind.REJECT, init=False)
    reason: str


# ---------------------------------------------------------------------------
# Providers (injected; none means "no such reduction is known")
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AsyncTarget:
    """Where the host delivers ASYNC results (§6.3 Strategy 4)."""

    sink: "SinkTarget"
    callback: str
    ttl: timedelta = timedelta(hours=24)


@dataclass(frozen=True)
class RewriteCandidate:
    plan: "ExportPlan"
    substitution: Substitution
    warning: str


@dataclass(frozen=True)
class SamplingPlan:
    """A sampled plan for which S1–S4 hold."""

    plan: "ExportPlan"
    fraction: float


@dataclass(frozen=True)
class Refinement:
    """A single BDD-valid descent step in L(G_AB) (C1)."""

    plan: "ExportPlan"
    rank: int = 0


@dataclass(frozen=True)
class Approximation:
    plan: "ExportPlan"
    warning: str


class RewriteProvider(Protocol):
    def candidates(self, plan: "ExportPlan") -> Sequence[RewriteCandidate]:
        """Strategy 1 rewrites of *plan*, at most one per rule."""
        ...


class SamplingProvider(Protocol):
    def sampling_plan(
        self, plan: "ExportPlan", budget: ExecutionBudget
    ) -> SamplingPlan | None:
        """A sampled plan when S1–S4 hold, else ``None``."""
        ...


class RefinementProvider(Protocol):
    def refinements(
        self, plan: "ExportPlan", budget: ExecutionBudget
    ) -> Sequence[Refinement]:
        """Recommender refinements of *plan* (C1)."""
        ...


class PrecomputeProvider(Protocol):
    def estimated_time(self, pc: "PreComputation") -> timedelta | None:
        """Estimated duration of *pc*; ``None`` when it cannot be estimated."""
        ...

    def approximate(
        self, plan: "ExportPlan", slow: Sequence["PreComputation"]
    ) -> Approximation | None:
        """The plan with every PreComputation in *slow* approximated, if possible."""
        ...


# ---------------------------------------------------------------------------
# ExecutionGate
# ---------------------------------------------------------------------------


def _plan_cost(plan: "ExportPlan") -> "CostEstimate":
    assert plan.cost_estimate is not None
    return plan.cost_estimate


class ExecutionGate:
    """The §6.3 decision procedure.  Stateless apart from its providers."""

    def __init__(
        self,
        *,
        rewrites: RewriteProvider | None = None,
        sampling: SamplingProvider | None = None,
        refinements: RefinementProvider | None = None,
        precompute: PrecomputeProvider | None = None,
        async_target: AsyncTarget | None = None,
    ) -> None:
        self._rewrites = rewrites
        self._sampling = sampling
        self._refinements = refinements
        self._precompute = precompute
        self._async_target = async_target

    # -- gate --------------------------------------------------------------

    def gate(self, plan: "ExportPlan", budget: ExecutionBudget) -> BudgetDecision:
        """Apply the §6.3 decision procedure to *plan* against *budget*."""
        if plan.cost_estimate is None:
            return RejectDecision(
                reason="The plan has no cost estimate; an unknown cost is not admitted."
            )
        z = zone(plan.cost_estimate, budget)
        if z is Zone.FORBIDDEN:  # Step 1
            return RejectDecision(
                reason=f"Query exceeds hard cost ceiling. {_NARROWING_HINT}"
            )
        guarded = self._precompute_guard(plan, budget)  # Step 1b
        if guarded is not None:
            return guarded
        viable: list[RewriteCandidate] = []
        if z > Zone.SYNC:  # Step 2
            viable = self._viable_rewrites(plan, budget, z)
            allowed = [r for r in viable if r.substitution in budget.substitutions]
            if allowed:
                r = self._best(allowed, budget)
                return RewriteDecision(
                    plan=r.plan,
                    substitution=r.substitution,
                    warning=r.warning,
                    then=self.deliver(r.plan, budget),
                )
        if z is Zone.OVER:  # Step 3
            return self._over(plan, budget, viable)
        return self.deliver(plan, budget)  # Step 4

    # -- delivery ----------------------------------------------------------

    def deliver(self, plan: "ExportPlan", budget: ExecutionBudget) -> Delivery:
        """The delivery of an executable plan; never reduces or refuses it.

        A plan without a row estimate is paginated: pagination delivers the
        exact result, so not knowing the size never changes the answer.
        """
        z = zone(_plan_cost(plan), budget)
        if z > Zone.DEFERRED:
            raise ValueError("deliver() needs a plan in the DEFERRED zone or below")
        if z is Zone.DEFERRED:
            if self._async_target is None:
                raise AsyncNotConfigured(
                    "the plan is in the DEFERRED zone and no async sink and "
                    "callback are configured"
                )
            t = self._async_target
            return AsyncDecision(sink=t.sink, callback=t.callback, ttl=t.ttl)
        if z is Zone.STREAMING:
            return StreamDecision()
        rows = plan.estimated_rows
        if rows is None or rows > budget.max_result_rows:
            return PaginateDecision(page_size=DEFAULT_PAGE_SIZE)
        return ProceedDecision()

    # -- Step 1b -----------------------------------------------------------

    def _slow_precomputations(
        self, plan: "ExportPlan", budget: ExecutionBudget
    ) -> list["PreComputation"] | None:
        """PreComputations over budget; ``None`` when a time is unknown."""
        if not plan.pre_compute:
            return []
        if self._precompute is None:
            return None
        slow = []
        for pc in plan.pre_compute:
            t = self._precompute.estimated_time(pc)
            if t is None:
                return None
            if t > budget.max_precompute_time:
                slow.append(pc)
        return slow

    def _executable_approximation(
        self, approx: Approximation | None, budget: ExecutionBudget
    ) -> bool:
        """An approximation counts only if it is priced within DEFERRED and
        each of its own PreComputations has a known time within budget."""
        if approx is None or approx.plan.cost_estimate is None:
            return False
        if zone(approx.plan.cost_estimate, budget) > Zone.DEFERRED:
            return False
        return self._slow_precomputations(approx.plan, budget) == []

    def _precompute_guard(
        self, plan: "ExportPlan", budget: ExecutionBudget
    ) -> BudgetDecision | None:
        slow = self._slow_precomputations(plan, budget)
        if slow is None:
            return RejectDecision(
                reason="A PreComputation has no time estimate; it is not started."
            )
        if not slow:
            return None
        assert self._precompute is not None
        approx = self._precompute.approximate(plan, slow)
        if self._executable_approximation(approx, budget):
            assert approx is not None and approx.plan.cost_estimate is not None
            if Substitution.APPROX_ALGORITHM in budget.substitutions:
                return RewriteDecision(
                    plan=approx.plan,
                    substitution=Substitution.APPROX_ALGORITHM,
                    warning=approx.warning,
                    then=self.deliver(approx.plan, budget),
                )
            return ClarifyDecision(
                options=(
                    ClarifyOption(
                        plan=approx.plan,
                        cost=approx.plan.cost_estimate,
                        kind=ClarifyKind.SUBSTITUTION,
                        substitution=Substitution.APPROX_ALGORITHM,
                        warning=approx.warning,
                    ),
                )
            )
        return RejectDecision(
            reason="PreComputation exceeds max_precompute_time and has no "
            f"executable approximation. {_NARROWING_HINT}"
        )

    # -- Step 2 ------------------------------------------------------------

    def _viable_rewrites(
        self, plan: "ExportPlan", budget: ExecutionBudget, z: Zone
    ) -> list[RewriteCandidate]:
        if self._rewrites is None:
            return []
        return [
            r
            for r in self._rewrites.candidates(plan)
            if r.substitution in _REWRITE_ORDER
            and r.plan.cost_estimate is not None
            and zone(r.plan.cost_estimate, budget) < z
        ]

    @staticmethod
    def _best(
        candidates: Sequence[RewriteCandidate], budget: ExecutionBudget
    ) -> RewriteCandidate:
        return min(
            candidates,
            key=lambda r: (
                zone(_plan_cost(r.plan), budget),
                _REWRITE_ORDER.index(r.substitution),
                cost_total(_plan_cost(r.plan)),
            ),
        )

    # -- Step 3 ------------------------------------------------------------

    def _over(
        self,
        plan: "ExportPlan",
        budget: ExecutionBudget,
        viable: Sequence[RewriteCandidate],
    ) -> BudgetDecision:
        s = self._sample(plan, budget)
        if s is not None and Substitution.SAMPLE in budget.substitutions:
            return SampleDecision(
                plan=s.plan,
                fraction=s.fraction,
                method=SampleMethod.RANDOM,
                error_bound=SAMPLE_MAX_ERROR,
                confidence=1 - SAMPLE_DELTA,
                then=self.deliver(s.plan, budget),
            )
        options = self._clarification_options(plan, budget, viable, s)
        if options:
            return ClarifyDecision(options=options)
        return RejectDecision(
            reason="Query exceeds max_estimated_cost and admits no allowed or "
            f"offerable reduction. {_NARROWING_HINT}"
        )

    def _sample(
        self, plan: "ExportPlan", budget: ExecutionBudget
    ) -> SamplingPlan | None:
        if self._sampling is None:
            return None
        s = self._sampling.sampling_plan(plan, budget)
        if s is None or s.plan.cost_estimate is None:
            return None
        if zone(s.plan.cost_estimate, budget) > Zone.DEFERRED:  # S4
            return None
        return s

    def _clarification_options(
        self,
        plan: "ExportPlan",
        budget: ExecutionBudget,
        viable: Sequence[RewriteCandidate],
        s: SamplingPlan | None,
    ) -> tuple[ClarifyOption, ...]:
        ranked = (
            self._refinement_options(plan, budget)
            + self._rewrite_options(viable, budget)
            + self._sample_options(s, budget)
        )
        executable = [
            (k, o)
            for k, o in ranked
            if zone(o.cost, budget) <= Zone.DEFERRED  # C3
        ]
        executable.sort(key=lambda pair: pair[0])
        return tuple(o for _, o in executable[:MAX_CLARIFY_OPTIONS])

    def _refinement_options(
        self, plan: "ExportPlan", budget: ExecutionBudget
    ) -> list[tuple[tuple[float, ...], ClarifyOption]]:
        if self._refinements is None:
            return []
        out = []
        for ref in self._refinements.refinements(plan, budget):
            if ref.plan.cost_estimate is None:
                continue
            opt = ClarifyOption(
                ref.plan, ref.plan.cost_estimate, ClarifyKind.REFINEMENT
            )
            out.append((self._order(opt, budget, 0, ref.rank), opt))
        return out

    def _rewrite_options(
        self, viable: Sequence[RewriteCandidate], budget: ExecutionBudget
    ) -> list[tuple[tuple[float, ...], ClarifyOption]]:
        out = []
        for r in viable:
            if r.substitution in budget.substitutions:
                continue
            opt = ClarifyOption(
                r.plan,
                _plan_cost(r.plan),
                ClarifyKind.SUBSTITUTION,
                r.substitution,
                r.warning,
            )
            rank = _REWRITE_ORDER.index(r.substitution)
            out.append((self._order(opt, budget, 1, rank), opt))
        return out

    def _sample_options(
        self, s: SamplingPlan | None, budget: ExecutionBudget
    ) -> list[tuple[tuple[float, ...], ClarifyOption]]:
        if s is None or Substitution.SAMPLE in budget.substitutions:
            return []
        opt = ClarifyOption(
            s.plan,
            _plan_cost(s.plan),
            ClarifyKind.SUBSTITUTION,
            Substitution.SAMPLE,
            f"Estimated (±{SAMPLE_MAX_ERROR * 100:.1f}% of range at "
            f"{(1 - SAMPLE_DELTA) * 100:.0f}% confidence)",
        )
        return [(self._order(opt, budget, 1, len(_REWRITE_ORDER)), opt)]

    @staticmethod
    def _order(
        opt: ClarifyOption, budget: ExecutionBudget, kind_rank: int, rank: int
    ) -> tuple[float, ...]:
        return (float(zone(opt.cost, budget)), cost_total(opt.cost), kind_rank, rank)

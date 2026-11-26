"""DGM Supervisor — planners (§12.4 *plan*, §12.5 *replan*).

A planner proposes a :class:`SupervisorPlan` from the goal and a
:class:`PlannerView` — the datasets the principal may READ and the tools that
are available (§12.10, *Visibility*).  It never sets datasets, idempotency
keys, principals or grants; the supervisor derives and checks those
(:mod:`._validation`).

* :class:`LLMPlanner` — a structured-output model call.  If the call fails,
  the failure is reported; there is no fallback to another planner (D42).
* :class:`KeywordDemoPlanner` — the phrase-based heuristic.  It is part of
  the explicit demo mode (D47) and is used only when the caller selects it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from pydantic import BaseModel, Field

from sqldim.core.query.dgm.nl.supervisor._types import (
    SupervisorPlan,
    ToolCallRequest,
    ToolCallResult,
    ToolCapabilitySchema,
    ToolError,
)

__all__ = [
    "KeywordDemoPlanner",
    "LLMPlanner",
    "Planner",
    "PlannerView",
]


@dataclass(frozen=True)
class PlannerView:
    """Everything a planner may see (§12.10: no grants, no hidden datasets)."""

    goal: str
    plan_id: str
    version: int
    datasets: tuple[str, ...]
    tools: tuple[ToolCapabilitySchema, ...]
    history: tuple[dict[str, str], ...] = ()
    previous: SupervisorPlan | None = None
    results: tuple[ToolCallResult, ...] = ()
    violations: tuple[ToolError, ...] = ()
    rejection: ToolCallResult | None = None
    remaining_fraction: float = 1.0
    tool_names: frozenset[str] = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "tool_names", frozenset(t.name for t in self.tools))


class Planner(Protocol):
    """Proposes a plan; validation decides whether it runs."""

    @property
    def demo(self) -> bool: ...

    def propose(self, view: PlannerView) -> SupervisorPlan: ...


# ---------------------------------------------------------------------------
# LLM planner
# ---------------------------------------------------------------------------

_PLANNER_SYSTEM_PROMPT = """\
You are the DGM Supervisor Planner.  Decompose the user's question into a
plan of tool calls.

Rules:
- Use only the tools and datasets listed in the request, with exactly those
  names, and arguments that match each tool's input schema.
- Give every step a unique call_id.  depends_on lists the call_ids whose
  results the step consumes; they must be earlier steps.  An argument that
  refers to another step's result holds that step's call_id.
- budget_fraction values sum to at most 1.
- If the question cannot be answered with the listed tools and datasets,
  return no steps and say why in reasoning.  Never substitute another tool
  or dataset for the one the question needs.
- When replanning, keep every SUCCEEDED step unchanged with its call_id and
  do not repeat a rejected call unchanged.
"""


class _PlannerStep(BaseModel):
    call_id: str
    tool_name: str
    arguments: dict[str, Any]
    depends_on: list[str] = Field(default_factory=list)
    budget_fraction: float


class _PlanResult(BaseModel):
    steps: list[_PlannerStep]
    confidence: float
    reasoning: str


def _render_tools(view: PlannerView) -> list[str]:
    import json

    return [
        f"- {tool.name}: {tool.description}; input schema "
        f"{json.dumps(tool.input_schema, sort_keys=True)}"
        for tool in view.tools
    ]


def _render_previous(view: PlannerView) -> list[str]:
    if view.previous is None:
        return []
    lines = [f"Current plan (version {view.previous.version}):"]
    lines += [
        f"  {s.call_id} = {s.tool_name}({s.arguments}) depends_on={s.depends_on}"
        for s in view.previous.steps
    ]
    lines += [
        f"  result {r.call_id}: {r.status.value}"
        + (f" {r.error.kind.value}: {r.error.message}" if r.error else "")
        for r in view.results
    ]
    lines.append(f"Remaining budget fraction: {view.remaining_fraction:.4f}")
    return lines


def _render_view(view: PlannerView) -> str:
    lines = [f"Datasets: {', '.join(view.datasets) or 'none'}", "Tools:"]
    lines += _render_tools(view)
    if view.history:
        lines.append("Prior conversation:")
        lines += [f"  {m.get('role')}: {m.get('content')}" for m in view.history]
    lines += _render_previous(view)
    if view.violations:
        lines.append("The previous proposal was invalid:")
        lines += [f"  {v.context.get('rule')}: {v.message}" for v in view.violations]
    lines.append(f"Question: {view.goal}")
    return "\n".join(lines)


class LLMPlanner:
    """Plans with a pydantic-ai model; failures propagate to the plan node."""

    demo = False

    def __init__(self, model: Any) -> None:
        if model is None:
            raise ValueError("LLMPlanner needs a model")
        self._model = model

    def propose(self, view: PlannerView) -> SupervisorPlan:
        from pydantic_ai import Agent  # lazy import

        agent: Any = Agent(
            model=self._model,
            output_type=_PlanResult,
            system_prompt=_PLANNER_SYSTEM_PROMPT,
        )
        out: _PlanResult = agent.run_sync(_render_view(view)).output
        return SupervisorPlan(
            plan_id=view.plan_id,
            version=view.version,
            goal=view.goal,
            steps=[
                ToolCallRequest(
                    call_id=s.call_id,
                    tool_name=s.tool_name,
                    arguments=s.arguments,
                    depends_on=s.depends_on,
                    budget_fraction=s.budget_fraction,
                )
                for s in out.steps
            ],
            confidence=min(max(out.confidence, 0.0), 1.0),
            reasoning=out.reasoning,
        )


# ---------------------------------------------------------------------------
# Keyword heuristic — demo mode only (D47)
# ---------------------------------------------------------------------------

_SHORT_UTTERANCE = 5

_QUESTION_MARKERS: frozenset[str] = frozenset(
    {
        "what",
        "who",
        "when",
        "where",
        "how",
        "why",
        "which",
        "show",
        "list",
        "get",
        "find",
        "top",
        "count",
        "sum",
        "average",
        "compare",
        "mean",
        "total",
        "trend",
    }
)


_DATASET_PHRASES = [
    "which dataset",
    "what dataset",
    "show dataset",
    "list dataset",
    "available dataset",
    "what data",
    "which data",
    "available data",
    "what can you see",
    "what do you see",
    "what do you know",
    "can you see",
    "you see",
    # questions-about-a-specific-dataset patterns
    "which questions",
    "what questions",
    "question for",
    "questions for",
    "for the dataset",
    "what can you answer",
    "what can i ask",
]

# ── Intent-to-tool phrase tables (§12.2 keyword routing) ─────────────────

_VALIDATE_PHRASES: tuple[str, ...] = (
    "validate",
    "invariant",
    "non-negative",
    "no null",
    "always positive",
    "constraint",
    "check that",
    "ensure that",
    "assert that",
    "verify that",
    "must be",
    "should be",
    "rule holds",
    "data quality",
)

_SENSITIVITY_PHRASES: tuple[str, ...] = (
    "sensitivity",
    "sweep",
    "parameter",
    "how sensitive",
    "vary the",
    "what if",
    "effect of",
    "impact of",
    "range of",
    "sweep the",
    "days_window",
)

_CORRELATE_PHRASES: tuple[str, ...] = (
    "correlat",
    "relationship between",
    "connection between",
    "association",
    "related to",
    "co-occur",
    "co-move",
    "linked to",
    "cross-domain",
    "across domain",
)

_EXPLORE_PHRASES: tuple[str, ...] = (
    "explore",
    "what else",
    "related question",
    "next step",
    "navigate",
    "follow-up",
    "what more",
    "dig deeper",
    "adjacent",
    "from this result",
    "from these results",
)

_STORE_PHRASES: tuple[str, ...] = (
    "store this",
    "store the finding",
    "store a finding",
    "store my",
    "save this",
    "save the finding",
    "save the result",
    "save a finding",
    "remember this",
    "remember that",
    "archive this",
    "persist this",
    "note that",
    "keep track",
    "log the finding",
)

_RETRIEVE_PHRASES: tuple[str, ...] = (
    "retrieve",
    "recall",
    "find related",
    "show previous",
    "what do we know",
    "past finding",
    "knowledge base",
    "previous finding",
    "earlier finding",
    "remembered",
)

# ── Composition trigger phrases ───────────────────────────────────────────

_COMPOSITION_PHRASES: tuple[str, ...] = (
    " then ",
    " and then ",
    " followed by ",
    " after that ",
    "combine ",
    "first.*then",
    "chain ",
)


_DATASET_ALIASES: dict[str, str] = {
    # commerce
    "ecommerce": "commerce",
    "e-commerce": "commerce",
    "retail": "commerce",
    "supply_chain": "commerce",
    "supply chain": "commerce",
    # growth
    "enterprise": "growth",
    "saas_growth": "growth",
    # telemetry
    "user_activity": "telemetry",
    "user activity": "telemetry",
    "observability": "telemetry",
    # entertainment
    "media": "entertainment",
    "social_network": "entertainment",
    "social network": "entertainment",
    # data models
    "dgm": "data_models",
    "hierarchy": "data_models",
}


def _canonicalize_available_datasets(datasets: list[str]) -> set[str]:
    """Normalize legacy dataset aliases to canonical domain names."""
    return {
        _DATASET_ALIASES.get(name.strip().lower(), name.strip().lower())
        for name in datasets
        if name and name.strip()
    }


_CANONICAL_DATASET_NAMES: frozenset[str] = frozenset(
    {
        "commerce",
        "data_models",
        "devops",
        "entertainment",
        "fintech",
        "growth",
        "nba_analytics",
        "telemetry",
    }
)

_DATASET_TOPIC_TERMS: dict[str, tuple[str, ...]] = {
    "commerce": (
        "orders",
        "order volume",
        "order value",
        "average order value",
        "revenue",
        "sales",
        "products",
        "product",
        "deliveries",
        "delivery",
        "returns",
        "7-day retention",
        "customer cohort",
        "customer cohorts",
        "refunds",
        "inventory",
        "warehouse",
        "supplier",
        "suppliers",
        "purchases",
        "shipments",
    ),
    "growth": (
        "churn",
        "subscription",
        "subscriptions",
        "signup",
        "signups",
        "sign up",
        "signed up",
        "user signup",
        "user signups",
        "retention",
        "mrr",
        "arr",
        "saas",
        "trial",
        "trials",
        "conversion",
    ),
    "fintech": (
        "transaction",
        "transactions",
        "payment",
        "payments",
        "banking",
        "counterparty",
        "counterparties",
        "ledger",
        "transfer",
        "transfers",
    ),
    "devops": (
        "github",
        "repository",
        "repositories",
        "commit",
        "commits",
        "contributor",
        "contributors",
        "pull request",
        "pull requests",
        "issue",
        "issues",
        "deployment",
        "deployments",
        "builds",
    ),
    "telemetry": (
        "error",
        "errors",
        "metric",
        "metrics",
        "trace",
        "traces",
        "span",
        "spans",
        "latency",
        "logs",
        "logging",
        "observability",
    ),
    "entertainment": (
        "movie",
        "movies",
        "film",
        "films",
        "followers",
        "posts",
        "engagement",
        "likes",
        "comments",
        "actors",
        "directors",
        "box office",
    ),
    "nba_analytics": (
        "nba",
        "basketball",
        "player",
        "players",
        "scorer",
        "scorers",
        "rebounds",
        "assists",
        "three pointers",
        "free throws",
    ),
    "data_models": (
        "dgm",
        "dimensional graph",
        "hierarchy",
        "hierarchies",
        "organization chart",
        "organizational chart",
        "star schema",
        "surrogate key",
        "slowly changing dimension",
        "scd",
    ),
}


def _mentioned_dataset_candidates(
    utterance: str,
    datasets: list[str],
) -> set[str]:
    """Find canonical domains explicitly named in the utterance."""
    import re as _re

    names: dict[str, str] = {
        name: name for name in set(datasets) | _CANONICAL_DATASET_NAMES
    }
    names.update(_DATASET_ALIASES)

    utt_lower = utterance.lower()
    return {
        canonical
        for name, canonical in names.items()
        if _re.search(r"\b" + _re.escape(name.lower()) + r"\b", utt_lower)
    }


def _find_mentioned_dataset(
    utterance: str,
    datasets: list[str],
) -> str | None:
    """Resolve one explicitly mentioned, available canonical dataset."""
    available = _canonicalize_available_datasets(datasets)
    matches = _mentioned_dataset_candidates(utterance, datasets)

    if len(matches) != 1:
        return None

    candidate = next(iter(matches))
    return candidate if candidate in available else None


def _topic_dataset_candidates(utterance: str) -> set[str]:
    """Find the most specific domain matches in the utterance."""
    import re as _re

    utt_lower = utterance.lower()
    matches: dict[str, list[str]] = {}

    for domain, terms in _DATASET_TOPIC_TERMS.items():
        matched = [
            term
            for term in terms
            if _re.search(r"\b" + _re.escape(term) + r"\b", utt_lower)
        ]
        if matched:
            matches[domain] = matched

    if not matches:
        return set()

    def specificity(terms: list[str]) -> int:
        return max(len(term.split()) for term in terms)

    best = max(specificity(terms) for terms in matches.values())

    return {domain for domain, terms in matches.items() if specificity(terms) == best}


def _infer_topic_dataset(
    utterance: str,
    datasets: list[str],
) -> str | None:
    """Infer a unique, specific topic domain within the available context."""
    available = _canonicalize_available_datasets(datasets)
    matches = _topic_dataset_candidates(utterance)
    contextual_matches = matches & available

    if len(contextual_matches) != 1:
        return None

    return next(iter(contextual_matches))


def _resolve_dataset(
    utterance: str,
    datasets: list[str],
) -> str | None:
    """Resolve explicit domain references before attempting topic inference.

    Explicit mentions take precedence. Conflicting explicit mentions remain
    ambiguous, even if one of their associated topics happens to match.
    """
    mentioned = _mentioned_dataset_candidates(utterance, datasets)

    if mentioned:
        if len(mentioned) != 1:
            return None

        candidate = next(iter(mentioned))
        return (
            candidate
            if candidate in _canonicalize_available_datasets(datasets)
            else None
        )

    return _infer_topic_dataset(utterance, datasets)


_TOOL_PHRASES = (
    "what can you do",
    "what you can do",
    "what can i do",
    "what do you do",
)


@dataclass(frozen=True)
class _Intents:
    """Boolean intent signals extracted from an utterance."""

    datasets: bool
    tools: bool
    validate: bool
    sensitivity: bool
    correlate: bool
    explore: bool
    store: bool
    retrieve: bool
    composition: bool
    analytical: bool

    @property
    def kb_or_link(self) -> bool:
        return self.correlate or self.explore or self.store or self.retrieve

    @property
    def requires_dataset(self) -> bool:
        if self.datasets or self.tools:
            return False
        if self.composition and self.analytical:
            return True
        if self.validate or self.sensitivity:
            return True
        return self.analytical and not self.kb_or_link


def _detect_intents(utt_lower: str) -> _Intents:
    import re as _re

    def has(phrases: Any) -> bool:
        return any(p in utt_lower for p in phrases)

    def matches(patterns: Any) -> bool:
        return any(_re.search(p, utt_lower) for p in patterns)

    return _Intents(
        datasets=has(_DATASET_PHRASES),
        tools=has(["tool", "capabilit"]) or has(_TOOL_PHRASES),
        validate=has(_VALIDATE_PHRASES),
        sensitivity=has(_SENSITIVITY_PHRASES),
        correlate=matches(_CORRELATE_PHRASES),
        explore=has(_EXPLORE_PHRASES),
        store=has(_STORE_PHRASES),
        retrieve=has(_RETRIEVE_PHRASES),
        composition=matches(_COMPOSITION_PHRASES),
        analytical=bool(set(utt_lower.split()) & _QUESTION_MARKERS),
    )


def _single_available_dataset(utterance: str, datasets: list[str]) -> str | None:
    """A single available domain is an explicit context constraint.

    It is not an arbitrary positional fallback: an explicit reference to an
    unavailable domain, or a topic match, is never overridden.
    """
    available = _canonicalize_available_datasets(datasets)
    explicit_mentions = _mentioned_dataset_candidates(utterance, datasets)
    topic_candidates = _topic_dataset_candidates(utterance)
    if len(available) == 1 and not explicit_mentions and not topic_candidates:
        return next(iter(available))
    return None


_Route = tuple[list[ToolCallRequest], float, str]


def _call(
    call_id: str,
    tool_name: str,
    arguments: dict[str, Any],
    fraction: float,
    depends_on: list[str] | None = None,
) -> ToolCallRequest:
    return ToolCallRequest(
        call_id=call_id,
        tool_name=tool_name,
        arguments=arguments,
        depends_on=depends_on or [],
        budget_fraction=fraction,
    )


def _route_discovery(intents: _Intents, view: PlannerView) -> _Route | None:
    """Discovery is answered from the planner's own view: no tool runs."""
    parts: list[str] = []
    if intents.datasets:
        parts.append("Datasets you can query: " + (", ".join(view.datasets) or "none"))
    if intents.tools:
        names = ", ".join(sorted(view.tool_names)) or "none"
        parts.append(f"Available tools: {names}")
    if not parts:
        return None
    return [], 0.7, ". ".join(parts) + "."


def _route_unavailable(intents: _Intents) -> _Route | None:
    """Intents whose tools are not implemented: say so, plan nothing."""
    wanted = [
        ("dgm_validate_invariant", intents.validate),
        ("dgm_sensitivity_sweep", intents.sensitivity),
        ("dgm_correlate", intents.correlate),
        ("dgm_explore_from", intents.explore or intents.composition),
        ("dgm_store_finding", intents.store),
        ("dgm_retrieve_related", intents.retrieve),
    ]
    tools = [name for name, hit in wanted if hit]
    if not tools:
        return None
    return [], 0.6, f"Requested capability not available: {', '.join(tools)}."


def _route_query(intents: _Intents, target_ds: str | None, utterance: str) -> _Route:
    if not intents.analytical:
        return [], 0.35, "No analytical intent detected; nothing was planned."
    if target_ds is None:
        return (
            [],
            0.35,
            "No single dataset could be identified for this question; "
            "name one of the datasets you can query.",
        )
    return (
        [_call("q1", "dgm_query", {"dataset": target_ds, "utterance": utterance}, 1.0)],
        0.75,
        f"Analytical query against '{target_ds}'.",
    )


def _penalise_short(utt: str, confidence: float, reasoning: str) -> tuple[float, str]:
    if not utt:
        return 0.1, "Empty utterance."
    if len(utt) < _SHORT_UTTERANCE:
        return min(confidence, 0.3), f"Very short utterance ({len(utt)} chars)."
    return confidence, reasoning


def _restrict_to_view(route: _Route, view: PlannerView) -> _Route:
    """The planner sees only available tools (§12.2); never swap one in."""
    steps, confidence, reasoning = route
    missing = sorted({s.tool_name for s in steps} - view.tool_names)
    if missing:
        return (
            [],
            confidence,
            f"Requested capability not available: {', '.join(missing)}.",
        )
    return route


class KeywordDemoPlanner:
    """Phrase-based routing for the explicit demo mode (D47)."""

    demo = True

    def propose(self, view: PlannerView) -> SupervisorPlan:
        utt = view.goal.strip()
        intents = _detect_intents(utt.lower())
        datasets = list(view.datasets)
        target_ds = _resolve_dataset(utt, datasets) or _single_available_dataset(
            utt, datasets
        )
        route = (
            _route_discovery(intents, view)
            or _route_unavailable(intents)
            or _route_query(intents, target_ds, utt)
        )
        steps, confidence, reasoning = _restrict_to_view(route, view)
        confidence, reasoning = _penalise_short(utt, confidence, reasoning)
        return SupervisorPlan(
            plan_id=view.plan_id,
            version=view.version,
            goal=view.goal,
            steps=steps,
            confidence=confidence,
            reasoning=reasoning,
        )

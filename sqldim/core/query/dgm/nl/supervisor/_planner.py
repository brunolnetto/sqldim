"""DGM Supervisor Agent — question decomposition planner (§12.2).

LLM-first planning
------------------
When a pydantic-ai model is supplied (via ``context.model``), the planner
runs a structured-output agent that reads the full tool registry and the
available datasets, then produces a typed ``_PlanResult`` which is
converted to a ``SupervisorPlan``.

The agent's system prompt explains:
* Which 9 DGM tools exist and their purpose
* The available datasets (injected at call time)
* Budget rules (all ``budget_fraction`` values must sum to ≤ 1.0)
* Confidence semantics

Keyword fallback
----------------
When no model is provided (``context.model is None``), or if the LLM call
fails, the planner falls back to phrase-based keyword routing.  This keeps
tests fast and deterministic, and lets CLI usage work without API keys.

Confidence
----------
* LLM path: the model sets ``confidence`` directly (0–1)
* Fallback path: heuristic scoring identical to the previous implementation

Plans with ``confidence < 0.5`` are routed to the ``reflect`` node in
the supervisor graph, which prepends orientation steps so the synthesiser
can produce a meaningful response even for vague inputs.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel

from sqldim.core.query.dgm.nl.supervisor._types import (
    SupervisorContext,
    SupervisorPlan,
    ToolCallRequest,
)

__all__ = ["decompose_question"]

# ---------------------------------------------------------------------------
# Pydantic-AI planner output schema
# ---------------------------------------------------------------------------

_TOOL_REGISTRY_SUMMARY = """
Available DGM tools (use exactly these names in tool_name):
  - dgm_query              : Execute an analytical NL query against ONE named dataset
  - dgm_what_can_i_answer  : List which datasets exist and what questions they can answer
  - dgm_map_analytical_space: Enumerate the full tool registry with descriptions
  - dgm_correlate          : JOIN / correlate results from two different analyses
  - dgm_explore_from       : Navigate from a result to related questions (recommender)
  - dgm_sensitivity_sweep  : Sweep a parameter space to find sensitivity boundaries
  - dgm_validate_invariant : Validate a formal model invariant holds
  - dgm_store_finding      : Persist an analytical finding to the knowledge base
  - dgm_retrieve_related   : Retrieve lattice-adjacent findings from the knowledge base
""".strip()

_PLANNER_SYSTEM_PROMPT = f"""\
You are a DGM Supervisor Planner.  Your job is to decompose the user's
question into an ordered plan of DGM tool calls.

{_TOOL_REGISTRY_SUMMARY}

Rules:
- Include ONLY tools that directly contribute to answering the question.
- For "which datasets / what data do you know about" style questions → dgm_what_can_i_answer
  with dataset="__all__".
- For "which questions / what can you answer for [dataset X]?" style questions →
  dgm_what_can_i_answer with dataset=X (use the exact dataset name from available_datasets).
- For "what tools / capabilities do you have" style questions → dgm_map_analytical_space.
- For analytical domain questions (churn, revenue, counts, trends…) → dgm_query, choosing
  the most TOPICALLY RELEVANT dataset from the available_datasets list — do NOT default to
  the first dataset alphabetically.  If no dataset is clearly relevant, use dgm_what_can_i_answer.
- For comparison / cross-domain questions → two dgm_query steps or dgm_correlate.
- For very short, vague, or purely conversational inputs with no analytical content (e.g.
  "hello", "hi", "help", "?", single-word greetings) → return an EMPTY steps list [],
  set confidence ≤ 0.3, and explain in reasoning that no analytical intent was detected.
  Do NOT default to dgm_query when there is no clear analytical intent.
- budget_fraction values MUST sum to exactly 1.0 across all steps (or omit if steps is empty).
- confidence: 0.9 for a clear question; 0.7 for a reasonable one; < 0.5 for vague/too short.
- reasoning: one sentence explaining which tool(s) you selected and why.
"""


class _PlannerStep(BaseModel):
    tool_name: str
    arguments: dict[str, Any]
    budget_fraction: float = 1.0


class _PlanResult(BaseModel):
    steps: list[_PlannerStep]
    confidence: float
    reasoning: str


# ---------------------------------------------------------------------------
# LLM planner path
# ---------------------------------------------------------------------------


def _llm_decompose(
    utterance: str,
    context: SupervisorContext,
    llm_model: Any,
) -> SupervisorPlan:
    """Run the pydantic-ai planner agent and convert its output to SupervisorPlan."""
    from pydantic_ai import Agent  # lazy import

    agent: Any = Agent(
        model=llm_model,
        output_type=_PlanResult,
        system_prompt=_PLANNER_SYSTEM_PROMPT,
    )

    dataset_list = (
        ", ".join(context.available_datasets) if context.available_datasets else "none"
    )
    user_message = f"Available datasets: {dataset_list}\n\nQuestion: {utterance}"

    result = agent.run_sync(user_message)
    plan_result: _PlanResult = result.output

    # Normalise budget fractions to sum exactly to 1.0
    raw_steps = plan_result.steps
    total = sum(s.budget_fraction for s in raw_steps) or 1.0
    steps = [
        ToolCallRequest(
            tool_name=s.tool_name,
            arguments=s.arguments,
            budget_fraction=round(s.budget_fraction / total, 6),
        )
        for s in raw_steps
        if s.tool_name
    ]
    if not steps:
        return _keyword_decompose(utterance, context)

    confidence = min(max(plan_result.confidence, 0.0), 1.0)
    return SupervisorPlan(
        goal=utterance,
        steps=steps,
        confidence=confidence,
        reasoning=plan_result.reasoning,
    )


# ---------------------------------------------------------------------------
# Keyword fallback (phrase-based, no LLM required)
# ---------------------------------------------------------------------------

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

_REFLECT_MIN_LENGTH = 5

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


def _budget_allows(tool_name: str, context: "SupervisorContext | None") -> bool:
    """Return True when the budget is sufficient (or unconstrained) for *tool_name*."""
    if context is None or context.budget is None:
        return True
    from sqldim.core.query.dgm.nl.supervisor._tools import TOOL_REGISTRY

    tool = TOOL_REGISTRY.get(tool_name)
    if tool is None:
        return True
    cost = tool.cost_estimate({})
    budget_limit = getattr(context.budget, "max_cost", None)
    if budget_limit is None:
        return True
    return cost <= budget_limit


_Route = tuple[list[ToolCallRequest], float, str]

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


def _call(
    tool_name: str, arguments: dict[str, Any], fraction: float
) -> ToolCallRequest:
    return ToolCallRequest(
        tool_name=tool_name, arguments=arguments, budget_fraction=fraction
    )


def _route_discovery(intents: _Intents, target_ds: str | None) -> _Route | None:
    if intents.datasets and intents.tools:
        return (
            [
                _call("dgm_what_can_i_answer", {"dataset": "__all__"}, 0.5),
                _call("dgm_map_analytical_space", {"dataset": "__all__"}, 0.5),
            ],
            0.9,
            "Both dataset and tool discovery requested.",
        )
    if intents.datasets:
        target = target_ds or "__all__"
        return (
            [_call("dgm_what_can_i_answer", {"dataset": target}, 1.0)],
            0.7,
            f"Dataset discovery requested for '{target}'.",
        )
    if intents.tools:
        return (
            [_call("dgm_map_analytical_space", {"dataset": "__all__"}, 1.0)],
            0.7,
            "Tool capability enumeration requested.",
        )
    return None


def _route_dataset_bound(
    intents: _Intents, target_ds: str | None, utt: str
) -> _Route | None:
    if intents.composition and intents.analytical:
        # Two-phase composition: query → explore
        assert target_ds is not None
        return (
            [
                _call("dgm_query", {"dataset": target_ds, "question": utt}, 0.7),
                _call("dgm_explore_from", {"result": {}, "direction": "forward"}, 0.3),
            ],
            0.65,
            f"Chained query + exploration plan on '{target_ds}'.",
        )
    if intents.validate:
        assert target_ds is not None
        return (
            [
                _call(
                    "dgm_validate_invariant",
                    {"invariant": utt, "dataset": target_ds},
                    1.0,
                )
            ],
            0.75,
            f"Invariant/constraint validation requested on '{target_ds}'.",
        )
    if intents.sensitivity:
        assert target_ds is not None
        args = {"dataset": target_ds, "question": utt, "param": "days_window"}
        return (
            [_call("dgm_sensitivity_sweep", args, 1.0)],
            0.75,
            f"Parameter sensitivity sweep requested on '{target_ds}'.",
        )
    return None


def _route_link_or_kb(intents: _Intents, utt: str) -> _Route | None:
    if intents.correlate:
        return (
            [_call("dgm_correlate", {"left": {}, "right": {}}, 1.0)],
            0.7,
            "Cross-domain correlation or relationship analysis requested.",
        )
    if intents.explore:
        return (
            [_call("dgm_explore_from", {"result": {}, "direction": "forward"}, 1.0)],
            0.7,
            "Related-question exploration from a prior result requested.",
        )
    if intents.store:
        return (
            [
                _call(
                    "dgm_store_finding", {"finding": utt, "tags": ["planner-auto"]}, 1.0
                )
            ],
            0.75,
            "Persist an analytical finding to the knowledge base.",
        )
    if intents.retrieve:
        return (
            [_call("dgm_retrieve_related", {"query": utt, "max_results": 5}, 1.0)],
            0.75,
            "Retrieve previously stored findings from the knowledge base.",
        )
    return None


def _route_analytical(
    intents: _Intents,
    target_ds: str | None,
    utterance: str,
    context: "SupervisorContext | None",
) -> _Route:
    if not intents.analytical:
        # Unknown / vague intent — return empty plan; reflect handles orientation
        return [], 0.35, "No analytical intent detected; routing to reflect."
    assert target_ds is not None
    # Budget-aware fallback: if dgm_query is too expensive, downgrade
    if not _budget_allows("dgm_query", context):
        return (
            [_call("dgm_what_can_i_answer", {"dataset": target_ds}, 1.0)],
            0.5,
            f"Budget exhausted; degraded to discovery for '{target_ds}'.",
        )
    return (
        [_call("dgm_query", {"dataset": target_ds, "question": utterance}, 1.0)],
        0.55,
        f"Analytical query against '{target_ds}'.",
    )


def _apply_confidence_penalties(
    utt: str, confidence: float, reasoning: str
) -> tuple[float, str]:
    if not utt:
        return 0.1, "Empty utterance."
    if len(utt) < _REFLECT_MIN_LENGTH:
        return (
            min(confidence, 0.3),
            f"Very short utterance ({len(utt)} chars); low confidence.",
        )
    return confidence, reasoning


def _keyword_decompose(
    utterance: str,
    context: "SupervisorContext | None",
) -> SupervisorPlan:
    """Phrase/keyword routing — used when no LLM is configured."""
    datasets = list(context.available_datasets) if context else []
    utt = utterance.strip()
    intents = _detect_intents(utt.lower())

    target_ds = _resolve_dataset(utterance, datasets)

    # Dataset-dependent routes must never invent a positional fallback.
    # Ambiguous or unsupported topics are handled by the graph's reflect node.
    if intents.requires_dataset and target_ds is None:
        target_ds = _single_available_dataset(utterance, datasets)

    if intents.requires_dataset and target_ds is None:
        return SupervisorPlan(
            goal=utterance,
            steps=[],
            confidence=min(0.35, 0.3) if len(utt) < _REFLECT_MIN_LENGTH else 0.35,
            reasoning=(
                "No unambiguous available dataset could be inferred; "
                "routing to reflect for orientation."
            ),
        )

    # ── Decision tree (priority order) ───────────────────────────────────
    steps, confidence, reasoning = (
        _route_discovery(intents, target_ds)
        or _route_dataset_bound(intents, target_ds, utt)
        or _route_link_or_kb(intents, utt)
        or _route_analytical(intents, target_ds, utterance, context)
    )

    confidence, reasoning = _apply_confidence_penalties(utt, confidence, reasoning)

    return SupervisorPlan(
        goal=utterance,
        steps=steps,
        confidence=confidence,
        reasoning=reasoning,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def decompose_question(
    utterance: str,
    context: SupervisorContext | None = None,
    *,
    model: Any = None,
) -> SupervisorPlan:
    """Decompose *utterance* into an ordered plan of DGM tool calls.

    When ``context.model`` is a pydantic-ai model instance the LLM planner
    is used, which understands the full analytical intent of the question.

    When no model is available the phrase-based keyword heuristic is used as
    a deterministic, cost-free fallback.  The ``model`` keyword argument (if
    provided) takes precedence over ``context.model``.
    """
    llm_model = model or (context.model if context else None)

    if llm_model is not None:
        try:
            return _llm_decompose(
                utterance,
                context or SupervisorContext(available_datasets=[]),
                llm_model,
            )
        except Exception:
            pass  # graceful degradation to keyword routing

    return _keyword_decompose(utterance, context)

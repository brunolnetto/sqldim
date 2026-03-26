"""DGM NL Interface — Pydantic-AI specialist agent factories (§11.10).

Five factory functions, one per LLM node in the eleven-node NL interface
graph (§11.10 table).  Each factory accepts a model name or model instance,
creates a fresh pydantic-ai ``Agent`` with the appropriate result-type schema
and system prompt, and registers the tools the agent is allowed to call.

Tools are designed for graceful degradation: when ``DGMContext.graph`` is
``None`` (schema-only mode, e.g. ``sqldim ask``), tools fall back to the
closed vocabulary carried in ``EntityRegistry``.

Public interface
----------------
make_entity_agent(model)         -> Agent[DGMContext, EntityResolutionResult]
make_temporal_agent(model)       -> Agent[DGMContext, TemporalClassificationResult]
make_compositional_agent(model)  -> Agent[DGMContext, CompositionalDetectionResult]
make_ranking_agent(model)        -> Agent[DGMContext, CandidateRankingResult]
make_explanation_agent(model)    -> Agent[DGMContext, ExplanationRenderResult]
"""

from __future__ import annotations

from typing import Any

from pydantic_ai import Agent, RunContext

from sqldim.core.query.dgm.nl._agent_types import (
    CandidateRankingResult,
    CompositionalDetectionResult,
    DGMContext,
    EntityResolutionResult,
    ExplanationRenderResult,
    TemporalClassificationResult,
)

__all__ = [
    "make_model",
    "make_ollama_model",
    "make_entity_agent",
    "make_temporal_agent",
    "make_compositional_agent",
    "make_ranking_agent",
    "make_explanation_agent",
]

#: Ollama server base URL (OpenAI-compatible endpoint).
_OLLAMA_BASE_URL: str = "http://localhost:11434/v1"

#: Default model names per provider.  Used when *model_name* is ``None``.
_PROVIDER_DEFAULTS: dict[str, str] = {
    "ollama": "llama3.2:latest",
    "openai": "gpt-4o-mini",
    "anthropic": "claude-3-5-haiku-latest",
    "gemini": "gemini-1.5-flash",
    "groq": "llama-3.1-8b-instant",
    "mistral": "mistral-small-latest",
}


def _make_ollama_model(name: str, base_url: str | None, api_key: str | None) -> Any:
    from pydantic_ai.models.openai import OpenAIChatModel
    from pydantic_ai.providers.openai import OpenAIProvider

    url = base_url or _OLLAMA_BASE_URL
    key = api_key or "ollama"  # value ignored by Ollama; must be non-empty
    return OpenAIChatModel(name, provider=OpenAIProvider(base_url=url, api_key=key))


def _make_openai_model(name: str, base_url: str | None, api_key: str | None) -> Any:
    from pydantic_ai.models.openai import OpenAIChatModel
    from pydantic_ai.providers.openai import OpenAIProvider

    kwargs: dict[str, Any] = {}
    if base_url:
        kwargs["base_url"] = base_url
    if api_key:
        kwargs["api_key"] = api_key
    return OpenAIChatModel(name, provider=OpenAIProvider(**kwargs))


def _make_anthropic_model(name: str, base_url: str | None, api_key: str | None) -> Any:
    from pydantic_ai.models.anthropic import AnthropicModel

    return AnthropicModel(name)  # type: ignore[arg-type]


def _make_gemini_model(name: str, base_url: str | None, api_key: str | None) -> Any:
    from pydantic_ai.models.google import GoogleModel

    return GoogleModel(name)  # type: ignore[arg-type]


def _make_groq_model(name: str, base_url: str | None, api_key: str | None) -> Any:
    from pydantic_ai.models.groq import GroqModel

    return GroqModel(name)  # type: ignore[arg-type]


def _make_mistral_model(name: str, base_url: str | None, api_key: str | None) -> Any:
    from pydantic_ai.models.mistral import MistralModel

    return MistralModel(name)  # type: ignore[arg-type]


#: Per-provider factory dispatch table.  Each entry maps a provider key
#: (lowercase) to a callable ``(name, base_url, api_key) -> model``.
_PROVIDER_FACTORIES: dict[str, Any] = {
    "ollama": _make_ollama_model,
    "openai": _make_openai_model,
    "anthropic": _make_anthropic_model,
    "gemini": _make_gemini_model,
    "groq": _make_groq_model,
    "mistral": _make_mistral_model,
}


def make_model(
    provider: str = "ollama",
    model_name: str | None = None,
    *,
    base_url: str | None = None,
    api_key: str | None = None,
) -> Any:
    """Return a pydantic-ai model instance for *provider*.

    Supported providers
    -------------------
    ``ollama`` (default)
        Local Ollama server via its OpenAI-compatible endpoint.
        Reads ``OLLAMA_HOST`` env variable or defaults to
        ``http://localhost:11434/v1``.  No API key required.
    ``openai``
        OpenAI cloud API.  Reads ``OPENAI_API_KEY`` from the environment.
    ``anthropic``
        Anthropic cloud API.  Reads ``ANTHROPIC_API_KEY`` from the environment.
    ``gemini``
        Google Gemini API (via Google AI / GLA).  Reads ``GOOGLE_API_KEY``
        from the environment.
    ``groq``
        Groq cloud API.  Reads ``GROQ_API_KEY`` from the environment.
    ``mistral``
        Mistral AI cloud API.  Reads ``MISTRAL_API_KEY`` from the environment.

    Parameters
    ----------
    provider:
        Backend name (case-insensitive).  Defaults to ``"ollama"``.
    model_name:
        Provider-specific model identifier.  When ``None``, the per-provider
        default from :data:`_PROVIDER_DEFAULTS` is used.
    base_url:
        Override the API base URL.  Only applicable to OpenAI-compatible
        backends (``ollama``, ``openai``).
    api_key:
        Override the API key.  When ``None``, the provider SDK reads the
        relevant environment variable.
    """
    p = provider.lower()
    name: str = model_name or _PROVIDER_DEFAULTS.get(p, "")
    factory = _PROVIDER_FACTORIES.get(p)
    if factory is None:
        raise ValueError(
            f"Unknown provider {provider!r}. Supported: {sorted(_PROVIDER_DEFAULTS)}"
        )
    return factory(name, base_url, api_key)


def make_ollama_model() -> Any:
    """Return an OpenAIChatModel wired to the local Ollama server.

    Convenience wrapper for ``make_model("ollama")``.  Ollama exposes an
    OpenAI-compatible chat-completions endpoint at ``/v1``.
    """
    return make_model("ollama")


# ---------------------------------------------------------------------------
# Task 1 — Entity resolution
# ---------------------------------------------------------------------------


def make_entity_agent(model: Any = None) -> Any:
    """Create the entity resolution specialist (§11.10 Agent 1).

    Maps natural-language terms to PropRef values grounded in the closed
    vocabulary of the ``EntityRegistry``.  When the live DGM graph is
    unavailable, falls back to ``prop_terms`` derived from the DuckDB schema.
    """
    _model = model if model is not None else make_ollama_model()
    agent: Any = Agent(
        model=_model,
        deps_type=DGMContext,
        output_type=EntityResolutionResult,  # type: ignore[arg-type]
        system_prompt=(
            "You resolve natural-language terms to DGM PropRef values.\n"
            "Use get_valid_prop_refs to verify which alias.prop values exist.\n"
            "Use get_verb_labels to confirm relationship labels.\n"
            "Only return PropRef values the tools confirm — mark the rest as "
            "unresolved.  Do not guess."
        ),
    )

    @agent.tool
    def get_valid_prop_refs(
        ctx: RunContext[DGMContext],
        node_type: str,  # noqa: ARG001
    ) -> list[str]:
        """Return all valid alias.prop strings from the entity registry.

        The ``node_type`` hint is accepted but ignored in schema-only mode
        (when ``graph`` is ``None``).  In both modes the closed vocabulary
        from ``EntityRegistry`` is returned.
        """
        return list(ctx.deps.entity_registry.prop_terms.values())

    @agent.tool
    def get_verb_labels(
        ctx: RunContext[DGMContext],
        from_type: str,  # noqa: ARG001
        to_type: str,  # noqa: ARG001
    ) -> list[str]:
        """Return all relationship verb labels from the entity registry.

        The ``from_type`` / ``to_type`` parameters are accepted for future
        live-graph filtering but ignored in schema-only mode.
        """
        return list(ctx.deps.entity_registry.verb_terms.values())

    return agent


# ---------------------------------------------------------------------------
# Task 3 — Temporal classification
# ---------------------------------------------------------------------------


def make_temporal_agent(model: Any = None) -> Any:
    """Create the temporal classification specialist (§11.10 Agent 3).

    Classifies NL temporal expressions into the ``TemporalIntent`` closed
    vocabulary.  Returns ``confidence = 0.0`` when no temporal signal exists.
    """
    _model = model if model is not None else make_ollama_model()
    return Agent(
        model=_model,
        deps_type=DGMContext,  # type: ignore[arg-type]
        output_type=TemporalClassificationResult,  # type: ignore[arg-type]
        system_prompt=(
            "You classify the temporal intent in a natural-language query.\n"
            "Choose mode from: EVENTUALLY, GLOBALLY, NEXT, UNTIL, SINCE, ONCE, PREVIOUSLY.\n"
            "Choose property_kind from: SAFETY, LIVENESS, RESPONSE, PERSISTENCE, RECURRENCE.\n"
            "Set window to YTD, ROLLING, or PERIOD when a time window is named.\n"
            "Only one of mode/property_kind/window may be non-null per fragment.\n"
            "Return confidence = 0.0 and all null when there is no temporal signal."
        ),
    )


# ---------------------------------------------------------------------------
# Task 4 — Compositional detection
# ---------------------------------------------------------------------------


def make_compositional_agent(model: Any = None) -> Any:
    """Create the compositional detection specialist (§11.10 Agent 4).

    Detects ``Q_algebra`` operations (UNION / INTERSECT / JOIN / CHAIN) in
    multi-question utterances.  JOIN must supply a valid ``join_key`` PropRef;
    the validator in ``CompositionalDetectionResult`` enforces this.
    """
    _model = model if model is not None else make_ollama_model()
    return Agent(
        model=_model,
        deps_type=DGMContext,  # type: ignore[arg-type]
        output_type=CompositionalDetectionResult,  # type: ignore[arg-type]
        system_prompt=(
            "You detect whether a natural-language question is compositional.\n"
            "Compositional means it combines two queries with: UNION, INTERSECT, JOIN, or CHAIN.\n"
            "If JOIN, you must supply a join_key as {alias: ..., prop: ...}.\n"
            "Return operation=null, join_key=null, confidence=0.0 for simple queries."
        ),
    )


# ---------------------------------------------------------------------------
# Task 2b — Candidate ranking
# ---------------------------------------------------------------------------


def make_ranking_agent(model: Any = None) -> Any:
    """Create the candidate ranking specialist (§11.10 Agent 2b).

    Ranks pre-validated ``QueryCandidate`` objects from the recommender by
    linguistic proximity.  The LLM never adds candidates — only reorders the
    provided list and assigns confidence scores.
    """
    _model = model if model is not None else make_ollama_model()
    return Agent(
        model=_model,
        deps_type=DGMContext,  # type: ignore[arg-type]
        output_type=CandidateRankingResult,  # type: ignore[arg-type]
        system_prompt=(
            "You rank query candidates by how well they match the user's intent.\n"
            "The candidates are pre-validated.  You may only reorder — never add entries.\n"
            "Return the same candidates in your preferred order with one confidence score each.\n"
            "Confidence values must be in [0, 1]; list length must match the input."
        ),
    )


# ---------------------------------------------------------------------------
# Task 5 — Explanation rendering
# ---------------------------------------------------------------------------


def make_explanation_agent(model: Any = None) -> Any:
    """Create the explanation rendering specialist (§11.10 Agent 5).

    Renders the query DAG decomposition in natural language.  Only calls
    ``dag_decomposition`` and ``get_next_suggestions`` — never produces SQL
    directly.  Both tools gracefully degrade when ``query_dag`` is ``None``.
    """
    _model = model if model is not None else make_ollama_model()
    agent: Any = Agent(
        model=_model,
        deps_type=DGMContext,
        output_type=ExplanationRenderResult,  # type: ignore[arg-type]
        system_prompt=(
            "You explain DGM query results and suggest follow-up queries.\n"
            "Use dag_decomposition to understand what was computed.\n"
            "Use get_next_suggestions to propose 1–3 follow-up questions.\n"
            "Write clear, concise explanations in plain language."
        ),
    )

    @agent.tool
    def dag_decomposition(ctx: RunContext[DGMContext], dag_node_id: int) -> dict:  # type: ignore[type-arg]
        """Return a structured decomposition of the given DAG node.

        Falls back to an empty dict when no ``query_dag`` is available.
        """
        if ctx.deps.query_dag is None:
            return {}
        try:
            node = ctx.deps.query_dag.get_node(dag_node_id)
            return {
                "id": dag_node_id,
                "description": getattr(node, "description", ""),
                "bands": getattr(node, "bands", []),
            }
        except Exception:  # noqa: BLE001
            return {}

    @agent.tool
    def get_next_suggestions(
        ctx: RunContext[DGMContext], dag_node_id: int, k: int = 3
    ) -> list[str]:
        """Return up to k follow-up query suggestions from the recommender.

        Falls back to an empty list when no ``recommender`` is available.
        """
        if ctx.deps.recommender is None:
            return []
        try:
            candidates = ctx.deps.recommender.clarification_options(dag_node_id, k)
            return [getattr(c, "description", str(c)) for c in candidates]
        except Exception:  # noqa: BLE001
            return []

    return agent

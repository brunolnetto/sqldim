"""DGM Natural Language Interface (§11) — public package."""

from sqldim.core.query.dgm.nl._types import (  # noqa: F401
    BandKind,
    TemporalIntent,
    CompositionOp,
    AmbiguityKind,
    SchemaEvolutionKind,
    SchemaEvolutionAction,
    EntityRegistry,
    PathResolutionResult,
)
from sqldim.core.query.dgm.nl._functions import (  # noqa: F401
    validate_propref,
    resolve_band,
    classify_temporal_intent,
    classify_compositional_intent,
    apply_schema_evolution,
)
from sqldim.core.query.dgm.nl._agent_types import (  # noqa: F401
    PropRefModel,
    QueryCandidate,
    EntityResolutionResult,
    CandidateRankingResult,
    TemporalClassificationResult,
    CompositionalDetectionResult,
    ExplanationRenderResult,
    DGMContext,
    NLInterfaceState,
)
from sqldim.core.query.dgm.nl._agents import (  # noqa: F401
    make_model,
    make_ollama_model,
    make_entity_agent,
    make_temporal_agent,
    make_compositional_agent,
    make_ranking_agent,
    make_explanation_agent,
)
from sqldim.core.query.dgm.nl._graph import (  # noqa: F401
    route_after_confirmation,
    route_budget_decision,
    build_nl_graph,
)

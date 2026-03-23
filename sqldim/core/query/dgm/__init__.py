# DGM sub-package public API — all symbols re-exported from their source modules.
from sqldim.core.query.dgm.core import DGMQuery, Query, TemporalContext  # noqa: F401
from sqldim.core.query.dgm._query_helpers import (  # noqa: F401
    _collect_fk_matches, _has_path_pred, _infer_on,
)
from sqldim.core.query.dgm.refs import (  # noqa: F401
    PropRef, AggRef, WinRef, SignatureRef,
    _format_value, _paren_if_compound, _ConstExpr, ArithExpr,
)
from sqldim.core.query.dgm.preds import (  # noqa: F401
    ScalarPred, AND, OR, NOT, RawPred,
    Strategy, ALL, SHORTEST, K_SHORTEST, MIN_WEIGHT, Quantifier,
    VerbHop, VerbHopInverse, BridgeHop, Compose, PathPred, PathAgg, SignaturePred,
    SAFETY, LIVENESS, RESPONSE, PERSISTENCE, RECURRENCE, SequenceMatch,
)
from sqldim.core.query.dgm.graph import (  # noqa: F401
    GraphAlgorithm, NodeAlg, PairAlg, SubgraphAlg, CommAlg,
    LOUVAIN, LABEL_PROPAGATION, CONNECTED_COMPONENTS, TARJAN_SCC,
    PAGE_RANK, BETWEENNESS_CENTRALITY, CLOSENESS_CENTRALITY, DEGREE, COMMUNITY_LABEL,
    OUTGOING_SIGNATURES, INCOMING_SIGNATURES, DOMINANT_OUTGOING_SIGNATURE,
    DOMINANT_INCOMING_SIGNATURE, SIGNATURE_DIVERSITY, SHORTEST_PATH_LENGTH,
    MIN_WEIGHT_PATH_LENGTH, REACHABLE, DISTINCT_SIGNATURES, DOMINANT_SIGNATURE,
    SIGNATURE_SIMILARITY, MAX_FLOW, DENSITY, DIAMETER,
    GLOBAL_SIGNATURE_COUNT, GLOBAL_DOMINANT_SIGNATURE, SIGNATURE_ENTROPY,
    NodeExpr, PairExpr, SubgraphExpr, GraphExpr,
    Endpoint, Bound, Free, FREE, RelationshipSubgraph, GraphStatistics,
    TrimCriterion, REACHABLE_BETWEEN, REACHABLE_FROM, REACHABLE_TO,
    MIN_DEGREE, SINK_FREE, SOURCE_FREE, TrimJoin,
)
from sqldim.core.query.dgm.annotations import (  # noqa: F401
    GrainKind, SCDKind, WeightConstraintKind, BridgeSemanticsKind,
    WriteModeKind, PipelineStateKind, RAGGED,
    SchemaAnnotation, Conformed, Grain, SCDType, Degenerate, RolePlaying,
    ProjectsFrom, FactlessFact, DerivedFact, WeightConstraint, BridgeSemantics,
    Hierarchy, PipelineArtifact, annotation_kind, AnnotationSigma,
)
from sqldim.core.query.dgm.temporal import (  # noqa: F401
    TemporalMode, EVENTUALLY, GLOBALLY, NEXT, ONCE, PREVIOUSLY, UntilMode, SinceMode,
    TemporalOrdering, BEFORE, AFTER, CONCURRENT, MEETS, MET_BY, OVERLAPS, OVERLAPPED_BY,
    STARTS, STARTED_BY, DURING, CONTAINS, FINISHES, FINISHED_BY, EQUALS,
    UntilOrdering, SinceOrdering, TemporalWindow, ROLLING, TRAILING, PERIOD,
    YTD, QTD, MTD, TemporalAgg, DeltaSpec,
    ADDED_NODES, REMOVED_NODES, ADDED_EDGES, REMOVED_EDGES,
    CHANGED_PROPERTY, ROLE_DRIFT, DeltaQuery,
)
from sqldim.core.query.dgm.recommender import (  # noqa: F401
    SuggestionKind, Suggestion, Stage1Result, Stage2Result,
    DGMRecommender, ENTROPY_THRESHOLD,
)
from sqldim.core.query.dgm.planner import (  # noqa: F401
    QueryTarget, SinkTarget, PreComputation, CostEstimate, ExportPlan,
    DGMPlanner, SMALL, CLOSURE_THRESHOLD, SMALL_GRAPH_THRESHOLD, DENSE,
)
from sqldim.core.query.dgm.exporters import (  # noqa: F401
    CypherExporter, SPARQLExporter, DGMJSONExporter, DGMYAMLExporter,
)
from sqldim.core.query.dgm.algebra import (  # noqa: F401
    ComposeOp, ComposedQuery, QuestionAlgebra,
)
from sqldim.core.query.dgm._cse import (  # noqa: F401
    find_shared_predicates, apply_cse,
)
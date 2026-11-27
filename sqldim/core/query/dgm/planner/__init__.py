"""DGM Query Planner — DGMPlanner, support types, thresholds, and gate (§6.2–6.3)."""

from sqldim.core.query.dgm.planner._targets import (  # noqa: F401
    SMALL,
    CLOSURE_THRESHOLD,
    SMALL_GRAPH_THRESHOLD,
    DENSE,
    QueryTarget,
    SinkTarget,
    UnsupportedSink,
    PreComputation,
    ApproximateVariant,
    CostEstimate,
    ExportPlan,
)
from sqldim.core.query.dgm.planner._core import (  # noqa: F401
    DGMPlanner,
)
from sqldim.core.query.dgm.planner._capabilities import (  # noqa: F401
    Capability,
    DGMQUERY_TO_SQL,
    Declared,
    POSTGRESQL_VIA_DUCKDB,
    Profile,
    QUERY_PROFILES,
    SINK_PROFILES,
    Support,
    UnsupportedCapability,
    query_profile,
    sink_profile,
)
from sqldim.core.query.dgm.planner._gate import (  # noqa: F401
    HARD_CEILING_FACTOR,
    SAMPLE_DELTA,
    SAMPLE_MAX_ERROR,
    MAX_CLARIFY_OPTIONS,
    Substitution,
    Zone,
    ClarifyKind,
    ClarifyOption,
    BudgetDecisionKind,
    SampleMethod,
    BudgetDecision,
    ProceedDecision,
    StreamDecision,
    AsyncDecision,
    RewriteDecision,
    SampleDecision,
    PaginateDecision,
    ClarifyDecision,
    RejectDecision,
    Delivery,
    InvalidBudget,
    AsyncNotConfigured,
    AsyncTarget,
    RewriteCandidate,
    SamplingPlan,
    Refinement,
    Approximation,
    RewriteProvider,
    SamplingProvider,
    RefinementProvider,
    PrecomputeProvider,
    ExecutionBudget,
    ExecutionGate,
    zone,
)
from sqldim.core.query.dgm.planner._precompute import (  # noqa: F401
    DeclaredPrecomputeProvider,
)
from sqldim.core.query.dgm.planner._canon import (  # noqa: F401
    CANON_VERSION,
    CanonicalEncodingError,
)
from sqldim.core.query.dgm.planner._ir import (  # noqa: F401
    PlanIR,
    PlanIRError,
    UnsupportedConstruct,
    UnsafeFragment,
    UnknownRelation,
    UndeclaredReads,
    PlanIdentityUnavailable,
)
from sqldim.core.query.dgm.planner._ir_emit import MAX_UNROLL  # noqa: F401
from sqldim.core.query.dgm.planner._rule3 import (  # noqa: F401
    Placement,
    GraphExprSchedule,
    PrecomputeEstimate,
    PrecomputeCostModel,
)
from sqldim.core.query.dgm.planner._deps import (  # noqa: F401
    PartitionKind,
    Partition,
    Partitioning,
)
from sqldim.core.query.dgm.planner._ir_emit import emit_sql  # noqa: F401
from sqldim.core.query.dgm.planner._ir_identity import (  # noqa: F401
    Dependency,
    RelationCatalog,
    StaticCatalog,
    dag_hash,
    predicate_hash,
    question_key,
    relations_read,
    deps,
)
from sqldim.core.query.dgm.planner._rule10 import ArtifactTarget  # noqa: F401

"""DGM runtime APIs of §11.10 — query DAG, recommender, executor, registry.

These are the live objects of ``DGMRuntime`` that the NL graph (§11.10)
calls.  Each works under its contract or raises a typed
:class:`RuntimeAPIError` naming what is missing; none returns an empty or
synthetic answer in place of a failure.
"""

from sqldim.core.query.dgm.runtime._errors import (  # noqa: F401
    RuntimeAPIError,
    Unauthorized,
    UnknownIdentifier,
    InvalidRuntimeArgument,
    UnsupportedOperation,
    NotImplementedCapability,
    NotConfigured,
    NotExecutable,
    SubmissionFailed,
    NoCandidates,
)
from sqldim.core.query.dgm.runtime._spec import (  # noqa: F401
    SPEC_FORMAT,
    InvalidSpec,
    QuestionSpec,
    encode_spec,
    decode_spec,
    spec_hash,
    band_coverage,
)
from sqldim.core.query.dgm.runtime._dag import (  # noqa: F401
    DagNodeId,
    QuestionKey,
    PlanRef,
    PlanRefLike,
    ResultKey,
    PlanEstimate,
    CostModel,
    QuestionStore,
    InMemoryQuestionStore,
    WatermarkReader,
    DecompositionStep,
    Decomposition,
    QueryDAG,
)
from sqldim.core.query.dgm.runtime._executor import (  # noqa: F401
    DatasetResolver,
    StaticDatasets,
    QueryBackend,
    DuckDBBackend,
    JobRequest,
    JobTicket,
    JobSink,
    ExecutionResult,
    PlanExecutor,
)
from sqldim.core.query.dgm.runtime._artifact_plan import (  # noqa: F401
    ArtifactState,
    TransitionKind,
    TransitionCondition,
    WriteOperation,
    TransitionPlan,
    WritePlan,
    ArtifactSnapshot,
    ArtifactPlan,
    plan_artifact_run,
    transition_key,
    ArtifactConflict,
    MaterialisationFailed,
    TransitionNotRecorded,
    RunCancelled,
)
from sqldim.core.query.dgm.runtime._artifact import (  # noqa: F401
    SinkWrite,
    WriteIdSink,
    ArtifactOutcome,
    ArtifactRuntime,
)
from sqldim.core.query.dgm.runtime._recommender import (  # noqa: F401
    Grounding,
    ProposedQuestion,
    QuestionSpace,
    QuestionCandidate,
    RefinementAtom,
    DeclaredQuestion,
    DeclaredQuestionSpace,
    RuntimeRecommender,
)
from sqldim.core.query.dgm.runtime._registry import (  # noqa: F401
    SchemaVocabulary,
    VerbLabels,
    GroundedEntityRegistry,
)
from sqldim.core.query.dgm.runtime._cache import (  # noqa: F401
    CacheStoreError,
    CachePolicy,
    CacheEntry,
    ResultStore,
    InMemoryResultStore,
    SQLiteResultStore,
    MissReason,
    NotStoredReason,
    CacheLookup,
    CachedResult,
    CacheExecutor,
    ResultCache,
)
from sqldim.core.query.dgm.runtime._watermarks import (  # noqa: F401
    WatermarkReadError,
    CommitCounters,
)

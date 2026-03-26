"""DGM predicate and path types (DGM §4)."""

from sqldim.core.query.dgm.preds._core import (  # noqa: F401
    ScalarPred,
    AND,
    OR,
    NOT,
    RawPred,
    Strategy,
    ALL,
    SHORTEST,
    K_SHORTEST,
    MIN_WEIGHT,
    Quantifier,
    _HopBase,
    VerbHop,
    VerbHopInverse,
    BridgeHop,
    Compose,
    _flatten_path,
    _path_pred_sql,
    PathPred,
    PathAgg,
)
from sqldim.core.query.dgm.preds._signature import (  # noqa: F401
    SAFETY,
    LIVENESS,
    RESPONSE,
    PERSISTENCE,
    RECURRENCE,
    SequenceMatch,
    SignaturePred,
)

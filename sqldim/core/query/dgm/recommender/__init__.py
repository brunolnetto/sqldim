"""DGM Recommender — DGMRecommender, support types, and ENTROPY_THRESHOLD (DGM §7)."""

from sqldim.core.query.dgm.recommender._types import (  # noqa: F401
    ENTROPY_THRESHOLD,
    SuggestionKind,
    Suggestion,
    Stage1Result,
    Stage2Result,
)
from sqldim.core.query.dgm.recommender._core import (  # noqa: F401
    DGMRecommender,
)

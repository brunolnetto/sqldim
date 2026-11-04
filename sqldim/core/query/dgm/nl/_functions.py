"""DGM NL Interface — intent classification and grounding functions (§11.3–11.8).

Pure functions:
  validate_propref()             — hallucination prevention (§11.8)
  resolve_band()                 — intent signal → BandKind (§11.3)
  classify_temporal_intent()     — NL phrase → TemporalIntent (§11.4)
  classify_compositional_intent()— NL phrase → CompositionOp (§11.4)
  classify_allen_relation()      — NL phrase → Allen interval relation (§3.2)
  classify_temporal_window()     — NL phrase → TemporalWindow kind (§4.2)
  extract_where_filters()        — NL phrase → candidate column filters (§11.5)
  detect_date_trunc_grain()      — NL phrase → DATE_TRUNC grain (§4.2)
  infer_join_tables()            — entity result → multi-table JOIN pairs (§11.3)
  classify_graph_intent()        — NL phrase → GraphAlgorithm class name (§16.4)
  apply_annotation_suggestions() — AnnotationSigma → Suggestion list (§7.4)
  apply_schema_evolution()       — SchemaEvolutionKind → actions (§11.7)
"""

from __future__ import annotations

from typing import Any

from sqldim.core.query.dgm.nl._types import (
    BandKind,
    CompositionOp,
    EntityRegistry,
    SchemaEvolutionAction,
    SchemaEvolutionKind,
    TemporalIntent,
)

__all__ = [
    "validate_propref",
    "resolve_band",
    "classify_temporal_intent",
    "classify_graph_intent",
    "apply_annotation_suggestions",
    "classify_compositional_intent",
    "classify_allen_relation",
    "classify_temporal_window",
    "extract_where_filters",
    "detect_date_trunc_grain",
    "infer_join_tables",
    "apply_schema_evolution",
]

# ---------------------------------------------------------------------------
# Band-intent keyword maps (§11.3)
# ---------------------------------------------------------------------------

_B2_SIGNALS = ("total", "sum", "average", "count", "avg")
_B3_SIGNALS = ("top ", "rank", "highest", "lowest", "ntile", "percentile")
_B1_TEMPORAL = ("has always", "never", "eventually", "once", "previously")
_B1_FILTER = ("where", "which", "filter", "that ")

# ---------------------------------------------------------------------------
# Temporal-intent phrase map (§11.4)
# ---------------------------------------------------------------------------

_TEMPORAL_MAP: list[tuple[str, TemporalIntent]] = [
    ("has always been", TemporalIntent.GLOBALLY),
    ("always eventually", TemporalIntent.LIVENESS),
    ("whenever ", TemporalIntent.RESPONSE),
    ("never violated", TemporalIntent.SAFETY),
    ("never ", TemporalIntent.SAFETY),
    ("eventually", TemporalIntent.EVENTUALLY),
    ("at the next step", TemporalIntent.NEXT),
    ("until ", TemporalIntent.UNTIL),
    ("since ", TemporalIntent.SINCE),
    ("was once", TemporalIntent.ONCE),
    ("once ", TemporalIntent.ONCE),
    ("previous value", TemporalIntent.PREVIOUSLY),
    ("previously", TemporalIntent.PREVIOUSLY),
    ("safety", TemporalIntent.SAFETY),
    ("liveness", TemporalIntent.LIVENESS),
]

# ---------------------------------------------------------------------------
# Compositional-intent phrase map (§11.4)
# ---------------------------------------------------------------------------

_COMP_MAP: list[tuple[str, CompositionOp]] = [
    ("then ", CompositionOp.CHAIN),
    ("first ", CompositionOp.CHAIN),
    ("compared to", CompositionOp.JOIN),
    ("alongside", CompositionOp.JOIN),
    ("or ", CompositionOp.UNION),
    ("and also", CompositionOp.INTERSECT),
    (" and ", CompositionOp.INTERSECT),
]

# ---------------------------------------------------------------------------
# Schema-evolution action map (§11.7)
# ---------------------------------------------------------------------------

_EVOLUTION_ACTIONS: dict[SchemaEvolutionKind, list[SchemaEvolutionAction]] = {
    SchemaEvolutionKind.ADDED_NODES: [
        SchemaEvolutionAction.SUGGEST_NEW_QUESTIONS,
    ],
    SchemaEvolutionKind.REMOVED_NODES: [
        SchemaEvolutionAction.INVALIDATE_SAVED_QUESTIONS,
        SchemaEvolutionAction.NOTIFY_SEMANTIC_CHANGE,
    ],
    SchemaEvolutionKind.ADDED_EDGES: [
        SchemaEvolutionAction.SUGGEST_NEW_QUESTIONS,
    ],
    SchemaEvolutionKind.REMOVED_EDGES: [
        SchemaEvolutionAction.INVALIDATE_SAVED_QUESTIONS,
    ],
    SchemaEvolutionKind.ROLE_DRIFT: [
        SchemaEvolutionAction.REVALIDATE_SAVED_QUESTIONS,
        SchemaEvolutionAction.NOTIFY_SEMANTIC_CHANGE,
    ],
    SchemaEvolutionKind.CHANGED_PROPERTY: [
        SchemaEvolutionAction.REFRESH_ENTITY_REGISTRY,
        SchemaEvolutionAction.REVALIDATE_SAVED_QUESTIONS,
    ],
}


# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------


def validate_propref(propref: str, registry: EntityRegistry) -> bool:
    """Return True iff *propref* is in the closed vocabulary (§11.8)."""
    return propref in registry.propref_vocabulary()


def resolve_band(phrase: str) -> BandKind:
    """Map a natural-language phrase to the most likely query band (§11.3).

    Returns B1 when no recognisable signal is found (safe default).
    """
    lower = phrase.lower()
    if any(sig in lower for sig in _B3_SIGNALS):
        return BandKind.B3
    if any(sig in lower for sig in _B2_SIGNALS):
        return BandKind.B2
    return BandKind.B1


def classify_temporal_intent(phrase: str) -> TemporalIntent | None:
    """Map a natural-language temporal phrase to a TemporalIntent (§11.4).

    Returns None when no temporal pattern is recognised.
    """
    lower = phrase.lower()
    for pattern, intent in _TEMPORAL_MAP:
        if pattern in lower:
            return intent
    return None


def classify_compositional_intent(phrase: str) -> CompositionOp | None:
    """Map a natural-language phrase to a Q_algebra operation (§11.4).

    Returns None when no compositional pattern is recognised.
    """
    lower = phrase.lower()
    for pattern, op in _COMP_MAP:
        if pattern in lower:
            return op
    return None


# ---------------------------------------------------------------------------
# Allen interval classification (§3.2)
# ---------------------------------------------------------------------------

_ALLEN_MAP: list[tuple[str, str]] = [
    ("before ", "BEFORE"),
    ("preceded by", "BEFORE"),
    ("earlier than", "BEFORE"),
    ("after ", "AFTER"),
    ("followed by", "AFTER"),
    ("later than", "AFTER"),
    ("at the same time", "CONCURRENT"),
    ("simultaneous", "CONCURRENT"),
    ("concurrent", "CONCURRENT"),
    ("meets ", "MEETS"),
    ("immediately followed", "MEETS"),
    ("met by", "MET_BY"),
    ("immediately preceded", "MET_BY"),
    ("overlaps with", "OVERLAPS"),
    ("partially overlaps", "OVERLAPS"),
    ("overlapped by", "OVERLAPPED_BY"),
    ("starts with", "STARTS"),
    ("started by", "STARTED_BY"),
    ("during ", "DURING"),
    ("within ", "DURING"),
    ("contains ", "CONTAINS"),
    ("encompasses", "CONTAINS"),
    ("finishes with", "FINISHES"),
    ("finished by", "FINISHED_BY"),
    ("equals ", "EQUALS"),
    ("same period", "EQUALS"),
]


def classify_allen_relation(phrase: str) -> str | None:
    """Map a natural-language phrase to an Allen interval relation name (§3.2).

    Returns one of the 13 Allen relation names (e.g. ``"BEFORE"``,
    ``"DURING"``, ``"MEETS"``) or ``None`` when no pattern matches.
    """
    lower = phrase.lower()
    for pattern, relation in _ALLEN_MAP:
        if pattern in lower:
            return relation
    return None


# ---------------------------------------------------------------------------
# Temporal window classification (§4.2)
# ---------------------------------------------------------------------------

_WINDOW_MAP: list[tuple[str, str]] = [
    ("year to date", "YTD"),
    ("year-to-date", "YTD"),
    ("ytd", "YTD"),
    ("this year", "YTD"),
    ("quarter to date", "QTD"),
    ("quarter-to-date", "QTD"),
    ("qtd", "QTD"),
    ("this quarter", "QTD"),
    ("month to date", "MTD"),
    ("month-to-date", "MTD"),
    ("mtd", "MTD"),
    ("this month", "MTD"),
    ("rolling ", "ROLLING"),
    ("moving average", "ROLLING"),
    ("trailing ", "TRAILING"),
    ("last n ", "TRAILING"),
    ("between ", "PERIOD"),
    ("from ", "PERIOD"),
]


def classify_temporal_window(phrase: str) -> str | None:
    """Map a natural-language phrase to a TemporalWindow kind (§4.2).

    Returns one of ``"YTD"``, ``"QTD"``, ``"MTD"``, ``"ROLLING"``,
    ``"TRAILING"``, ``"PERIOD"`` or ``None``.
    """
    lower = phrase.lower()
    for pattern, window in _WINDOW_MAP:
        if pattern in lower:
            return window
    return None


# ---------------------------------------------------------------------------
# WHERE filter extraction (§11.5)
# ---------------------------------------------------------------------------

_FILTER_SIGNALS: tuple[str, ...] = (
    "where ",
    "which ",
    "filter ",
    "that ",
    "whose ",
    "only ",
    "just ",
    "with ",
    "having ",
    "for ",
)

_COMPARISON_PATTERNS: list[tuple[str, str]] = [
    (" greater than ", ">"),
    (" more than ", ">"),
    (" above ", ">"),
    (" over ", ">"),
    (" exceeds ", ">"),
    (" less than ", "<"),
    (" fewer than ", "<"),
    (" below ", "<"),
    (" under ", "<"),
    (" at least ", ">="),
    (" at most ", "<="),
    (" equal to ", "="),
    (" equals ", "="),
    (" is ", "="),
]


def extract_where_filters(
    phrase: str,
    known_columns: list[str],
) -> list[tuple[str, str, str]]:
    """Extract candidate WHERE clauses from a natural-language phrase.

    Returns a list of ``(column, operator, value)`` triples grounded
    against *known_columns*.  Only columns that appear in the closed
    vocabulary are returned — unknown terms are silently dropped (§11.8).

    This is a deterministic heuristic; false positives are harmless since
    the generated SQL is validated against DuckDB before execution.
    """
    import re as _re

    lower = phrase.lower()
    if not any(sig in lower for sig in _FILTER_SIGNALS):
        return []

    filters: list[tuple[str, str, str]] = []
    col_lower_map = {c.lower(): c for c in known_columns}

    for col_lower, col_real in col_lower_map.items():
        # Check for comparison patterns: "revenue greater than 1000"
        for pattern, op in _COMPARISON_PATTERNS:
            # Look for "<column> <pattern> <value>"
            rx = _re.compile(
                rf"\b{_re.escape(col_lower)}\b{_re.escape(pattern)}(\S+)",
                _re.IGNORECASE,
            )
            m = rx.search(lower)
            if m:
                filters.append((col_real, op, m.group(1)))
                break

        # Check for equality: "region is west" / "status = active"
        eq_rx = _re.compile(rf"\b{_re.escape(col_lower)}\s*=\s*(\S+)", _re.IGNORECASE)
        eq_m = eq_rx.search(lower)
        if eq_m and not any(f[0] == col_real for f in filters):
            filters.append((col_real, "=", eq_m.group(1)))

    return filters


# ---------------------------------------------------------------------------
# DATE_TRUNC grain detection (§4.2)
# ---------------------------------------------------------------------------

_GRAIN_MAP: list[tuple[str, str]] = [
    ("yearly", "year"),
    ("per year", "year"),
    ("by year", "year"),
    ("annual", "year"),
    ("quarterly", "quarter"),
    ("per quarter", "quarter"),
    ("by quarter", "quarter"),
    ("monthly", "month"),
    ("per month", "month"),
    ("by month", "month"),
    ("weekly", "week"),
    ("per week", "week"),
    ("by week", "week"),
    ("daily", "day"),
    ("per day", "day"),
    ("by day", "day"),
    ("by date", "day"),
]


def detect_date_trunc_grain(phrase: str) -> str | None:
    """Detect a DATE_TRUNC grain from a natural-language phrase (§4.2).

    Returns one of ``"year"``, ``"quarter"``, ``"month"``, ``"week"``,
    ``"day"`` or ``None``.
    """
    lower = phrase.lower()
    for pattern, grain in _GRAIN_MAP:
        if pattern in lower:
            return grain
    return None


# ---------------------------------------------------------------------------
# Multi-table JOIN inference (§11.3)
# ---------------------------------------------------------------------------


def infer_join_tables(
    resolved_proprefs: list[object],
    registry: EntityRegistry,
) -> list[tuple[str, str, str]]:
    """Infer JOIN pairs from entity-resolved PropRefs spanning multiple tables.

    Parameters
    ----------
    resolved_proprefs:
        List of ``PropRefModel`` (or similar with ``.alias`` attribute).
    registry:
        The active ``EntityRegistry``.

    Returns
    -------
    list of (table_a, table_b, join_column):
        Candidate equi-join triples where *join_column* is a ``*_id`` column
        present in both tables.  Empty when fewer than two tables are resolved.
    """
    tables: set[str] = set()
    for pr in resolved_proprefs:
        alias = getattr(pr, "alias", None)
        if alias and alias in registry.node_terms.values():
            tables.add(alias)
        elif alias and alias in registry.node_terms:
            resolved = registry.node_terms[alias]
            tables.add(resolved)

    if len(tables) < 2:
        return []

    # Build per-table column sets from the registry's prop vocabulary
    table_cols: dict[str, set[str]] = {}
    for propref in registry.prop_terms.values():
        parts = propref.split(".", 1)
        if len(parts) == 2:
            tbl, col = parts
            if tbl in tables:
                table_cols.setdefault(tbl, set()).add(col)

    # Find _id columns shared between table pairs
    joins: list[tuple[str, str, str]] = []
    sorted_tables = sorted(tables)
    for i, t1 in enumerate(sorted_tables):
        for t2 in sorted_tables[i + 1 :]:
            cols1 = table_cols.get(t1, set())
            cols2 = table_cols.get(t2, set())
            shared_id = {c for c in cols1 & cols2 if c.endswith("_id") or c == "id"}
            for col in sorted(shared_id):
                joins.append((t1, t2, col))
    return joins


def apply_schema_evolution(kind: SchemaEvolutionKind) -> list[SchemaEvolutionAction]:
    """Return the actions triggered by a schema evolution event (§11.7)."""
    return list(_EVOLUTION_ACTIONS.get(kind, []))


# ---------------------------------------------------------------------------
# Graph algorithm intent classification (§16.4)
# ---------------------------------------------------------------------------

_GRAPH_ALGO_MAP: list[tuple[str, str]] = [
    ("community detection", "LOUVAIN"),
    ("louvain", "LOUVAIN"),
    ("label propagation", "LABEL_PROPAGATION"),
    ("connected component", "CONNECTED_COMPONENTS"),
    ("strongly connected", "TARJAN_SCC"),
    ("tarjan scc", "TARJAN_SCC"),
    ("page rank", "PAGE_RANK"),
    ("pagerank", "PAGE_RANK"),
    ("betweenness centrality", "BETWEENNESS_CENTRALITY"),
    ("betweenness", "BETWEENNESS_CENTRALITY"),
    ("closeness centrality", "CLOSENESS_CENTRALITY"),
    ("closeness", "CLOSENESS_CENTRALITY"),
    ("node degree", "DEGREE"),
    ("degree distribution", "DEGREE"),
    ("shortest path", "SHORTEST_PATH_LENGTH"),
    ("max flow", "MAX_FLOW"),
    ("reachable from", "REACHABLE"),
    ("graph density", "DENSITY"),
    ("graph diameter", "DIAMETER"),
    ("cluster", "LABEL_PROPAGATION"),
]


def classify_graph_intent(phrase: str) -> str | None:
    """Map a NL phrase to a graph algorithm class name (DGM §16.4).

    Returns one of the concrete :class:`GraphAlgorithm` subclass names
    (e.g. ``"PAGE_RANK"``, ``"LOUVAIN"``) or ``None`` when no pattern matches.
    """
    lower = phrase.lower()
    for pattern, algo in _GRAPH_ALGO_MAP:
        if pattern in lower:
            return algo
    return None


# ---------------------------------------------------------------------------
# Annotation-driven recommender hook (§7.4)
# ---------------------------------------------------------------------------


def apply_annotation_suggestions(sigma: Any, statistics: Any = None) -> list[Any]:
    """Apply annotation-driven recommender rules (DGM §7.4).

    Constructs a :class:`~sqldim.core.query.dgm.recommender.DGMRecommender`
    from *sigma* and *statistics* and runs
    :meth:`~sqldim.core.query.dgm.recommender.DGMRecommender.run_annotation_rules`.

    Returns an empty list when *sigma* is empty or has no applicable rules.
    """
    from sqldim.core.query.dgm.recommender._core import DGMRecommender

    return DGMRecommender(sigma, statistics).run_annotation_rules()

"""Node-implementation factories for the eleven-node NL graph (§11.10).

Keeps the closure-heavy per-node logic out of ``_graph.py`` so that module
stays focused on graph wiring and compilation.

Two public factories are exported:

``make_llm_nodes(context, model)``
    Returns the five LLM-backed node functions keyed by node name.
``make_operational_nodes(context)``
    Returns the six non-LLM engine node functions keyed by node name.
"""

from __future__ import annotations

import concurrent.futures
from typing import Any

from sqldim.core.query.dgm.nl._agent_types import NLInterfaceState, QueryCandidate
from sqldim.core.query.dgm.nl._agent_types import (
    CompositionalDetectionResult,
    TemporalClassificationResult,
)
from sqldim.core.query.dgm.nl._agents import (
    make_compositional_agent,
    make_entity_agent,
    make_explanation_agent,
    make_ranking_agent,
    make_sql_agent,
    make_temporal_agent,
)
from sqldim.core.query.dgm.planner._gate import ExecutionGate

__all__ = ["make_llm_nodes", "make_operational_nodes"]

# Ordered node names — used by _graph.py when building stub nodes.
LLM_NODE_NAMES = (
    "entity_resolution",
    "temporal_classification",
    "compositional_detection",
    "candidate_ranking",
    "explanation_rendering",
)
OPERATIONAL_NODE_NAMES = (
    "candidate_generation",
    "confirmation_loop",
    "dag_construction",
    "budget_gate",
    "execution",
    "clarification",
)

_NodeFn = Any  # Callable[[NLInterfaceState], dict[str, Any]]

# ---------------------------------------------------------------------------
# Lightweight keyword screens — used to skip unnecessary LLM round-trips
# ---------------------------------------------------------------------------

_TEMPORAL_KEYWORDS: frozenset[str] = frozenset(
    {
        "year",
        "quarter",
        "month",
        "week",
        "day",
        "date",
        "time",
        "since",
        "before",
        "after",
        "latest",
        "current",
        "recent",
        "yesterday",
        "today",
        "last",
        "historical",
        "historic",
        "previous",
        "prior",
        "next",
        "when",
        "period",
        "rolling",
        "ytd",
        "this year",
        "this month",
        "this quarter",
    }
)

_COMPOSITIONAL_KEYWORDS: frozenset[str] = frozenset(
    {
        "compared to",
        "versus",
        " vs ",
        "combined with",
        "along with",
        "together with",
        "union",
        "intersect",
        "merge",
        "both ",
        "either ",
    }
)


def _has_temporal_signal(utterance: str) -> bool:
    """Return True if *utterance* likely contains a temporal intent."""
    lower = utterance.lower()
    return any(kw in lower for kw in _TEMPORAL_KEYWORDS)


def _has_compositional_signal(utterance: str) -> bool:
    """Return True if *utterance* likely contains a compositional intent."""
    lower = utterance.lower()
    return any(kw in lower for kw in _COMPOSITIONAL_KEYWORDS)


def make_llm_nodes(context: Any, model: Any) -> dict[str, _NodeFn]:
    """Build the five LLM specialist node functions.

    Returns a dict keyed by node name; every function is a closure over
    *context* and the pre-built pydantic-ai agent instance.
    """
    _entity_agent = make_entity_agent(model)
    _temporal_agent = make_temporal_agent(model)
    _compositional_agent = make_compositional_agent(model)
    _ranking_agent = make_ranking_agent(model)
    _explanation_agent = make_explanation_agent(model)

    def _entity_node(state: NLInterfaceState) -> dict[str, Any]:
        """Run entity classification; skip temporal/compositional agents when
        the utterance clearly lacks those signals (saves 1-2 LLM round-trips)."""
        utterance = state.utterance
        out: dict[str, Any] = {}

        def _call(agent: Any, key: str) -> tuple[str, Any]:
            try:
                r = agent.run_sync(
                    utterance,
                    deps=context,
                    model_settings={"timeout": 30.0},
                )
                return key, r.output
            except Exception:  # noqa: BLE001
                return key, None

        needs_temporal = _has_temporal_signal(utterance)
        needs_compositional = _has_compositional_signal(utterance)

        workers = 1 + int(needs_temporal) + int(needs_compositional)
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            futs = [pool.submit(_call, _entity_agent, "entity_result")]
            if needs_temporal:  # pragma: no cover
                futs.append(pool.submit(_call, _temporal_agent, "temporal_result"))
            if needs_compositional:  # pragma: no cover
                futs.append(pool.submit(_call, _compositional_agent, "compositional"))

            for fut in concurrent.futures.as_completed(futs):
                key, val = fut.result()
                if val is not None:
                    out[key] = val

        # Set null defaults so downstream nodes skip their LLM calls
        if not needs_temporal:
            out.setdefault("temporal_result", TemporalClassificationResult())
        if not needs_compositional:
            out.setdefault(
                "compositional",
                CompositionalDetectionResult(
                    operation=None, join_key=None, confidence=0.0
                ),
            )

        return out

    def _temporal_node(state: NLInterfaceState) -> dict[str, Any]:  # pragma: no cover
        # Already populated by the parallel entity_resolution node.
        if state.temporal_result is not None:
            return {}
        try:
            r = _temporal_agent.run_sync(
                state.utterance,
                deps=context,
                model_settings={"timeout": 30.0},
            )
            return {"temporal_result": r.output}
        except Exception:  # noqa: BLE001
            return {}

    def _compositional_node(
        state: NLInterfaceState,
    ) -> dict[str, Any]:  # pragma: no cover
        # Already populated by the parallel entity_resolution node.
        if state.compositional is not None:
            return {}
        try:
            r = _compositional_agent.run_sync(
                state.utterance,
                deps=context,
                model_settings={"timeout": 30.0},
            )
            return {"compositional": r.output}
        except Exception:  # noqa: BLE001
            return {}

    def _ranking_node(state: NLInterfaceState) -> dict[str, Any]:  # pragma: no cover
        if not state.candidates:
            return {}
        try:
            prompt = f"{state.utterance}\n\nCandidates:\n" + "\n".join(
                f"{i}. {c.description}" for i, c in enumerate(state.candidates)
            )
            r = _ranking_agent.run_sync(
                prompt,
                deps=context,
                model_settings={"timeout": 30.0},
            )
            return {"ranking_result": r.output}
        except Exception:  # noqa: BLE001
            return {}

    def _explanation_node(state: NLInterfaceState) -> dict[str, Any]:
        try:
            result = state.result
            result_count = result.get("count", 0) if isinstance(result, dict) else 0

            # Skip LLM call when there is no data to explain
            if result is None or result_count == 0:
                sql_preview = f" SQL: {state.q_current}" if state.q_current else ""
                return {"explanation": (f"The query returned no results.{sql_preview}")}

            prompt_parts = [f"Question: {state.utterance}"]
            if state.q_current:
                prompt_parts.append(f"SQL executed: {state.q_current}")
            if isinstance(result, dict):
                cols: list[str] = result.get("columns", [])
                rows: list[list[str]] = result.get("rows", [])
                count: int = result.get("count", 0)
                if cols:
                    prompt_parts.append(f"Result columns: {', '.join(cols)}")
                prompt_parts.append(f"Row count: {count}")
                if rows and cols:
                    preview = rows[:5]
                    preview_lines = [
                        "  " + ", ".join(f"{c}={v}" for c, v in zip(cols, row))
                        for row in preview
                    ]
                    prompt_parts.append("Sample rows:\n" + "\n".join(preview_lines))
            prompt = "\n".join(prompt_parts)
            r = _explanation_agent.run_sync(
                prompt,
                deps=context,
                model_settings={"timeout": 30.0},
            )
            return {"explanation": r.output.explanation}
        except Exception:  # noqa: BLE001
            return {}

    return {
        "entity_resolution": _entity_node,
        "temporal_classification": _temporal_node,
        "compositional_detection": _compositional_node,
        "candidate_ranking": _ranking_node,
        "explanation_rendering": _explanation_node,
    }


def make_operational_nodes(context: Any, model: Any = None) -> dict[str, _NodeFn]:
    """Build the six non-LLM machinery node functions.

    Returns a dict keyed by node name; every function captures *context* to
    access the EntityRegistry, budget, and live DuckDB connection.
    When *model* is provided, ``dag_construction`` uses an LLM to generate
    intent-aware SQL instead of the template fallback.
    """
    _sql_agent = make_sql_agent(model) if model is not None else None

    def _build_schema_str(active_tables: set[str] | None = None) -> str:
        """Build a richly-typed schema description for the SQL agent prompt.

        Queries DuckDB's DESCRIBE for each table to include column types, then
        infers FK join hints by matching ``*_id`` column names across tables.
        This gives the model enough signal to choose the right join key instead
        of guessing between e.g. ``product_id`` (INTEGER) and ``sku`` (VARCHAR).

        Args:
            active_tables: When provided, join hints are only emitted for table
                pairs that are *both* in this set.  Pass the entity-resolution
                result so that single-table queries never see spurious join hints
                that tempt the model into unnecessary JOINs.
        """
        con = context.con  # type: ignore[union-attr]
        registry = context.entity_registry

        # Collect table names from the registry
        table_names: list[str] = sorted(set(registry.node_terms.values()))

        # SCD2 audit columns are pipeline internals — never expose them to the
        # SQL agent so it cannot accidentally filter on e.g. is_current = TRUE.
        _SCD_COLS: frozenset[str] = frozenset(
            {"valid_from", "valid_to", "is_current", "checksum"}
        )

        # Fetch column metadata from DuckDB
        table_cols: dict[str, list[tuple[str, str]]] = {}  # tbl -> [(col, type)]
        for tbl in table_names:
            try:
                rows = con.execute(f"DESCRIBE {tbl}").fetchall()
                # DESCRIBE returns (column_name, column_type, null, key, default, extra)
                table_cols[tbl] = [
                    (r[0], r[1].split("(")[0].upper())
                    for r in rows
                    if r[0] not in _SCD_COLS
                ]
            except Exception:  # noqa: BLE001  # pragma: no cover
                # Fall back to registry column names without types
                cols = [
                    prop_ref.split(".", 1)[1]
                    for prop_ref in registry.prop_terms.values()
                    if prop_ref.startswith(f"{tbl}.")
                    and prop_ref.split(".", 1)[1] not in _SCD_COLS
                ]
                table_cols[tbl] = [(c, "") for c in sorted(cols)]

        # Build table definitions
        lines = ["Available tables (column: TYPE):"]
        for tbl in table_names:
            col_defs = ", ".join(f"{c}: {t}" if t else c for c, t in table_cols[tbl])
            lines.append(f"  {tbl}({col_defs})")

        # Infer FK join hints: columns ending in _id that appear in multiple tables
        id_cols: dict[str, list[str]] = {}  # col_name -> [table, ...]
        for tbl, cols in table_cols.items():
            for col, _ in cols:
                if col.endswith("_id") or col == "id":
                    id_cols.setdefault(col, []).append(tbl)

        join_hints: list[str] = []
        for col, tbls in sorted(id_cols.items()):
            if len(tbls) >= 2:
                for i, t1 in enumerate(tbls):
                    for t2 in tbls[i + 1 :]:
                        join_hints.append(f"  {t1}.{col} = {t2}.{col}")

        # Only emit hints when both tables in the pair are actively in scope AND
        # there are at least two active tables (i.e. the query genuinely spans
        # multiple tables).  Suppressing hints for single-table queries prevents
        # the model from adding unnecessary JOINs.
        if active_tables is not None:  # pragma: no cover
            if len(active_tables) < 2:
                join_hints = []  # single-table query — no join hints needed
            else:

                def _hint_tbls(h: str) -> tuple[str, str]:
                    parts = h.strip().split(" = ")
                    return parts[0].split(".")[0], parts[1].split(".")[0]

                join_hints = [
                    h
                    for h in join_hints
                    if all(t in active_tables for t in _hint_tbls(h))
                ]

        if join_hints:
            lines.append(
                "\nJoin key relationships (use these exact columns for JOINs):"
            )
            lines.extend(join_hints)

        return "\n".join(lines)

    def _candidate_generation_node(state: NLInterfaceState) -> dict[str, Any]:
        registry = context.entity_registry  # type: ignore[union-attr]

        tables: set[str] = set()
        if state.entity_result:
            for pr in state.entity_result.resolved:
                tables.add(pr.alias)

        if not tables:
            tables = set(registry.node_terms.values())

        candidates: list[QueryCandidate] = []
        for i, table in enumerate(sorted(tables)):
            props = [
                propref
                for propref in registry.prop_terms.values()
                if propref.startswith(f"{table}.")
            ]
            col_preview = ", ".join(p.split(".", 1)[1] for p in props[:3])
            suffix = "..." if len(props) > 3 else ""
            desc = (
                f"SELECT from {table} ({col_preview}{suffix})"
                if props
                else f"SELECT from {table}"
            )
            candidates.append(
                QueryCandidate(
                    dag_node_id=i,
                    description=desc,
                    band_coverage=["B1"],
                    cost_estimate=float(len(props)),
                )
            )
        return {"candidates": candidates}

    def _confirmation_loop_node(state: NLInterfaceState) -> dict[str, Any]:
        if state.confirmed:
            return {}
        if state.ranking_result and state.ranking_result.ranked:
            return {"confirmed": True, "user_selection": 0}
        if state.candidates:
            return {"confirmed": True, "user_selection": 0}
        return {"confirmed": True}

    def _dag_construction_node(state: NLInterfaceState) -> dict[str, Any]:
        from sqldim.core.query.dgm.planner._targets import ExportPlan, QueryTarget

        # ── LLM-based SQL (intent-aware) ──────────────────────────────────
        import logging as _logging

        _dag_log = _logging.getLogger("sqldim.nl.dag_construction")
        if _sql_agent is not None:
            try:
                # Collect the tables entity-resolution identified so we can
                # suppress join hints for tables that are not in scope.
                _active: set[str] | None = None
                if state.entity_result and state.entity_result.resolved:
                    _active = {pr.alias for pr in state.entity_result.resolved}
                prompt = f"{state.utterance}\n\n{_build_schema_str(_active)}"
                _dag_log.debug("SQL agent prompt:\n%s", prompt)
                r = _sql_agent.run_sync(
                    prompt,
                    deps=context,
                    model_settings={"timeout": 180.0},
                )
                sql_raw: str = r.output.strip()
                _dag_log.debug("SQL agent raw output: %s", sql_raw)
                # Strip markdown code fences if the model added them
                if sql_raw.startswith("```"):
                    sql_raw = "\n".join(
                        line
                        for line in sql_raw.splitlines()
                        if not line.strip().startswith("```")
                    ).strip()
                if sql_raw.upper().startswith("SELECT"):
                    plan = ExportPlan(
                        query_target=QueryTarget.SQL_DUCKDB,
                        query_text=sql_raw,
                    )
                    context.q_current = plan  # type: ignore[union-attr]
                    return {"q_current": sql_raw}
                else:
                    _dag_log.warning(
                        "SQL agent output did not start with SELECT: %s",
                        sql_raw[:200],
                    )
            except Exception as exc:  # noqa: BLE001
                _dag_log.warning("SQL agent failed: %s: %s", type(exc).__name__, exc)
                # fall through to template

        # ── Template fallback (keyword-driven heuristic SQL) ─────────────
        registry = context.entity_registry  # type: ignore[union-attr]
        utterance_lower = state.utterance.lower()

        from sqldim.core.query.dgm.nl._functions import (
            apply_annotation_suggestions,
            classify_graph_intent,
            classify_temporal_window,
            detect_date_trunc_grain,
            extract_where_filters,
            infer_join_tables,
            validate_propref,
        )

        # Pick the best table from entity resolution or candidates ----------
        selected: QueryCandidate | None = None
        idx = state.user_selection or 0
        if state.ranking_result and state.ranking_result.ranked:
            if 0 <= idx < len(state.ranking_result.ranked):
                selected = state.ranking_result.ranked[idx]
        if selected is None and state.candidates:
            if 0 <= idx < len(state.candidates):
                selected = state.candidates[idx]
            elif state.candidates:
                selected = state.candidates[0]

        table: str | None = None
        if selected is not None:
            desc = selected.description
            if "SELECT from " in desc:
                rest = desc.split("SELECT from ", 1)[1].strip()
                table = rest.split()[0].rstrip(" (")

        if table is None or table not in registry.node_terms.values():
            sql = "SELECT 1 AS placeholder"
            plan = ExportPlan(
                query_target=QueryTarget.SQL_DUCKDB,
                query_text=sql,
            )
            context.q_current = plan  # type: ignore[union-attr]
            return {"q_current": sql}

        # Gather columns for the selected table
        props = [
            propref
            for propref in registry.prop_terms.values()
            if propref.startswith(f"{table}.")
        ]
        all_cols = [p.split(".", 1)[1] for p in props]

        # Validate columns against closed vocabulary (§11.8)
        validated_cols = [
            c for c in all_cols if validate_propref(f"{table}.{c}", registry)
        ]
        if not validated_cols:
            validated_cols = all_cols  # fallback: use all if registry is sparse

        # Detect intent from utterance keywords ---------------------------
        _AGG_SIGNALS = {
            "total": "SUM",
            "sum": "SUM",
            "revenue": "SUM",
            "sales": "SUM",
            "average": "AVG",
            "avg": "AVG",
            "mean": "AVG",
            "count": "COUNT",
            "how many": "COUNT",
            "number of": "COUNT",
            "most common": "COUNT",
        }
        _RANK_SIGNALS = {"top", "best", "highest", "most", "largest", "greatest"}
        _ORDER_DESC = {"top", "best", "highest", "most", "largest", "greatest"}
        _GROUPBY_SIGNALS = {
            "by",
            "per",
            "each",
            "breakdown",
            "category",
            "type",
            "group",
            "common",
        }
        _NUMERIC_SUFFIXES = {
            "amount",
            "price",
            "total",
            "revenue",
            "cost",
            "value",
            "salary",
            "count",
            "quantity",
            "rate",
            "score",
            "points",
        }
        _DATE_SUFFIXES = {"date", "time", "timestamp", "created_at", "updated_at", "at"}

        # Detect aggregation function
        agg_fn: str | None = None
        for signal, fn in _AGG_SIGNALS.items():
            if signal in utterance_lower:
                agg_fn = fn
                break

        # Detect ranking / top-N
        wants_rank = any(s in utterance_lower for s in _RANK_SIGNALS)
        limit_n = 10  # default for top-N
        import re as _re

        top_match = _re.search(r"top\s+(\d+)", utterance_lower)
        if top_match:
            limit_n = int(top_match.group(1))

        # Detect GROUP BY column (by <column_name>)
        group_col: str | None = None
        for signal in _GROUPBY_SIGNALS:
            pattern = _re.compile(rf"\b{signal}\s+(\w+)", _re.IGNORECASE)
            m = pattern.search(utterance_lower)
            if m:
                candidate = m.group(1).lower()
                # Match against actual columns
                for col in validated_cols:
                    if candidate in col.lower() or col.lower() in candidate:
                        group_col = col
                        break
                if group_col:
                    break

        # Pick a numeric column for aggregation
        numeric_col: str | None = None
        for col in validated_cols:
            if any(
                col.lower().endswith(s) or col.lower() == s for s in _NUMERIC_SUFFIXES
            ):
                numeric_col = col
                break

        # Detect ordering column from utterance
        order_col: str | None = None
        for col in validated_cols:
            if col.lower() in utterance_lower.replace("_", " "):
                order_col = col
                break

        # Detect date column for temporal features
        date_col: str | None = None
        for col in validated_cols:
            if any(col.lower().endswith(s) or col.lower() == s for s in _DATE_SUFFIXES):
                date_col = col
                break

        # ── WHERE clause from NL filter extraction (§11.5) ─────────────
        where_clauses: list[str] = []
        nl_filters = extract_where_filters(state.utterance, validated_cols)
        for col, op, val in nl_filters:
            # Try numeric first; fall back to quoted string
            try:
                float(val)
                where_clauses.append(f"{col} {op} {val}")
            except ValueError:
                where_clauses.append(f"{col} {op} '{val}'")
        where_sql = " AND ".join(where_clauses) if where_clauses else ""

        # ── DATE_TRUNC grain detection (§4.2) ────────────────────────────
        date_grain = detect_date_trunc_grain(state.utterance)
        date_trunc_expr: str | None = None
        if date_grain and date_col:
            alias = f"{date_col}_{date_grain}"
            date_trunc_expr = f"DATE_TRUNC('{date_grain}', {date_col}) AS {alias}"
            # Use as GROUP BY column if none specified
            if not group_col:
                group_col = alias

        # ── Temporal window detection (§4.2) ──────────────────────────────
        window_kind = classify_temporal_window(state.utterance)
        temporal_where: str | None = None
        if window_kind and date_col:
            if window_kind == "YTD":
                temporal_where = f"{date_col} >= DATE_TRUNC('year', CURRENT_DATE)"
            elif window_kind == "QTD":
                temporal_where = f"{date_col} >= DATE_TRUNC('quarter', CURRENT_DATE)"
            elif window_kind == "MTD":
                temporal_where = f"{date_col} >= DATE_TRUNC('month', CURRENT_DATE)"
            elif window_kind == "ROLLING":
                rm = _re.search(r"rolling\s+(\d+)\s*(\w+)", utterance_lower)
                if rm:
                    temporal_where = f"{date_col} >= CURRENT_DATE - INTERVAL '{rm.group(1)} {rm.group(2)}'"
                else:
                    temporal_where = f"{date_col} >= CURRENT_DATE - INTERVAL '30 days'"
            elif window_kind == "TRAILING":
                tm = _re.search(r"(?:trailing|last)\s+(\d+)\s*(\w+)", utterance_lower)
                if tm:
                    temporal_where = f"{date_col} >= CURRENT_DATE - INTERVAL '{tm.group(1)} {tm.group(2)}'"
        if temporal_where:
            if where_sql:
                where_sql = f"{where_sql} AND {temporal_where}"
            else:
                where_sql = temporal_where

        # ── Multi-table JOIN inference (§11.3) ────────────────────────────
        join_sql = ""
        join_tables: list[tuple[str, str, str]] = []
        if state.entity_result and state.entity_result.resolved:
            join_tables = infer_join_tables(state.entity_result.resolved, registry)
        if join_tables:
            # Build JOIN clauses for the first join pair
            for _t1, t2, jcol in join_tables[:1]:
                join_sql = f" JOIN {t2} ON {table}.{jcol} = {t2}.{jcol}"

        # ── Graph algorithm intent detection (§16.4) ──────────────────────
        graph_algo = classify_graph_intent(state.utterance)

        # ── Annotation-driven recommendations hook (§7.4) ─────────────────
        _ann_suppress_groupby = False
        if context.recommender is not None:  # type: ignore[union-attr]
            try:
                _ann_suggestions = apply_annotation_suggestions(
                    context.recommender.sigma,  # type: ignore[union-attr]
                    getattr(context.recommender, "statistics", None),  # type: ignore[union-attr]
                )
                for _sug in _ann_suggestions:
                    if _sug.kind.name == "SUPPRESS" and group_col:
                        _ann_suppress_groupby = True
                        break
            except Exception:  # noqa: BLE001
                pass
        if _ann_suppress_groupby:
            group_col = None

        # ── Compositional intent → CTE (§8.13 QuestionAlgebra) ───────────
        comp = state.compositional
        uses_composition = (
            comp is not None
            and comp.operation is not None
            and comp.confidence >= 0.5
            and len(state.candidates) >= 2
        )

        # Build SQL based on detected intent --------------------------------
        # ── Graph algorithm query takes priority (§16.4) ─────────────────
        if graph_algo and not uses_composition:
            from sqldim.core.query.dgm.planner._targets import (
                ExportPlan,
                QueryTarget,
            )

            algo_fn = graph_algo.lower()
            sql = (
                f"SELECT node_id, {algo_fn}() AS {algo_fn}_score"
                f" FROM {table} ORDER BY {algo_fn}_score DESC LIMIT 100"
            )
            _dag_log.info("Graph algorithm SQL: %s", sql)
            plan = ExportPlan(
                query_target=QueryTarget.SQL_DUCKDB,
                query_text=sql,
            )
            context.q_current = plan  # type: ignore[union-attr]
            return {"q_current": sql}

        if uses_composition and comp is not None:
            from sqldim.core.query.dgm.nl._types import CompositionOp

            tables_involved: list[str] = []
            for cand in state.candidates[:2]:
                desc = cand.description
                if "SELECT from " in desc:
                    rest = desc.split("SELECT from ", 1)[1].strip()
                    tables_involved.append(rest.split()[0].rstrip(" ("))

            if len(tables_involved) >= 2:
                t1, t2 = tables_involved[0], tables_involved[1]
                cols1 = (
                    ", ".join(
                        p.split(".", 1)[1]
                        for p in registry.prop_terms.values()
                        if p.startswith(f"{t1}.")
                    )[:200]
                    or "*"
                )
                cols2 = (
                    ", ".join(
                        p.split(".", 1)[1]
                        for p in registry.prop_terms.values()
                        if p.startswith(f"{t2}.")
                    )[:200]
                    or "*"
                )

                op = comp.operation
                if op is CompositionOp.UNION:
                    sql = (
                        f"WITH q1 AS (SELECT {cols1} FROM {t1} LIMIT 100), "
                        f"q2 AS (SELECT {cols2} FROM {t2} LIMIT 100) "
                        f"SELECT * FROM q1 UNION ALL SELECT * FROM q2"
                    )
                elif op is CompositionOp.INTERSECT:
                    sql = (
                        f"WITH q1 AS (SELECT {cols1} FROM {t1} LIMIT 100), "
                        f"q2 AS (SELECT {cols2} FROM {t2} LIMIT 100) "
                        f"SELECT * FROM q1 INTERSECT SELECT * FROM q2"
                    )
                elif op is CompositionOp.JOIN:
                    jk = str(comp.join_key) if comp.join_key else "id"
                    jcol = jk.split(".")[-1]
                    sql = (
                        f"SELECT q1.*, q2.* FROM {t1} q1 "
                        f"JOIN {t2} q2 ON q1.{jcol} = q2.{jcol} LIMIT 100"
                    )
                else:
                    # CHAIN: sequential CTE where q2 reads from q1
                    sql = (
                        f"WITH q1 AS (SELECT {cols1} FROM {t1} LIMIT 100) "
                        f"SELECT q2.* FROM {t2} q2 "
                        f"WHERE EXISTS (SELECT 1 FROM q1 WHERE q1.id = q2.id)"
                    )
            else:
                # Fall through to single-table logic
                uses_composition = False

        if not uses_composition:
            from_clause = f"{table}{join_sql}"
            where_prefix = f" WHERE {where_sql}" if where_sql else ""

            if agg_fn and group_col:
                # Aggregation with GROUP BY: SUM(amount) ... GROUP BY category
                measure = numeric_col or "*"
                if agg_fn == "COUNT":
                    select_expr = "COUNT(*) AS total_count"
                else:
                    select_expr = f"{agg_fn}({measure}) AS {agg_fn.lower()}_{measure}"
                select_cols = group_col
                if date_trunc_expr and group_col != date_trunc_expr.split(" AS ")[1]:
                    select_cols = f"{date_trunc_expr}, {select_expr}"
                    group_by_col = date_trunc_expr.split(" AS ")[1]
                else:
                    select_cols = f"{group_col}, {select_expr}"
                    group_by_col = group_col
                sql = (
                    f"SELECT {select_cols} FROM {from_clause}"
                    f"{where_prefix} "
                    f"GROUP BY {group_by_col} ORDER BY {select_expr.split(' AS ')[1]} DESC"
                )
            elif agg_fn and date_trunc_expr:  # pragma: no cover
                # Aggregation with date-truncated GROUP BY (unreachable: date_trunc
                # always sets group_col to the alias via the detection block above)
                measure = numeric_col or "*"
                if agg_fn == "COUNT":
                    select_expr = "COUNT(*) AS total_count"
                else:
                    select_expr = f"{agg_fn}({measure}) AS {agg_fn.lower()}_{measure}"
                group_alias = date_trunc_expr.split(" AS ")[1]
                sql = (
                    f"SELECT {date_trunc_expr}, {select_expr} FROM {from_clause}"
                    f"{where_prefix} "
                    f"GROUP BY {group_alias} ORDER BY {group_alias}"
                )
            elif agg_fn and not group_col:
                # Simple aggregation without GROUP BY
                measure = numeric_col or "*"
                if agg_fn == "COUNT":
                    sql = f"SELECT COUNT(*) AS total_count FROM {from_clause}{where_prefix}"
                else:
                    sql = (
                        f"SELECT {agg_fn}({measure}) AS {agg_fn.lower()}_{measure}"
                        f" FROM {from_clause}{where_prefix}"
                    )
            elif wants_rank and (numeric_col or order_col):
                # Ranking: top N by some column
                rank_col = order_col or numeric_col
                cols = (
                    ", ".join(validated_cols[:8])
                    if len(validated_cols) > 8
                    else ", ".join(validated_cols)
                )
                order_dir = (
                    "DESC" if any(s in utterance_lower for s in _ORDER_DESC) else "ASC"
                )
                sql = (
                    f"SELECT {cols} FROM {from_clause}"
                    f"{where_prefix} ORDER BY {rank_col} {order_dir} LIMIT {limit_n}"
                )
            elif date_trunc_expr and numeric_col:
                # Time-series: date grain + measure
                group_alias = date_trunc_expr.split(" AS ")[1]
                sql = (
                    f"SELECT {date_trunc_expr}, SUM({numeric_col}) AS total_{numeric_col}"
                    f" FROM {from_clause}{where_prefix}"
                    f" GROUP BY {group_alias} ORDER BY {group_alias}"
                )
            else:
                # Plain select with reasonable limit
                cols = (
                    ", ".join(validated_cols[:10])
                    if len(validated_cols) > 10
                    else ", ".join(validated_cols)
                )
                sql = f"SELECT {cols} FROM {from_clause}{where_prefix} LIMIT 100"

        _dag_log.info("Template fallback SQL: %s", sql)
        plan = ExportPlan(
            query_target=QueryTarget.SQL_DUCKDB,
            query_text=sql,
        )
        context.q_current = plan  # type: ignore[union-attr]
        return {"q_current": sql}

    def _budget_gate_node(state: NLInterfaceState) -> dict[str, Any]:
        from sqldim.core.query.dgm.planner._targets import (
            CostEstimate,
            ExportPlan,
            QueryTarget,
        )

        sql = state.q_current or "SELECT 1"
        cpu_ops = max(100, len(sql) * 10)
        plan = ExportPlan(
            query_target=QueryTarget.SQL_DUCKDB,
            query_text=sql,
            cost_estimate=CostEstimate(cpu_ops=cpu_ops, io_ops=100),
        )
        gate = ExecutionGate()
        decision = gate.gate(plan, context.budget)  # type: ignore[union-attr]
        return {"budget_decision": decision}

    def _execution_node(state: NLInterfaceState) -> dict[str, Any]:
        sql = state.q_current
        con = context.con  # type: ignore[union-attr]
        if not sql or con is None:
            return {"result": None}
        try:
            cursor = con.execute(sql)
            columns = (
                [col[0] for col in cursor.description] if cursor.description else []
            )
            rows = cursor.fetchall()
            return {
                "result": {
                    "columns": columns,
                    "rows": [[str(v) for v in row] for row in rows[:100]],
                    "count": len(rows),
                }
            }
        except Exception:  # noqa: BLE001
            return {"result": None}

    def _clarification_node(state: NLInterfaceState) -> dict[str, Any]:
        if state.candidates:
            return {
                "confirmed": True,
                "user_selection": 0,
                "want_more_options": False,
            }
        return {"confirmed": True, "want_more_options": False}

    return {
        "candidate_generation": _candidate_generation_node,
        "confirmation_loop": _confirmation_loop_node,
        "dag_construction": _dag_construction_node,
        "budget_gate": _budget_gate_node,
        "execution": _execution_node,
        "clarification": _clarification_node,
    }

"""DGM algebra benchmarks (group T): QuestionAlgebra, CSE, CORRELATE.

Group T — three profile families:

* ``algebra-build``   — QuestionAlgebra construction + to_sql() emission
* ``algebra-cse``     — find_shared_predicates() + apply_cse() throughput
* ``algebra-correlate`` — suggest_correlations() throughput

All pure compute benchmarks (no DuckDB scan involved); ``n`` is the number
of algebra operations performed per case.  The metric reported as
``rows_per_sec`` is *operations/sec* (algebra builds per second).
"""
from __future__ import annotations

import time
import traceback as _traceback

from sqldim.application.benchmarks._dataset import (
    BenchmarkDatasetGenerator,
    SCALE_TIERS,
)
from sqldim.application.benchmarks.infra import (
    BenchmarkResult,
    _select_tiers,
)
from sqldim.application.benchmarks.memory_probe import MemoryProbe
from sqldim.core.query.dgm.algebra import ComposeOp, QuestionAlgebra
from sqldim.core.query.dgm.annotations import AnnotationSigma
from sqldim.core.query.dgm.bdd import BDDManager, DGMPredicateBDD
from sqldim.core.query.dgm._cse import find_shared_predicates, apply_cse
from sqldim.core.query.dgm.recommender import DGMRecommender
from sqldim import DGMQuery, ScalarPred, PropRef
from sqldim.core.query.dgm.preds import AND

_T_TIERS: dict[str, int] = {"xs": 500, "s": 5_000, "m": 50_000}


# ═══════════════════════════════════════════════════════════════════════════
# ── Shared fixtures ───────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════

def _make_leaf_query(i: int) -> DGMQuery:
    """Create a unique leaf DGMQuery to avoid cache aliasing in large loops."""
    return (
        DGMQuery()
        .anchor("t_fact", "f")
        .where(ScalarPred(PropRef("f", f"col_{i % 10}"), "=", i))
    )


def _make_shared_pred_query(anchor: str = "t_fact") -> tuple[DGMQuery, DGMQuery]:
    """Return two leaf DGMQueries sharing the *same* predicate object."""
    pred = ScalarPred(PropRef("f", "region"), "=", "US")
    return (
        DGMQuery().anchor(anchor, "f").where(pred),
        DGMQuery().anchor(anchor, "f").where(pred),
    )


# ═══════════════════════════════════════════════════════════════════════════
# ── T1  QuestionAlgebra build + to_sql throughput ────────────────────────
# ═══════════════════════════════════════════════════════════════════════════


def _t1_algebra_build(tier: str, n: int, temp_dir: str) -> BenchmarkResult:
    """Build an algebra with 4 CTEs (2 leaves + UNION + JOIN) and emit to_sql()."""
    cid    = f"T-algebra-build-{tier}"
    result = BenchmarkResult(
        case_id=cid, group="T", profile="algebra-build", tier=tier,
        processor="QuestionAlgebra", sink="none",
        source="synthetic", phase="compute", n_rows=n, n_changed=0,
    )
    try:
        MemoryProbe.check_safe_to_run(label=cid)
        q1 = _make_leaf_query(0)
        q2 = _make_leaf_query(1)
        probe = MemoryProbe(temp_dir=temp_dir, label=cid)
        with probe:
            t0 = time.perf_counter()
            for _ in range(n):
                qa = QuestionAlgebra()
                qa.add("q1", q1).add("q2", q2)
                qa.compose("q1", ComposeOp.UNION, "q2", name="q_union")
                qa.to_sql("q_union")
            result.wall_s = time.perf_counter() - t0
        m = probe.report
        result.peak_rss_gb      = m.peak_rss_gb
        result.min_sys_avail_gb = m.min_sys_avail_gb
        result.rows_per_sec     = n / max(result.wall_s, 0.001)
        result.inserted         = n
    except RuntimeError as exc:
        result.ok = False; result.error = f"SKIPPED: {exc}"
    except Exception as exc:
        result.ok    = False
        result.error = (f"{type(exc).__name__}: {exc}\n"
                        + _traceback.format_exc()[-600:])
    return result


def _t2_algebra_deep_chain(tier: str, n: int, temp_dir: str) -> BenchmarkResult:
    """Build a depth-5 CTE chain and emit to_sql() — tests O(n) SQL emission."""
    cid    = f"T-algebra-deep-chain-{tier}"
    result = BenchmarkResult(
        case_id=cid, group="T", profile="algebra-deep", tier=tier,
        processor="QuestionAlgebra", sink="none",
        source="synthetic", phase="compute", n_rows=n, n_changed=0,
    )
    try:
        MemoryProbe.check_safe_to_run(label=cid)
        # Pre-build the algebra once (registry references, no deep copy).
        qa = QuestionAlgebra()
        for i in range(5):
            qa.add(f"leaf_{i}", _make_leaf_query(i))
        qa.compose("leaf_0", ComposeOp.UNION,     "leaf_1", name="u01")
        qa.compose("u01",    ComposeOp.INTERSECT,  "leaf_2", name="i012")
        qa.compose("i012",   ComposeOp.UNION,      "leaf_3", name="u0123")
        qa.compose("u0123",  ComposeOp.EXCEPT,     "leaf_4", name="final")
        probe = MemoryProbe(temp_dir=temp_dir, label=cid)
        with probe:
            t0 = time.perf_counter()
            for _ in range(n):
                qa.to_sql("final")
            result.wall_s = time.perf_counter() - t0
        m = probe.report
        result.peak_rss_gb      = m.peak_rss_gb
        result.min_sys_avail_gb = m.min_sys_avail_gb
        result.rows_per_sec     = n / max(result.wall_s, 0.001)
        result.inserted         = n
    except RuntimeError as exc:
        result.ok = False; result.error = f"SKIPPED: {exc}"
    except Exception as exc:
        result.ok    = False
        result.error = (f"{type(exc).__name__}: {exc}\n"
                        + _traceback.format_exc()[-600:])
    return result


# ═══════════════════════════════════════════════════════════════════════════
# ── T3  find_shared_predicates + apply_cse throughput ────────────────────
# ═══════════════════════════════════════════════════════════════════════════


def _t3_cse_detection(tier: str, n: int, temp_dir: str) -> BenchmarkResult:
    """Throughput of find_shared_predicates() on a 4-CTE algebra with one shared
    predicate group (2 sharable CTEs + 2 unique CTEs)."""
    cid    = f"T-cse-detection-{tier}"
    result = BenchmarkResult(
        case_id=cid, group="T", profile="algebra-cse", tier=tier,
        processor="find_shared_predicates", sink="none",
        source="synthetic", phase="compute", n_rows=n, n_changed=0,
    )
    try:
        MemoryProbe.check_safe_to_run(label=cid)
        q_a, q_b = _make_shared_pred_query()
        q_c = _make_leaf_query(10)
        q_d = _make_leaf_query(20)
        qa  = QuestionAlgebra()
        qa.add("a", q_a).add("b", q_b).add("c", q_c).add("d", q_d)
        bdd = DGMPredicateBDD(BDDManager())
        probe = MemoryProbe(temp_dir=temp_dir, label=cid)
        with probe:
            t0 = time.perf_counter()
            for _ in range(n):
                find_shared_predicates(qa, bdd)
            result.wall_s = time.perf_counter() - t0
        m = probe.report
        result.peak_rss_gb      = m.peak_rss_gb
        result.min_sys_avail_gb = m.min_sys_avail_gb
        result.rows_per_sec     = n / max(result.wall_s, 0.001)
        result.inserted         = n
    except RuntimeError as exc:
        result.ok = False; result.error = f"SKIPPED: {exc}"
    except Exception as exc:
        result.ok    = False
        result.error = (f"{type(exc).__name__}: {exc}\n"
                        + _traceback.format_exc()[-600:])
    return result


def _t4_cse_apply(tier: str, n: int, temp_dir: str) -> BenchmarkResult:
    """Throughput of apply_cse() — full non-mutating CSE injection pass."""
    cid    = f"T-cse-apply-{tier}"
    result = BenchmarkResult(
        case_id=cid, group="T", profile="algebra-cse", tier=tier,
        processor="apply_cse", sink="none",
        source="synthetic", phase="compute", n_rows=n, n_changed=0,
    )
    try:
        MemoryProbe.check_safe_to_run(label=cid)
        q_a, q_b = _make_shared_pred_query()
        q_c, q_d = _make_shared_pred_query("t_events")  # second shared group
        q_e = _make_leaf_query(99)
        qa  = QuestionAlgebra()
        qa.add("a", q_a).add("b", q_b).add("c", q_c).add("d", q_d).add("e", q_e)
        bdd = DGMPredicateBDD(BDDManager())
        probe = MemoryProbe(temp_dir=temp_dir, label=cid)
        with probe:
            t0 = time.perf_counter()
            for _ in range(n):
                apply_cse(qa, bdd)
            result.wall_s = time.perf_counter() - t0
        m = probe.report
        result.peak_rss_gb      = m.peak_rss_gb
        result.min_sys_avail_gb = m.min_sys_avail_gb
        result.rows_per_sec     = n / max(result.wall_s, 0.001)
        result.inserted         = n
    except RuntimeError as exc:
        result.ok = False; result.error = f"SKIPPED: {exc}"
    except Exception as exc:
        result.ok    = False
        result.error = (f"{type(exc).__name__}: {exc}\n"
                        + _traceback.format_exc()[-600:])
    return result


# ═══════════════════════════════════════════════════════════════════════════
# ── T5  suggest_correlations throughput ──────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════


def _t5_correlate_suggest(tier: str, n: int, temp_dir: str) -> BenchmarkResult:
    """Throughput of suggest_correlations() on a 6-CTE algebra where 4 CTEs
    share one anchor (producing 6 pair suggestions) and 2 CTEs are distinct."""
    cid    = f"T-correlate-suggest-{tier}"
    result = BenchmarkResult(
        case_id=cid, group="T", profile="algebra-correlate", tier=tier,
        processor="DGMRecommender.suggest_correlations", sink="none",
        source="synthetic", phase="compute", n_rows=n, n_changed=0,
    )
    try:
        MemoryProbe.check_safe_to_run(label=cid)
        qa = QuestionAlgebra()
        for i in range(4):
            qa.add(f"fact_{i}", DGMQuery().anchor("t_fact", "f").where(
                ScalarPred(PropRef("f", f"seg"), "=", i)
            ))
        qa.add("events_a", DGMQuery().anchor("t_events", "e"))
        qa.add("events_b", DGMQuery().anchor("t_events", "e").where(
            ScalarPred(PropRef("e", "type"), "=", "click")
        ))
        rec = DGMRecommender(AnnotationSigma([]))
        probe = MemoryProbe(temp_dir=temp_dir, label=cid)
        with probe:
            t0 = time.perf_counter()
            for _ in range(n):
                rec.suggest_correlations(qa)
            result.wall_s = time.perf_counter() - t0
        m = probe.report
        result.peak_rss_gb      = m.peak_rss_gb
        result.min_sys_avail_gb = m.min_sys_avail_gb
        # Total suggestion pairs = C(4,2) + C(2,2) = 6+1=7 per call
        result.rows_per_sec     = n * 7 / max(result.wall_s, 0.001)
        result.inserted         = n * 7
    except RuntimeError as exc:
        result.ok = False; result.error = f"SKIPPED: {exc}"
    except Exception as exc:
        result.ok    = False
        result.error = (f"{type(exc).__name__}: {exc}\n"
                        + _traceback.format_exc()[-600:])
    return result


# ═══════════════════════════════════════════════════════════════════════════
# ── Group T entry point ───────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════


def group_t_question_algebra(
    gen: BenchmarkDatasetGenerator,
    temp_dir: str,
    max_tier: str = "m",
    **_,
) -> list[BenchmarkResult]:
    """Group T — QuestionAlgebra construction/emission, CSE, CORRELATE.

    Five profiles:
    * T-algebra-build     — QuestionAlgebra build + to_sql (4-CTE)
    * T-algebra-deep      — to_sql on a depth-5 chain
    * T-cse-detection     — find_shared_predicates (4 CTEs, 1 shared group)
    * T-cse-apply         — apply_cse (5 CTEs, 2 shared groups)
    * T-correlate-suggest — suggest_correlations (6 CTEs, 2 anchor groups)
    """
    tier_order   = ["xs", "s", "m"]
    active_tiers = _select_tiers(tier_order, _T_TIERS, max_tier)
    results: list[BenchmarkResult] = []
    for tier in active_tiers:
        n = _T_TIERS[tier]
        results.append(_t1_algebra_build(tier, n, temp_dir))
        results.append(_t2_algebra_deep_chain(tier, n, temp_dir))
        results.append(_t3_cse_detection(tier, n, temp_dir))
        results.append(_t4_cse_apply(tier, n, temp_dir))
        results.append(_t5_correlate_suggest(tier, n, temp_dir))
    return results

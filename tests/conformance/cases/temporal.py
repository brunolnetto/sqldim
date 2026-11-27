"""TemporalOrdering (§3.2, §8.8) and Monitor verdicts (§4.1)."""

from __future__ import annotations

from tests.conformance.cases import DERIVED, Case
from tests.conformance.oracle import REJECTED
from tests.conformance.oracle.allen import ALLEN

ALLEN_CLAUSE = "§3.2 TemporalOrdering semantics (Allen, half-open, NULL bounds)"
DOMAINS = {
    "proper": "bounded intervals with start < end",
    "open": "intervals with a NULL (unbounded) start or end",
    "degenerate": "an interval with start ≥ end satisfies no relation",
}

ALLEN_CASES = [
    Case(
        f"TO-{rel}-{dom}",
        "allen",
        ALLEN_CLAUSE,
        f"{rel} over {DOMAINS[dom]}",
        "interval_grid",
        {"relation": rel, "domain": dom},
        DERIVED,
    )
    for rel in ALLEN
    for dom in DOMAINS
]

POINT_CASES = [
    Case(
        f"TO-{rel}-{tag}",
        "point",
        "§3.2 point relations (NULL TimePoint is UNKNOWN)",
        f"{rel}{'' if tol is None else f' tolerance {tol}'} over points with NULLs",
        "point_grid",
        {"relation": rel, "tolerance": tol},
        DERIVED,
    )
    for rel, tag, tol in (
        ("BEFORE", "pt", None),
        ("AFTER", "pt", None),
        ("CONCURRENT", "pt", None),
        ("CONCURRENT", "tol1", 1),
    )
]

TYPING = "§3.2 Typing of TemporalOrdering (TO1–TO4)"

TYPING_CASES = [
    Case("TO-TYPE-01", "ordering_typing", TYPING,
         "TO2: MEETS over two TimePoints is rejected", "none",
         {"relation": "MEETS", "args": ("point", "point")}, REJECTED),
    Case("TO-TYPE-02", "ordering_typing", TYPING,
         "TO1: BEFORE with an Interval where a TimePoint is required", "none",
         {"relation": "BEFORE", "args": ("interval", "point")}, REJECTED),
    Case("TO-TYPE-03", "ordering_typing", TYPING,
         "TO3: a negative tolerance is rejected", "none",
         {"relation": "CONCURRENT", "args": ("point", "point"), "tolerance": -1},
         REJECTED),
    Case("TO-TYPE-04", "ordering_typing", TYPING,
         "TO4: Date and Timestamp are never mixed", "none",
         {"relation": "BEFORE", "args": ("point", "point"), "types": ("Timestamp", "Date")},
         REJECTED),
]  # fmt: skip

THEOREM_CASES = [
    Case(
        "TO-THM-01",
        "allen_theorem",
        "§8.8 Allen exhaustiveness theorem",
        "exactly one of the 13 relations holds for every pair of the domain",
        "interval_grid",
        {"domains": ("proper", "open")},
        True,
    ),
    Case(
        "TO-THM-02",
        "allen_theorem",
        "§8.8 Outside the domain",
        "a degenerate interval satisfies none of the 13 relations",
        "interval_grid",
        {"domains": ("degenerate",), "none": True},
        True,
    ),
]

MON = "§4.1 Recorded traces and monitoring verdicts"
IN_FLIGHT = [{"from_state": "Missing", "kind": "scheduled", "to_state": "In-flight"}]
DONE_TRACE = IN_FLIGHT + [
    {"from_state": "In-flight", "kind": "succeeded", "to_state": "Complete"}
]
FAILED_TRACE = IN_FLIGHT + [
    {"from_state": "In-flight", "kind": "failed", "to_state": "Failed"}
]
DONE = ("prop", "to_state", "=", "Complete")
BAD = ("prop", "to_state", "=", "Failed")
NOW = "2024-06-30T00:00:00"


def _mon(cid, rule, exp, prop=("LIVENESS", DONE), traces=(IN_FLIGHT,), **inp):
    return Case(
        cid,
        "monitor",
        MON,
        rule,
        "artifact_w",
        {"property": prop, "traces": list(traces), "now": NOW, **inp},
        exp,
    )


MONITOR_CASES = [
    _mon("MON-01", "LIVENESS, log open, no deadline", "UNDETERMINED"),
    _mon("MON-02", "deadline window_end 2024-06-29 + 2 days not expired", "UNDETERMINED",
         deadline=("2024-06-29T00:00:00", 2)),
    _mon("MON-03", "deadline window_end 2024-06-20 + 2 days expired", "VIOLATED",
         deadline=("2024-06-20T00:00:00", 2)),
    _mon("MON-04", "log CLOSED: the record is complete", "VIOLATED", log="CLOSED"),
    _mon("MON-05", "after a 'succeeded' transition, log open", "SATISFIED",
         traces=(DONE_TRACE,)),
    _mon("MON-06", "after a 'succeeded' transition, log closed", "SATISFIED",
         traces=(DONE_TRACE,), log="CLOSED"),
    _mon("MON-07", "SAFETY open: bad recorded", DERIVED, prop=("SAFETY", BAD),
         traces=(FAILED_TRACE,)),
    _mon("MON-08", "SAFETY open: bad not yet recorded", DERIVED, prop=("SAFETY", BAD)),
    _mon("MON-09", "RESPONSE is always UNDETERMINED while open", DERIVED,
         prop=("RESPONSE", ("prop", "kind", "=", "scheduled"), DONE),
         traces=(DONE_TRACE,)),
    _mon("MON-10", "no trace, log open", DERIVED, traces=()),
    _mon("MON-11", "no trace, log closed, REQUIRE_WITNESS", DERIVED, traces=(),
         log="CLOSED"),
    _mon("MON-12", "no trace, log closed, VACUOUS", DERIVED, traces=(), log="CLOSED",
         on_empty="VACUOUS"),
    _mon("MON-13", "final holds at the terminal: the trace is closed", DERIVED,
         traces=(FAILED_TRACE,), final=BAD),
    _mon("MON-14", "one trace SATISFIED, one UNDETERMINED", DERIVED,
         traces=(DONE_TRACE, IN_FLIGHT)),
    _mon("MON-15", "PERSISTENCE closed: Complete holds from some point on", DERIVED,
         prop=("PERSISTENCE", DONE), traces=(DONE_TRACE,), log="CLOSED"),
]  # fmt: skip

CASES = ALLEN_CASES + POINT_CASES + TYPING_CASES + THEOREM_CASES + MONITOR_CASES

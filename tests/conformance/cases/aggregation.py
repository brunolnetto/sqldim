"""Additivity (§2.4), LATEST/EARLIEST (§4.2) and drill-across (§5.2.2)."""

from __future__ import annotations

from tests.conformance.cases import DERIVED, Case
from tests.conformance.cases.fixtures import ACCOUNT_BALANCE
from tests.conformance.oracle import REJECTED

VALID, WARN = "VALID", "VALID_WITH_WARNING"
AD = "§2.4 Additivity AD1–AD4 and grain defaults"


def _ad(cid, rule, exp, **inp):
    return Case(cid, "additivity", AD, rule, "account_balance", inp, exp)


ADDITIVITY = [
    _ad("ADD-01", "Grain(PERIOD): SUM by branch sums a balance over many days",
        REJECTED, grain="PERIOD", fn="SUM", group_by_axes=["Branch"]),
    _ad("ADD-02", "Grain(PERIOD): SUM by branch and day is valid", VALID,
        grain="PERIOD", fn="SUM", group_by_axes=["Branch", "TIME"]),
    _ad("ADD-03", "Grain(PERIOD): TemporalAgg(SUM, …, LATEST) is valid", VALID,
        grain="PERIOD", fn="SUM", group_by_axes=["Branch"], window="LATEST"),
    _ad("ADD-04", "Grain(PERIOD): AVG is valid (AD4)", VALID, grain="PERIOD", fn="AVG",
        group_by_axes=["Branch"]),
    _ad("ADD-05", "Grain(EVENT) defaults to ADDITIVE: SUM valid (AD1)", DERIVED,
        grain="EVENT", fn="SUM"),
    _ad("ADD-06", "NON_ADDITIVE: SUM rejected (AD3)", DERIVED, grain="EVENT",
        kind="NON_ADDITIVE", fn="SUM"),
    _ad("ADD-07", "NON_ADDITIVE: AVG accepted with a warning (AD3)", DERIVED,
        grain="EVENT", kind="NON_ADDITIVE", fn="AVG"),
    _ad("ADD-08", "NON_ADDITIVE: MIN valid (AD4)", DERIVED, grain="EVENT",
        kind="NON_ADDITIVE", fn="MIN"),
    _ad("ADD-09", "SEMI_ADDITIVE({Account}): SUM grouped by day only", DERIVED,
        grain="EVENT", kind=("SEMI_ADDITIVE", ["Account"]), fn="SUM",
        group_by_axes=["TIME"]),
    _ad("ADD-10", "SEMI_ADDITIVE({Account}): a Where pins one account", DERIVED,
        grain="EVENT", kind=("SEMI_ADDITIVE", ["Account"]), fn="SUM",
        where_pins=["Account"]),
    _ad("ADD-11", "Grain(PERIOD): a Where pins one period", DERIVED, grain="PERIOD",
        fn="SUM", where_pins=["TIME"]),
    _ad("ADD-12", "Grain(PERIOD): TemporalAgg(SUM, …, EARLIEST) is valid", DERIVED,
        grain="PERIOD", fn="SUM", window="EARLIEST"),
    _ad("ADD-13", "Grain(ACCUMULATING) defaults to ADDITIVE", DERIVED,
        grain="ACCUMULATING", fn="SUM"),
    _ad("ADD-14", "SEMI_ADDITIVE({Account}): AVG is valid (AD4)", DERIVED,
        grain="EVENT", kind=("SEMI_ADDITIVE", ["Account"]), fn="AVG"),
    _ad("ADD-15", "SEMI_ADDITIVE({TIME}): MAX is valid (AD4)", DERIVED,
        grain="EVENT", kind=("SEMI_ADDITIVE", ["TIME"]), fn="MAX"),
]  # fmt: skip

TAGG = "§4.2 TemporalWindow LATEST / EARLIEST"

TEMPORAL_AGG = [
    Case(f"TAGG-{i:02d}", "temporal_agg", TAGG,
         f"TemporalAgg(SUM, b.balance, b.day, {w}) by branch", "account_balance",
         {"rows": ACCOUNT_BALANCE, "group": "branch", "ts": "day", "arg": "balance",
          "window": w}, DERIVED)
    for i, w in ((1, "LATEST"), (2, "EARLIEST"))
]  # fmt: skip

DRILL = "§5.2.2 Drill-across (steps 1–3, DA4)"

DRILL_ACROSS = [
    Case(
        "DA-01",
        "drill_across",
        DRILL,
        "per-branch aggregation aligned by FULL OUTER JOIN; COUNT of an absent "
        "branch is 0, other aggregates NULL; division by zero is NULL",
        "constellation",
        {
            "aggregates": [
                ("n_sales", "COUNT", "sale", None),
                ("revenue", "SUM", "sale", "revenue"),
                ("n_returns", "COUNT", "return_ev", None),
                ("rate", "DIV", "n_returns", "n_sales"),
            ]
        },
        [
            ("C1", 2, 300, 3, 1.5),
            ("C2", 2, 120, 0, 0.0),
            ("C3", 0, None, 1, None),
        ],
    ),
    Case(
        "DA-02",
        "drill_across",
        DRILL,
        "each measure equals the one computed on its own fact alone",
        "constellation",
        {
            "aggregates": [
                ("revenue", "SUM", "sale", "revenue"),
                ("returned", "SUM", "return_ev", "amount"),
            ]
        },
        DERIVED,
    ),
]

WF = "§5.2.2 Well-formedness of a constellation (DA1–DA3)"


def _wf(cid, rule, exp, **inp):
    base = {"conformed_for_all": ["Customer"]}
    base.update(inp)
    return Case(cid, "constellation_check", WF, rule, "constellation", base, exp)


WELL_FORMED = [
    _wf("DA-WF-01", "DA1: an aggregate over PropRefs of two branches", REJECTED,
        aggregate_branches=[["sale", "return_ev"]]),
    _wf("DA-WF-02", "DA2: a GroupBy key on a dimension not conformed for every branch",
        REJECTED, group_by_dims=["Store"]),
    _wf("DA-WF-03", "DA3: a Where comparing PropRefs of two branches", REJECTED,
        where_branches=[["sale", "return_ev"]]),
    _wf("DA-WF-04", "one branch per aggregate, conformed keys, per-branch Where", VALID,
        aggregate_branches=[["sale"], ["return_ev"]], group_by_dims=["Customer"],
        where_branches=[["sale"]]),
]  # fmt: skip

CASES = ADDITIVITY + TEMPORAL_AGG + DRILL_ACROSS + WELL_FORMED

"""SCD resolution, version and validity at the reference instant (§2.5, §2.6, ADR 0003)."""

from __future__ import annotations

from tests.conformance.cases import DERIVED, Case
from tests.conformance.cases.fixtures import (
    BRIDGE_REPORTS_TO,
    BRIDGE_REPORTS_TO_OPEN,
    DIM_CUSTOMER_INTRADAY,
    DIM_CUSTOMER_REGION,
    DIM_CUSTOMER_SEGMENT,
    FACT_SALE_INTRADAY,
    FACT_SALE_REGION,
    FACT_SALE_SEGMENT,
)
from tests.conformance.oracle import REJECTED

RES = "§2.5 SCD resolution: as-was and as-is"
VER = "§2.6 Version and validity at an instant (half-open windows)"
TAU = "ADR 0003 / §5.1 one reference instant τ"

FACT_SALE_REGION_VK = [
    {**f, "dim_ref": vk} for f, vk in zip(FACT_SALE_REGION, (101, 102))
]


def _m(cid, clause, rule, fixture, dims, facts, attr, exp, **inp):
    return Case(
        cid,
        "scd_measure",
        clause,
        rule,
        fixture,
        {"dim_rows": dims, "facts": facts, "attr": attr, "measure": "revenue", **inp},
        exp,
    )


REGION = [
    _m("SCD-01", RES, "AS_WAS (the default): South 600, North 400", "customer_region",
       DIM_CUSTOMER_REGION, FACT_SALE_REGION, "region",
       [("North", 400), ("South", 600)], tau="2024-06-30"),
    _m("SCD-02", RES, "AS_IS at τ: North 1 000", "customer_region",
       DIM_CUSTOMER_REGION, FACT_SALE_REGION, "region", [("North", 1000)],
       tau="2024-06-30", mode="AS_IS"),
    _m("SCD-03", RES, "VERSION_KEY under AS_WAS reads the stored version",
       "customer_region", DIM_CUSTOMER_REGION, FACT_SALE_REGION_VK, "region",
       [("North", 400), ("South", 600)], tau="2024-06-30", reference="VERSION_KEY"),
    _m("SCD-04", RES, "VERSION_KEY under AS_IS maps to the entity and reads τ",
       "customer_region", DIM_CUSTOMER_REGION, FACT_SALE_REGION_VK, "region",
       [("North", 1000)], tau="2024-06-30", mode="AS_IS", reference="VERSION_KEY"),
    _m("SCD-05", RES, "AS_WAS does not move when τ moves", "customer_region",
       DIM_CUSTOMER_REGION, FACT_SALE_REGION, "region",
       [("North", 400), ("South", 600)], tau="2025-01-01"),
]  # fmt: skip

SEGMENT = [
    _m("SCD-V-01", VER, "no version at τ, LAX: the tuple is dropped", "customer_segment",
       DIM_CUSTOMER_SEGMENT, FACT_SALE_SEGMENT, "segment", [], tau="2022-12-31",
       mode="AS_IS", node_resolution="LAX"),
    _m("SCD-V-02", VER, "no version at τ, STRICT: rejected", "customer_segment",
       DIM_CUSTOMER_SEGMENT, FACT_SALE_SEGMENT, "segment", REJECTED, tau="2022-12-31",
       mode="AS_IS", node_resolution="STRICT"),
    _m("SCD-V-03", VER, "τ inside version 101", "customer_segment",
       DIM_CUSTOMER_SEGMENT, FACT_SALE_SEGMENT, "segment", [("retail", 600)],
       tau="2023-12-31", mode="AS_IS"),
    _m("SCD-V-04", VER, "the boundary instant belongs to the new version",
       "customer_segment", DIM_CUSTOMER_SEGMENT, FACT_SALE_SEGMENT, "segment",
       [("wholesale", 600)], tau="2024-03-01", mode="AS_IS"),
    _m("SCD-V-05", VER, "τ inside version 102", "customer_segment",
       DIM_CUSTOMER_SEGMENT, FACT_SALE_SEGMENT, "segment", [("wholesale", 600)],
       tau="2024-06-30", mode="AS_IS"),
    _m("SCD-V-06", VER, "τ before an intraday boundary reads the old version",
       "customer_intraday", DIM_CUSTOMER_INTRADAY, FACT_SALE_INTRADAY, "segment",
       DERIVED, tau="2024-03-01 12:00:00", mode="AS_IS"),
    _m("SCD-V-07", VER, "τ at an intraday boundary reads the new version",
       "customer_intraday", DIM_CUSTOMER_INTRADAY, FACT_SALE_INTRADAY, "segment",
       DERIVED, tau="2024-03-01 18:00:00", mode="AS_IS"),
]  # fmt: skip


def _e(cid, clause, rule, edges, target, tau, exp, **inp):
    return Case(
        cid,
        "edge_validity",
        clause,
        rule,
        "reports_to",
        {"edges": edges, "target": target, "tau": tau, **inp},
        exp,
    )


EDGES = [
    _e("SCD-E-01", VER, "direct reports of M1 at 2023-06-30", BRIDGE_REPORTS_TO, "M1",
       "2023-06-30", ["E7"]),
    _e("SCD-E-02", VER, "direct reports of M1 at 2024-06-30", BRIDGE_REPORTS_TO, "M1",
       "2024-06-30", []),
    _e("SCD-E-03", VER, "direct reports of M2 at 2024-06-30", BRIDGE_REPORTS_TO, "M2",
       "2024-06-30", ["E7"]),
    _e("SCD-E-04", VER, "valid_to is exclusive", BRIDGE_REPORTS_TO, "M1",
       "2024-01-01", DERIVED),
    _e("SCD-E-05", VER, "valid_from is inclusive", BRIDGE_REPORTS_TO, "M2",
       "2024-01-01", DERIVED),
    _e("SCD-E-06", VER, "a NULL valid_from is open", BRIDGE_REPORTS_TO_OPEN, "M1",
       "2023-06-30", DERIVED),
    _e("SCD-TAU-01", TAU, "τ = TemporalContext.default_as_of resolves validity",
       BRIDGE_REPORTS_TO, "M1", "2024-06-30", DERIVED, tau_source="context"),
]  # fmt: skip

CASES = REGION + SEGMENT + EDGES

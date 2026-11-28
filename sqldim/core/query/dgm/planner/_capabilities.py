"""Declared capabilities of each query target and write sink (§6.2 Rule 8, §6.4).

Each :class:`QueryTarget` and :class:`SinkTarget` has a :class:`Profile`
stating, per :class:`Capability`, whether sqldim supports it, supports it
only in part, or does not support it, with the reason.  The declaration is
what sqldim implements, not what the specification allows: §6.4 lists what
each engine *could* express; a target with no emitter here is unsupported
whatever the engine offers.

The planner reads the profiles: a query target without an emitter, or an
unsupported capability, raises :class:`UnsupportedCapability`.  Nothing is
approximated or routed to another target.  The executable evidence for
every declaration is the per-backend matrix (``tests/conformance``,
``docs/reference/dgm-backend-matrix.md``, issue #138).
"""

from __future__ import annotations

import re
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import TYPE_CHECKING

from sqldim.core.query.dgm.planner._ir import (
    BandQuery,
    Compose,
    DrillAcross,
    UnsupportedConstruct,
)
from sqldim.core.query.dgm.planner._targets import QueryTarget, SinkTarget

if TYPE_CHECKING:
    from sqldim.core.query.dgm.planner._ir import PlanIR, PlanNode

__all__ = [
    "Capability",
    "Support",
    "Declared",
    "Profile",
    "UnsupportedCapability",
    "QUERY_PROFILES",
    "SINK_PROFILES",
    "DGMQUERY_TO_SQL",
    "POSTGRESQL_VIA_DUCKDB",
    "query_profile",
    "require_ir",
    "sink_profile",
    "window_capability",
]


class UnsupportedCapability(UnsupportedConstruct):
    """The target or sink does not support the capability (fail-closed)."""


class Capability(Enum):
    """What a backend is asked to do."""

    # query targets
    JOIN = "join"
    NULL = "null"
    TEMPORAL = "temporal"
    AGGREGATION = "aggregation"
    CTE = "cte"
    WINDOW = "window"
    WINDOW_AGGREGATE = "window_aggregate"
    PATHPRED = "pathpred"
    # sinks
    APPEND = "append"
    SCD2_CLOSE = "scd2_close"
    MERGE = "merge"


QUERY_CAPABILITIES = (
    Capability.JOIN,
    Capability.NULL,
    Capability.TEMPORAL,
    Capability.AGGREGATION,
    Capability.CTE,
    Capability.WINDOW,
    Capability.WINDOW_AGGREGATE,
    Capability.PATHPRED,
)
SINK_CAPABILITIES = (Capability.APPEND, Capability.SCD2_CLOSE, Capability.MERGE)


class Support(Enum):
    SUPPORTED = "supported"
    PARTIAL = "partial"
    UNSUPPORTED = "unsupported"


@dataclass(frozen=True)
class Declared:
    support: Support
    note: str = ""


@dataclass(frozen=True)
class Profile:
    """The capabilities of one target or sink.

    ``emitter`` names the code that produces its statements, or is ``None``
    when sqldim has none; ``min_version`` is the engine version the
    declaration was verified against.
    """

    name: str
    emitter: str | None
    min_version: str
    capabilities: Mapping[Capability, Declared] = field(default_factory=dict)

    def declared(self, capability: Capability) -> Declared:
        return self.capabilities.get(
            capability, Declared(Support.UNSUPPORTED, "not declared")
        )

    def require_emitter(self) -> str:
        if self.emitter is None:
            raise UnsupportedCapability(
                f"{self.name}: sqldim has no emitter for this target; it is "
                "never routed to another target"
            )
        return self.emitter

    def require(self, capability: Capability) -> Declared:
        """The declaration, or :class:`UnsupportedCapability` when unsupported."""
        d = self.declared(capability)
        if d.support is Support.UNSUPPORTED:
            raise UnsupportedCapability(
                f"{self.name} does not support {capability.value}: {d.note}"
            )
        return d


S, P, U = Support.SUPPORTED, Support.PARTIAL, Support.UNSUPPORTED

_DUCKDB_SQL: dict[Capability, Declared] = {
    Capability.JOIN: Declared(
        P,
        "PathJoin over verb and bridge hops; a constellation is evaluated by "
        "drill-across once its fact branches are declared (drill_across, "
        "§5.2.2): DGMQuery reads no model and cannot find them itself",
    ),
    Capability.NULL: Declared(S, "Kleene logic; NULL is not a literal"),
    Capability.TEMPORAL: Declared(
        P,
        "as_of with SCD resolution and half-open validity; TemporalContext "
        "raises UnsupportedConstruct",
    ),
    Capability.AGGREGATION: Declared(
        S, "over = ENTITIES / PATHS; drill-across with DA1–DA4 (§5.2.2)"
    ),
    Capability.CTE: Declared(
        P,
        "Repeat over a one-hop body: WALK with 1 ≤ min ≤ max ≤ 8 unrolled; "
        "SIMPLE, TRAIL, min = 0 and other bounds as WITH RECURSIVE over the "
        "node identities of a declared model (DGMPlanner(model=…)), with "
        "MAX_REPEAT_DEPTH reported; a multi-hop body and depth_alias are refused",
    ),
    Capability.WINDOW: Declared(
        S, "ranking window functions (ROW_NUMBER, RANK, DENSE_RANK) with QUALIFY"
    ),
    Capability.WINDOW_AGGREGATE: Declared(
        S, "aggregate and other window functions (SUM, AVG, LAG, …) with QUALIFY"
    ),
    Capability.PATHPRED: Declared(
        P,
        "EXISTS / FORALL; strategy ALL, and SHORTEST, K_SHORTEST and MIN_WEIGHT "
        "(unweighted hops) over the entity keys of a declared model; "
        "EVENTUALLY, GLOBALLY and UNTIL over one evaluation position per "
        "instance, or over the iterations of one Repeat with a declared model; "
        "NEXT over the first hop; several or no positions on a Repeat-free "
        "path, past-time modes, a mode with a strategy and on_empty are refused",
    ),
}


def _unsupported(reason: str) -> dict[Capability, Declared]:
    return {c: Declared(U, reason) for c in QUERY_CAPABILITIES}


_NO_EMITTER = "no emitter: DGMPlanner.plan_query raises UnsupportedCapability"

QUERY_PROFILES: Mapping[QueryTarget, Profile] = MappingProxyType(
    {
        QueryTarget.SQL_DUCKDB: Profile(
            "SQL_DUCKDB", "plan IR → DuckDB SQL (#148)", "DuckDB 1.5.0", _DUCKDB_SQL
        ),
        QueryTarget.SQL_MOTHERDUCK: Profile(
            "SQL_MOTHERDUCK",
            "plan IR → DuckDB SQL (#148), the SQL_DUCKDB emitter",
            "DuckDB 1.5.0",
            _DUCKDB_SQL,
        ),
        QueryTarget.SQL_POSTGRESQL: Profile(
            "SQL_POSTGRESQL",
            None,
            "—",
            _unsupported(
                _NO_EMITTER + "; PostgreSQL data is queried with SQL_DUCKDB over "
                "a database attached with DuckDB's postgres extension (PG(DD), §6.4)"
            ),
        ),
        QueryTarget.CYPHER: Profile(
            "CYPHER", None, "—", _unsupported(_NO_EMITTER + " (§6.4 cells unbuilt)")
        ),
        QueryTarget.SPARQL: Profile(
            "SPARQL", None, "—", _unsupported(_NO_EMITTER + " (§6.4 cells unbuilt)")
        ),
        QueryTarget.DGM_JSON: Profile(
            "DGM_JSON",
            None,
            "—",
            _unsupported(_NO_EMITTER + "; DGMJSONExporter serialises a plan"),
        ),
        QueryTarget.DGM_YAML: Profile(
            "DGM_YAML",
            None,
            "—",
            _unsupported(_NO_EMITTER + "; DGMYAMLExporter serialises a plan"),
        ),
    }
)

#: ``DGMQuery.to_sql()``, the SQL of ``DGMQuery.execute()``: not a planner
#: target, but an execution path with its own refusals.
DGMQUERY_TO_SQL = Profile(
    "DGMQuery.to_sql()",
    "DGMQuery.to_sql() → DuckDB SQL",
    "DuckDB 1.5.0",
    {
        **_DUCKDB_SQL,
        Capability.CTE: Declared(
            U, "a Repeat raises UnsupportedConstruct; plan it with DGMPlanner"
        ),
        Capability.PATHPRED: Declared(
            P,
            "EXISTS / FORALL, strategy ALL; EVENTUALLY, GLOBALLY and UNTIL over "
            "one evaluation position per instance; NEXT over the first hop; "
            "it reads no model, so a mode over a Repeat's iterations and other "
            "strategies are refused, as are past-time modes and on_empty",
        ),
    },
)

#: PG(DD) of §6.4: the SQL_DUCKDB emitter's SQL run by DuckDB over tables of
#: a PostgreSQL database attached with the postgres extension.  The planner
#: does not see where a table is stored, so it cannot gate this profile.
POSTGRESQL_VIA_DUCKDB = Profile(
    "SQL_DUCKDB over PostgreSQL (PG(DD))",
    "plan IR → DuckDB SQL (#148), executed by DuckDB's postgres extension",
    "DuckDB 1.5.0 postgres extension",
    {
        **_DUCKDB_SQL,
        Capability.WINDOW: Declared(
            P, "ROW_NUMBER with QUALIFY is verified; RANK and DENSE_RANK are not"
        ),
        Capability.WINDOW_AGGREGATE: Declared(
            U,
            "a SUM window over a PostgreSQL scan raises NotImplementedException "
            "(PostgresScanSerialize) in DuckDB 1.5.0's postgres extension; "
            "DGMPlanner(execution_profile=POSTGRESQL_VIA_DUCKDB) refuses it "
            "before execution",
        ),
    },
)

#: Ranking functions: Capability.WINDOW.  Any other window function —
#: an aggregate, a value function or one not recognised — is
#: Capability.WINDOW_AGGREGATE (fail-closed).
_RANKING = frozenset({"ROW_NUMBER", "RANK", "DENSE_RANK"})
_FN = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*\(")


def window_capability(sql: str) -> Capability:
    """The capability a window expression needs, by its function name."""
    m = _FN.match(sql)
    if m and m.group(1).upper() in _RANKING:
        return Capability.WINDOW
    return Capability.WINDOW_AGGREGATE


def _leaves(node: "PlanNode") -> Iterator[BandQuery]:
    if isinstance(node, BandQuery):
        yield node
    elif isinstance(node, DrillAcross):
        yield from node.branches
    elif isinstance(node, Compose):
        yield from _leaves(node.left)
        yield from _leaves(node.right)


def require_ir(ir: "PlanIR", profile: Profile) -> None:
    """Refuse *ir* when *profile* does not support one of its window functions.

    Raised while planning, so the engine never receives a query it would
    fail on (:class:`UnsupportedCapability`).
    """
    nodes = [ir.root, *(c.node for c in ir.ctes)]
    for leaf in (leaf for n in nodes for leaf in _leaves(n)):
        for name, frag in leaf.windows:
            cap = window_capability(frag.sql)
            if profile.declared(cap).support is Support.UNSUPPORTED:
                raise UnsupportedCapability(
                    f"{profile.name} does not support {cap.value} "
                    f"({name} = {frag.sql}): {profile.declared(cap).note}"
                )


_MERGE_RULE10 = "Rule 10 plans a MERGE WritePlan"

SINK_PROFILES: Mapping[SinkTarget, Profile] = MappingProxyType(
    {
        SinkTarget.DUCKDB: Profile(
            "DUCKDB",
            "DuckDBSink; Rule 8: CREATE TABLE AS / INSERT INTO",
            "DuckDB 1.5.0",
            {
                Capability.APPEND: Declared(S, "INSERT INTO the attached file"),
                Capability.SCD2_CLOSE: Declared(S, "UPDATE in place"),
                Capability.MERGE: Declared(S, _MERGE_RULE10),
            },
        ),
        SinkTarget.MOTHERDUCK: Profile(
            "MOTHERDUCK",
            "MotherDuckSink; Rule 8: CREATE TABLE AS / INSERT INTO",
            "DuckDB 1.5.0",
            {
                Capability.APPEND: Declared(S, "INSERT INTO the attached database"),
                Capability.SCD2_CLOSE: Declared(S, "UPDATE in place"),
                Capability.MERGE: Declared(S, _MERGE_RULE10),
            },
        ),
        SinkTarget.POSTGRESQL: Profile(
            "POSTGRESQL",
            "PostgreSQLSink through DuckDB's postgres extension",
            "DuckDB 1.5.0 postgres extension",
            {
                Capability.APPEND: Declared(S, "INSERT INTO the attached database"),
                Capability.SCD2_CLOSE: Declared(S, "UPDATE … FROM in place"),
                Capability.MERGE: Declared(S, _MERGE_RULE10),
            },
        ),
        SinkTarget.PARQUET: Profile(
            "PARQUET",
            "ParquetSink; Rule 8: COPY TO (FORMAT PARQUET)",
            "DuckDB 1.5.0",
            {
                Capability.APPEND: Declared(
                    S,
                    "COPY TO … APPEND adds new files; partition columns are "
                    "written into the files and read back with their type",
                ),
                Capability.SCD2_CLOSE: Declared(
                    P,
                    "the whole table is rewritten into a staging directory that "
                    "replaces it, keeping every version; local filesystem only "
                    "(a remote URI raises SinkUnsupported), and not atomic for "
                    "a concurrent reader",
                ),
                Capability.MERGE: Declared(P, _MERGE_RULE10),
            },
        ),
        SinkTarget.DELTA: Profile(
            "DELTA",
            "DeltaLakeSink; Rule 8 write plan",
            "DuckDB 1.5.0 delta extension",
            {
                Capability.APPEND: Declared(
                    P,
                    "§6.2: APPEND only, by ATTACH … (TYPE delta) and INSERT INTO "
                    "the attached table; DuckDB 1.5.0 cannot create the table",
                ),
                Capability.SCD2_CLOSE: Declared(
                    U,
                    "§6.2: the delta extension rejects UPDATE; "
                    "DeltaLakeSink.close_versions raises SinkUnsupported",
                ),
                Capability.MERGE: Declared(
                    U, "§6.2 Delta writes: MERGE is rejected (Rule 10)"
                ),
            },
        ),
        SinkTarget.ICEBERG: Profile(
            "ICEBERG",
            "IcebergSink (PyIceberg append); Rule 8 write plan",
            "DuckDB 2.0 (documented, §6.2 Iceberg writes)",
            {
                Capability.APPEND: Declared(
                    P,
                    "§6.2: INSERT INTO catalog.schema.table on the attached REST "
                    "catalog declared with IcebergCatalog; without one Rule 8 "
                    "rejects the plan",
                ),
                Capability.SCD2_CLOSE: Declared(P, "partition overwrite via PyIceberg"),
                Capability.MERGE: Declared(
                    P,
                    "§6.2: Rule 10 plans it only for a merge-on-read table on a "
                    "documented engine version (DuckDB 2.0, 2.0.1)",
                ),
            },
        ),
    }
)


def query_profile(target: QueryTarget) -> Profile:
    if not isinstance(target, QueryTarget):
        raise UnsupportedCapability(f"unknown query target {target!r}")
    return QUERY_PROFILES[target]


def sink_profile(sink: SinkTarget) -> Profile:
    if not isinstance(sink, SinkTarget):
        raise UnsupportedCapability(f"unknown sink {sink!r}")
    return SINK_PROFILES[sink]

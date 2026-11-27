"""Planner Rule 10 — PipelineArtifact state-aware write planning (§6.2, §2.4).

Rule 10 reads the declared ``PipelineArtifact`` and one artifact's
:class:`ArtifactSnapshot` and emits a typed plan; it writes nothing
(ADR 0005):

* Complete with ``now − completed_at > ttl`` → :class:`ExpiryPlan`, the
  ``expired`` transition (``ON_EXPIRY``) and no write;
* Missing or Failed → an :class:`ArtifactPlan` for a backfill, only when the
  window is eligible: ``now − backfill_horizon ≤ window_end ≤ now``;
* Stale → an :class:`ArtifactPlan` for a refresh.

The operation follows ``write_mode`` (BACKFILL → APPEND, REFRESH → MERGE,
ADAPTIVE → APPEND from Missing/Failed, MERGE from Stale).  Every other case is
a typed error: In-flight, Complete within ``ttl`` and a window outside the
horizon are :class:`NotEligible`; no sink, an unknown ``write_mode`` or an
inconsistent window are :class:`InvalidArtifactPlan`; MERGE into DELTA is
:class:`UnsupportedArtifactPlan` (Delta sinks, §6.2).  An ICEBERG plan needs
the attached catalog (:class:`IcebergCatalog`), and a MERGE into ICEBERG also
needs an engine version the §6.2 capability table covers and a merge-on-read
table; otherwise it is :class:`UnsupportedArtifactPlan` (§6.2 Iceberg writes).
Nothing falls back to a default sink, mode, horizon or window.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from sqldim.core.query.dgm.annotations import PipelineArtifact, WriteModeKind
from sqldim.core.query.dgm.planner._artifact_plan import (
    ArtifactPlan,
    ArtifactSnapshot,
    ArtifactState,
    ExpiryPlan,
    InvalidArtifactPlan,
    NotEligible,
    UnsupportedArtifactPlan,
    WriteOperation,
    WritePlan,
    instant,
    plan_artifact_run,
)
from sqldim.core.query.dgm.planner._rule8 import (
    ICEBERG_DOCUMENTED_VERSIONS,
    IcebergCatalog,
)
from sqldim.core.query.dgm.planner._targets import ExportPlan, SinkTarget

__all__ = ["ArtifactTarget", "plan_rule_10", "backfill_eligible", "operation_for"]


@dataclass(frozen=True)
class ArtifactTarget:
    """Where a planned run writes: the sink relation and its window column."""

    relation: str
    window_column: str | None = None


_FIXED: dict[WriteModeKind, WriteOperation] = {
    WriteModeKind.BACKFILL: WriteOperation.APPEND,
    WriteModeKind.REFRESH: WriteOperation.MERGE,
}
_ADAPTIVE: dict[ArtifactState, WriteOperation] = {
    ArtifactState.MISSING: WriteOperation.APPEND,
    ArtifactState.FAILED: WriteOperation.APPEND,
    ArtifactState.STALE: WriteOperation.MERGE,
}
_GAP = (ArtifactState.MISSING, ArtifactState.FAILED)


def _count(value: object, what: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise InvalidArtifactPlan(f"{what} must be an int ≥ 0, got {value!r}")
    return value


def _check(artifact: object, snapshot: object) -> PipelineArtifact:
    if not isinstance(artifact, PipelineArtifact):
        raise InvalidArtifactPlan("Rule 10 needs the declared PipelineArtifact")
    if not isinstance(snapshot, ArtifactSnapshot):
        raise InvalidArtifactPlan("Rule 10 needs an ArtifactSnapshot")
    if snapshot.artifact_type != artifact.fact:
        raise InvalidArtifactPlan(
            f"the snapshot is of {snapshot.artifact_type!r}, the annotation of "
            f"{artifact.fact!r}"
        )
    if not isinstance(snapshot.state, ArtifactState):
        raise InvalidArtifactPlan(f"unknown artifact state {snapshot.state!r}")
    _count(artifact.ttl, "ttl (seconds)")
    _count(artifact.backfill_horizon, "backfill_horizon (days)")
    return artifact


def operation_for(mode: object, state: ArtifactState) -> WriteOperation:
    """§2.4 ``write_mode``: BACKFILL → APPEND, REFRESH → MERGE, ADAPTIVE by state."""
    if mode is WriteModeKind.ADAPTIVE:
        return _ADAPTIVE[state]
    op = _FIXED.get(mode)  # type: ignore[call-overload]
    if op is None:
        raise InvalidArtifactPlan(f"unknown write_mode {mode!r} (§2.4)")
    return op


def backfill_eligible(window_end: datetime, now: datetime, horizon_days: int) -> bool:
    """``now − H ≤ window_end ≤ now`` with ``H = horizon_days × 24 h`` (§2.4)."""
    end = instant(window_end, "window_end")
    at = instant(now, "now")
    return at - timedelta(days=horizon_days) <= end <= at


def _expiry(
    artifact: PipelineArtifact, snapshot: ArtifactSnapshot, now: datetime
) -> ExpiryPlan:
    if snapshot.last_transition is None or snapshot.last_occurred_at is None:
        raise InvalidArtifactPlan(
            "a Complete snapshot needs its last transition and its occurred_at"
        )
    plan = ExpiryPlan(
        snapshot.artifact_type,
        snapshot.artifact_key,
        snapshot.last_transition,
        snapshot.last_occurred_at,
        artifact.ttl,
    )
    if not plan.due(now):
        raise NotEligible(
            f"artifact {snapshot.artifact_key!r} is Complete and its ttl "
            f"({artifact.ttl}s) has not elapsed at {now}"
        )
    return plan


def _window(
    snapshot: ArtifactSnapshot, window: tuple[datetime, datetime] | None
) -> tuple[datetime, datetime]:
    stored = snapshot.window
    if stored is None and window is None:
        raise InvalidArtifactPlan(
            f"artifact {snapshot.artifact_key!r} has no row yet; pass its window"
        )
    if stored is not None and window is not None:
        given = (instant(window[0], "window[0]"), instant(window[1], "window[1]"))
        if given != (instant(stored[0], "start"), instant(stored[1], "end")):
            raise InvalidArtifactPlan(
                f"the window {window!r} is not the artifact's window {stored!r}"
            )
    chosen = stored if stored is not None else window
    assert chosen is not None
    return chosen


def _check_sink(
    sink: SinkTarget, op: WriteOperation, catalog: IcebergCatalog | None
) -> None:
    """§6.2 Delta and Iceberg writes: what the sink admits for *op*."""
    if op is WriteOperation.MERGE and sink is SinkTarget.DELTA:
        raise UnsupportedArtifactPlan(
            "a DELTA WritePlan is APPEND only; a MERGE needs another sink"
        )
    if sink is not SinkTarget.ICEBERG:
        return
    if not isinstance(catalog, IcebergCatalog):
        raise UnsupportedArtifactPlan(
            "an ICEBERG WritePlan needs an attached Iceberg REST catalog "
            "(iceberg_catalog); there is none (§6.2 Iceberg writes)"
        )
    if op is not WriteOperation.MERGE:
        return
    if not catalog.documented:
        raise UnsupportedArtifactPlan(
            f"a MERGE into ICEBERG on DuckDB {catalog.engine_version}: the §6.2 "
            f"capability table covers {sorted(ICEBERG_DOCUMENTED_VERSIONS)} only, "
            "and on another version every Iceberg plan but APPEND is rejected"
        )
    if not catalog.merge_on_read:
        raise UnsupportedArtifactPlan(
            "a MERGE into ICEBERG needs a merge-on-read table; copy-on-write is "
            "not supported (§6.2 Iceberg writes)"
        )


def plan_rule_10(
    artifact: PipelineArtifact,
    snapshot: ArtifactSnapshot,
    *,
    now: datetime,
    sink: SinkTarget | None,
    iceberg_catalog: IcebergCatalog | None = None,
    target: ArtifactTarget,
    query: ExportPlan,
    window: tuple[datetime, datetime] | None = None,
) -> ArtifactPlan | ExpiryPlan:
    """Rule 10; see the module docstring."""
    artifact = _check(artifact, snapshot)
    now = instant(now, "now")
    state = snapshot.state
    if state is ArtifactState.COMPLETE:
        return _expiry(artifact, snapshot, now)
    if state is ArtifactState.IN_FLIGHT:
        raise NotEligible(
            f"artifact {snapshot.artifact_key!r} is In-flight; no run starts from "
            "it, and an unresolved run is decided by the runtime's recovery"
        )
    if not isinstance(sink, SinkTarget):
        raise InvalidArtifactPlan(
            "the planner has no sink_target; there is no default sink"
        )
    if not isinstance(target, ArtifactTarget):
        raise InvalidArtifactPlan("Rule 10 needs an ArtifactTarget")
    start, end = _window(snapshot, window)
    if state in _GAP and not backfill_eligible(end, now, artifact.backfill_horizon):
        raise NotEligible(
            f"window ending {end} is not eligible for backfill at {now}: it must "
            f"satisfy now − {artifact.backfill_horizon} days ≤ window_end ≤ now"
        )
    op = operation_for(artifact.write_mode, state)
    _check_sink(sink, op, iceberg_catalog)
    write = WritePlan(sink, target.relation, op, target.window_column)
    return plan_artifact_run(
        snapshot, window_start=start, window_end=end, query=query, write=write
    )

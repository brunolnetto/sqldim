"""The result cache of §8.14: watermark invalidation, opt-in staleness, D44.

**Identity.**  An entry is stored under ``(tenant_id, result_key)``, where
``result_key = H(question_key, W)`` is :meth:`QueryDAG.result_key` over the
plan IR — ``question_key`` is the #148 key of the plan (``dag_hash`` of the
IR, schema version, params) and ``W`` the sorted watermarks of
``deps(plan)``.  The plan IR has no τ-dependent construct (``Now``) yet, so τ
and the time zone do not enter the key (§8.14 includes them only for a
τ-dependent query).  The skeleton keys of the question lattice (§12.7) hash
another payload and are **not** cache keys: a lattice question reaches the
cache only as the plan-IR spec it maps to, stored in the :class:`QuestionStore`
under its own ``question_key``.  A lattice key passed as a ``PlanRef`` is
unknown to the store and fails with ``UNKNOWN``.

**Lookup** (ADR 0004 as amended by ADR 0011, D41)::

    e = store.get(tenant_id, result_key)
    e = NONE                                           → miss ABSENT
    e.unversioned = ()                                 → hit
    policy.accept_stale set, now − computed_at ≤ it    → hit, possibly_stale
    accept_stale unset                                 → miss UNVERSIONED
    otherwise                                          → miss TOO_OLD

A dependency whose watermark advances changes ``W`` and so the key: the old
entry is simply never found again.  No component sets ``accept_stale`` on the
caller's behalf; :class:`CachePolicy` is a required argument of every call.

**Reading watermarks (D45, rule 2).**  :meth:`ResultCache.run` reads ``W``
before executing, runs the plan, reads ``W`` again and stores the result only
if the key is unchanged.  Otherwise the result is returned with
``stored=False`` and ``not_stored=WATERMARK_ADVANCED``.  ``computed_at`` is the
instant ``W`` was first read, so an entry's age is never understated.  No read
is pinned to a version (rule 1), which the backends here cannot do.

**Authorization (D44, §12.10).**  The cache is an entry point: it is built on a
:class:`PlanExecutor`, which holds the caller's ``AuthContext``.  ``READ`` on
the dataset of every relation of ``deps(plan)`` is checked before the store is
touched, and ``READ`` on the datasets the entry records before it is served.
The tenant is part of the store key, so an entry computed for one tenant is
never served to another.

**Fail closed.**  A store that raises, returns something that is not a
:class:`CacheEntry`, or returns an entry recorded for another tenant, key,
question or set of unversioned sources raises :class:`CacheStoreError`
(``CACHE_STORE``) — never a miss, never a hit.  A write that fails raises too.
Only the ``PROCEED`` delivery (the whole result) is cached; ``STREAM`` and
``PAGINATE`` run on the executor directly.
"""

from __future__ import annotations

import base64
import json
import sqlite3
import threading
import uuid
from collections import OrderedDict
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Protocol

from sqldim.core.query.dgm.planner._targets import ExportPlan
from sqldim.core.query.dgm.runtime._dag import PlanRefLike, QueryDAG, ResultKey
from sqldim.core.query.dgm.runtime._errors import (
    InvalidRuntimeArgument,
    NotConfigured,
    RuntimeAPIError,
)
from sqldim.core.query.dgm.runtime._executor import ExecutionResult

__all__ = [
    "CacheStoreError",
    "CachePolicy",
    "CacheEntry",
    "ResultStore",
    "InMemoryResultStore",
    "SQLiteResultStore",
    "MissReason",
    "NotStoredReason",
    "CacheLookup",
    "CachedResult",
    "CacheExecutor",
    "ResultCache",
]


class CacheStoreError(RuntimeAPIError):
    """The result store failed or returned an entry it cannot vouch for."""

    code = "CACHE_STORE"


# ---------------------------------------------------------------------------
# Values
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CachePolicy:
    """``CachePolicy(accept_stale)`` of §8.14, supplied with every request.

    ``accept_stale`` is the largest age at which the caller accepts an entry
    over an unversioned source; ``None`` (the default) accepts none.
    """

    accept_stale: timedelta | None = None

    def __post_init__(self) -> None:
        a = self.accept_stale
        if a is not None and (not isinstance(a, timedelta) or a < timedelta(0)):
            raise InvalidRuntimeArgument(
                f"accept_stale is None or a non-negative timedelta, got {a!r}"
            )


@dataclass(frozen=True)
class CacheEntry:
    """``cache[result_key] = (result_set, computed_at, unversioned)`` of §8.14."""

    tenant_id: str
    result_key: str
    question_key: str
    unversioned: tuple[str, ...]
    datasets: frozenset[str]
    columns: tuple[str, ...]
    rows: tuple[tuple, ...]
    computed_at: datetime


class ResultStore(Protocol):
    """Where entries live; every method raises on failure, never hides it."""

    def get(self, tenant_id: str, result_key: str) -> CacheEntry | None: ...

    def put(self, entry: CacheEntry) -> None: ...


class MissReason(Enum):
    ABSENT = "ABSENT"  # no entry under (tenant_id, result_key)
    UNVERSIONED = "UNVERSIONED"  # entry over an unversioned source; no opt-in
    TOO_OLD = "TOO_OLD"  # entry over an unversioned source, older than accepted


class NotStoredReason(Enum):
    WATERMARK_ADVANCED = "WATERMARK_ADVANCED"  # D45 rule 2: W changed meanwhile


@dataclass(frozen=True)
class CacheLookup:
    """The outcome of :meth:`ResultCache.lookup`."""

    key: ResultKey
    datasets: frozenset[str]
    entry: CacheEntry | None
    miss: MissReason | None
    possibly_stale: bool = False

    @property
    def hit(self) -> bool:
        return self.miss is None


@dataclass(frozen=True)
class CachedResult:
    """A ``PROCEED`` result, served from the cache or computed now.

    ``possibly_stale`` and ``computed_at`` are reported whenever an entry over
    an unversioned source was served (ADR 0011).  ``stored`` says whether a
    computed result was written; when not, ``not_stored`` says why.
    """

    result_key: ResultKey
    columns: tuple[str, ...]
    rows: list[tuple]
    datasets: frozenset[str]
    computed_at: datetime
    from_cache: bool
    possibly_stale: bool = False
    miss: MissReason | None = None
    stored: bool = False
    not_stored: NotStoredReason | None = None


# ---------------------------------------------------------------------------
# Stores
# ---------------------------------------------------------------------------


def _check_entry(entry: object) -> CacheEntry:
    if not isinstance(entry, CacheEntry):
        raise CacheStoreError(f"not a CacheEntry: {type(entry).__name__}")
    if entry.computed_at.tzinfo is None:
        raise CacheStoreError("computed_at must be timezone-aware")
    return entry


class InMemoryResultStore:
    """A :class:`ResultStore` in this process, evicting the least recently used.

    Eviction is by capacity only; correctness never depends on it (ADR 0004).
    """

    def __init__(self, capacity: int) -> None:
        if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity <= 0:
            raise NotConfigured(f"capacity must be a positive int, got {capacity!r}")
        self._capacity = capacity
        self._entries: OrderedDict[tuple[str, str], CacheEntry] = OrderedDict()
        self._lock = threading.Lock()

    def get(self, tenant_id: str, result_key: str) -> CacheEntry | None:
        with self._lock:
            entry = self._entries.get((tenant_id, result_key))
            if entry is not None:
                self._entries.move_to_end((tenant_id, result_key))
            return entry

    def put(self, entry: CacheEntry) -> None:
        _check_entry(entry)
        with self._lock:
            self._entries[(entry.tenant_id, entry.result_key)] = entry
            self._entries.move_to_end((entry.tenant_id, entry.result_key))
            while len(self._entries) > self._capacity:
                self._entries.popitem(last=False)

    def __len__(self) -> int:
        return len(self._entries)


# -- value codec of the SQLite store ----------------------------------------

_ENCODERS: tuple[tuple[type, str, Callable[[Any], Any]], ...] = (
    (bool, "b", lambda v: v),
    (int, "i", str),
    (float, "f", repr),
    (str, "s", lambda v: v),
    (Decimal, "d", str),
    (datetime, "dt", lambda v: v.isoformat()),
    (date, "da", lambda v: v.isoformat()),
    (time, "t", lambda v: v.isoformat()),
    (timedelta, "td", lambda v: [v.days, v.seconds, v.microseconds]),
    (bytes, "x", lambda v: base64.b64encode(v).decode("ascii")),
    (uuid.UUID, "u", str),
)

_DECODERS: dict[str, Callable[[Any], Any]] = {
    "b": bool,
    "i": int,
    "f": float,
    "s": str,
    "d": Decimal,
    "dt": datetime.fromisoformat,
    "da": date.fromisoformat,
    "t": time.fromisoformat,
    "td": lambda v: timedelta(days=v[0], seconds=v[1], microseconds=v[2]),
    "x": base64.b64decode,
    "u": uuid.UUID,
}


def _encode(v: object) -> list:
    if v is None:
        return ["n", None]
    if isinstance(v, (list, tuple)):
        return ["l" if isinstance(v, list) else "tu", [_encode(x) for x in v]]
    if isinstance(v, dict):
        return ["m", [[_encode(k), _encode(x)] for k, x in v.items()]]
    for typ, tag, enc in _ENCODERS:
        if type(v) is typ:
            return [tag, enc(v)]
    raise CacheStoreError(f"a {type(v).__name__} value cannot be stored")


def _decode(cell: list) -> object:
    tag, payload = cell
    if tag == "n":
        return None
    if tag in ("l", "tu"):
        items = [_decode(x) for x in payload]
        return items if tag == "l" else tuple(items)
    if tag == "m":
        return {_decode(k): _decode(x) for k, x in payload}
    return _DECODERS[tag](payload)


def _to_json(entry: CacheEntry) -> str:
    return json.dumps(
        {
            "question_key": entry.question_key,
            "unversioned": list(entry.unversioned),
            "datasets": sorted(entry.datasets),
            "columns": list(entry.columns),
            "rows": [[_encode(v) for v in row] for row in entry.rows],
            "computed_at": entry.computed_at.isoformat(),
        }
    )


def _from_json(tenant_id: str, result_key: str, text: str) -> CacheEntry:
    d = json.loads(text)
    return CacheEntry(
        tenant_id=tenant_id,
        result_key=result_key,
        question_key=d["question_key"],
        unversioned=tuple(d["unversioned"]),
        datasets=frozenset(d["datasets"]),
        columns=tuple(d["columns"]),
        rows=tuple(tuple(_decode(c) for c in row) for row in d["rows"]),
        computed_at=datetime.fromisoformat(d["computed_at"]),
    )


class SQLiteResultStore:
    """A durable :class:`ResultStore` in one SQLite file, shared across processes.

    Values are stored with a type tag so a row reads back as it was written;
    a value of a type the codec does not know is refused at ``put``.  An
    unreadable record is :class:`CacheStoreError`, never a miss.
    """

    _DDL = (
        "CREATE TABLE IF NOT EXISTS sqldim_result_cache ("
        " tenant_id TEXT NOT NULL, result_key TEXT NOT NULL, payload TEXT NOT NULL,"
        " PRIMARY KEY (tenant_id, result_key))"
    )

    def __init__(self, path: str | Path) -> None:
        if not path:
            raise NotConfigured("SQLiteResultStore needs a path")
        self._path = str(path)
        self._lock = threading.Lock()
        try:
            with self._connect() as con:
                con.execute(self._DDL)
        except sqlite3.Error as exc:
            raise CacheStoreError(f"cannot open the result store: {exc}") from exc

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        con = sqlite3.connect(self._path, timeout=30)
        try:
            with con:  # commits, or rolls back on error
                yield con
        finally:
            con.close()

    def get(self, tenant_id: str, result_key: str) -> CacheEntry | None:
        sql = (
            "SELECT payload FROM sqldim_result_cache "
            "WHERE tenant_id = ? AND result_key = ?"
        )
        with self._lock, self._connect() as con:
            row = con.execute(sql, (tenant_id, result_key)).fetchone()
        if row is None:
            return None
        try:
            return _from_json(tenant_id, result_key, row[0])
        except (ValueError, KeyError, TypeError, IndexError) as exc:
            raise CacheStoreError(f"unreadable cache record: {exc}") from exc

    def put(self, entry: CacheEntry) -> None:
        payload = _to_json(_check_entry(entry))
        sql = "INSERT OR REPLACE INTO sqldim_result_cache VALUES (?, ?, ?)"
        with self._lock, self._connect() as con:
            con.execute(sql, (entry.tenant_id, entry.result_key, payload))


# ---------------------------------------------------------------------------
# ResultCache
# ---------------------------------------------------------------------------


class CacheExecutor(Protocol):
    """What the cache needs from :class:`PlanExecutor` (its AuthContext holder)."""

    @property
    def tenant_id(self) -> str: ...

    def authorize_read(
        self, plan: ExportPlan, also: frozenset[str] = ...
    ) -> frozenset[str]: ...

    def run(self, plan: ExportPlan | None, mode: Any) -> ExecutionResult: ...


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _guarded(what: str, call: Callable[[], Any]) -> Any:
    try:
        return call()
    except CacheStoreError:
        raise
    except Exception as exc:  # noqa: BLE001 — reported as a typed failure
        raise CacheStoreError(f"the result store failed to {what}: {exc}") from exc


class ResultCache:
    """The §8.14 result cache over a :class:`QueryDAG` and a :class:`PlanExecutor`.

    *dag* gives ``result_key`` (and so ``deps(plan)`` and ``W``); *executor*
    holds the caller's ``AuthContext`` and runs a missed plan.  Both must be
    built over the same relation catalog.  *clock* returns a timezone-aware
    instant.
    """

    def __init__(
        self,
        *,
        dag: QueryDAG,
        executor: CacheExecutor,
        store: ResultStore,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        for name, dep in (("dag", dag), ("executor", executor), ("store", store)):
            if dep is None:
                raise NotConfigured(f"ResultCache needs a {name}")
        self._dag = dag
        self._executor = executor
        self._store = store
        self._clock = clock

    def _now(self) -> datetime:
        now = self._clock()
        if not isinstance(now, datetime) or now.tzinfo is None:
            raise NotConfigured("the cache clock must return an aware datetime")
        return now

    # -- lookup --------------------------------------------------------------

    def _fetch(self, tenant: str, key: ResultKey) -> CacheEntry | None:
        got = _guarded("read", lambda: self._store.get(tenant, key.value))
        if got is None:
            return None
        entry = _check_entry(got)
        recorded = (entry.tenant_id, entry.result_key, entry.question_key)
        if recorded != (tenant, key.value, key.question_key):
            raise CacheStoreError("the store returned an entry for another key")
        if tuple(entry.unversioned) != key.unversioned:
            raise CacheStoreError("the entry disagrees with the key on unversioned")
        return entry

    def _decide(
        self, entry: CacheEntry | None, policy: CachePolicy, now: datetime
    ) -> tuple[MissReason | None, bool]:
        if entry is None:
            return MissReason.ABSENT, False
        if not entry.unversioned:
            return None, False
        if policy.accept_stale is None:
            return MissReason.UNVERSIONED, False
        if now - entry.computed_at <= policy.accept_stale:
            return None, True
        return MissReason.TOO_OLD, False

    def _lookup(
        self, ref: PlanRefLike, policy: CachePolicy
    ) -> tuple[CacheLookup, ExportPlan, datetime]:
        if not isinstance(policy, CachePolicy):
            raise InvalidRuntimeArgument("a CachePolicy is required with every call")
        plan = self._dag.plan_for(ref)
        datasets = self._executor.authorize_read(plan)  # D44: before the store
        now = self._now()
        key = self._dag.result_key(ref)
        entry = self._fetch(self._executor.tenant_id, key)
        if entry is not None:
            self._executor.authorize_read(plan, frozenset(entry.datasets))
        miss, stale = self._decide(entry, policy, now)
        return CacheLookup(key, datasets, entry, miss, stale), plan, now

    def lookup(self, ref: PlanRefLike, *, policy: CachePolicy) -> CacheLookup:
        """Look *ref* up without executing anything."""
        return self._lookup(ref, policy)[0]

    # -- run -----------------------------------------------------------------

    @staticmethod
    def _served(look: CacheLookup) -> CachedResult:
        e = look.entry
        assert e is not None
        return CachedResult(
            look.key,
            e.columns,
            [tuple(r) for r in e.rows],
            frozenset(e.datasets),
            e.computed_at,
            from_cache=True,
            possibly_stale=look.possibly_stale,
        )

    def run(self, ref: PlanRefLike, *, policy: CachePolicy) -> CachedResult:
        """Serve *ref* from the cache, or run it (``PROCEED``) and store it (D45)."""
        look, plan, started = self._lookup(ref, policy)
        if look.hit:
            return self._served(look)
        res = self._executor.run(plan, "PROCEED")
        rows: Sequence[tuple] = res.rows if res.rows is not None else ()
        after = self._dag.result_key(ref)
        out = CachedResult(
            look.key,
            res.columns,
            list(rows),
            res.datasets,
            started,
            from_cache=False,
            miss=look.miss,
        )
        if after.value != look.key.value:
            return replace(out, not_stored=NotStoredReason.WATERMARK_ADVANCED)
        entry = CacheEntry(
            self._executor.tenant_id,
            look.key.value,
            look.key.question_key,
            look.key.unversioned,
            res.datasets,
            res.columns,
            tuple(tuple(r) for r in rows),
            started,
        )
        _guarded("write", lambda: self._store.put(entry))
        return replace(out, stored=True)

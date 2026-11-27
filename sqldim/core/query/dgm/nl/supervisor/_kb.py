"""DGM Supervisor — tenant-partitioned knowledge base (§12.7, §12.10, §12.11).

A finding is stored with its coordinates ``(q, π)`` — ``question_key`` and
``parameters`` — the datasets it derives from, its content, the time it was
stored and the ``idempotency_key`` of the call that stored it.

Partitioning (§12.10, D44): every row carries its ``tenant_id`` and every read
filters on it; a tool only ever holds a :class:`TenantKnowledgeBase`, a handle
bound to one tenant, so one tenant's findings are never stored in or returned
to another.

Idempotency (§12.11): ``finding_id = H(tenant_id, idempotency_key)`` (§12.3)
is the primary key, so storing twice under one key stores once — also across
processes, because the database enforces it.  Reusing a key for a *different*
finding is refused with :class:`FindingConflict`; nothing is overwritten.

:class:`SQLiteKnowledgeBase` is the durable implementation: with a file path
the findings survive the process; with ``":memory:"`` they live as long as the
object.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

__all__ = [
    "FindingConflict",
    "KnowledgeBase",
    "KnowledgeBaseError",
    "SQLiteKnowledgeBase",
    "StoredFinding",
    "TenantKnowledgeBase",
    "finding_id_for",
    "tenant_handle",
]


class KnowledgeBaseError(RuntimeError):
    """The backend could not complete an operation (reported, never hidden)."""


class FindingConflict(KnowledgeBaseError):
    """An ``idempotency_key`` already names a different finding."""


@dataclass(frozen=True)
class StoredFinding:
    """One finding as persisted in the knowledge base."""

    tenant_id: str
    finding_id: str
    idempotency_key: str
    question_key: str
    parameters: dict[str, Any]
    datasets: tuple[str, ...]
    content: str
    stored_at: datetime


def finding_id_for(tenant_id: str, idempotency_key: str) -> str:
    """``H(tenant_id, idempotency_key)`` of §12.3 (unambiguous encoding)."""
    payload = json.dumps([tenant_id, idempotency_key], separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


@runtime_checkable
class KnowledgeBase(Protocol):
    """A knowledge-base backend shared by every tenant of a deployment."""

    def store(
        self,
        tenant_id: str,
        idempotency_key: str,
        *,
        question_key: str,
        parameters: dict[str, Any],
        datasets: Iterable[str],
        content: str,
    ) -> StoredFinding:
        """Store once per ``(tenant_id, idempotency_key)``; return the stored row."""
        ...

    def findings_at(
        self, tenant_id: str, question_keys: Iterable[str]
    ) -> list[StoredFinding]:
        """The tenant's findings whose ``question_key`` is in *question_keys*."""
        ...

    def for_tenant(self, tenant_id: str) -> "TenantKnowledgeBase":
        """A handle that can only reach *tenant_id*'s partition."""
        ...


class TenantKnowledgeBase:
    """The handle a tool receives: one backend, one tenant (§12.2 ``kb``)."""

    def __init__(self, backend: KnowledgeBase, tenant_id: str) -> None:
        if not tenant_id:
            raise ValueError("a knowledge-base handle needs a tenant_id")
        self._backend = backend
        self._tenant_id = tenant_id

    @property
    def tenant_id(self) -> str:
        return self._tenant_id

    def store(
        self,
        idempotency_key: str,
        *,
        question_key: str,
        parameters: dict[str, Any],
        datasets: Iterable[str],
        content: str,
    ) -> StoredFinding:
        return self._backend.store(
            self._tenant_id,
            idempotency_key,
            question_key=question_key,
            parameters=parameters,
            datasets=datasets,
            content=content,
        )

    def findings_at(self, question_keys: Iterable[str]) -> list[StoredFinding]:
        return self._backend.findings_at(self._tenant_id, question_keys)


def tenant_handle(kb: Any, tenant_id: str) -> TenantKnowledgeBase | None:
    """Scope a deployment's KB to one tenant; ``None`` when there is no KB."""
    if kb is None:
        return None
    if not isinstance(kb, KnowledgeBase):
        raise TypeError(f"{type(kb).__name__} is not a KnowledgeBase")
    return kb.for_tenant(tenant_id)


# ---------------------------------------------------------------------------
# SQLite backend
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS findings (
    tenant_id       TEXT    NOT NULL,
    finding_id      TEXT    NOT NULL,
    idempotency_key TEXT    NOT NULL,
    question_key    TEXT    NOT NULL,
    parameters      TEXT    NOT NULL,
    datasets        TEXT    NOT NULL,
    content         TEXT    NOT NULL,
    stored_at_us    INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, finding_id),
    UNIQUE (tenant_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS findings_by_question
    ON findings (tenant_id, question_key);
"""

_COLUMNS = (
    "tenant_id, finding_id, idempotency_key, question_key, "
    "parameters, datasets, content, stored_at_us"
)

Clock = Callable[[], datetime]


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_CHUNK = 500  # bound on SQL variables per lookup


def _to_us(at: datetime) -> int:
    if at.tzinfo is None:
        raise ValueError("stored_at must be timezone-aware")
    return (at - _EPOCH) // timedelta(microseconds=1)


def _from_us(us: int) -> datetime:
    return _EPOCH + timedelta(microseconds=us)


def _row(row: tuple[Any, ...]) -> StoredFinding:
    return StoredFinding(
        tenant_id=row[0],
        finding_id=row[1],
        idempotency_key=row[2],
        question_key=row[3],
        parameters=json.loads(row[4]),
        datasets=tuple(json.loads(row[5])),
        content=row[6],
        stored_at=_from_us(row[7]),
    )


class SQLiteKnowledgeBase:
    """Durable, tenant-partitioned findings in one SQLite database."""

    def __init__(self, path: str | Path = ":memory:", *, clock: Clock = _utcnow):
        self._clock = clock
        self._lock = threading.Lock()
        self._con = sqlite3.connect(str(path), check_same_thread=False)
        with self._lock, self._con:
            self._con.executescript(_SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._con.close()

    def for_tenant(self, tenant_id: str) -> TenantKnowledgeBase:
        return TenantKnowledgeBase(self, tenant_id)

    def store(
        self,
        tenant_id: str,
        idempotency_key: str,
        *,
        question_key: str,
        parameters: dict[str, Any],
        datasets: Iterable[str],
        content: str,
    ) -> StoredFinding:
        if not tenant_id or not idempotency_key:
            raise ValueError("tenant_id and idempotency_key must be non-empty")
        fid = finding_id_for(tenant_id, idempotency_key)
        values = (
            question_key,
            _canonical(parameters),
            _canonical(sorted(set(datasets))),
            content,
        )
        try:
            with self._lock, self._con:
                self._con.execute(
                    f"INSERT OR IGNORE INTO findings ({_COLUMNS}) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (tenant_id, fid, idempotency_key, *values, _to_us(self._clock())),
                )
                row = self._con.execute(
                    f"SELECT {_COLUMNS} FROM findings "
                    "WHERE tenant_id = ? AND finding_id = ?",
                    (tenant_id, fid),
                ).fetchone()
        except sqlite3.Error as exc:
            raise KnowledgeBaseError(f"store failed: {exc}") from exc
        if row is None or row[2] != idempotency_key:
            raise KnowledgeBaseError("the stored finding could not be read back")
        if tuple(row[3:7]) != values:
            raise FindingConflict("idempotency_key already stores a different finding")
        return _row(row)

    def findings_at(
        self, tenant_id: str, question_keys: Iterable[str]
    ) -> list[StoredFinding]:
        keys = sorted(set(question_keys))
        rows: list[tuple[Any, ...]] = []
        try:
            with self._lock:
                for i in range(0, len(keys), _CHUNK):
                    chunk = keys[i : i + _CHUNK]
                    marks = ", ".join("?" for _ in chunk)
                    rows.extend(
                        self._con.execute(
                            f"SELECT {_COLUMNS} FROM findings "
                            f"WHERE tenant_id = ? AND question_key IN ({marks})",
                            (tenant_id, *chunk),
                        ).fetchall()
                    )
        except sqlite3.Error as exc:
            raise KnowledgeBaseError(f"lookup failed: {exc}") from exc
        return [_row(r) for r in rows]

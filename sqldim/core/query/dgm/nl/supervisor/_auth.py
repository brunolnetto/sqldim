"""DGM Supervisor — authorization context and scoped data access (§12.10, D44).

The host builds an :class:`AuthContext` from the authenticated session before a
run starts.  It lives in the runtime context only: it is never checkpointed,
never shown to a model and never built from request arguments.

:class:`DataAccess` is the only way a tool reaches data.  The dispatcher hands
each call a view scoped to the datasets of that request (§12.10, *Isolation*);
opening any other dataset raises :class:`AccessDenied`.

Non-disclosure policy: a dataset that is not granted and a dataset that does
not exist are reported identically — ``UNAUTHORIZED`` with the message
:data:`NOT_AUTHORIZED_OR_NOT_FOUND` — so a denial never reveals existence.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

__all__ = [
    "NOT_AUTHORIZED_OR_NOT_FOUND",
    "AccessDenied",
    "Action",
    "AuthContext",
    "DataAccess",
    "DatasetGrant",
    "DatasetOpener",
    "describe_grants",
    "grants_for",
]

NOT_AUTHORIZED_OR_NOT_FOUND = "not authorized or not found"


class Action(str, Enum):
    """Actions a :class:`DatasetGrant` may allow (§12.2, §12.10)."""

    READ = "READ"
    CORRELATE = "CORRELATE"
    WRITE_KB = "WRITE_KB"
    WRITE = "WRITE"


@dataclass(frozen=True)
class DatasetGrant:
    """The actions one principal may perform on one dataset."""

    dataset_id: str
    actions: frozenset[Action]

    def __post_init__(self) -> None:
        if not self.dataset_id:
            raise ValueError("DatasetGrant.dataset_id must be non-empty")
        if not self.actions:
            raise ValueError("DatasetGrant.actions must be non-empty")
        if not all(isinstance(a, Action) for a in self.actions):
            raise TypeError("DatasetGrant.actions must contain Action members")


@dataclass(frozen=True)
class AuthContext:
    """Identity and grants of the principal a run acts for (§12.2)."""

    principal_id: str
    tenant_id: str
    grants: frozenset[DatasetGrant] = field(default_factory=frozenset)

    def __post_init__(self) -> None:
        if not self.principal_id:
            raise ValueError("AuthContext.principal_id must be non-empty")
        if not self.tenant_id:
            raise ValueError("AuthContext.tenant_id must be non-empty")
        seen: set[str] = set()
        for grant in self.grants:
            if grant.dataset_id in seen:
                raise ValueError(
                    f"AuthContext has two grants for dataset {grant.dataset_id!r}"
                )
            seen.add(grant.dataset_id)

    def actions_on(self, dataset_id: str) -> frozenset[Action]:
        for grant in self.grants:
            if grant.dataset_id == dataset_id:
                return grant.actions
        return frozenset()

    def allows(self, dataset_id: str, actions: Iterable[Action]) -> bool:
        return set(actions) <= self.actions_on(dataset_id)

    def readable_datasets(self) -> list[str]:
        """Datasets with a READ grant, sorted — the only ones a model may see."""
        return sorted(g.dataset_id for g in self.grants if Action.READ in g.actions)


class AccessDenied(PermissionError):
    """Raised by :class:`DataAccess` for a dataset outside the call's scope."""

    def __init__(self) -> None:
        super().__init__(NOT_AUTHORIZED_OR_NOT_FOUND)


DatasetOpener = Callable[[str], Any]
"""Host-provided function that opens one dataset for the principal."""


class DataAccess:
    """Dataset access restricted to the grants of one principal (§12.10).

    The dispatcher derives a per-call view with :meth:`scoped`; tools only ever
    see that view.  ``open`` re-checks the grant on every call, so a grant
    revoked during a run takes effect immediately.
    """

    def __init__(
        self,
        auth: AuthContext,
        opener: DatasetOpener,
        *,
        scope: frozenset[str] | None = None,
        required: frozenset[Action] = frozenset({Action.READ}),
    ) -> None:
        self._auth = auth
        self._opener = opener
        self._scope = scope
        self._required = required

    @property
    def scope(self) -> frozenset[str] | None:
        return self._scope

    def scoped(self, datasets: Iterable[str]) -> "DataAccess":
        """A view that may open only *datasets* (a subset of the current scope)."""
        requested = frozenset(datasets)
        if self._scope is not None and not requested <= self._scope:
            raise AccessDenied()
        return DataAccess(
            self._auth, self._opener, scope=requested, required=self._required
        )

    def open(self, dataset_id: str) -> Any:
        """Open *dataset_id*, or raise :class:`AccessDenied` without touching it."""
        if self._scope is None or dataset_id not in self._scope:
            raise AccessDenied()
        if not self._auth.allows(dataset_id, self._required):
            raise AccessDenied()
        return self._opener(dataset_id)


def grants_for(
    datasets: Iterable[str], actions: Iterable[Action]
) -> frozenset[DatasetGrant]:
    """Grant the same *actions* on every dataset — a host-side convenience."""
    acts = frozenset(actions)
    return frozenset(DatasetGrant(ds, acts) for ds in datasets)


def describe_grants(auth: AuthContext) -> Mapping[str, list[str]]:
    """Grants as plain data, for host-side diagnostics (never for a model)."""
    return {
        g.dataset_id: sorted(a.value for a in g.actions)
        for g in sorted(auth.grants, key=lambda g: g.dataset_id)
    }

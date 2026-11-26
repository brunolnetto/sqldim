"""§12.10 / D44: AuthContext and DataAccess fail closed."""

from __future__ import annotations

import pytest

from sqldim.core.query.dgm.nl.supervisor._auth import (
    NOT_AUTHORIZED_OR_NOT_FOUND,
    AccessDenied,
    Action,
    AuthContext,
    DataAccess,
    DatasetGrant,
    grants_for,
)
from tests.supervisor._fakes import Opener, auth


class TestAuthContext:
    @pytest.mark.parametrize("principal,tenant", [("", "t"), ("p", "")])
    def test_identity_is_required(self, principal: str, tenant: str) -> None:
        with pytest.raises(ValueError):
            AuthContext(principal_id=principal, tenant_id=tenant)

    def test_duplicate_grants_rejected(self) -> None:
        with pytest.raises(ValueError, match="two grants"):
            AuthContext(
                principal_id="p",
                tenant_id="t",
                grants=frozenset(
                    {
                        DatasetGrant("a", frozenset({Action.READ})),
                        DatasetGrant("a", frozenset({Action.CORRELATE})),
                    }
                ),
            )

    def test_grant_needs_actions_and_dataset(self) -> None:
        with pytest.raises(ValueError):
            DatasetGrant("a", frozenset())
        with pytest.raises(ValueError):
            DatasetGrant("", frozenset({Action.READ}))
        with pytest.raises(TypeError):
            DatasetGrant("a", frozenset({"READ"}))  # type: ignore[arg-type]

    def test_no_grants_allows_nothing(self) -> None:
        ctx = AuthContext(principal_id="p", tenant_id="t")
        assert not ctx.allows("a", {Action.READ})
        assert ctx.readable_datasets() == []

    def test_allows_requires_every_action(self) -> None:
        ctx = auth({"a": {Action.READ}})
        assert ctx.allows("a", {Action.READ})
        assert not ctx.allows("a", {Action.READ, Action.CORRELATE})
        assert not ctx.allows("b", {Action.READ})

    def test_readable_datasets_excludes_non_read_grants(self) -> None:
        ctx = auth({"b": {Action.READ}, "a": {Action.READ}, "w": {Action.WRITE_KB}})
        assert ctx.readable_datasets() == ["a", "b"]

    def test_immutable(self) -> None:
        ctx = auth()
        with pytest.raises(AttributeError):
            ctx.tenant_id = "other"  # type: ignore[misc]

    def test_grants_for(self) -> None:
        grants = grants_for(["a", "b"], {Action.READ})
        assert {g.dataset_id for g in grants} == {"a", "b"}


class TestDataAccess:
    def test_unscoped_access_opens_nothing(self) -> None:
        opener = Opener()
        with pytest.raises(AccessDenied):
            DataAccess(auth(), opener).open("sales")
        assert opener.opened == []

    def test_scoped_access_opens_only_its_datasets(self) -> None:
        opener = Opener()
        data = DataAccess(auth({"a": {Action.READ}, "b": {Action.READ}}), opener)
        view = data.scoped(["a"])
        assert view.open("a") == "handle:a"
        with pytest.raises(AccessDenied):
            view.open("b")
        assert opener.opened == ["a"]

    def test_scope_cannot_be_widened(self) -> None:
        view = DataAccess(auth(), Opener()).scoped(["sales"])
        with pytest.raises(AccessDenied):
            view.scoped(["sales", "other"])

    def test_scope_without_grant_opens_nothing(self) -> None:
        opener = Opener()
        view = DataAccess(auth({"a": {Action.READ}}), opener).scoped(["secret"])
        with pytest.raises(AccessDenied):
            view.open("secret")
        assert opener.opened == []

    def test_grant_without_read_opens_nothing(self) -> None:
        opener = Opener()
        view = DataAccess(auth({"a": {Action.WRITE_KB}}), opener).scoped(["a"])
        with pytest.raises(AccessDenied):
            view.open("a")
        assert opener.opened == []

    def test_denial_does_not_reveal_existence(self) -> None:
        view = DataAccess(auth({"a": {Action.READ}}), Opener()).scoped(["a"])
        messages = []
        for ds in ("exists-but-ungranted", "no-such-dataset"):
            with pytest.raises(AccessDenied) as exc:
                view.open(ds)
            messages.append(str(exc.value))
        assert messages == [NOT_AUTHORIZED_OR_NOT_FOUND] * 2

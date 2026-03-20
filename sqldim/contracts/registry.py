"""sqldim/contracts/registry.py — ContractRegistry."""

from __future__ import annotations

from sqldim.contracts.contract import DataContract
from sqldim.contracts.version import ContractVersion


class ContractRegistry:
    """Stores all versions of every registered DataContract."""

    def __init__(self) -> None:
        # name → list[DataContract] in registration order
        self._store: dict[str, list[DataContract]] = {}

    def register(self, contract: DataContract) -> None:
        self._store.setdefault(contract.name, []).append(contract)

    def get(self, name: str) -> DataContract:
        """Return the latest version for *name*.  Raises ``KeyError`` if unknown."""
        versions = self._store.get(name)
        if not versions:
            raise KeyError(name)
        return max(versions, key=lambda c: c.version)

    def get_version(self, name: str, version: str) -> DataContract:
        """Return the exact *version* for *name*.  Raises ``KeyError`` if not found."""
        target = ContractVersion.parse(version)
        for c in self._store.get(name, []):
            if c.version == target:
                return c
        raise KeyError(f"{name}@{version}")

    def history(self, name: str) -> list[DataContract]:
        """Return all versions of *name* sorted ascending."""
        return sorted(self._store.get(name, []), key=lambda c: c.version)

    def is_registered(self, name: str) -> bool:
        return name in self._store and bool(self._store[name])

    def all_names(self) -> list[str]:
        return list(self._store.keys())

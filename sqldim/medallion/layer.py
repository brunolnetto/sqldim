"""sqldim/medallion/layer.py — Layer enum and MedallionRegistry."""

from __future__ import annotations

from enum import Enum


_ORDER = ["bronze", "silver", "gold"]


class Layer(str, Enum):
    """The three tiers of the Medallion architecture."""

    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"

    def __str__(self) -> str:
        return self.value

    def can_promote_to(self, target: "Layer") -> bool:
        """Return True only when *target* is the immediate next layer."""
        return _ORDER.index(target.value) == _ORDER.index(self.value) + 1


class MedallionRegistry:
    """Registry that tracks which layer each named dataset belongs to."""

    def __init__(self) -> None:
        self._map: dict[str, Layer] = {}

    def register(self, dataset: str, layer: Layer) -> None:
        """Register (or re-register) *dataset* under *layer*."""
        self._map[dataset] = layer

    def get_layer(self, dataset: str) -> Layer:
        """Return the layer for *dataset*.  Raises ``KeyError`` when unknown."""
        return self._map[dataset]

    def is_registered(self, dataset: str) -> bool:
        return dataset in self._map

    def datasets_in(self, layer: Layer) -> list[str]:
        return [ds for ds, lyr in self._map.items() if lyr is layer]

    def all_datasets(self) -> list[str]:
        return list(self._map.keys())

"""sqldim/medallion/build_order.py — Silver-layer population ordering.

Within Silver, models must be built in dependency order:

    DIMENSION → FACT → BRIDGE → GRAPH

* Dimensions are self-contained lookup tables.
* Facts reference dimension foreign keys, so dimensions must exist first.
* Bridges join across multiple facts/dimensions for M:N resolution, so they
  come after facts.
* SchemaGraph wraps the full dimensional model (vertices=dimensions,
  edges=facts) and requires all three above to be populated first.
"""
from __future__ import annotations

from enum import Enum
from typing import Sequence


class ModelKind(str, Enum):
    """Canonical population order within the Silver layer."""
    DIMENSION = "dimension"
    FACT      = "fact"
    BRIDGE    = "bridge"
    GRAPH     = "graph"


# Explicit prerequisite sets per kind.
_PREREQUISITES: dict[ModelKind, frozenset[ModelKind]] = {
    ModelKind.DIMENSION: frozenset(),
    ModelKind.FACT:      frozenset({ModelKind.DIMENSION}),
    ModelKind.BRIDGE:    frozenset({ModelKind.DIMENSION, ModelKind.FACT}),
    ModelKind.GRAPH:     frozenset({ModelKind.DIMENSION, ModelKind.FACT, ModelKind.BRIDGE}),
}


class SilverBuildOrder:
    """Utility that classifies model classes and returns dependency-ordered lists.

    Usage
    -----
    >>> sbo = SilverBuildOrder()
    >>> sbo.build_order([SchemaGraph, Bridge, Fact, Dim])
    [Dim, Fact, Bridge, SchemaGraph]
    """

    POPULATION_ORDER: list[ModelKind] = [
        ModelKind.DIMENSION,
        ModelKind.FACT,
        ModelKind.BRIDGE,
        ModelKind.GRAPH,
    ]

    # -----------------------------------------------------------------
    # classify
    # -----------------------------------------------------------------

    _KIND_CHECKS: list = []

    def classify(self, model_cls: type) -> ModelKind:
        """Return the ``ModelKind`` for *model_cls*.

        Raises ``TypeError`` when the class is not a recognised Silver model.
        """
        from sqldim.core.kimball.models import DimensionModel, FactModel, BridgeModel
        from sqldim.core.graph.schema_graph import SchemaGraph

        if not SilverBuildOrder._KIND_CHECKS:
            SilverBuildOrder._KIND_CHECKS = [
                (DimensionModel, ModelKind.DIMENSION),
                (BridgeModel,    ModelKind.BRIDGE),
                (FactModel,      ModelKind.FACT),
                (SchemaGraph,    ModelKind.GRAPH),
            ]

        try:
            for base, kind in SilverBuildOrder._KIND_CHECKS:
                if issubclass(model_cls, base):
                    return kind
        except TypeError:
            pass
        raise TypeError(f"Cannot classify {model_cls!r} as a Silver model kind")

    # -----------------------------------------------------------------
    # can_build_after
    # -----------------------------------------------------------------

    def can_build_after(self, candidate: ModelKind, existing: set[ModelKind]) -> bool:
        """Return True when all prerequisites for *candidate* are in *existing*."""
        return _PREREQUISITES[candidate].issubset(existing)

    # -----------------------------------------------------------------
    # build_order
    # -----------------------------------------------------------------

    def build_order(self, models: Sequence[type]) -> list[type]:
        """Return *models* sorted by Silver population order.

        Within the same ``ModelKind`` the original insertion order is preserved
        (stable sort).  Raises ``TypeError`` for unclassifiable models.
        """
        order_index = {k: i for i, k in enumerate(self.POPULATION_ORDER)}
        return sorted(models, key=lambda m: order_index[self.classify(m)])

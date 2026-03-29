"""RED tests for DGM parameter space Π(q) — §8.15.

Tests for ParameterFamily, ParameterTypeKind, ParameterSlot, and
ParameterSpace models.
"""

from __future__ import annotations

import pytest

from sqldim.core.query.dgm.parameters import (
    ParameterFamily,
    ParameterSlot,
    ParameterSpace,
    ParameterTypeKind,
)


# ---------------------------------------------------------------------------
# ParameterFamily enum
# ---------------------------------------------------------------------------


class TestParameterFamily:
    """Six parameter families from §8.15."""

    def test_temporal(self) -> None:
        assert ParameterFamily.TEMPORAL.value == "TEMPORAL"

    def test_strategy(self) -> None:
        assert ParameterFamily.STRATEGY.value == "STRATEGY"

    def test_filter(self) -> None:
        assert ParameterFamily.FILTER.value == "FILTER"

    def test_aggregation(self) -> None:
        assert ParameterFamily.AGGREGATION.value == "AGGREGATION"

    def test_window(self) -> None:
        assert ParameterFamily.WINDOW.value == "WINDOW"

    def test_algorithm(self) -> None:
        assert ParameterFamily.ALGORITHM.value == "ALGORITHM"

    def test_all_members(self) -> None:
        assert {m.value for m in ParameterFamily} == {
            "TEMPORAL",
            "STRATEGY",
            "FILTER",
            "AGGREGATION",
            "WINDOW",
            "ALGORITHM",
        }


# ---------------------------------------------------------------------------
# ParameterTypeKind enum
# ---------------------------------------------------------------------------


class TestParameterTypeKind:
    """Three computational types from §8.15 stratification."""

    def test_discrete_finite(self) -> None:
        assert ParameterTypeKind.DISCRETE_FINITE.value == "DISCRETE_FINITE"

    def test_discrete_infinite(self) -> None:
        assert ParameterTypeKind.DISCRETE_INFINITE.value == "DISCRETE_INFINITE"

    def test_continuous(self) -> None:
        assert ParameterTypeKind.CONTINUOUS.value == "CONTINUOUS"

    def test_all_members(self) -> None:
        assert {m.value for m in ParameterTypeKind} == {
            "DISCRETE_FINITE",
            "DISCRETE_INFINITE",
            "CONTINUOUS",
        }


# ---------------------------------------------------------------------------
# ParameterSlot dataclass
# ---------------------------------------------------------------------------


class TestParameterSlot:
    """ParameterSlot: a single parameterisable dimension within Π(q)."""

    def test_basic_fields(self) -> None:
        slot = ParameterSlot(
            name="as_of",
            family=ParameterFamily.TEMPORAL,
            type_kind=ParameterTypeKind.CONTINUOUS,
        )
        assert slot.name == "as_of"
        assert slot.family is ParameterFamily.TEMPORAL
        assert slot.type_kind is ParameterTypeKind.CONTINUOUS
        assert slot.allowed_values is None

    def test_with_allowed_values(self) -> None:
        slot = ParameterSlot(
            name="agg_fn",
            family=ParameterFamily.AGGREGATION,
            type_kind=ParameterTypeKind.DISCRETE_FINITE,
            allowed_values=frozenset({"SUM", "COUNT", "AVG"}),
        )
        assert slot.allowed_values == frozenset({"SUM", "COUNT", "AVG"})

    def test_is_discrete(self) -> None:
        slot_fin = ParameterSlot(
            name="resolution",
            family=ParameterFamily.TEMPORAL,
            type_kind=ParameterTypeKind.DISCRETE_FINITE,
        )
        slot_inf = ParameterSlot(
            name="k",
            family=ParameterFamily.STRATEGY,
            type_kind=ParameterTypeKind.DISCRETE_INFINITE,
        )
        slot_cont = ParameterSlot(
            name="damping",
            family=ParameterFamily.ALGORITHM,
            type_kind=ParameterTypeKind.CONTINUOUS,
        )
        assert slot_fin.is_discrete is True
        assert slot_inf.is_discrete is True
        assert slot_cont.is_discrete is False

    def test_frozen(self) -> None:
        slot = ParameterSlot(
            name="as_of",
            family=ParameterFamily.TEMPORAL,
            type_kind=ParameterTypeKind.CONTINUOUS,
        )
        with pytest.raises(AttributeError):
            slot.name = "changed"  # type: ignore[misc]

    def test_equality(self) -> None:
        a = ParameterSlot("k", ParameterFamily.STRATEGY, ParameterTypeKind.DISCRETE_INFINITE)
        b = ParameterSlot("k", ParameterFamily.STRATEGY, ParameterTypeKind.DISCRETE_INFINITE)
        assert a == b

    def test_repr_contains_name(self) -> None:
        slot = ParameterSlot("as_of", ParameterFamily.TEMPORAL, ParameterTypeKind.CONTINUOUS)
        assert "as_of" in repr(slot)


# ---------------------------------------------------------------------------
# ParameterSpace dataclass
# ---------------------------------------------------------------------------


class TestParameterSpace:
    """ParameterSpace: the product of all ParameterSlots for a question."""

    @pytest.fixture()
    def temporal_slot(self) -> ParameterSlot:
        return ParameterSlot("as_of", ParameterFamily.TEMPORAL, ParameterTypeKind.CONTINUOUS)

    @pytest.fixture()
    def strategy_slot(self) -> ParameterSlot:
        return ParameterSlot(
            "strategy",
            ParameterFamily.STRATEGY,
            ParameterTypeKind.DISCRETE_FINITE,
            allowed_values=frozenset({"ALL", "SHORTEST", "K_SHORTEST", "MIN_WEIGHT"}),
        )

    @pytest.fixture()
    def agg_slot(self) -> ParameterSlot:
        return ParameterSlot(
            "agg_fn",
            ParameterFamily.AGGREGATION,
            ParameterTypeKind.DISCRETE_FINITE,
            allowed_values=frozenset({"SUM", "COUNT", "AVG", "MIN", "MAX"}),
        )

    @pytest.fixture()
    def algo_slot(self) -> ParameterSlot:
        return ParameterSlot(
            "damping",
            ParameterFamily.ALGORITHM,
            ParameterTypeKind.CONTINUOUS,
        )

    def test_empty(self) -> None:
        ps = ParameterSpace(question_id="q1", slots=())
        assert ps.question_id == "q1"
        assert len(ps.slots) == 0

    def test_basic_construction(
        self, temporal_slot: ParameterSlot, strategy_slot: ParameterSlot
    ) -> None:
        ps = ParameterSpace(question_id="q1", slots=(temporal_slot, strategy_slot))
        assert len(ps.slots) == 2

    def test_families(
        self,
        temporal_slot: ParameterSlot,
        strategy_slot: ParameterSlot,
        agg_slot: ParameterSlot,
    ) -> None:
        ps = ParameterSpace(
            question_id="q1", slots=(temporal_slot, strategy_slot, agg_slot)
        )
        fams = ps.families
        assert ParameterFamily.TEMPORAL in fams
        assert ParameterFamily.STRATEGY in fams
        assert ParameterFamily.AGGREGATION in fams
        assert ParameterFamily.WINDOW not in fams

    def test_slots_by_family(
        self,
        temporal_slot: ParameterSlot,
        strategy_slot: ParameterSlot,
        agg_slot: ParameterSlot,
    ) -> None:
        ps = ParameterSpace(
            question_id="q1", slots=(temporal_slot, strategy_slot, agg_slot)
        )
        assert ps.slots_by_family(ParameterFamily.TEMPORAL) == (temporal_slot,)
        assert ps.slots_by_family(ParameterFamily.STRATEGY) == (strategy_slot,)
        assert ps.slots_by_family(ParameterFamily.WINDOW) == ()

    def test_discrete_skeleton(
        self,
        temporal_slot: ParameterSlot,
        strategy_slot: ParameterSlot,
        algo_slot: ParameterSlot,
    ) -> None:
        ps = ParameterSpace(
            question_id="q1", slots=(temporal_slot, strategy_slot, algo_slot)
        )
        skeleton = ps.discrete_skeleton
        # Only strategy_slot is DISCRETE_FINITE; algo is continuous, temporal is continuous
        assert len(skeleton) == 1
        assert skeleton[0] is strategy_slot

    def test_frozen(self, temporal_slot: ParameterSlot) -> None:
        ps = ParameterSpace(question_id="q1", slots=(temporal_slot,))
        with pytest.raises(AttributeError):
            ps.question_id = "changed"  # type: ignore[misc]

    def test_equality(self, temporal_slot: ParameterSlot) -> None:
        a = ParameterSpace(question_id="q1", slots=(temporal_slot,))
        b = ParameterSpace(question_id="q1", slots=(temporal_slot,))
        assert a == b

    def test_slot_count(
        self,
        temporal_slot: ParameterSlot,
        strategy_slot: ParameterSlot,
        agg_slot: ParameterSlot,
        algo_slot: ParameterSlot,
    ) -> None:
        ps = ParameterSpace(
            question_id="q1",
            slots=(temporal_slot, strategy_slot, agg_slot, algo_slot),
        )
        assert ps.slot_count == 4

    def test_has_family(
        self, temporal_slot: ParameterSlot, strategy_slot: ParameterSlot
    ) -> None:
        ps = ParameterSpace(question_id="q1", slots=(temporal_slot, strategy_slot))
        assert ps.has_family(ParameterFamily.TEMPORAL) is True
        assert ps.has_family(ParameterFamily.ALGORITHM) is False

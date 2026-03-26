"""Tests for sqldim.medallion — Layer enum and MedallionRegistry."""

import pytest
from sqldim.medallion import Layer, MedallionRegistry


# ── Layer enum ────────────────────────────────────────────────────────────────


class TestLayer:
    def test_has_three_tiers(self):
        assert {Layer.BRONZE, Layer.SILVER, Layer.GOLD} == set(Layer)

    def test_string_values(self):
        assert Layer.BRONZE.value == "bronze"
        assert Layer.SILVER.value == "silver"
        assert Layer.GOLD.value == "gold"

    def test_from_string(self):
        assert Layer("bronze") is Layer.BRONZE
        assert Layer("silver") is Layer.SILVER
        assert Layer("gold") is Layer.GOLD

    def test_invalid_string_raises(self):
        with pytest.raises(ValueError):
            Layer("platinum")

    def test_str_coercion_is_value(self):
        # Layer is a str enum — useful in f-strings and dicts
        assert str(Layer.BRONZE) == "bronze"

    def test_ordered_index(self):
        order = [Layer.BRONZE, Layer.SILVER, Layer.GOLD]
        assert order.index(Layer.BRONZE) < order.index(Layer.SILVER)
        assert order.index(Layer.SILVER) < order.index(Layer.GOLD)

    # can_promote_to
    def test_bronze_can_promote_to_silver(self):
        assert Layer.BRONZE.can_promote_to(Layer.SILVER) is True

    def test_silver_can_promote_to_gold(self):
        assert Layer.SILVER.can_promote_to(Layer.GOLD) is True

    def test_cannot_skip_layers(self):
        assert Layer.BRONZE.can_promote_to(Layer.GOLD) is False

    def test_cannot_promote_backwards(self):
        assert Layer.GOLD.can_promote_to(Layer.SILVER) is False
        assert Layer.SILVER.can_promote_to(Layer.BRONZE) is False

    def test_cannot_promote_to_self(self):
        assert Layer.BRONZE.can_promote_to(Layer.BRONZE) is False


# ── MedallionRegistry ─────────────────────────────────────────────────────────


class TestMedallionRegistry:
    def setup_method(self):
        self.reg = MedallionRegistry()

    def test_empty_registry(self):
        assert self.reg.datasets_in(Layer.BRONZE) == []

    def test_register_and_get_layer(self):
        self.reg.register("raw_orders", Layer.BRONZE)
        assert self.reg.get_layer("raw_orders") is Layer.BRONZE

    def test_register_multiple_datasets(self):
        self.reg.register("raw_orders", Layer.BRONZE)
        self.reg.register("clean_orders", Layer.SILVER)
        self.reg.register("daily_revenue", Layer.GOLD)
        assert self.reg.get_layer("raw_orders") is Layer.BRONZE
        assert self.reg.get_layer("clean_orders") is Layer.SILVER
        assert self.reg.get_layer("daily_revenue") is Layer.GOLD

    def test_datasets_in_layer(self):
        self.reg.register("a", Layer.BRONZE)
        self.reg.register("b", Layer.BRONZE)
        self.reg.register("c", Layer.SILVER)
        bronze = self.reg.datasets_in(Layer.BRONZE)
        assert sorted(bronze) == ["a", "b"]
        assert self.reg.datasets_in(Layer.SILVER) == ["c"]
        assert self.reg.datasets_in(Layer.GOLD) == []

    def test_is_registered_true(self):
        self.reg.register("x", Layer.GOLD)
        assert self.reg.is_registered("x") is True

    def test_is_registered_false(self):
        assert self.reg.is_registered("unknown") is False

    def test_get_layer_unknown_raises(self):
        with pytest.raises(KeyError):
            self.reg.get_layer("nonexistent")

    def test_re_register_updates_layer(self):
        self.reg.register("ds", Layer.BRONZE)
        self.reg.register("ds", Layer.SILVER)  # promote
        assert self.reg.get_layer("ds") is Layer.SILVER

    def test_all_datasets(self):
        self.reg.register("a", Layer.BRONZE)
        self.reg.register("b", Layer.GOLD)
        assert sorted(self.reg.all_datasets()) == ["a", "b"]

    def test_all_datasets_empty(self):
        assert self.reg.all_datasets() == []

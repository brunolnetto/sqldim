"""Tests for sqldim/examples/features/dgm/models.py — SQLModel declarations.

Importing the five model classes exercises every class-body line (class
headers and field declarations are evaluated at import time in Python).
"""

from sqldim.application.examples.features.dgm.models import (
    CustomerDim,
    ProductDim,
    SegmentDim,
    SaleFact,
    ProductSegmentBridge,
)


class TestDGMModels:
    """Import-and-introspect tests to drive 100% line coverage of models.py."""

    def test_customer_dim_tablename_and_natural_key(self):
        assert CustomerDim.__tablename__ == "dgm_showcase_customer"
        assert CustomerDim.__natural_key__ == ["email"]

    def test_product_dim_tablename_and_natural_key(self):
        assert ProductDim.__tablename__ == "dgm_showcase_product"
        assert ProductDim.__natural_key__ == ["sku"]

    def test_segment_dim_tablename_and_natural_key(self):
        assert SegmentDim.__tablename__ == "dgm_showcase_segment"
        assert SegmentDim.__natural_key__ == ["code"]

    def test_sale_fact_tablename_and_grain(self):
        assert SaleFact.__tablename__ == "dgm_showcase_sale"
        assert SaleFact.__grain__ == "one row per transaction"

    def test_bridge_tablename_and_bridge_keys(self):
        assert ProductSegmentBridge.__tablename__ == "dgm_showcase_prod_seg"
        assert ProductSegmentBridge.__bridge_keys__ == ["product_id", "segment_id"]

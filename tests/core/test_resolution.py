import pytest
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine, SQLModel, Field as SM_Field
from sqldim import DimensionModel, Field, SCD2Mixin
from sqldim.core.loaders.resolution import SKResolver, ConfigurationError
from sqldim.exceptions import SKResolutionError


class VendorDim(DimensionModel, SCD2Mixin, table=True):
    __natural_key__ = ["vendor_code"]
    id: int = Field(primary_key=True, surrogate_key=True)
    vendor_code: str
    region: str


class InferredVendorDim(DimensionModel, SCD2Mixin, table=True):
    """Dimension with is_inferred support for late-arriving member tests."""

    __natural_key__ = ["vendor_code"]
    id: int = Field(primary_key=True, surrogate_key=True)
    vendor_code: str
    region: str = SM_Field(default="UNKNOWN")
    is_inferred: bool = SM_Field(default=False)


@pytest.fixture(scope="module")
def session():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        v = VendorDim(vendor_code="V1", region="US", is_current=True)
        s.add(v)
        s.commit()
        s.refresh(v)
        yield s
    engine.dispose()


@pytest.fixture(scope="module")
def inferred_session():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        v = InferredVendorDim(vendor_code="V1", region="US", is_current=True)
        s.add(v)
        s.commit()
        s.refresh(v)
        yield s
    engine.dispose()


def test_resolve_hits_db(session):
    resolver = SKResolver(session)
    sk = resolver.resolve(VendorDim, "vendor_code", "V1")
    assert sk is not None


def test_resolve_cache_hit(session):
    resolver = SKResolver(session)
    sk1 = resolver.resolve(VendorDim, "vendor_code", "V1")
    sk2 = resolver.resolve(VendorDim, "vendor_code", "V1")
    assert sk1 == sk2
    assert (VendorDim, "vendor_code", "V1") in resolver._cache


def test_resolve_missing_returns_none(session):
    resolver = SKResolver(session)
    sk = resolver.resolve(VendorDim, "vendor_code", "MISSING")
    assert sk is None


def test_resolve_missing_raises_when_configured(session):
    resolver = SKResolver(session, raise_on_missing=True)
    with pytest.raises(SKResolutionError):
        resolver.resolve(VendorDim, "vendor_code", "MISSING")


def test_resolve_multi(session):
    resolver = SKResolver(session)
    sk = resolver.resolve_multi(VendorDim, {"vendor_code": "V1", "region": "US"})
    assert sk is not None


def test_resolve_multi_missing_raises(session):
    resolver = SKResolver(session, raise_on_missing=True)
    with pytest.raises(SKResolutionError):
        resolver.resolve_multi(VendorDim, {"vendor_code": "NOPE", "region": "EU"})


def test_resolve_multi_cache_hit(session):
    resolver = SKResolver(session)
    sk1 = resolver.resolve_multi(VendorDim, {"vendor_code": "V1", "region": "US"})
    # Second call must hit cache (line 51)
    sk2 = resolver.resolve_multi(VendorDim, {"vendor_code": "V1", "region": "US"})
    assert sk1 == sk2
    cache_key = (
        VendorDim,
        tuple(sorted({"vendor_code": "V1", "region": "US"}.items())),
    )
    assert cache_key in resolver._cache


def test_warm_preloads_cache(session):
    resolver = SKResolver(session)
    count = resolver.warm(VendorDim, "vendor_code")
    assert count == 1
    assert (VendorDim, "vendor_code", "V1") in resolver._cache


def test_clear_empties_cache(session):
    resolver = SKResolver(session)
    resolver.warm(VendorDim, "vendor_code")
    resolver.clear()
    assert resolver._cache == {}


# ---------------------------------------------------------------------------
# infer_missing paths
# ---------------------------------------------------------------------------


def test_require_is_inferred_raises_when_field_absent(session):
    resolver = SKResolver(session, infer_missing=True)
    with pytest.raises(ConfigurationError, match="is_inferred"):
        resolver._require_is_inferred_column(VendorDim)


def test_infer_missing_creates_placeholder(inferred_session):
    resolver = SKResolver(inferred_session, infer_missing=True)
    sk = resolver.resolve(InferredVendorDim, "vendor_code", "NEW_VENDOR")
    assert sk is not None
    # Placeholder should be in DB
    from sqlmodel import select

    rows = inferred_session.exec(
        select(InferredVendorDim).where(InferredVendorDim.vendor_code == "NEW_VENDOR")
    ).all()
    assert len(rows) == 1
    assert rows[0].is_inferred is True


def test_infer_missing_uses_inferred_defaults(inferred_session):
    resolver = SKResolver(
        inferred_session,
        infer_missing=True,
        inferred_defaults={"region": "PLACEHOLDER"},
    )
    resolver.resolve(InferredVendorDim, "vendor_code", "UNKNOWN_V")
    from sqlmodel import select

    row = inferred_session.exec(
        select(InferredVendorDim).where(InferredVendorDim.vendor_code == "UNKNOWN_V")
    ).one()
    assert row.region == "PLACEHOLDER"


def test_infer_missing_caches_result(inferred_session):
    resolver = SKResolver(inferred_session, infer_missing=True)
    sk1 = resolver.resolve(InferredVendorDim, "vendor_code", "CACHE_V")
    sk2 = resolver.resolve(InferredVendorDim, "vendor_code", "CACHE_V")
    assert sk1 == sk2
    assert (InferredVendorDim, "vendor_code", "CACHE_V") in resolver._cache


def test_infer_missing_emits_lineage_event(inferred_session):
    events = []
    resolver = SKResolver(
        inferred_session,
        infer_missing=True,
        lineage_emitter=events.append,
    )
    resolver.resolve(
        InferredVendorDim, "vendor_code", "LINEAGE_V", fact_table="sales_fact"
    )
    assert len(events) == 1
    from sqldim.lineage.events import InferredMemberEventType

    assert events[0].event_type == InferredMemberEventType.CREATED
    assert events[0].natural_key == "LINEAGE_V"
    assert events[0].fact_table == "sales_fact"


def test_infer_missing_resolve_multi(inferred_session):
    resolver = SKResolver(inferred_session, infer_missing=True)
    sk = resolver.resolve_multi(
        InferredVendorDim,
        {"vendor_code": "MULTI_V", "region": "EU"},
    )
    assert sk is not None
    from sqlmodel import select

    row = inferred_session.exec(
        select(InferredVendorDim).where(InferredVendorDim.vendor_code == "MULTI_V")
    ).one()
    assert row.is_inferred is True
    assert row.region == "EU"


# ---------------------------------------------------------------------------
# SCD2 temporal field fallback paths (lines 123, 127 in resolution.py)
# A model with valid_from/is_current declared but without default_factory /
# without a "truthy" default causes the explicit fallback assignment.
# ---------------------------------------------------------------------------

from datetime import datetime
from typing import Optional as Opt


class BareTemporalDim(DimensionModel, table=True):
    """Model with SCD2-like fields but no default_factory / default values."""

    __natural_key__ = ["code"]
    id: int = Field(primary_key=True, surrogate_key=True)
    code: str
    is_inferred: bool = SM_Field(default=False)
    # valid_from: present but NO default_factory → triggers line 123
    valid_from: Opt[datetime] = SM_Field(default=None)
    # is_current: present with default=None → condition `default is not None` is False → triggers line 127
    is_current: Opt[bool] = SM_Field(default=None)


@pytest.fixture(scope="module")
def bare_temporal_session():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        yield s
    engine.dispose()


def test_infer_missing_sets_scd2_temporal_fallbacks(bare_temporal_session):
    """Covers resolution.py lines 123 (valid_from fallback) and 127 (is_current fallback)."""
    resolver = SKResolver(bare_temporal_session, infer_missing=True)
    sk = resolver.resolve(BareTemporalDim, "code", "BARE_01")
    assert sk is not None
    from sqlmodel import select

    row = bare_temporal_session.exec(
        select(BareTemporalDim).where(BareTemporalDim.code == "BARE_01")
    ).one()
    assert row.valid_from is not None  # was set by fallback (line 123)
    assert row.is_current is True  # was set by fallback (line 127)

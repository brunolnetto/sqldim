import pytest
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine, SQLModel
from sqldim import DimensionModel, Field, SCD2Mixin
from sqldim.loaders.resolution import SKResolver
from sqldim.exceptions import SKResolutionError

class VendorDim(DimensionModel, SCD2Mixin, table=True):
    __natural_key__ = ["vendor_code"]
    id: int = Field(primary_key=True, surrogate_key=True)
    vendor_code: str
    region: str

@pytest.fixture
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
    cache_key = (VendorDim, tuple(sorted({"vendor_code": "V1", "region": "US"}.items())))
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

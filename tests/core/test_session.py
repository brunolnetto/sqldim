import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from sqldim.session import AsyncDimensionalSession
from sqldim.config import SqldimConfig

def test_from_url_creates_instance():
    session = AsyncDimensionalSession.from_url("sqlite+aiosqlite:///:memory:")
    assert isinstance(session, AsyncDimensionalSession)
    assert isinstance(session.config, SqldimConfig)

def test_custom_config():
    cfg = SqldimConfig(default_batch_size=500)
    from sqlalchemy.ext.asyncio import create_async_engine
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    session = AsyncDimensionalSession(engine, config=cfg)
    assert session.config.default_batch_size == 500

@pytest.mark.asyncio
async def test_aenter_yields_session():
    ads = AsyncDimensionalSession.from_url("sqlite+aiosqlite:///:memory:")
    async with ads as session:
        assert session is not None

@pytest.mark.asyncio
async def test_aexit_is_noop():
    ads = AsyncDimensionalSession.from_url("sqlite+aiosqlite:///:memory:")
    # __aexit__ is called implicitly by async with; test it directly too
    result = await ads.__aexit__(None, None, None)
    assert result is None

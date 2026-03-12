from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, AsyncEngine
from sqldim.config import SqldimConfig

class AsyncDimensionalSession:
    """
    Async session factory for sqldim operations.

    Usage:
        async with AsyncDimensionalSession(engine) as session:
            loader = DimensionalLoader(session)
            await loader.run()
    """

    def __init__(self, engine: AsyncEngine, config: SqldimConfig | None = None):
        self._engine = engine
        self.config = config or SqldimConfig()
        self._session: AsyncSession | None = None

    async def __aenter__(self) -> AsyncSession:
        self._session = AsyncSession(self._engine)
        return self._session

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    @classmethod
    def from_url(cls, url: str, **kwargs) -> "AsyncDimensionalSession":
        """Create a session from a connection URL (e.g. 'sqlite+aiosqlite:///db.sqlite')."""
        engine = create_async_engine(url, **kwargs)
        return cls(engine)

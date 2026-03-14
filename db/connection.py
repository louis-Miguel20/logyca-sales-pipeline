import asyncpg
from contextlib import asynccontextmanager
from config.settings import get_settings

_pool = None

async def create_pool():
    global _pool
    settings = get_settings()
    if _pool is None:
        _pool = await asyncpg.create_pool(
            dsn=settings.database_url,
            min_size=2,
            max_size=10
        )
    return _pool

async def get_pool():
    global _pool
    if _pool is None:
        await create_pool()
    return _pool

async def close_pool():
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None

@asynccontextmanager
async def get_connection():
    pool = await get_pool()
    async with pool.acquire() as connection:
        yield connection

"""Shared asyncpg pool management for the pipeline.

Eliminates per-module pool creation. All stages share pools from here.
"""
from __future__ import annotations

import os
import socket
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

import asyncpg

# Module-level pool singletons (set by init_pools, closed by close_pools)
_db1_pool: Optional[asyncpg.Pool] = None
_db2_pool: Optional[asyncpg.Pool] = None


async def _init_db1_conn(conn: asyncpg.Connection) -> None:
    await conn.execute("SET statement_timeout = '600000'")  # 10 min


async def _init_db2_conn(conn: asyncpg.Connection) -> None:
    await conn.execute("SET statement_timeout = '300000'")  # 5 min


async def init_pools(
    db1_url: Optional[str] = None,
    db2_url: Optional[str] = None,
    db1_min: int = 2,
    db1_max: int = 5,
    db2_min: int = 1,
    db2_max: int = 3,
) -> None:
    """Create shared DB1/DB2 pools. Call once at pipeline start."""
    global _db1_pool, _db2_pool

    db1_url = db1_url or os.environ.get("DATABASE_URL", "")
    db2_url = db2_url or os.environ.get("DB2_DATABASE_URL", "")

    if db1_url and _db1_pool is None:
        try:
            _db1_pool = await asyncpg.create_pool(
                dsn=db1_url,
                min_size=db1_min,
                max_size=db1_max,
                statement_cache_size=0,
                command_timeout=1200,
                init=_init_db1_conn,
                family=socket.AF_INET,
            )
        except OSError as exc:
            host = db1_url.split("@")[-1].split("/")[0] if "@" in db1_url else db1_url
            raise RuntimeError(
                f"DB1 connection failed ({host}): {exc}\n"
                f"Hint: ensure DATABASE_URL uses the Supabase pooler (port 6543), "
                f"not the direct connection (port 5432). "
                f"Find it in Supabase Dashboard → Settings → Database → Connection string."
            ) from exc

    if db2_url and _db2_pool is None:
        try:
            _db2_pool = await asyncpg.create_pool(
                dsn=db2_url,
                min_size=db2_min,
                max_size=db2_max,
                statement_cache_size=0,
                command_timeout=300,
                init=_init_db2_conn,
                family=socket.AF_INET,
            )
        except OSError as exc:
            host = db2_url.split("@")[-1].split("/")[0] if "@" in db2_url else db2_url
            raise RuntimeError(
                f"DB2 connection failed ({host}): {exc}\n"
                f"Hint: ensure DB2_DATABASE_URL uses the Supabase pooler (port 6543), "
                f"not the direct connection (port 5432). "
                f"Find it in Supabase Dashboard → Settings → Database → Connection string."
            ) from exc


async def close_pools() -> None:
    """Close all pools. Call at pipeline end."""
    global _db1_pool, _db2_pool
    if _db1_pool:
        await _db1_pool.close()
        _db1_pool = None
    if _db2_pool:
        await _db2_pool.close()
        _db2_pool = None


def get_db1_pool() -> asyncpg.Pool:
    """Get the shared DB1 pool."""
    if _db1_pool is None:
        raise RuntimeError("DB1 pool not initialized. Call init_pools() first.")
    return _db1_pool


def get_db2_pool() -> asyncpg.Pool:
    """Get the shared DB2 pool."""
    if _db2_pool is None:
        raise RuntimeError("DB2 pool not initialized. Call init_pools() first.")
    return _db2_pool


@asynccontextmanager
async def db1_conn() -> AsyncGenerator[asyncpg.Connection, None]:
    """Get a DB1 connection from the shared pool."""
    pool = get_db1_pool()
    async with pool.acquire() as conn:
        yield conn


@asynccontextmanager
async def db2_conn() -> AsyncGenerator[asyncpg.Connection, None]:
    """Get a DB2 connection from the shared pool."""
    pool = get_db2_pool()
    async with pool.acquire() as conn:
        yield conn


async def db1_health_check() -> bool:
    """Check DB1 pool health."""
    try:
        pool = get_db1_pool()
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return True
    except Exception:
        return False


async def db2_health_check() -> bool:
    """Check DB2 pool health."""
    try:
        pool = get_db2_pool()
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return True
    except Exception:
        return False


async def db1_reconnect() -> None:
    """Recreate DB1 pool if connections are stale."""
    global _db1_pool
    if _db1_pool:
        await _db1_pool.close()
    db1_url = os.environ.get("DATABASE_URL", "")
    _db1_pool = await asyncpg.create_pool(
        dsn=db1_url,
        min_size=2,
        max_size=5,
        statement_cache_size=0,
        command_timeout=1200,
        init=_init_db1_conn,
        family=socket.AF_INET,
    )


async def db2_reconnect() -> None:
    """Recreate DB2 pool if connections are stale."""
    global _db2_pool
    if _db2_pool:
        await _db2_pool.close()
    db2_url = os.environ.get("DB2_DATABASE_URL", "")
    _db2_pool = await asyncpg.create_pool(
        dsn=db2_url,
        min_size=1,
        max_size=3,
        statement_cache_size=0,
        command_timeout=300,
        init=_init_db2_conn,
        family=socket.AF_INET,
    )

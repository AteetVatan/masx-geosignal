"""SQLAlchemy async engine + session factory."""

from __future__ import annotations

from functools import lru_cache

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from core.config import get_settings


@lru_cache(maxsize=1)
def get_async_engine() -> AsyncEngine:
    """Create and cache an async engine.

    NOTE: Supabase uses pgBouncer in transaction mode, which does NOT
    support prepared statements. We must disable asyncpg's statement
    cache via connect_args.
    """
    settings = get_settings()
    return create_async_engine(
        settings.database_url,
        echo=False,
        pool_size=20,
        max_overflow=10,
        pool_pre_ping=True,
        pool_recycle=300,
        connect_args={
            "statement_cache_size": 0,
            "prepared_statement_cache_size": 0,
            # pgBouncer doesn't support named prepared statements at all.
            # Returning "" forces asyncpg to use unnamed (anonymous) statements.
            "prepared_statement_name_func": lambda: "",
        },
    )


def get_async_session() -> async_sessionmaker[AsyncSession]:
    """Return an async session factory."""
    return async_sessionmaker(
        get_async_engine(),
        class_=AsyncSession,
        expire_on_commit=False,
    )

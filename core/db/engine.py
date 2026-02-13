"""SQLAlchemy async engine + session factory."""

from __future__ import annotations

import asyncio
import functools
from collections.abc import Callable
from functools import lru_cache
from typing import Any, TypeVar

import structlog
from sqlalchemy.exc import DBAPIError, DisconnectionError, OperationalError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from core.config import get_settings

logger = structlog.get_logger(__name__)

T = TypeVar("T")


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


def retry_on_disconnect(
    max_retries: int = 3,
    base_delay: float = 1.0,
) -> Callable[..., Any]:
    """Decorator that retries an async function on transient DB disconnects.

    Handles the case where a long-held session's underlying TCP connection
    drops (e.g. network blip to Supabase/pgBouncer). On disconnect, the
    connection is invalidated so the pool replaces it on the next use.

    Retries with exponential backoff: base_delay * 2^attempt seconds.
    """

    _TRANSIENT_ERRORS = (
        ConnectionResetError,
        ConnectionRefusedError,
        ConnectionAbortedError,
        BrokenPipeError,
        OSError,
    )

    def _is_disconnect(exc: BaseException) -> bool:
        """Check if an exception chain indicates a transient disconnect."""
        if isinstance(exc, (DisconnectionError, *_TRANSIENT_ERRORS)):
            return True
        if isinstance(exc, (DBAPIError, OperationalError)):
            # Walk the cause chain for underlying connection errors
            cause = exc.__cause__
            while cause is not None:
                if isinstance(cause, _TRANSIENT_ERRORS):
                    return True
                # asyncpg-specific error names
                cause_name = type(cause).__name__
                if cause_name in (
                    "ConnectionDoesNotExistError",
                    "InterfaceError",
                    "InternalClientError",
                ):
                    return True
                cause = cause.__cause__
        return False

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exc: BaseException | None = None
            for attempt in range(max_retries + 1):
                try:
                    return await fn(*args, **kwargs)
                except Exception as exc:
                    if not _is_disconnect(exc) or attempt == max_retries:
                        raise
                    last_exc = exc
                    delay = base_delay * (2 ** attempt)
                    logger.warning(
                        "transient_db_disconnect_retrying",
                        attempt=attempt + 1,
                        max_retries=max_retries,
                        delay_s=delay,
                        error=str(exc),
                    )
                    # Invalidate the connection so the pool drops it
                    session = _find_session(args, kwargs)
                    if session is not None:
                        conn = await session.connection()
                        await conn.invalidate()
                    await asyncio.sleep(delay)
            raise last_exc  # type: ignore[misc]  # unreachable

        return wrapper
    return decorator


def _find_session(args: tuple[Any, ...], kwargs: dict[str, Any]) -> AsyncSession | None:
    """Try to extract an AsyncSession from method args (self.session pattern)."""
    # Check kwargs first
    if "session" in kwargs and isinstance(kwargs["session"], AsyncSession):
        return kwargs["session"]
    # Check if first arg is a repo-like object with .session
    if args and hasattr(args[0], "session") and isinstance(args[0].session, AsyncSession):
        return args[0].session
    return None

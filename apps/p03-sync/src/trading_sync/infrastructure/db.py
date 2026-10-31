"""Engine, session factory and SQLSTATE classification.

Two things live here that the rest of the sink depends on.

**One session per task.** :func:`session_factory` returns a factory, never a
shared session. SQLAlchemy's ``AsyncSession`` is not safe to share across
concurrent tasks, and sharing one is the kind of bug that shows up as
corrupted-looking data rather than as an obvious error (design 8.4, WF3-128).

**SQLSTATE drives retry policy, not exception text.** :func:`classify_db_error`
maps a driver error onto our taxonomy. Only ``40001`` (serialization failure)
and ``40P01`` (deadlock) become retryable, and even then the *whole*
transaction is replayed -- a partial retry would not be idempotent. A lost
connection becomes :class:`DatabaseUnavailableError`, which deliberately does
not claim to know whether the COMMIT landed (design 6.5, 9.2).
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from sqlalchemy.exc import (
    DBAPIError,
    DisconnectionError,
    IntegrityError,
    InterfaceError,
    OperationalError,
    SQLAlchemyError,
)
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from trading_sync.config import Settings
from trading_sync.errors import (
    DatabaseUnavailableError,
    StorageFullError,
    SyncError,
    TransientDatabaseError,
)

__all__ = [
    "RETRYABLE_SQLSTATES",
    "classify_db_error",
    "create_engine",
    "session_factory",
    "transaction",
]

#: Only these two. Widening this set is how a genuine integrity failure ends up
#: being retried until it looks intermittent (design 9.2).
RETRYABLE_SQLSTATES = frozenset({"40001", "40P01"})

_DISK_FULL_SQLSTATES = frozenset({"53100", "53200", "53300", "53400"})
_CONNECTION_SQLSTATES = frozenset({"08000", "08003", "08006", "08001", "08004", "57P01", "57P02", "57P03"})


def create_engine(settings: Settings) -> AsyncEngine:
    """Build the pooled async engine.

    The pool is bounded and ``pool_pre_ping`` is on: a connection killed by a
    database restart should surface as one failed acquisition rather than as a
    mid-transaction surprise (WF3-119).
    """
    return create_async_engine(
        settings.sqlalchemy_url,
        pool_size=settings.postgres.pool_size,
        max_overflow=settings.postgres.max_overflow,
        pool_timeout=settings.postgres.pool_timeout_seconds,
        pool_pre_ping=True,
        echo=settings.postgres.echo_sql,
        connect_args=settings.connect_args(),
    )


def session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Return a factory producing one independent session per call."""
    return async_sessionmaker(engine, expire_on_commit=False, autoflush=False)


@asynccontextmanager
async def transaction(
    factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncSession]:
    """Run a block in one transaction, translating driver errors on the way out.

    Exceptions are classified here so that callers reason about
    :class:`~trading_sync.errors.SyncError` subclasses rather than about
    driver internals.
    """
    session = factory()
    try:
        async with session.begin():
            yield session
    except SyncError:
        raise
    except (SQLAlchemyError, OSError) as exc:
        # OSError matters: a refused or reset TCP connection surfaces as a bare
        # ConnectionError from the driver without a SQLSTATE, and letting it
        # escape unclassified would bypass the whole retry policy.
        raise classify_db_error(exc) from exc
    finally:
        await session.close()


def classify_db_error(exc: BaseException) -> SyncError:
    """Translate a SQLAlchemy or driver exception into our taxonomy."""
    sqlstate = _extract_sqlstate(exc)
    if sqlstate in RETRYABLE_SQLSTATES:
        return TransientDatabaseError(
            "database transaction must be replayed", sqlstate=sqlstate
        )
    if sqlstate in _DISK_FULL_SQLSTATES:
        return StorageFullError("database is out of storage", sqlstate=sqlstate)
    if sqlstate in _CONNECTION_SQLSTATES or isinstance(exc, OperationalError):
        return DatabaseUnavailableError(
            "database connection failed; commit outcome is unknown",
            sqlstate=sqlstate,
        )
    if _looks_disconnected(exc):
        return DatabaseUnavailableError(
            "database connection failed; commit outcome is unknown",
            sqlstate=sqlstate,
        )
    if isinstance(exc, IntegrityError):
        # A constraint fired that the application did not anticipate. Not
        # retryable: the same statement would fail the same way.
        return SyncError("unexpected integrity violation", sqlstate=sqlstate)
    return SyncError("database error", sqlstate=sqlstate, kind=type(exc).__name__)


def _looks_disconnected(exc: BaseException) -> bool:
    """Recognise a lost connection that carries no usable SQLSTATE.

    A terminated backend, a refused TCP connect and a reset socket all arrive
    this way. Each is reported as "outcome unknown" rather than as a failure,
    because a connection lost during COMMIT proves nothing either way
    (design 6.5).
    """
    if isinstance(exc, (InterfaceError, DisconnectionError)):
        return True
    if getattr(exc, "connection_invalidated", False):
        return True
    for candidate in (exc, getattr(exc, "orig", None)):
        if isinstance(candidate, OSError):
            return True
        if candidate is not None and type(candidate).__name__ in {
            "ConnectionDoesNotExistError",
            "ConnectionFailureError",
            "ConnectionRefusedError",
            "ConnectionResetError",
            "AdminShutdownError",
            "CannotConnectNowError",
            "TooManyConnectionsError",
        }:
            return True
    return False


def _extract_sqlstate(exc: BaseException) -> str | None:
    """Pull a SQLSTATE out of whatever layer happens to be carrying it."""
    orig = getattr(exc, "orig", None)
    for candidate in (orig, exc):
        if candidate is None:
            continue
        code = getattr(candidate, "sqlstate", None)
        if code:
            return str(code)
        pgcode = getattr(candidate, "pgcode", None)
        if pgcode:
            return str(pgcode)
    if isinstance(exc, DBAPIError) and exc.orig is not None:
        nested = getattr(exc.orig, "__cause__", None)
        code = getattr(nested, "sqlstate", None)
        if code:
            return str(code)
    return None

"""Publisher-side persistence: schema, the outbox store, and its ports.

``open_store`` is the only place that chooses a backend, so the relay, the
writer and the CLI all stay backend-agnostic and a test can substitute either
one without a conditional of its own.
"""

from __future__ import annotations

from typing import Any

from ..clock import Clock
from ..errors import ConfigurationError
from ..settings import Settings
from .ports import (
    AttemptOutcome,
    Claim,
    EventState,
    OutboxRecord,
    OutboxStats,
    OutboxStore,
    Transaction,
)
from .sqlite_store import SqliteOutboxStore

__all__ = [
    "AttemptOutcome",
    "Claim",
    "EventState",
    "OutboxRecord",
    "OutboxStats",
    "OutboxStore",
    "SqliteOutboxStore",
    "Transaction",
    "open_store",
]


def open_store(settings: Settings, *, clock: Clock | None = None) -> Any:
    """Build the configured store. Does not migrate."""
    if settings.store_backend == "sqlite":
        return SqliteOutboxStore(settings.sqlite_path, clock=clock)
    if settings.store_backend == "postgres":
        if not settings.pg_dsn:
            raise ConfigurationError("store_backend=postgres requires pg_dsn")
        from .postgres_store import PostgresOutboxStore  # noqa: PLC0415 - optional dependency

        return PostgresOutboxStore(settings.pg_dsn, clock=clock, pool_max=settings.relay_pg_pool_max)
    raise ConfigurationError(f"unknown store backend {settings.store_backend!r}")

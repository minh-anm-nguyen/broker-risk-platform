"""Alembic environment.

The URL is never read from ``alembic.ini``; it comes from the environment so no
credential is ever committed (design 11.2). ``SYNC_MIGRATION_DATABASE_URL``
takes precedence over ``SYNC_DATABASE_URL`` so the DDL owner can differ from
the application role (design 11.1).
"""

from __future__ import annotations

import asyncio
import os

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from trading_sync.infrastructure.db_models import metadata

config = context.config
target_metadata = metadata


def _database_url() -> str:
    url = os.environ.get("SYNC_MIGRATION_DATABASE_URL") or os.environ.get(
        "SYNC_DATABASE_URL"
    )
    if not url:
        raise RuntimeError(
            "set SYNC_MIGRATION_DATABASE_URL or SYNC_DATABASE_URL before running alembic"
        )
    return url


def run_migrations_offline() -> None:
    context.configure(
        url=_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def _run(connection: Connection) -> None:
    context.configure(
        connection=connection, target_metadata=target_metadata, compare_type=True
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online_async() -> None:
    section = config.get_section(config.config_ini_section, {})
    section["sqlalchemy.url"] = _database_url()
    engine = async_engine_from_config(
        section, prefix="sqlalchemy.", poolclass=pool.NullPool
    )
    async with engine.connect() as connection:
        await connection.run_sync(_run)
    await engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online_async())

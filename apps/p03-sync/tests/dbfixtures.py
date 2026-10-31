"""The autouse table reset, shared by every database-backed package.

It lives here rather than in the root ``conftest`` because it must declare
``sessions`` as a real fixture dependency. Resolving an async fixture lazily
from inside an async fixture fails, and making it autouse at the root would
also force a database on the pure-unit suite, which is exactly what the
design's test pyramid separates (design 15.1).
"""

from __future__ import annotations

from typing import Any, AsyncIterator

import pytest_asyncio
from sqlalchemy import text

#: Children before parents, so foreign keys never block the sweep.
TRUNCATE_ORDER = (
    "sync_audit",
    "quarantine_items",
    "sync_page_receipts",
    "account_staging",
    "position_staging",
    "accounts_current",
    "positions_current",
    "deals",
    "sync_checkpoints",
    "sync_runs",
    "source_epochs",
    "sync_sources",
)


@pytest_asyncio.fixture(autouse=True)
async def clean_database(sessions: Any) -> AsyncIterator[None]:
    """Empty every table before each test.

    ``TRUNCATE`` rather than ``DELETE``: the deals table carries a trigger
    that rejects row deletion, and truncation does not fire row triggers. That
    the trigger blocks ``DELETE`` is asserted separately in ``test_schema``.
    """
    async with sessions() as session:
        await session.execute(text("TRUNCATE " + ", ".join(TRUNCATE_ORDER) + " CASCADE"))
        await session.commit()
    yield

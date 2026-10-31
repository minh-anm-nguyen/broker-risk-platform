"""Staging retention and quota guards (design 9.3, group M/O).

Staging rows are evidence until they are not. The sweep below is deliberately
conservative, because the two ways to get this wrong have very different costs:
keeping rows too long wastes disk, while deleting them too early destroys the
only record of what an aborted scan actually read.

So three guards apply, and all three must hold before a row is removed:

*Terminal status only.* A scan still ``FETCHING``, ``STAGED``, ``VALIDATING``
or ``PUBLISHING`` is live, and a scan being re-run must find its staging
intact. Status is read from the database inside the same transaction as the
delete, so a scan that restarts between the check and the sweep is still
protected (WF3-174, WF3-071).

*Past retention.* Recent evidence stays, even for an aborted scan, so an
incident can still be investigated.

*Bounded batches.* The sweep competes with publishers for the same tables, so
it takes small bites rather than locking a whole table while a promotion waits
(WF3-155).

The quota check is the other half: when staging has grown past its budget, new
full snapshots are refused rather than started. A refused snapshot leaves the
previous published projection serving reads, which is strictly better than
filling the disk and failing mid-promotion (WF3-149).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import and_, delete, func, literal, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from trading_sync.domain.snapshots import Entity, ScanStatus
from trading_sync.errors import StorageFullError
from trading_sync.infrastructure import db_models as m
from trading_sync.infrastructure.db import transaction

__all__ = ["RetentionPolicy", "StagingSweeper", "SweepResult", "TERMINAL_SCAN_STATES"]

#: Only a scan in one of these states has finished with its staging rows.
TERMINAL_SCAN_STATES = (
    str(ScanStatus.PUBLISHED),
    str(ScanStatus.ABORTED),
    str(ScanStatus.QUARANTINED),
    str(ScanStatus.BLOCKED_EPOCH),
)

_STAGING_TABLES = {
    Entity.ACCOUNTS: m.account_staging,
    Entity.POSITIONS: m.position_staging,
}


@dataclass(frozen=True, slots=True)
class RetentionPolicy:
    """How long staging survives, and how much of it may exist at once."""

    retention_seconds: int = 24 * 3600
    #: Per entity. Beyond this, new snapshot scans are refused.
    max_staging_rows: int = 2_000_000
    #: Rows removed per transaction, so the sweep never holds a long lock.
    batch_size: int = 1000


@dataclass(frozen=True, slots=True)
class SweepResult:
    entity: Entity
    rows_removed: int
    scans_cleared: int
    protected_scans: int


class StagingSweeper:
    """Removes staging rows that are provably finished with.

    Runs as a separate privileged task: the ingestion role holds no row-removal
    grant at all, so a compromised or buggy sync worker cannot delete evidence
    even if it tried (design 11.1, WF3-184).
    """

    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        policy: RetentionPolicy | None = None,
    ) -> None:
        self._factory = factory
        self._policy = policy or RetentionPolicy()

    async def staging_rows(self, entity: Entity) -> int:
        table = _require_staging(entity)
        async with transaction(self._factory) as session:
            return int(
                (
                    await session.execute(select(func.count()).select_from(table))
                ).scalar_one()
            )

    async def assert_quota_available(self, entity: Entity) -> None:
        """Refuse a new snapshot when staging is already over budget.

        Refusing keeps the last complete projection serving reads. Starting
        anyway would risk running out of space during the promotion, which is
        the one transaction that must not fail halfway (WF3-149, WF3-232).
        """
        rows = await self.staging_rows(entity)
        if rows >= self._policy.max_staging_rows:
            raise StorageFullError(
                "staging is at its configured quota; refusing a new snapshot",
                entity=str(entity),
                staging_rows=rows,
                max_staging_rows=self._policy.max_staging_rows,
            )

    async def sweep(self, entity: Entity) -> SweepResult:
        """Delete one bounded batch of eligible staging rows."""
        table = _require_staging(entity)
        policy = self._policy
        cutoff = func.clock_timestamp() - func.make_interval(
            0, 0, 0, 0, 0, 0, float(policy.retention_seconds)
        )

        async with transaction(self._factory) as session:
            # Eligibility and deletion in one transaction: a scan that restarts
            # in between would otherwise lose staging it is about to reuse.
            eligible = (
                select(m.sync_runs.c.scan_id)
                .where(
                    and_(
                        m.sync_runs.c.entity == str(entity),
                        m.sync_runs.c.status.in_(TERMINAL_SCAN_STATES),
                        m.sync_runs.c.ended_at.is_not(None),
                        m.sync_runs.c.ended_at < cutoff,
                    )
                )
                .limit(policy.batch_size)
            )
            scan_ids = (await session.execute(eligible)).scalars().all()
            if not scan_ids:
                protected = await self._count_protected(session, entity)
                return SweepResult(entity, 0, 0, protected)

            removed = await session.execute(
                delete(table).where(table.c.scan_id.in_(list(scan_ids)))
            )
            protected = await self._count_protected(session, entity)
            return SweepResult(
                entity=entity,
                rows_removed=removed.rowcount or 0,
                scans_cleared=len(scan_ids),
                protected_scans=protected,
            )

    async def _count_protected(self, session: AsyncSession, entity: Entity) -> int:
        """Scans whose staging the sweep must not touch."""
        return int(
            (
                await session.execute(
                    select(func.count())
                    .select_from(m.sync_runs)
                    .where(
                        and_(
                            m.sync_runs.c.entity == str(entity),
                            m.sync_runs.c.status.not_in(TERMINAL_SCAN_STATES),
                        )
                    )
                )
            ).scalar_one()
        )


def _require_staging(entity: Entity) -> Any:
    table = _STAGING_TABLES.get(entity)
    if table is None:
        raise StorageFullError(
            "only accounts and positions use staging", entity=str(entity)
        )
    return table

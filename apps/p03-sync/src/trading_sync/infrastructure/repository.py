"""The PostgreSQL sink: every durable decision this service makes.

The shape of this module follows one rule from the design: *the guard and the
write share a transaction*. Checking the lease, the epoch status and the
previous watermark in Python and then issuing the write is a
time-of-check/time-of-use gap, and a lease takeover landing inside that gap is
precisely the failure INV-09 exists to prevent. So every state-changing method
starts by locking its checkpoint row and re-reading ownership from the database
(design 8.2, 18.2).

Three consequences worth naming:

*The database clock decides lease validity.* ``clock_timestamp()`` appears in
the guard predicates rather than a Python timestamp, so a worker with a skewed
clock cannot talk itself into ownership (WF3-126).

*A page replay is recognised, not re-applied.* If a receipt already exists with
an identical hash, the page is acknowledged idempotently. That is what makes a
COMMIT whose acknowledgement was lost recoverable without either duplicating or
skipping (design 6.5, WF3-053).

*Quarantine is written in its own transaction.* The page that caused the
incident has been rolled back; the evidence has to outlive that rollback
(design 18.2).
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Mapping, Sequence

from datetime import UTC
from collections.abc import Callable

from sqlalchemy import BigInteger, Text, Uuid, and_, func, insert, literal, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from trading_sync.application.outbox_emission import build_deal_event, outbox_row
from trading_sync.application.ports import (
    EpochHandle,
    Lease,
    PageCommitResult,
    PublicationResult,
    ScanHandle,
)
from trading_sync.config import Settings
from trading_sync.domain.entities import AccountRecord, DealRecord, PositionRecord
from trading_sync.domain.identifiers import FILTER_UNFILTERED
from trading_sync.domain.snapshots import (
    Entity,
    EpochStatus,
    Presence,
    ScanStatus,
    SourceIdentity,
    ValidatedPage,
)
from trading_sync.errors import (
    ConfigurationError,
    DuplicateKeyConflict,
    EpochBlockedError,
    FilterScopeError,
    ImmutableDealConflict,
    IncompleteScanError,
    LeaseLostError,
    OutboxEmissionError,
    ReceiptConflict,
    StaleScanError,
    SyncError,
)
from trading_sync.infrastructure import db_models as m
from trading_sync.infrastructure import outbox_models as ob
from trading_sync.infrastructure.db import transaction
from trading_sync.infrastructure.logging import get_logger, hash_source_key, sanitize
from trading_sync.infrastructure.metrics import SyncMetrics

__all__ = ["PostgresSink", "CheckpointGuard", "MAX_QUARANTINE_ROWS_PER_SCAN"]

_LOG = get_logger("trading_sync.sink")

#: Epoch states in which a scan may commit. ``DISCOVERED`` is excluded on
#: purpose: an epoch nobody approved must not be ingested (design 8.3 item 4).
_COMMITTABLE_EPOCH_STATES = (EpochStatus.ACTIVE, EpochStatus.DEGRADED)

#: Per scan. A malformed batch can produce one incident per record, and an
#: unbounded quarantine table turns a data-quality problem into a disk problem.
#: Past the cap a single summary row is written instead, so the incident stays
#: visible while its volume stops growing (WF3-151).
MAX_QUARANTINE_ROWS_PER_SCAN = 100

_STAGING_TABLES = {
    Entity.ACCOUNTS: m.account_staging,
    Entity.POSITIONS: m.position_staging,
}
_CURRENT_TABLES = {
    Entity.ACCOUNTS: m.accounts_current,
    Entity.POSITIONS: m.positions_current,
}
_KEY_COLUMNS = {
    Entity.ACCOUNTS: "account_id",
    Entity.POSITIONS: "position_id",
}


class CheckpointGuard:
    """The checkpoint row as read under lock, plus the facts guarding a commit."""

    __slots__ = ("watermark", "published_scan_id", "lease_version", "lease_token", "db_now")

    def __init__(
        self,
        *,
        watermark: int,
        published_scan_id: uuid.UUID | None,
        lease_version: int,
        lease_token: uuid.UUID | None,
        db_now: datetime,
    ) -> None:
        self.watermark = watermark
        self.published_scan_id = published_scan_id
        self.lease_version = lease_version
        self.lease_token = lease_token
        self.db_now = db_now


class PostgresSink:
    """Implements :class:`~trading_sync.application.ports.SyncSink`."""

    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        settings: Settings,
        *,
        metrics: SyncMetrics | None = None,
        barriers: Any | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._factory = factory
        self._settings = settings
        self._metrics = metrics
        #: Test-only crash barriers; ``None`` in any deployed process.
        self._barriers = barriers
        #: Injectable so an outbox row's ``created_at`` is reproducible in a
        #: test. Wall clock in a deployed process.
        self._clock = clock or (lambda: datetime.now(UTC))

    # -- barriers -------------------------------------------------------------

    async def _barrier(self, name: str) -> None:
        if self._barriers is not None:
            await self._barriers.reach(name)

    # -- enrollment and epochs ------------------------------------------------

    async def enroll_source(self, source_id: str, source_kind: str) -> None:
        async with transaction(self._factory) as session:
            await session.execute(
                pg_insert(m.sync_sources)
                .values(source_id=source_id, source_kind=source_kind)
                .on_conflict_do_nothing(index_elements=["source_id"])
            )

    async def register_epoch(self, identity: SourceIdentity) -> EpochHandle:
        """Record an epoch and its three checkpoint rows in one transaction.

        Enrollment has to be atomic and idempotent together: a startup that
        retries must converge on exactly one set of rows, not a half-built
        epoch with checkpoints missing (WF3-005).
        """
        async with transaction(self._factory) as session:
            await session.execute(
                pg_insert(m.sync_sources)
                .values(source_id=identity.source_id, source_kind=str(identity.source_kind))
                .on_conflict_do_nothing(index_elements=["source_id"])
            )
            epoch_id = uuid.uuid4()
            await session.execute(
                pg_insert(m.source_epochs)
                .values(
                    epoch_id=epoch_id,
                    source_id=identity.source_id,
                    run_id=identity.run_id,
                    generation=identity.generation,
                    status=str(EpochStatus.DISCOVERED),
                )
                .on_conflict_do_nothing(
                    index_elements=["source_id", "run_id", "generation"]
                )
            )
            row = (
                await session.execute(
                    select(m.source_epochs).where(
                        and_(
                            m.source_epochs.c.source_id == identity.source_id,
                            m.source_epochs.c.run_id == identity.run_id,
                            m.source_epochs.c.generation == identity.generation,
                        )
                    )
                )
            ).mappings().one()
            for entity in Entity:
                await session.execute(
                    pg_insert(m.sync_checkpoints)
                    .values(
                        epoch_id=row["epoch_id"],
                        entity=str(entity),
                        filter_hash=FILTER_UNFILTERED,
                        watermark=0,
                    )
                    .on_conflict_do_nothing(
                        index_elements=["epoch_id", "entity", "filter_hash"]
                    )
                )
            return _epoch_handle(row)

    async def approve_epoch(self, epoch_id: uuid.UUID, *, reason: str) -> EpochHandle:
        """Activate an epoch, archiving any epoch it supersedes.

        Archiving rather than deleting: the previous epoch's deals and
        snapshots stay queryable as history. "Starting again" must not mean
        discarding what the last run recorded (design 8.3 item 3).
        """
        async with transaction(self._factory) as session:
            row = (
                await session.execute(
                    select(m.source_epochs)
                    .where(m.source_epochs.c.epoch_id == epoch_id)
                    .with_for_update()
                )
            ).mappings().one_or_none()
            if row is None:
                raise ConfigurationError("unknown epoch", epoch_id=str(epoch_id))
            if row["status"] == str(EpochStatus.BLOCKED):
                raise EpochBlockedError(
                    "a blocked epoch cannot be approved without an explicit rebootstrap",
                    epoch_id=str(epoch_id),
                )
            await session.execute(
                update(m.source_epochs)
                .where(
                    and_(
                        m.source_epochs.c.source_id == row["source_id"],
                        m.source_epochs.c.epoch_id != epoch_id,
                        m.source_epochs.c.status.in_(
                            [str(EpochStatus.ACTIVE), str(EpochStatus.DEGRADED)]
                        ),
                    )
                )
                .values(status=str(EpochStatus.ARCHIVED))
            )
            await session.execute(
                update(m.source_epochs)
                .where(m.source_epochs.c.epoch_id == epoch_id)
                .values(status=str(EpochStatus.ACTIVE), activated_at=func.now())
            )
            await self._audit_in_session(
                session, epoch_id, None, "EPOCH_APPROVED", reason_code=reason
            )
            refreshed = (
                await session.execute(
                    select(m.source_epochs).where(m.source_epochs.c.epoch_id == epoch_id)
                )
            ).mappings().one()
            return _epoch_handle(refreshed)

    async def get_active_epoch(self, source_id: str) -> EpochHandle | None:
        async with transaction(self._factory) as session:
            row = (
                await session.execute(
                    select(m.source_epochs).where(
                        and_(
                            m.source_epochs.c.source_id == source_id,
                            m.source_epochs.c.status.in_(
                                [str(s) for s in _COMMITTABLE_EPOCH_STATES]
                            ),
                        )
                    )
                )
            ).mappings().one_or_none()
            return None if row is None else _epoch_handle(row)

    async def block_epoch(self, epoch_id: uuid.UUID, *, reason_code: str) -> None:
        """Fence an epoch so nothing more can commit against it.

        Also clears the lease, so a worker still holding it finds itself
        fenced at its next guard rather than at its next heartbeat (WF3-019).
        """
        async with transaction(self._factory) as session:
            await session.execute(
                update(m.source_epochs)
                .where(m.source_epochs.c.epoch_id == epoch_id)
                .values(status=str(EpochStatus.BLOCKED), blocked_reason=reason_code)
            )
            await session.execute(
                update(m.sync_checkpoints)
                .where(m.sync_checkpoints.c.epoch_id == epoch_id)
                .values(lease_token=None, lease_until=None, updated_at=func.now())
            )
            await self._audit_in_session(
                session, epoch_id, None, "EPOCH_BLOCKED", reason_code=reason_code
            )

    async def list_epochs(self, source_id: str) -> list[EpochHandle]:
        async with transaction(self._factory) as session:
            rows = (
                await session.execute(
                    select(m.source_epochs)
                    .where(m.source_epochs.c.source_id == source_id)
                    .order_by(m.source_epochs.c.created_at)
                )
            ).mappings().all()
            return [_epoch_handle(row) for row in rows]

    # -- leases ---------------------------------------------------------------

    async def acquire_lease(
        self,
        epoch_id: uuid.UUID,
        entity: Entity,
        filter_scope: str,
        *,
        ttl_seconds: int,
    ) -> Lease | None:
        """Take the lease if it is free or expired, by the database's clock.

        A single conditional UPDATE does the whole thing, so two workers racing
        for a free lease resolve to exactly one winner without an application
        lock: the loser updates zero rows (WF3-121).
        """
        ttl = func.make_interval(0, 0, 0, 0, 0, 0, float(ttl_seconds))
        new_token = uuid.uuid4()
        async with transaction(self._factory) as session:
            result = await session.execute(
                update(m.sync_checkpoints)
                .where(
                    and_(
                        m.sync_checkpoints.c.epoch_id == epoch_id,
                        m.sync_checkpoints.c.entity == str(entity),
                        m.sync_checkpoints.c.filter_hash == filter_scope,
                        _lease_is_free(),
                    )
                )
                .values(
                    lease_token=new_token,
                    lease_version=m.sync_checkpoints.c.lease_version + 1,
                    lease_until=func.clock_timestamp() + ttl,
                    updated_at=func.clock_timestamp(),
                )
                .returning(
                    m.sync_checkpoints.c.lease_version,
                    m.sync_checkpoints.c.lease_until,
                    m.sync_checkpoints.c.watermark,
                )
            )
            row = result.mappings().one_or_none()
            if row is None:
                return None
            if self._metrics is not None and row["lease_version"] > 1:
                self._metrics.lease_takeovers_total.labels(entity=str(entity)).inc()
            return Lease(
                epoch_id=epoch_id,
                entity=entity,
                filter_scope=filter_scope,
                token=new_token,
                version=row["lease_version"],
                expires_at=row["lease_until"],
                watermark=row["watermark"],
            )

    async def heartbeat_lease(self, lease: Lease, *, ttl_seconds: int) -> Lease:
        """Extend the lease, or raise if this worker no longer owns it.

        Matching on token *and* version matters: a worker whose lease expired
        and was taken over, then released, could otherwise re-acquire a row
        whose token happens to be writable again. The version only moves
        forward (design 8.2).
        """
        ttl = func.make_interval(0, 0, 0, 0, 0, 0, float(ttl_seconds))
        async with transaction(self._factory) as session:
            result = await session.execute(
                update(m.sync_checkpoints)
                .where(
                    and_(
                        m.sync_checkpoints.c.epoch_id == lease.epoch_id,
                        m.sync_checkpoints.c.entity == str(lease.entity),
                        m.sync_checkpoints.c.filter_hash == lease.filter_scope,
                        m.sync_checkpoints.c.lease_token == lease.token,
                        m.sync_checkpoints.c.lease_version == lease.version,
                        m.sync_checkpoints.c.lease_until > func.clock_timestamp(),
                    )
                )
                .values(
                    lease_until=func.clock_timestamp() + ttl,
                    updated_at=func.clock_timestamp(),
                )
                .returning(
                    m.sync_checkpoints.c.lease_until, m.sync_checkpoints.c.watermark
                )
            )
            row = result.mappings().one_or_none()
            if row is None:
                raise LeaseLostError(
                    "lease is no longer held by this worker",
                    entity=str(lease.entity),
                    lease_version=lease.version,
                )
            return Lease(
                epoch_id=lease.epoch_id,
                entity=lease.entity,
                filter_scope=lease.filter_scope,
                token=lease.token,
                version=lease.version,
                expires_at=row["lease_until"],
                watermark=row["watermark"],
            )

    async def release_lease(self, lease: Lease) -> None:
        """Release the lease without bumping the version.

        Only the current holder can release, so a worker that already lost the
        lease cannot clear the new owner's claim on its way out (WF3-161).
        """
        async with transaction(self._factory) as session:
            await session.execute(
                update(m.sync_checkpoints)
                .where(
                    and_(
                        m.sync_checkpoints.c.epoch_id == lease.epoch_id,
                        m.sync_checkpoints.c.entity == str(lease.entity),
                        m.sync_checkpoints.c.filter_hash == lease.filter_scope,
                        m.sync_checkpoints.c.lease_token == lease.token,
                        m.sync_checkpoints.c.lease_version == lease.version,
                    )
                )
                .values(lease_until=None, updated_at=func.clock_timestamp())
            )

    # -- the shared commit guard ----------------------------------------------

    async def _lock_and_guard(self, session: AsyncSession, lease: Lease) -> CheckpointGuard:
        """Lock the checkpoint row and re-verify ownership from the database.

        Every state-changing path funnels through here. The row lock serializes
        competing publishers for the same scope, and the predicates below are
        evaluated against freshly-read state inside the same transaction that
        is about to write (design 8.2, WF3-124).
        """
        row = (
            await session.execute(
                select(
                    m.sync_checkpoints.c.watermark,
                    m.sync_checkpoints.c.published_scan_id,
                    m.sync_checkpoints.c.lease_token,
                    m.sync_checkpoints.c.lease_version,
                    m.sync_checkpoints.c.lease_until,
                    m.source_epochs.c.status.label("epoch_status"),
                    func.clock_timestamp().label("db_now"),
                )
                .select_from(
                    m.sync_checkpoints.join(
                        m.source_epochs,
                        m.source_epochs.c.epoch_id == m.sync_checkpoints.c.epoch_id,
                    )
                )
                .where(
                    and_(
                        m.sync_checkpoints.c.epoch_id == lease.epoch_id,
                        m.sync_checkpoints.c.entity == str(lease.entity),
                        m.sync_checkpoints.c.filter_hash == lease.filter_scope,
                    )
                )
                .with_for_update(of=m.sync_checkpoints)
            )
        ).mappings().one_or_none()
        if row is None:
            raise ConfigurationError(
                "no checkpoint row for this scope; enroll the source first",
                entity=str(lease.entity),
                scope=lease.filter_scope,
            )
        if row["epoch_status"] not in [str(s) for s in _COMMITTABLE_EPOCH_STATES]:
            raise EpochBlockedError(
                "epoch is not in a committable state",
                epoch_status=row["epoch_status"],
                entity=str(lease.entity),
            )
        owns = (
            row["lease_token"] == lease.token
            and row["lease_version"] == lease.version
            and row["lease_until"] is not None
            and row["lease_until"] > row["db_now"]
        )
        if not owns:
            raise LeaseLostError(
                "lease no longer valid at the commit guard",
                entity=str(lease.entity),
                held_version=lease.version,
                current_version=row["lease_version"],
            )
        return CheckpointGuard(
            watermark=row["watermark"],
            published_scan_id=row["published_scan_id"],
            lease_version=row["lease_version"],
            lease_token=row["lease_token"],
            db_now=row["db_now"],
        )

    # -- scans ----------------------------------------------------------------

    async def read_watermark(
        self, epoch_id: uuid.UUID, entity: Entity, filter_scope: str
    ) -> int:
        async with transaction(self._factory) as session:
            value = (
                await session.execute(
                    select(m.sync_checkpoints.c.watermark).where(
                        and_(
                            m.sync_checkpoints.c.epoch_id == epoch_id,
                            m.sync_checkpoints.c.entity == str(entity),
                            m.sync_checkpoints.c.filter_hash == filter_scope,
                        )
                    )
                )
            ).scalar_one_or_none()
            if value is None:
                raise ConfigurationError(
                    "no checkpoint row for this scope",
                    entity=str(entity),
                    scope=filter_scope,
                )
            return int(value)

    async def create_scan(self, lease: Lease, *, start_watermark: int) -> ScanHandle:
        """Open a scan, binding it to the watermark it started from.

        ``start_watermark`` is stored rather than re-read at publication time:
        the terminal transaction compares it against the live watermark as a
        compare-and-set, which is what detects another writer having moved the
        watermark underneath us (design 6.4).
        """
        scan_id = uuid.uuid4()
        async with transaction(self._factory) as session:
            guard = await self._lock_and_guard(session, lease)
            if guard.watermark != start_watermark:
                raise StaleScanError(
                    "watermark moved between read and scan creation",
                    expected=start_watermark,
                    actual=guard.watermark,
                )
            await session.execute(
                insert(m.sync_runs).values(
                    scan_id=scan_id,
                    epoch_id=lease.epoch_id,
                    entity=str(lease.entity),
                    filter_hash=lease.filter_scope,
                    start_watermark=start_watermark,
                    status=str(ScanStatus.FETCHING),
                    owner_lease_version=lease.version,
                )
            )
            return ScanHandle(
                scan_id=scan_id,
                epoch_id=lease.epoch_id,
                entity=lease.entity,
                filter_scope=lease.filter_scope,
                start_watermark=start_watermark,
                owner_lease_version=lease.version,
            )

    async def mark_scan_status(
        self,
        scan: ScanHandle,
        status: str,
        *,
        reason_code: str | None = None,
        snapshot_seq: int | None = None,
    ) -> None:
        values: dict[str, Any] = {"status": status, "error_code": reason_code}
        if snapshot_seq is not None:
            values["snapshot_seq"] = snapshot_seq
        if status in (
            str(ScanStatus.PUBLISHED),
            str(ScanStatus.ABORTED),
            str(ScanStatus.QUARANTINED),
            str(ScanStatus.BLOCKED_EPOCH),
        ):
            values["ended_at"] = func.now()
        async with transaction(self._factory) as session:
            await session.execute(
                update(m.sync_runs)
                .where(m.sync_runs.c.scan_id == scan.scan_id)
                .values(**values)
            )

    # -- deal path ------------------------------------------------------------

    async def persist_deal_page(
        self, lease: Lease, scan: ScanHandle, page: ValidatedPage
    ) -> PageCommitResult:
        """Insert one page of deals plus its receipt, atomically.

        ``ON CONFLICT DO NOTHING`` handles the replay case, but it cannot
        distinguish a harmless duplicate from the same id carrying different
        money. So every row the insert declined is read back and compared by
        content hash. A mismatch rolls the whole page back and the stored value
        stands -- the alternative, ``DO UPDATE``, would overwrite a commission
        and leave no trace (design 6.3, INV-05, WF3-028).
        """
        async with transaction(self._factory) as session:
            guard = await self._lock_and_guard(session, lease)
            if guard.watermark != scan.start_watermark:
                raise StaleScanError(
                    "watermark moved while the scan was running",
                    expected=scan.start_watermark,
                    actual=guard.watermark,
                )
            await self._require_scan_status(session, scan, ScanStatus.FETCHING)

            replay = await self._existing_receipt(session, scan, page)
            if replay is not None:
                return replay
            await self._require_expected_page_index(session, scan, page.page_index)

            records: Sequence[DealRecord] = page.records  # type: ignore[assignment]
            inserted = 0
            emitted = 0
            if records:
                ordered = sorted(records, key=lambda record: record.deal_id)
                values = [
                    {
                        "epoch_id": scan.epoch_id,
                        "deal_id": record.deal_id,
                        "source_seq": record.source_seq,
                        "account_id": record.account_id,
                        "position_id": record.position_id,
                        "symbol": record.symbol,
                        "entry_kind": record.entry_kind,
                        "side": str(record.side),
                        "volume_lots": record.volume_lots,
                        "price": record.price,
                        "realized_pnl_gross": record.realized_pnl_gross,
                        "commission": record.commission,
                        "swap": record.swap,
                        "executed_at": record.executed_at,
                        "payload_hash": record.payload_hash,
                        "first_scan_id": scan.scan_id,
                    }
                    for record in ordered
                ]
                result = await session.execute(
                    pg_insert(m.deals)
                    .values(values)
                    .on_conflict_do_nothing(index_elements=["epoch_id", "deal_id"])
                    .returning(m.deals.c.deal_id)
                )
                inserted_ids = set(result.scalars().all())
                inserted = len(inserted_ids)
                await self._barrier("K04_after_deal_insert_before_receipt")
                declined = {
                    record.deal_id: record
                    for record in ordered
                    if record.deal_id not in inserted_ids
                }
                if declined:
                    await self._verify_identical_deals(session, scan, declined)

                if inserted_ids and self._settings.sync.emit_outbox_events:
                    emitted = await self._emit_deal_events(
                        session,
                        scan,
                        [record for record in ordered if record.deal_id in inserted_ids],
                    )
                    await self._barrier("K04b_after_outbox_insert_before_receipt")

            await self._store_receipt(session, scan, page)
            await self._barrier("K05_after_receipt_before_commit")
            await session.execute(
                update(m.sync_runs)
                .where(m.sync_runs.c.scan_id == scan.scan_id)
                .values(
                    page_count=m.sync_runs.c.page_count + 1,
                    record_count=m.sync_runs.c.record_count + inserted,
                    duplicate_count=m.sync_runs.c.duplicate_count + page.duplicate_count,
                    snapshot_seq=page.snapshot_seq,
                )
            )
        await self._barrier("K06_after_commit_before_ack")
        if self._metrics is not None:
            self._metrics.pages_committed_total.labels(entity=str(scan.entity)).inc()
            self._metrics.deals_inserted_total.labels(
                source_group=self._settings.source.source_id
            ).inc(inserted)
            if page.duplicate_count:
                self._metrics.deals_duplicate_total.labels(entity=str(scan.entity)).inc(
                    page.duplicate_count
                )
            # After the commit, like every other counter here. Incrementing
            # inside the transaction would over-report each page that rolled
            # back afterwards, and an events-emitted count that exceeds the
            # rows in event_outbox is exactly the kind of metric that sends an
            # operator looking for a bug in the relay.
            if emitted:
                self._metrics_outbox_emitted(emitted)
        return PageCommitResult(
            page_index=page.page_index,
            inserted=inserted,
            duplicates=page.duplicate_count,
        )

    # -- outbox emission (contract gap CG-08) ---------------------------------

    async def _emit_deal_events(
        self,
        session: AsyncSession,
        scan: ScanHandle,
        records: Sequence[DealRecord],
    ) -> int:
        """Insert one ``event_outbox`` row per newly committed Deal.

        Runs on the caller's ``session``, so the rows land in the same
        transaction as the Deals themselves. That is the entire reason this
        exists here instead of in Project 07: P07's own writer is synchronous
        psycopg and would open a second connection, which is a second
        transaction, which is the arrangement its first design decision
        forbids.

        Ordering within an aggregate comes from ``outbox_aggregate_heads``.
        The reservation is an ``ON CONFLICT DO UPDATE`` that sets
        ``last_version`` to itself: a no-op write whose only purpose is to take
        the row lock. ``DO NOTHING`` would not lock it, and two concurrent
        writers could then both read the same ``last_version`` and allocate the
        same version to different events.

        Anything that cannot be expressed as a valid event raises, the
        transaction rolls back, and the Deals are not committed either. A Deal
        that exists without its event is the one outcome this must never
        produce (contract gap CG-10 describes the case that triggers it).
        """
        source_id = self._settings.source.source_id
        allowlist = (source_id,)
        created_at = self._clock()
        emitted = 0

        for account_id in sorted({record.account_id for record in records}):
            await session.execute(
                pg_insert(ob.outbox_aggregate_heads)
                .values(
                    source_id=source_id,
                    epoch_id=scan.epoch_id,
                    aggregate_key=account_id,
                    last_version=0,
                )
                .on_conflict_do_update(
                    index_elements=["source_id", "epoch_id", "aggregate_key"],
                    set_={"last_version": ob.outbox_aggregate_heads.c.last_version},
                )
            )

        for record in sorted(records, key=lambda item: (item.account_id, item.source_seq)):
            version = await self._next_aggregate_version(session, scan, record.account_id)
            event = build_deal_event(
                record,
                source_id=source_id,
                epoch_id=scan.epoch_id,
                aggregate_version=version,
                created_at=created_at,
                source_allowlist=allowlist,
                provenance={
                    "scan_id": scan.scan_id,
                    "source_seq": record.source_seq,
                },
            )
            await session.execute(
                insert(ob.event_outbox).values(outbox_row(event, created_at=created_at))
            )
            emitted += 1

        return emitted

    async def _next_aggregate_version(
        self, session: AsyncSession, scan: ScanHandle, account_id: str
    ) -> int:
        """Allocate the next version for one aggregate, under the row lock."""
        source_id = self._settings.source.source_id
        heads = ob.outbox_aggregate_heads
        result = await session.execute(
            update(heads)
            .where(
                and_(
                    heads.c.source_id == source_id,
                    heads.c.epoch_id == scan.epoch_id,
                    heads.c.aggregate_key == account_id,
                )
            )
            .values(last_version=heads.c.last_version + 1)
            .returning(heads.c.last_version)
        )
        version = result.scalar_one_or_none()
        if version is None:
            # The reservation above should have created the row. If it is gone,
            # something else deleted it mid-transaction; allocating a version
            # from nothing would risk reusing one.
            raise OutboxEmissionError(
                f"aggregate head missing for {source_id}/{scan.epoch_id}/{account_id}"
            )
        return int(version)

    def _metrics_outbox_emitted(self, count: int) -> None:
        """Count emitted events when the metric exists.

        Guarded rather than assumed: the metric set is defined in P03's own
        metrics module, and emission was added after it.
        """
        counter = getattr(self._metrics, "outbox_events_emitted_total", None)
        if counter is None:
            return
        counter.labels(source_group=self._settings.source.source_id).inc(count)

    async def _verify_identical_deals(
        self,
        session: AsyncSession,
        scan: ScanHandle,
        declined: Mapping[str, DealRecord],
    ) -> None:
        """Compare every row the insert declined against what is stored.

        ``FOR SHARE`` holds the comparison stable: without it a concurrent
        writer could change the row between the read and the decision, which is
        the race WF3-103 describes.
        """
        stored = (
            await session.execute(
                select(m.deals.c.deal_id, m.deals.c.payload_hash)
                .where(
                    and_(
                        m.deals.c.epoch_id == scan.epoch_id,
                        m.deals.c.deal_id.in_(list(declined)),
                    )
                )
                .with_for_update(read=True)
            )
        ).mappings().all()
        stored_hashes = {row["deal_id"]: row["payload_hash"] for row in stored}
        for deal_id, record in declined.items():
            existing = stored_hashes.get(deal_id)
            if existing is None:
                # Declined but absent: the only way is a concurrent transaction
                # that has not committed, so this page cannot be verified now.
                raise ImmutableDealConflict(
                    "deal insert was declined but no stored row is visible",
                    source_key_hash=hash_source_key(deal_id),
                )
            if existing != record.payload_hash:
                raise ImmutableDealConflict(
                    "stored deal content differs from the source replay",
                    source_key_hash=hash_source_key(deal_id),
                    entity=str(scan.entity),
                )

    async def publish_deal_checkpoint(
        self,
        lease: Lease,
        scan: ScanHandle,
        *,
        old_watermark: int,
        new_watermark: int,
    ) -> None:
        """Advance the deal watermark, with every precondition checked at once.

        The set of guards is the point. Each one alone is insufficient: a valid
        lease does not prove the scan was complete, a complete receipt chain
        does not prove nobody else moved the watermark, and an unfiltered scope
        does not prove the epoch is still live. All of them hold inside one
        transaction, or the watermark does not move (design 6.4).
        """
        if lease.filter_scope != FILTER_UNFILTERED:
            raise FilterScopeError(
                "only an unfiltered scan may drive the global deal watermark",
                scope=lease.filter_scope,
            )
        if scan.entity is not Entity.DEALS:
            raise FilterScopeError(
                "deal checkpoints belong to the deals entity", entity=str(scan.entity)
            )
        if new_watermark < old_watermark:
            raise StaleScanError(
                "refusing to move the watermark backwards",
                old_watermark=old_watermark,
                new_watermark=new_watermark,
            )

        async with transaction(self._factory) as session:
            guard = await self._lock_and_guard(session, lease)
            if guard.watermark != old_watermark:
                raise StaleScanError(
                    "watermark changed; this scan's completeness proof no longer applies",
                    expected=old_watermark,
                    actual=guard.watermark,
                )
            await self._require_scan_status(session, scan, ScanStatus.FETCHING)
            await self._require_complete_receipt_chain(session, scan)
            await self._require_no_quarantine(session, scan)
            await self._barrier("K08_before_checkpoint_select_for_update")

            result = await session.execute(
                update(m.sync_checkpoints)
                .where(
                    and_(
                        m.sync_checkpoints.c.epoch_id == lease.epoch_id,
                        m.sync_checkpoints.c.entity == str(Entity.DEALS),
                        m.sync_checkpoints.c.filter_hash == FILTER_UNFILTERED,
                        # Compare-and-set: the watermark must still be the one
                        # this scan started from.
                        m.sync_checkpoints.c.watermark == old_watermark,
                        m.sync_checkpoints.c.lease_token == lease.token,
                        m.sync_checkpoints.c.lease_version == lease.version,
                        m.sync_checkpoints.c.lease_until > func.clock_timestamp(),
                    )
                )
                .values(
                    watermark=new_watermark,
                    published_scan_id=scan.scan_id,
                    last_success_at=func.clock_timestamp(),
                    updated_at=func.clock_timestamp(),
                )
            )
            if result.rowcount != 1:
                raise StaleScanError(
                    "checkpoint compare-and-set matched no row",
                    expected=old_watermark,
                    new_watermark=new_watermark,
                )
            await session.execute(
                update(m.sync_runs)
                .where(m.sync_runs.c.scan_id == scan.scan_id)
                .values(
                    status=str(ScanStatus.PUBLISHED),
                    snapshot_seq=new_watermark,
                    ended_at=func.now(),
                )
            )
            await self._audit_in_session(
                session,
                scan.epoch_id,
                scan.scan_id,
                "DEAL_CHECKPOINT_PUBLISHED",
                safe_details={
                    "watermark_before": old_watermark,
                    "watermark_after": new_watermark,
                },
            )
            await self._barrier("K09_after_checkpoint_update_before_commit")
        await self._barrier("K10_after_checkpoint_commit_before_status_response")
        if self._metrics is not None:
            self._metrics.checkpoint_source_seq.labels(
                source_id=self._settings.source.source_id
            ).set(new_watermark)
            self._metrics.snapshot_seq.labels(
                source_id=self._settings.source.source_id, entity=str(Entity.DEALS)
            ).set(new_watermark)

    # -- mutable snapshot path ------------------------------------------------

    async def stage_snapshot_page(
        self, lease: Lease, scan: ScanHandle, page: ValidatedPage
    ) -> PageCommitResult:
        """Write a page into staging, where no reader can see it.

        Staging is what makes the all-or-nothing publication possible. Upserting
        straight into the current projection would make a scan that dies on its
        last page leave a mixture of two snapshots behind, and nothing
        downstream could tell (design 7.1, WF3-061).
        """
        table = _STAGING_TABLES.get(scan.entity)
        if table is None:
            raise FilterScopeError(
                "only accounts and positions use staging", entity=str(scan.entity)
            )
        key_column = _KEY_COLUMNS[scan.entity]

        async with transaction(self._factory) as session:
            await self._lock_and_guard(session, lease)
            await self._require_scan_status(session, scan, ScanStatus.FETCHING)
            replay = await self._existing_receipt(session, scan, page)
            if replay is not None:
                return replay
            await self._require_expected_page_index(session, scan, page.page_index)

            inserted = 0
            if page.records:
                ordered = sorted(page.records, key=lambda record: record.key)
                values = [_staging_values(scan, record) for record in ordered]
                result = await session.execute(
                    pg_insert(table)
                    .values(values)
                    .on_conflict_do_nothing(index_elements=["scan_id", key_column])
                    .returning(table.c[key_column])
                )
                inserted_keys = set(result.scalars().all())
                inserted = len(inserted_keys)
                declined = {
                    record.key: record
                    for record in ordered
                    if record.key not in inserted_keys
                }
                if declined:
                    await self._verify_identical_staged(
                        session, scan, table, key_column, declined
                    )

            await self._store_receipt(session, scan, page)
            await self._barrier("K12_after_snapshot_stage_page")
            await session.execute(
                update(m.sync_runs)
                .where(m.sync_runs.c.scan_id == scan.scan_id)
                .values(
                    page_count=m.sync_runs.c.page_count + 1,
                    record_count=m.sync_runs.c.record_count + inserted,
                    duplicate_count=m.sync_runs.c.duplicate_count + page.duplicate_count,
                    snapshot_seq=page.snapshot_seq,
                )
            )
        if self._metrics is not None:
            self._metrics.pages_committed_total.labels(entity=str(scan.entity)).inc()
        return PageCommitResult(
            page_index=page.page_index,
            inserted=inserted,
            duplicates=page.duplicate_count,
        )

    async def _verify_identical_staged(
        self,
        session: AsyncSession,
        scan: ScanHandle,
        table: Any,
        key_column: str,
        declined: Mapping[str, Any],
    ) -> None:
        """A repeated key inside one snapshot is only acceptable if identical.

        Two different balances for one account in a single snapshot means the
        snapshot is not a coherent view of anything, so the scan is rejected
        rather than one of the two values being picked (WF3-062, WF3-076).
        """
        stored = (
            await session.execute(
                select(table.c[key_column], table.c.payload_hash).where(
                    and_(
                        table.c.scan_id == scan.scan_id,
                        table.c[key_column].in_(list(declined)),
                    )
                )
            )
        ).mappings().all()
        hashes = {row[key_column]: row["payload_hash"] for row in stored}
        for key, record in declined.items():
            if hashes.get(key) != record.payload_hash:
                raise DuplicateKeyConflict(
                    "the same key appeared twice in one snapshot with different content",
                    entity=str(scan.entity),
                    source_key_hash=hash_source_key(key),
                )

    async def publish_snapshot(
        self, lease: Lease, scan: ScanHandle, *, snapshot_seq: int
    ) -> PublicationResult:
        """Promote a staged snapshot in exactly one transaction.

        The two statements -- upsert the staged rows, then mark everything else
        absent -- have to share a transaction. Split across two, a reader
        landing between them would see the new balances alongside stale
        presence flags, which is the hybrid state INV-07 forbids.

        Rows the snapshot did not contain become ``ABSENT``, never ``CLOSED``.
        Absence is a fact about our snapshot; closure is a fact about the
        business, and only the source can assert it (INV-08, WF3-073).
        """
        staging = _STAGING_TABLES.get(scan.entity)
        current = _CURRENT_TABLES.get(scan.entity)
        if staging is None or current is None:
            raise FilterScopeError(
                "only accounts and positions are published this way",
                entity=str(scan.entity),
            )
        if lease.filter_scope != FILTER_UNFILTERED:
            # Absence can only be inferred from a complete listing. A filtered
            # scan would mark everything outside its filter absent (WF3-066,
            # WF3-079).
            raise FilterScopeError(
                "a filtered scan may not publish a full snapshot",
                scope=lease.filter_scope,
            )
        key_column = _KEY_COLUMNS[scan.entity]

        async with transaction(self._factory) as session:
            guard = await self._lock_and_guard(session, lease)
            await self._require_scan_status(session, scan, ScanStatus.STAGED)
            await self._require_complete_receipt_chain(session, scan)
            await self._require_no_quarantine(session, scan)
            await self._require_not_stale_snapshot(session, guard, snapshot_seq)
            await self._barrier("K13_before_snapshot_promote")

            # Typed literals rather than casts: casting 'PRESENT' to the
            # payload_hash CHAR(64) type would blank-pad it to 64 characters.
            staged_columns = [staging.c[name] for name in _PUBLISH_COLUMNS[scan.entity]]
            select_staged = (
                select(
                    literal(scan.epoch_id, Uuid).label("epoch_id"),
                    *staged_columns,
                    literal(str(Presence.PRESENT), Text).label("presence"),
                    literal(scan.scan_id, Uuid).label("published_scan_id"),
                    staging.c.payload_hash,
                    literal(snapshot_seq, BigInteger).label("snapshot_seq"),
                )
                .where(staging.c.scan_id == scan.scan_id)
                # Deterministic key order limits deadlocks when two
                # transactions touch overlapping rows (design 8.4).
                .order_by(staging.c[key_column])
            )

            insert_columns = (
                ["epoch_id"]
                + list(_PUBLISH_COLUMNS[scan.entity])
                + ["presence", "published_scan_id", "payload_hash", "snapshot_seq"]
            )
            upsert = pg_insert(current).from_select(insert_columns, select_staged)
            updatable = {
                name: upsert.excluded[name]
                for name in insert_columns
                if name not in ("epoch_id", key_column)
            }
            updatable["published_at"] = func.now()
            result = await session.execute(
                upsert.on_conflict_do_update(
                    index_elements=["epoch_id", key_column], set_=updatable
                )
            )
            rows_published = result.rowcount or 0
            await self._barrier("K14_mid_snapshot_promote_transaction")

            absent = await session.execute(
                update(current)
                .where(
                    and_(
                        current.c.epoch_id == scan.epoch_id,
                        current.c.published_scan_id != scan.scan_id,
                    )
                )
                .values(
                    presence=str(Presence.ABSENT),
                    published_scan_id=scan.scan_id,
                    snapshot_seq=snapshot_seq,
                    published_at=func.now(),
                )
            )
            rows_absent = absent.rowcount or 0

            await session.execute(
                update(m.sync_checkpoints)
                .where(
                    and_(
                        m.sync_checkpoints.c.epoch_id == lease.epoch_id,
                        m.sync_checkpoints.c.entity == str(scan.entity),
                        m.sync_checkpoints.c.filter_hash == FILTER_UNFILTERED,
                        m.sync_checkpoints.c.lease_token == lease.token,
                        m.sync_checkpoints.c.lease_version == lease.version,
                        m.sync_checkpoints.c.lease_until > func.clock_timestamp(),
                    )
                )
                .values(
                    watermark=snapshot_seq,
                    published_scan_id=scan.scan_id,
                    last_success_at=func.clock_timestamp(),
                    updated_at=func.clock_timestamp(),
                )
            )
            await session.execute(
                update(m.sync_runs)
                .where(m.sync_runs.c.scan_id == scan.scan_id)
                .values(
                    status=str(ScanStatus.PUBLISHED),
                    snapshot_seq=snapshot_seq,
                    ended_at=func.now(),
                )
            )
            await self._audit_in_session(
                session,
                scan.epoch_id,
                scan.scan_id,
                "SNAPSHOT_PUBLISHED",
                safe_details={
                    "entity": str(scan.entity),
                    "snapshot_seq": snapshot_seq,
                    "rows_published": rows_published,
                    "rows_marked_absent": rows_absent,
                },
            )
        await self._barrier("K15_after_snapshot_commit_before_ack")
        if self._metrics is not None:
            self._metrics.snapshot_seq.labels(
                source_id=self._settings.source.source_id, entity=str(scan.entity)
            ).set(snapshot_seq)
        return PublicationResult(
            scan_id=scan.scan_id,
            snapshot_seq=snapshot_seq,
            rows_published=rows_published,
            rows_marked_absent=rows_absent,
        )

    # -- guards and receipts --------------------------------------------------

    async def _require_scan_status(
        self, session: AsyncSession, scan: ScanHandle, expected: ScanStatus
    ) -> None:
        """Read the scan's status from the database, never from memory.

        After a restart the only trustworthy answer is the stored one: a
        process that logged "success" before its COMMIT failed is exactly the
        case WF3-056 and WF3-196 describe.
        """
        status = (
            await session.execute(
                select(m.sync_runs.c.status).where(m.sync_runs.c.scan_id == scan.scan_id)
            )
        ).scalar_one_or_none()
        if status is None:
            raise ConfigurationError("unknown scan", scan_status="MISSING")
        if status != str(expected):
            raise StaleScanError(
                "scan is not in the state this operation requires",
                expected=str(expected),
                actual=status,
            )

    async def _existing_receipt(
        self, session: AsyncSession, scan: ScanHandle, page: ValidatedPage
    ) -> PageCommitResult | None:
        """Resolve a page replay against durable receipts.

        An identical hash means this exact page already committed, so the
        caller is acknowledged without re-applying anything. A different hash
        for the same index means the source changed its answer, which is an
        incident rather than a retry (design 6.5, WF3-053, WF3-059).
        """
        row = (
            await session.execute(
                select(m.sync_page_receipts.c.page_hash, m.sync_page_receipts.c.item_count)
                .where(
                    and_(
                        m.sync_page_receipts.c.scan_id == scan.scan_id,
                        m.sync_page_receipts.c.page_index == page.page_index,
                    )
                )
            )
        ).mappings().one_or_none()
        if row is None:
            return None
        if row["page_hash"] != page.page_hash:
            raise ReceiptConflict(
                "page index was already committed with different content",
                page_index=page.page_index,
            )
        return PageCommitResult(
            page_index=page.page_index,
            inserted=0,
            duplicates=page.duplicate_count,
            already_committed=True,
        )

    async def _require_expected_page_index(
        self, session: AsyncSession, scan: ScanHandle, page_index: int
    ) -> None:
        """Receipts must form an unbroken chain from zero."""
        count = (
            await session.execute(
                select(func.count())
                .select_from(m.sync_page_receipts)
                .where(m.sync_page_receipts.c.scan_id == scan.scan_id)
            )
        ).scalar_one()
        if page_index != count:
            raise IncompleteScanError(
                "page index does not continue the receipt chain",
                expected=int(count),
                actual=page_index,
            )

    async def _store_receipt(
        self, session: AsyncSession, scan: ScanHandle, page: ValidatedPage
    ) -> None:
        await session.execute(
            insert(m.sync_page_receipts).values(
                scan_id=scan.scan_id,
                page_index=page.page_index,
                item_count=page.record_count + page.duplicate_count,
                page_hash=page.page_hash,
                is_terminal=page.is_terminal,
            )
        )

    async def _require_complete_receipt_chain(
        self, session: AsyncSession, scan: ScanHandle
    ) -> None:
        """Prove the scan read the whole snapshot before anything is published.

        Four facts together: at least one page, indices 0..n-1 with no hole,
        and exactly one terminal page. A terminal page on its own is not
        enough -- it says the source stopped sending, not that we committed
        everything it sent (design 6.4, SQL oracle in 15.3).
        """
        row = (
            await session.execute(
                select(
                    func.count().label("n"),
                    func.min(m.sync_page_receipts.c.page_index).label("min_idx"),
                    func.max(m.sync_page_receipts.c.page_index).label("max_idx"),
                    func.count()
                    .filter(m.sync_page_receipts.c.is_terminal)
                    .label("terminals"),
                ).where(m.sync_page_receipts.c.scan_id == scan.scan_id)
            )
        ).mappings().one()
        pages = int(row["n"])
        if pages == 0:
            raise IncompleteScanError("scan committed no pages", scan_pages=0)
        if row["min_idx"] != 0 or row["max_idx"] != pages - 1:
            raise IncompleteScanError(
                "receipt chain has a hole",
                scan_pages=pages,
                min_index=int(row["min_idx"]),
                max_index=int(row["max_idx"]),
            )
        if int(row["terminals"]) != 1:
            raise IncompleteScanError(
                "scan does not have exactly one terminal page",
                terminal_pages=int(row["terminals"]),
            )

    async def _require_no_quarantine(
        self, session: AsyncSession, scan: ScanHandle
    ) -> None:
        count = (
            await session.execute(
                select(func.count())
                .select_from(m.quarantine_items)
                .where(m.quarantine_items.c.scan_id == scan.scan_id)
            )
        ).scalar_one()
        if count:
            raise IncompleteScanError(
                "scan has quarantined items and cannot publish",
                quarantined=int(count),
            )

    async def _require_not_stale_snapshot(
        self, session: AsyncSession, guard: CheckpointGuard, snapshot_seq: int
    ) -> None:
        """Refuse a scan that finished behind one already published.

        A slow scan of snapshot 100 completing after snapshot 110 published
        would roll the current projection backwards in time (WF3-068).
        """
        if guard.published_scan_id is None:
            return
        published_seq = (
            await session.execute(
                select(m.sync_runs.c.snapshot_seq).where(
                    m.sync_runs.c.scan_id == guard.published_scan_id
                )
            )
        ).scalar_one_or_none()
        if published_seq is not None and snapshot_seq < int(published_seq):
            raise StaleScanError(
                "a newer snapshot is already published",
                incoming_snapshot_seq=snapshot_seq,
                published_snapshot_seq=int(published_seq),
            )

    # -- incidents and reporting ----------------------------------------------

    async def quarantine(
        self,
        epoch_id: uuid.UUID,
        scan_id: uuid.UUID | None,
        entity: Entity,
        *,
        reason_code: str,
        source_key: str | None = None,
        source_key_hash: str | None = None,
        safe_details: dict[str, Any] | None = None,
    ) -> None:
        """Record an incident in its own transaction.

        Only a digest of the offending key is stored. A quarantine table
        holding raw payloads would become a second copy of the data, outside
        the controls the main tables have (design 11.6, WF3-190).

        ``source_key_hash`` lets a caller supply a digest it already holds --
        the application layer sees conflicts as errors carrying a hash, and
        never the raw key, so the raw value stays inside this module.
        """
        async with transaction(self._factory) as session:
            if scan_id is not None:
                recorded = int(
                    (
                        await session.execute(
                            select(func.count())
                            .select_from(m.quarantine_items)
                            .where(m.quarantine_items.c.scan_id == scan_id)
                        )
                    ).scalar_one()
                )
                if recorded > MAX_QUARANTINE_ROWS_PER_SCAN:
                    # Already capped and summarised; drop silently rather than
                    # keep growing the table for one bad batch.
                    return
                if recorded == MAX_QUARANTINE_ROWS_PER_SCAN:
                    safe_details = {
                        "truncated_after": MAX_QUARANTINE_ROWS_PER_SCAN,
                        "original_reason_code": reason_code,
                    }
                    reason_code = "QUARANTINE_FLOOD_TRUNCATED"
                    source_key = None
                    source_key_hash = None
            await session.execute(
                insert(m.quarantine_items).values(
                    quarantine_id=uuid.uuid4(),
                    epoch_id=epoch_id,
                    scan_id=scan_id,
                    entity=str(entity),
                    source_key_hash=source_key_hash
                    or (None if source_key is None else hash_source_key(source_key)),
                    reason_code=reason_code,
                    safe_details=sanitize(safe_details or {}),
                )
            )
        if self._metrics is not None:
            self._metrics.quarantine_items_total.labels(
                entity=str(entity), reason_code=reason_code
            ).inc()

    async def audit(
        self,
        epoch_id: uuid.UUID,
        scan_id: uuid.UUID | None,
        event_type: str,
        *,
        reason_code: str | None = None,
        safe_details: dict[str, Any] | None = None,
    ) -> None:
        async with transaction(self._factory) as session:
            await self._audit_in_session(
                session,
                epoch_id,
                scan_id,
                event_type,
                reason_code=reason_code,
                safe_details=safe_details,
            )

    async def _audit_in_session(
        self,
        session: AsyncSession,
        epoch_id: uuid.UUID,
        scan_id: uuid.UUID | None,
        event_type: str,
        *,
        reason_code: str | None = None,
        safe_details: dict[str, Any] | None = None,
    ) -> None:
        """Write an audit row inside the caller's transaction.

        Publication audit must share the publication transaction: an audit
        trail that can be missing for a checkpoint that did happen is not an
        audit trail (WF3-203).
        """
        await session.execute(
            insert(m.sync_audit).values(
                audit_id=uuid.uuid4(),
                epoch_id=epoch_id,
                scan_id=scan_id,
                event_type=event_type,
                reason_code=reason_code,
                safe_details=sanitize(safe_details or {}),
            )
        )

    async def read_checkpoints(self, epoch_id: uuid.UUID) -> list[dict[str, Any]]:
        """Per-entity freshness, for the status surface."""
        async with transaction(self._factory) as session:
            rows = (
                await session.execute(
                    select(
                        m.sync_checkpoints.c.entity,
                        m.sync_checkpoints.c.filter_hash,
                        m.sync_checkpoints.c.watermark,
                        m.sync_checkpoints.c.published_scan_id,
                        m.sync_checkpoints.c.last_success_at,
                        m.sync_checkpoints.c.lease_version,
                        func.clock_timestamp().label("db_now"),
                    ).where(m.sync_checkpoints.c.epoch_id == epoch_id)
                )
            ).mappings().all()
            return [dict(row) for row in rows]

    async def recover_page_state(self, scan_id: uuid.UUID) -> dict[str, Any]:
        """Re-read durable state after an ambiguous COMMIT.

        The recovery procedure in design 6.5: open a fresh connection, read the
        receipts and the checkpoint, and let the database say what happened.
        A worker's own log cannot answer this, because the log entry may have
        been written before a COMMIT that then failed.
        """
        async with transaction(self._factory) as session:
            run = (
                await session.execute(
                    select(m.sync_runs).where(m.sync_runs.c.scan_id == scan_id)
                )
            ).mappings().one_or_none()
            receipts = (
                await session.execute(
                    select(
                        m.sync_page_receipts.c.page_index,
                        m.sync_page_receipts.c.page_hash,
                        m.sync_page_receipts.c.is_terminal,
                    )
                    .where(m.sync_page_receipts.c.scan_id == scan_id)
                    .order_by(m.sync_page_receipts.c.page_index)
                )
            ).mappings().all()
            checkpoint = None
            if run is not None:
                checkpoint = (
                    await session.execute(
                        select(
                            m.sync_checkpoints.c.watermark,
                            m.sync_checkpoints.c.published_scan_id,
                        ).where(
                            and_(
                                m.sync_checkpoints.c.epoch_id == run["epoch_id"],
                                m.sync_checkpoints.c.entity == run["entity"],
                                m.sync_checkpoints.c.filter_hash == run["filter_hash"],
                            )
                        )
                    )
                ).mappings().one_or_none()
            return {
                "run": None if run is None else dict(run),
                "receipts": [dict(r) for r in receipts],
                "checkpoint": None if checkpoint is None else dict(checkpoint),
            }


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

#: Columns copied verbatim from staging into the current projection. Presence,
#: provenance and the snapshot sequence are added by the publisher.
_PUBLISH_COLUMNS: dict[Entity, tuple[str, ...]] = {
    Entity.ACCOUNTS: ("account_id", "currency", "balance", "credit"),
    Entity.POSITIONS: (
        "position_id",
        "account_id",
        "symbol",
        "side",
        "volume_lots",
        "open_price",
        "source_status",
    ),
}


def _lease_is_free() -> Any:
    """Predicate for a lease nobody holds, judged by the database clock."""
    return (m.sync_checkpoints.c.lease_until.is_(None)) | (
        m.sync_checkpoints.c.lease_until <= func.clock_timestamp()
    )


def _epoch_handle(row: Mapping[str, Any]) -> EpochHandle:
    return EpochHandle(
        epoch_id=row["epoch_id"],
        source_id=row["source_id"],
        run_id=row["run_id"],
        generation=int(row["generation"]),
        status=EpochStatus(row["status"]),
    )


def _staging_values(scan: ScanHandle, record: Any) -> dict[str, Any]:
    if isinstance(record, AccountRecord):
        return {
            "scan_id": scan.scan_id,
            "account_id": record.account_id,
            "currency": record.currency,
            "balance": record.balance,
            "credit": record.credit,
            "payload_hash": record.payload_hash,
        }
    if isinstance(record, PositionRecord):
        return {
            "scan_id": scan.scan_id,
            "position_id": record.position_id,
            "account_id": record.account_id,
            "symbol": record.symbol,
            "side": str(record.side),
            "volume_lots": record.volume_lots,
            "open_price": record.open_price,
            "source_status": record.source_status,
            "payload_hash": record.payload_hash,
        }
    raise SyncError("unstageable record type", kind=type(record).__name__)

"""Full-snapshot synchronization for accounts and positions.

Deals can be replayed incrementally because they are immutable and carry an
event sequence. Accounts and positions have neither property: the source
returns current state, with no change feed and no deletions. So there is no way
to learn "account 7 was removed" from a stream of updates -- the only evidence
is its absence from a listing that is known to be complete.

Hence this shape: read the whole snapshot into staging, and promote it in one
transaction that both upserts what was present and marks everything else
absent. Upserting straight into the current projection instead would leave a
scan that dies on its last page having published a mixture of two snapshots,
undetectably (design 7.1, WF3-061, INV-06, INV-07).

Two rules the publication must honour, both encoded in the sink:

* absence sets ``presence=ABSENT`` and never ``source_status=CLOSED``. We know
  the row was missing; only the source knows whether the position closed
  (INV-08, WF3-073);
* only an unfiltered scan may publish. A ``status=OPEN`` listing says nothing
  about positions outside that filter, so using one would mark them all absent
  (WF3-066, WF3-079).
"""

from __future__ import annotations

import time
from typing import Callable

from trading_sync.application.killpoints import Barriers
from trading_sync.application.ports import EpochHandle, Lease, ScanHandle, SyncSink, SyncSource
from trading_sync.application.source_epoch_guard import SourceEpochGuard
from trading_sync.application.sync_deals import ScanOutcome, _aclose
from trading_sync.config import Settings
from trading_sync.domain.identifiers import FILTER_UNFILTERED
from trading_sync.domain.snapshots import Entity, ScanStatus
from trading_sync.domain.validation import ScanValidationState, validate_page
from trading_sync.errors import (
    DataIntegrityError,
    IncompleteScanError,
    ScanBudgetExceededError,
    SyncError,
)
from trading_sync.infrastructure.logging import get_logger
from trading_sync.infrastructure.metrics import SyncMetrics

__all__ = ["SnapshotSyncService"]

_LOG = get_logger("trading_sync.sync_snapshot")


class SnapshotSyncService:
    """Stages and publishes one full snapshot of accounts or positions."""

    def __init__(
        self,
        sink: SyncSink,
        settings: Settings,
        guard: SourceEpochGuard,
        *,
        metrics: SyncMetrics | None = None,
        barriers: Barriers | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._sink = sink
        self._settings = settings
        self._guard = guard
        self._metrics = metrics
        self._barriers = barriers
        self._monotonic = monotonic

    async def _barrier(self, name: str) -> None:
        if self._barriers is not None:
            await self._barriers.reach(name)

    async def run_once(self, source: SyncSource, entity: Entity) -> ScanOutcome:
        if entity not in (Entity.ACCOUNTS, Entity.POSITIONS):
            raise SyncError("entity does not use snapshot sync", entity=str(entity))
        epoch, _identity = await self._guard.resolve(source, entity=entity)
        lease = await self._sink.acquire_lease(
            epoch.epoch_id,
            entity,
            FILTER_UNFILTERED,
            ttl_seconds=self._settings.lease.ttl_seconds,
        )
        if lease is None:
            watermark = await self._sink.read_watermark(
                epoch.epoch_id, entity, FILTER_UNFILTERED
            )
            return ScanOutcome(
                scan_id=None,
                entity=entity,
                status=ScanStatus.ABORTED,
                pages=0,
                records=0,
                duplicates=0,
                snapshot_seq=None,
                watermark_before=watermark,
                watermark_after=watermark,
                reason_code="LEASE_HELD_ELSEWHERE",
            )
        try:
            return await self._scan_with_lease(source, epoch, lease, entity)
        finally:
            try:
                await self._sink.release_lease(lease)
            except SyncError:
                _LOG.warning("lease release failed", extra={"entity": str(entity)})

    async def _scan_with_lease(
        self, source: SyncSource, epoch: EpochHandle, lease: Lease, entity: Entity
    ) -> ScanOutcome:
        watermark = await self._sink.read_watermark(
            epoch.epoch_id, entity, FILTER_UNFILTERED
        )
        scan = await self._sink.create_scan(lease, start_watermark=watermark)
        state = ScanValidationState(
            entity=entity,
            filter_scope=FILTER_UNFILTERED,
            start_watermark=0,
            max_page_items=self._settings.source.page_size,
            max_pages=self._settings.sync.max_pages_per_scan,
            allowed_currencies=self._settings.source.allowed_currencies,
        )
        started = self._monotonic()
        pages = 0
        records = 0
        duplicates = 0
        snapshot_seq: int | None = None
        terminal = False
        last_heartbeat = started

        if self._metrics is not None:
            self._metrics.scan_inflight.labels(entity=str(entity)).inc()
        stream = (
            source.iter_accounts(self._settings.source.source_id)
            if entity is Entity.ACCOUNTS
            else source.iter_positions(self._settings.source.source_id)
        )
        try:
            await self._barrier("K11_before_snapshot_stage")
            async for raw in stream:
                self._check_budget(started, pages)
                page = validate_page(raw, state)
                self._guard.verify_page(epoch, page)
                lease, last_heartbeat = await self._maybe_heartbeat(lease, last_heartbeat)
                result = await self._sink.stage_snapshot_page(lease, scan, page)
                pages += 1
                records += result.inserted
                duplicates += result.duplicates
                snapshot_seq = page.snapshot_seq
                if page.is_terminal:
                    terminal = True
                    break
        except DataIntegrityError as exc:
            await self._sink.quarantine(
                epoch.epoch_id,
                scan.scan_id,
                entity,
                reason_code=exc.reason_code,
                source_key_hash=exc.safe_details.get("source_key_hash"),
                safe_details=exc.safe_details,
            )
            await self._sink.mark_scan_status(
                scan, str(ScanStatus.QUARANTINED), reason_code=exc.reason_code
            )
            self._record_outcome(entity, "quarantined", started)
            raise
        except SyncError as exc:
            await self._abort(scan, epoch, exc)
            self._record_outcome(entity, "aborted", started)
            raise
        except BaseException as exc:
            await self._abort(scan, epoch, exc)
            raise
        finally:
            if self._metrics is not None:
                self._metrics.scan_inflight.labels(entity=str(entity)).dec()
            await _aclose(stream)

        if not terminal or snapshot_seq is None:
            # Staging rows stay behind for the retention sweep; what matters is
            # that the current projection is untouched, so a reader still sees
            # the last complete snapshot (WF3-061, fixture G14).
            error = IncompleteScanError(
                "snapshot scan never reached a terminal page", pages=pages
            )
            await self._abort(scan, epoch, error)
            self._record_outcome(entity, "aborted", started)
            raise error

        # STAGED is a durable fact, not an in-memory one: the publisher
        # re-reads it, so a restart between staging and promotion cannot let a
        # half-staged scan promote itself (INV-21).
        await self._sink.mark_scan_status(
            scan, str(ScanStatus.STAGED), snapshot_seq=snapshot_seq
        )
        publication = await self._sink.publish_snapshot(
            lease, scan, snapshot_seq=snapshot_seq
        )
        self._record_outcome(entity, "published", started)
        return ScanOutcome(
            scan_id=scan.scan_id,
            entity=entity,
            status=ScanStatus.PUBLISHED,
            pages=pages,
            records=publication.rows_published,
            duplicates=duplicates,
            snapshot_seq=snapshot_seq,
            watermark_before=watermark,
            watermark_after=snapshot_seq,
        )

    def _check_budget(self, started: float, pages: int) -> None:
        elapsed = self._monotonic() - started
        if elapsed > self._settings.sync.scan_max_duration_seconds:
            raise ScanBudgetExceededError(
                "scan exceeded its wall-clock budget",
                elapsed_seconds=round(elapsed, 3),
                budget_seconds=self._settings.sync.scan_max_duration_seconds,
            )
        if pages >= self._settings.sync.max_pages_per_scan:
            raise ScanBudgetExceededError(
                "scan exceeded its page budget",
                pages=pages,
                max_pages=self._settings.sync.max_pages_per_scan,
            )

    async def _maybe_heartbeat(self, lease: Lease, last: float) -> tuple[Lease, float]:
        now = self._monotonic()
        if now - last < self._settings.lease.heartbeat_seconds:
            return lease, last
        renewed = await self._sink.heartbeat_lease(
            lease, ttl_seconds=self._settings.lease.ttl_seconds
        )
        return renewed, now

    async def _abort(
        self, scan: ScanHandle, epoch: EpochHandle, error: BaseException
    ) -> None:
        reason = getattr(error, "reason_code", type(error).__name__)
        try:
            await self._sink.mark_scan_status(
                scan, str(ScanStatus.ABORTED), reason_code=reason
            )
            await self._sink.audit(
                epoch.epoch_id,
                scan.scan_id,
                "SCAN_ABORTED",
                reason_code=reason,
                safe_details=getattr(error, "safe_details", {}) or {},
            )
        except SyncError:
            _LOG.warning("could not record scan abort", extra={"reason_code": reason})

    def _record_outcome(self, entity: Entity, outcome: str, started: float) -> None:
        if self._metrics is None:
            return
        self._metrics.scan_total.labels(entity=str(entity), outcome=outcome).inc()
        self._metrics.scan_duration_seconds.labels(
            entity=str(entity), outcome=outcome
        ).observe(self._monotonic() - started)

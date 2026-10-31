"""Incremental deal synchronization.

Deals are immutable and ordered by a source event sequence, so they can be
replayed incrementally: read everything above the durable watermark, insert
idempotently, and move the watermark only once the whole snapshot is proven
read. That last clause is the entire design. Committing 199 of 200 pages and
then advancing the watermark would skip a page permanently, and nothing
downstream could ever notice (INV-01, INV-02).

The loop below therefore has a specific shape:

* one page in memory at a time, and the next page is requested only after the
  previous one has committed. That is both the memory bound and the
  backpressure mechanism (INV-15, WF3-146);
* the watermark is written exactly once, at the end, by a transaction that
  re-proves the receipt chain, the lease and the epoch;
* a failure anywhere leaves the watermark alone. The next scan starts from the
  same place and the idempotent insert absorbs whatever was already written
  (sections 10.1 to 10.3).
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any, AsyncIterator, Callable

from trading_sync.application.killpoints import Barriers
from trading_sync.application.ports import EpochHandle, Lease, ScanHandle, SyncSink, SyncSource
from trading_sync.application.source_epoch_guard import SourceEpochGuard
from trading_sync.config import Settings
from trading_sync.domain.identifiers import FILTER_UNFILTERED
from trading_sync.domain.snapshots import Entity, RawPage, ScanStatus, ValidatedPage
from trading_sync.domain.validation import ScanValidationState, validate_page
from trading_sync.errors import (
    DataIntegrityError,
    IncompleteScanError,
    LeaseLostError,
    ScanBudgetExceededError,
    SyncError,
)
from trading_sync.infrastructure.logging import get_logger
from trading_sync.infrastructure.metrics import SyncMetrics

__all__ = ["DealSyncService", "ScanOutcome"]

_LOG = get_logger("trading_sync.sync_deals")


@dataclass(frozen=True, slots=True)
class ScanOutcome:
    """What one scan attempt did. Derived from durable state, not from memory."""

    scan_id: Any
    entity: Entity
    status: ScanStatus
    pages: int
    records: int
    duplicates: int
    snapshot_seq: int | None
    watermark_before: int
    watermark_after: int
    reason_code: str | None = None
    boundary_anomalies: int = 0

    @property
    def advanced(self) -> bool:
        return self.watermark_after > self.watermark_before


class DealSyncService:
    """Runs one deal scan per :meth:`run_once` call."""

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

    async def run_once(self, source: SyncSource) -> ScanOutcome:
        """Resolve the epoch, take the lease, scan, and publish if complete."""
        epoch, _identity = await self._guard.resolve(source, entity=Entity.DEALS)
        lease = await self._sink.acquire_lease(
            epoch.epoch_id,
            Entity.DEALS,
            FILTER_UNFILTERED,
            ttl_seconds=self._settings.lease.ttl_seconds,
        )
        if lease is None:
            # Another worker owns this scope. Not an error: the design allows
            # one scan per entity per source, so the correct action is to
            # yield (design 8.4).
            watermark = await self._sink.read_watermark(
                epoch.epoch_id, Entity.DEALS, FILTER_UNFILTERED
            )
            return ScanOutcome(
                scan_id=None,
                entity=Entity.DEALS,
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
            return await self._scan_with_lease(source, epoch, lease)
        finally:
            # Best effort: a failed release just means the next worker waits
            # for the TTL, which is safe. Raising here would mask the real error.
            try:
                await self._sink.release_lease(lease)
            except SyncError:
                _LOG.warning("lease release failed", extra={"entity": str(Entity.DEALS)})

    async def _scan_with_lease(
        self, source: SyncSource, epoch: EpochHandle, lease: Lease
    ) -> ScanOutcome:
        watermark = await self._sink.read_watermark(
            epoch.epoch_id, Entity.DEALS, FILTER_UNFILTERED
        )
        scan = await self._sink.create_scan(lease, start_watermark=watermark)
        state = ScanValidationState(
            entity=Entity.DEALS,
            filter_scope=FILTER_UNFILTERED,
            start_watermark=watermark,
            max_page_items=self._settings.source.page_size,
            max_pages=self._settings.sync.max_pages_per_scan,
        )
        started = self._monotonic()
        pages = 0
        records = 0
        duplicates = 0
        snapshot_seq: int | None = None
        terminal = False
        last_heartbeat = started

        if self._metrics is not None:
            self._metrics.scan_inflight.labels(entity=str(Entity.DEALS)).inc()
        stream = source.iter_deals(self._settings.source.source_id, watermark)
        try:
            await self._barrier("K00_before_source_call")
            async for raw in stream:
                await self._barrier("K01_after_source_response")
                self._check_budget(started, pages)
                page = validate_page(raw, state)
                self._guard.verify_page(epoch, page)
                await self._barrier("K02_after_page_validate")

                lease, last_heartbeat = await self._maybe_heartbeat(lease, last_heartbeat)
                result = await self._sink.persist_deal_page(lease, scan, page)

                pages += 1
                records += result.inserted
                duplicates += result.duplicates
                snapshot_seq = page.snapshot_seq
                if page.is_terminal:
                    terminal = True
                    break
        except DataIntegrityError as exc:
            # The page transaction rolled back, so the evidence has to be
            # written separately or it is lost along with it (design 18.2).
            await self._sink.quarantine(
                epoch.epoch_id,
                scan.scan_id,
                Entity.DEALS,
                reason_code=exc.reason_code,
                source_key_hash=exc.safe_details.get("source_key_hash"),
                safe_details=exc.safe_details,
            )
            await self._sink.mark_scan_status(
                scan, str(ScanStatus.QUARANTINED), reason_code=exc.reason_code
            )
            self._record_outcome(Entity.DEALS, "quarantined", started)
            raise
        except SyncError as exc:
            await self._abort(scan, epoch, exc)
            self._record_outcome(Entity.DEALS, "aborted", started)
            raise
        except BaseException as exc:
            # Cancellation lands here. Mark the scan aborted best-effort and
            # re-raise: a cancelled coroutine must never be reported as success,
            # and an already-committed page is not undone by it (design 18.4).
            await self._abort(scan, epoch, exc)
            raise
        finally:
            if self._metrics is not None:
                self._metrics.scan_inflight.labels(entity=str(Entity.DEALS)).dec()
            # Close the generator so a fetch in flight cannot deliver a page
            # into a scan that has already ended (WF3-143).
            await _aclose(stream)

        if not terminal or snapshot_seq is None:
            error = IncompleteScanError(
                "source never produced a terminal page", pages=pages
            )
            await self._abort(scan, epoch, error)
            self._record_outcome(Entity.DEALS, "aborted", started)
            raise error

        await self._barrier("K07_after_last_page_commit")
        if state.boundary_anomaly_count and self._metrics is not None:
            self._metrics.boundary_anomalies_total.labels(
                entity=str(Entity.DEALS)
            ).inc(state.boundary_anomaly_count)

        # Allowed to equal the old watermark: a snapshot with no new deals still
        # proves the window was read, and non-deal events move the source
        # sequence forward on their own (design 6.2).
        new_watermark = max(snapshot_seq, watermark)
        await self._sink.publish_deal_checkpoint(
            lease, scan, old_watermark=watermark, new_watermark=new_watermark
        )
        self._record_outcome(Entity.DEALS, "published", started)
        return ScanOutcome(
            scan_id=scan.scan_id,
            entity=Entity.DEALS,
            status=ScanStatus.PUBLISHED,
            pages=pages,
            records=records,
            duplicates=duplicates,
            snapshot_seq=snapshot_seq,
            watermark_before=watermark,
            watermark_after=new_watermark,
            boundary_anomalies=state.boundary_anomaly_count,
        )

    def _check_budget(self, started: float, pages: int) -> None:
        """Stop a scan that is about to outlive the source's cursor.

        Reaching the cursor TTL mid-scan wastes the whole attempt, so the budget
        is deliberately shorter than the TTL and the scan aborts itself first,
        recoverably (WF3-047, WF3-150).
        """
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
        """Renew the lease on schedule, mid-scan.

        A long scan has to keep proving it is alive. Otherwise its lease
        expires, another worker takes over, and this one learns of the loss only
        at its final commit, after doing all the work (design 8.2).
        """
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
            # Best effort. The watermark has not moved, and that is the fact
            # that actually keeps the data safe.
            _LOG.warning("could not record scan abort", extra={"reason_code": reason})

    def _record_outcome(self, entity: Entity, outcome: str, started: float) -> None:
        if self._metrics is None:
            return
        self._metrics.scan_total.labels(entity=str(entity), outcome=outcome).inc()
        self._metrics.scan_duration_seconds.labels(
            entity=str(entity), outcome=outcome
        ).observe(self._monotonic() - started)


async def _aclose(stream: AsyncIterator[RawPage]) -> None:
    """Close a page generator, swallowing the noise a closed loop can raise."""
    closer = getattr(stream, "aclose", None)
    if closer is not None:
        try:
            await closer()
        except (RuntimeError, asyncio.CancelledError):
            pass

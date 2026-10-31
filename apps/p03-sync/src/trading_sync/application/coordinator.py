"""Admission control and scan-level retry.

Two jobs, both about bounding work rather than doing it.

**Admission.** A thousand triggers must not become a thousand scans. The gate
holds a concurrency slot count and a bounded waiting room; beyond that,
requests are *rejected* rather than queued, because an unbounded queue just
moves the memory problem somewhere less visible (WF3-148, INV-15).

**Scan retry.** One place decides whether a failed scan is attempted again,
using the policy in :mod:`trading_sync.application.retry`. Retries always start
a brand-new scan from the durable watermark -- never resume the old one, whose
cursor may be dead and whose lease may have moved (design 9.2).

A failed entity does not fail the others. One quarantined entity with two
healthy ones is a real and reportable state; collapsing it into a single
service-wide verdict would either hide the quarantine or falsely condemn the
healthy entities (WF3-221).
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Callable

from trading_sync.application.ports import SyncSink, SyncSource
from trading_sync.application.retry import RetryBudget, ScanRetryPolicy
from trading_sync.application.sync_deals import DealSyncService, ScanOutcome
from trading_sync.application.sync_snapshot import SnapshotSyncService
from trading_sync.config import Settings
from trading_sync.domain.snapshots import Entity
from trading_sync.errors import AdmissionRejectedError, SyncError
from trading_sync.infrastructure.logging import get_logger
from trading_sync.infrastructure.metrics import SyncMetrics

__all__ = ["EntityResult", "SyncCoordinator"]

_LOG = get_logger("trading_sync.coordinator")


@dataclass(frozen=True, slots=True)
class EntityResult:
    """Outcome for one entity: either a scan result or a classified failure."""

    entity: Entity
    outcome: ScanOutcome | None
    error: SyncError | None = None
    attempts: int = 1

    @property
    def ok(self) -> bool:
        return self.error is None

    @property
    def reason_code(self) -> str | None:
        return None if self.error is None else self.error.reason_code


class SyncCoordinator:
    """Drives the entity services under admission and retry policy."""

    def __init__(
        self,
        sink: SyncSink,
        settings: Settings,
        deals: DealSyncService,
        snapshots: SnapshotSyncService,
        *,
        metrics: SyncMetrics | None = None,
        sleep: Callable[[float], object] = asyncio.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._sink = sink
        self._settings = settings
        self._deals = deals
        self._snapshots = snapshots
        self._metrics = metrics
        self._sleep = sleep
        self._monotonic = monotonic
        self._slots = asyncio.Semaphore(settings.sync.worker_concurrency)
        self._waiting = 0
        self._budget = RetryBudget(
            max_attempts=settings.sync.scan_retry_attempts + 1,
            window_seconds=600.0,
            clock=monotonic,
        )

    @property
    def waiting(self) -> int:
        return self._waiting

    def _policy(self) -> ScanRetryPolicy:
        return ScanRetryPolicy(
            max_attempts=self._settings.sync.scan_retry_attempts,
            base_seconds=self._settings.sync.scan_retry_base_seconds,
            cap_seconds=self._settings.sync.scan_retry_max_seconds,
            budget=self._budget,
        )

    async def sync_entity(self, source: SyncSource, entity: Entity) -> EntityResult:
        """Run one entity to completion, retrying only what policy allows."""
        policy = self._policy()
        attempt = 0
        while True:
            try:
                outcome = await self._run_admitted(source, entity)
                return EntityResult(entity=entity, outcome=outcome, attempts=attempt + 1)
            except SyncError as exc:
                decision = policy.decide(exc, attempt=attempt)
                _LOG.warning(
                    "scan failed",
                    extra={
                        "entity": str(entity),
                        "reason_code": exc.reason_code,
                        "attempt": attempt,
                        "will_retry": decision.retry,
                    },
                )
                if not decision.retry:
                    return EntityResult(
                        entity=entity, outcome=None, error=exc, attempts=attempt + 1
                    )
                await self._sleep(decision.delay_seconds)
                attempt += 1

    async def _run_admitted(self, source: SyncSource, entity: Entity) -> ScanOutcome:
        if self._waiting >= self._settings.sync.scan_queue_capacity:
            # Rejecting is the containment. Queueing without a bound would
            # convert a trigger storm into unbounded memory growth.
            raise AdmissionRejectedError(
                "scan queue is at capacity",
                entity=str(entity),
                waiting=self._waiting,
                capacity=self._settings.sync.scan_queue_capacity,
            )
        self._waiting += 1
        queued_at = self._monotonic()
        try:
            async with self._slots:
                if self._metrics is not None:
                    self._metrics.queue_wait_seconds.labels(operation="scan").observe(
                        self._monotonic() - queued_at
                    )
                if entity is Entity.DEALS:
                    return await self._deals.run_once(source)
                return await self._snapshots.run_once(source, entity)
        finally:
            self._waiting -= 1

    async def sync_all(self, source: SyncSource) -> list[EntityResult]:
        """Sync every entity in sequence, keeping their outcomes independent.

        Sequential rather than concurrent: the MVP allows one scan per entity
        and the upstream connector is a single serial actor, so fanning out
        would only queue behind the narrowest bottleneck while multiplying
        in-flight state (design 8.4, WF3-131).

        Accounts and positions run before deals so that when a deal references
        a position, the current projection is as recent as it can be. It is
        not a consistency guarantee -- there is no cross-entity cut -- just a
        reduction in avoidable unresolved references (WF3-219).
        """
        results: list[EntityResult] = []
        for entity in (Entity.ACCOUNTS, Entity.POSITIONS, Entity.DEALS):
            results.append(await self.sync_entity(source, entity))
        return results

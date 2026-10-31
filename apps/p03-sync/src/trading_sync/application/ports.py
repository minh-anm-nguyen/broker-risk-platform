"""Ports: the only interfaces the application layer is allowed to depend on.

The source port intentionally does **not** mirror any method Project 02 is
asserted to already expose (design 2.2). It describes what this service needs;
the adapter in ``infrastructure.source_adapter`` is responsible for bridging
whatever the connector actually offers, and the fake in the test suite
implements the same port so orchestration logic is exercised identically
against both.

One rule is structural rather than documented: :meth:`SyncSource.iter_deals`
and friends are async *generators*, and the application awaits a sink commit
between iterations. A source implementation must not fetch page N+1 before the
consumer asks for it. Prefetching would both break the backpressure bound
(INV-15, WF3-146) and make a mid-scan abort leave fetched-but-unwanted pages
in flight.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, AsyncIterator, Protocol, runtime_checkable
from uuid import UUID

from trading_sync.domain.snapshots import (
    Entity,
    EpochStatus,
    SourceIdentity,
    RawPage,
    ValidatedPage,
)

__all__ = [
    "EpochHandle",
    "Lease",
    "PageCommitResult",
    "PublicationResult",
    "ScanHandle",
    "SyncSink",
    "SyncSource",
]


@dataclass(frozen=True, slots=True)
class EpochHandle:
    """A durable source epoch: one ``(source_id, run_id, generation)``."""

    epoch_id: UUID
    source_id: str
    run_id: str
    generation: int
    status: EpochStatus

    @property
    def label(self) -> str:
        return f"{self.run_id}:G{self.generation}"


@dataclass(frozen=True, slots=True)
class Lease:
    """Proof of ownership for one ``(epoch, entity, scope)``.

    ``token`` and ``version`` together are the fencing token. The version is
    monotonic, so a worker that was paused past its TTL can be recognised and
    refused at the commit guard even if it still holds a plausible-looking
    token (design 8.2, INV-09).

    ``expires_at`` comes from the *database* clock. A worker's own clock is not
    used for ownership decisions, because a skewed worker would otherwise
    either hold a dead lease or surrender a live one (WF3-126).
    """

    epoch_id: UUID
    entity: Entity
    filter_scope: str
    token: UUID
    version: int
    expires_at: datetime
    watermark: int


@dataclass(frozen=True, slots=True)
class ScanHandle:
    """One scan attempt. A retry is a new scan, never a resumed one."""

    scan_id: UUID
    epoch_id: UUID
    entity: Entity
    filter_scope: str
    start_watermark: int
    owner_lease_version: int


@dataclass(frozen=True, slots=True)
class PageCommitResult:
    """Outcome of one page transaction.

    ``already_committed`` is how an ambiguous COMMIT resolves: the page was
    found already durably receipted with an identical hash, so the retry is
    acknowledged rather than re-applied (design 6.5, WF3-053).
    """

    page_index: int
    inserted: int
    duplicates: int
    already_committed: bool = False


@dataclass(frozen=True, slots=True)
class PublicationResult:
    scan_id: UUID
    snapshot_seq: int
    rows_published: int
    rows_marked_absent: int


@runtime_checkable
class SyncSource(Protocol):
    """Read-only view of an upstream source, via the Project 02 connector."""

    async def read_meta(self, source_id: str) -> SourceIdentity:
        """Return the source's self-reported identity and capabilities.

        Must raise rather than synthesise a missing ``run_id`` or
        ``generation`` (WF3-001, WF3-002).
        """
        ...

    def iter_accounts(self, source_id: str) -> AsyncIterator[RawPage]: ...

    def iter_positions(self, source_id: str) -> AsyncIterator[RawPage]: ...

    def iter_deals(self, source_id: str, after_seq: int) -> AsyncIterator[RawPage]:
        """Pages of deals with ``source_seq > after_seq``, exclusive (AS-04)."""
        ...


@runtime_checkable
class SyncSink(Protocol):
    """The durable side. PostgreSQL is the only authority for these facts.

    Every method that could advance visible state takes the :class:`Lease`, and
    each one re-verifies ownership, epoch status and the previous watermark
    *inside its own transaction*. Checking in Python before calling would be a
    time-of-check/time-of-use gap wide enough for a takeover to land in
    (design 8.2, WF3-122).
    """

    # -- enrollment and epochs ------------------------------------------------

    async def enroll_source(self, source_id: str, source_kind: str) -> None:
        """Idempotently register a configured source."""
        ...

    async def register_epoch(self, identity: SourceIdentity) -> EpochHandle:
        """Record a newly observed epoch as ``DISCOVERED``.

        Discovery alone does not authorise ingestion: a brand-new
        ``(run_id, generation)`` needs an explicit approval before it can be
        bootstrapped, so a source reset cannot quietly restart sync against a
        different dataset (design 8.3 item 4).
        """
        ...

    async def approve_epoch(self, epoch_id: UUID, *, reason: str) -> EpochHandle: ...

    async def get_active_epoch(self, source_id: str) -> EpochHandle | None: ...

    async def block_epoch(self, epoch_id: UUID, *, reason_code: str) -> None:
        """Fence an epoch. No scan of it may commit or publish afterwards."""
        ...

    # -- leases ---------------------------------------------------------------

    async def acquire_lease(
        self, epoch_id: UUID, entity: Entity, filter_scope: str, *, ttl_seconds: int
    ) -> Lease | None:
        """Take the lease, or return ``None`` if someone else holds a live one."""
        ...

    async def heartbeat_lease(self, lease: Lease, *, ttl_seconds: int) -> Lease:
        """Extend the lease, or raise :class:`LeaseLostError` if it is gone."""
        ...

    async def release_lease(self, lease: Lease) -> None: ...

    # -- checkpoints and scans ------------------------------------------------

    async def read_watermark(
        self, epoch_id: UUID, entity: Entity, filter_scope: str
    ) -> int: ...

    async def create_scan(
        self, lease: Lease, *, start_watermark: int
    ) -> ScanHandle: ...

    async def mark_scan_status(
        self,
        scan: ScanHandle,
        status: str,
        *,
        reason_code: str | None = None,
        snapshot_seq: int | None = None,
    ) -> None: ...

    # -- deal path ------------------------------------------------------------

    async def persist_deal_page(
        self, lease: Lease, scan: ScanHandle, page: ValidatedPage
    ) -> PageCommitResult:
        """Insert a page of deals and its receipt in one transaction.

        An existing row with the same key and an identical payload is a no-op.
        An existing row with *different* content raises
        :class:`ImmutableDealConflict` and rolls the page back; the stored
        value stands (INV-05, WF3-028).
        """
        ...

    async def publish_deal_checkpoint(
        self, lease: Lease, scan: ScanHandle, *, old_watermark: int, new_watermark: int
    ) -> None:
        """Advance the deal watermark, under every guard at once.

        Compare-and-set on the old watermark, a live lease at the matching
        version, an ``ACTIVE`` epoch, an unfiltered scope, and a complete
        receipt chain with exactly one terminal page. Any one failing aborts
        the transaction and leaves the watermark where it was (design 6.4).
        """
        ...

    # -- mutable snapshot path ------------------------------------------------

    async def stage_snapshot_page(
        self, lease: Lease, scan: ScanHandle, page: ValidatedPage
    ) -> PageCommitResult:
        """Write a page into staging. Readers see nothing of it (design 7.2)."""
        ...

    async def publish_snapshot(
        self, lease: Lease, scan: ScanHandle, *, snapshot_seq: int
    ) -> PublicationResult:
        """Promote a staged snapshot in a single transaction.

        Upsert every staged row, mark everything the snapshot did not contain
        ``ABSENT`` -- never ``CLOSED`` (INV-08) -- and record the publication.
        A concurrent reader sees the old set or the new set, never a mixture
        (INV-07).
        """
        ...

    # -- incidents and reporting ----------------------------------------------

    async def quarantine(
        self,
        epoch_id: UUID,
        scan_id: UUID | None,
        entity: Entity,
        *,
        reason_code: str,
        source_key: str | None = None,
        source_key_hash: str | None = None,
        safe_details: dict[str, Any] | None = None,
    ) -> None:
        """Record an incident in its own transaction.

        Separate on purpose: the offending page has been rolled back, and the
        evidence has to survive that rollback (design 18.2).
        """
        ...

    async def audit(
        self,
        epoch_id: UUID,
        scan_id: UUID | None,
        event_type: str,
        *,
        reason_code: str | None = None,
        safe_details: dict[str, Any] | None = None,
    ) -> None: ...

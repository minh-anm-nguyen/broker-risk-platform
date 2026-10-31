"""Multi-page scans: snapshot continuity, bounded memory, honest outcomes.

A scan is the unit a consumer cares about, because a *page* being valid says
nothing about whether a dataset was read completely. This module owns the
difference.

What it guarantees, and the failure each guarantee prevents:

* **Every page comes from the snapshot the first page pinned.** Run,
  generation and ``snapshot_seq`` are captured from page 1 and every later
  page is held to them. A page from a different snapshot aborts the scan
  rather than being appended, because the union of two snapshots is a set
  that never existed at any instant (INV-07, WF2-I-06).
* **A cursor loop terminates.** Cursor hashes are remembered, so a source
  that keeps handing back the same cursor is detected rather than paginated
  forever (WF2-I-02).
* **Memory is bounded, and when a bound is hit the scan says so.** Duplicate
  detection keeps keys in a set, and a 1,000,000-deal scan would make that
  set the dominant memory cost. Past
  :attr:`~mt5_connector.config.Settings.max_scan_tracked_keys` the scan stops
  tracking and records ``global_uniqueness_verified=False``. It does **not**
  keep claiming a guarantee it stopped checking - a false "no duplicates" is
  worse than an acknowledged gap in verification (section 7.4, WF2-I-10,
  WF2-M-08).
* **Only a terminal page produces a checkpoint candidate.** Every other
  outcome yields ``None``. A partial scan has no watermark to offer, and this
  is where that is enforced rather than hoped for (WF2-I-06, WF2-K-03).

Backpressure is deliberate: :meth:`Scanner.run` awaits the sink before
fetching the next page. A connector that raced ahead of a slow consumer would
buy throughput by buffering pages in memory, which is the unbounded growth
INV-03 forbids (WF2-Q-02, WF2-K-11).
"""

from __future__ import annotations

import logging
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Awaitable, Callable

from .client import Connector, ScanExpectation
from .errors import (
    ConnectorError,
    DataIntegrityError,
    ResyncRequiredError,
    SourceContractError,
    SourceEpochChangedError,
)
from .ports import (
    CheckpointCandidate,
    Entity,
    Page,
    ReadPlan,
    ScanOutcome,
    ScanResult,
    SourceIdentity,
)
from .validation import cursor_fingerprint, filter_digest

__all__ = ["ScanContext", "Scanner", "ScanAborted"]

logger = logging.getLogger("mt5_connector.scan")

#: Scan failures that mean the *source* needs renegotiating, not just this
#: scan. Everything else - an expired cursor, a page that failed validation -
#: is reported to the consumer as ``RESYNC_REQUIRED`` for the scan while the
#: session keeps serving.
_SESSION_LEVEL_RESYNC: frozenset[str] = frozenset(
    {SourceEpochChangedError.code, ResyncRequiredError.code}
)


class ScanAborted(ConnectorError):
    """A scan hit one of its own bounds rather than a source failure.

    Separate from a source error so a consumer can tell "the source misbehaved"
    from "this scan was larger or longer than the connector is configured to
    attempt", which have different remedies (WF2-M-08).
    """

    code = "SCAN_ABORTED"
    category = "capacity"
    resync_required = True
    safe_action = (
        "the scan exceeded a configured bound and is incomplete. Raise the bound or "
        "narrow the read plan; do not treat the pages already read as a complete set"
    )


@dataclass(slots=True)
class ScanContext:
    """Mutable state for one scan. One per scan, never shared."""

    scan_id: str
    entity: Entity
    filter_digest: str
    requested_after_seq: int | None
    max_pages: int
    max_tracked_keys: int

    run_id: str | None = None
    generation: int | None = None
    snapshot_seq: int | None = None
    worker_epoch_at_start: int | None = None
    cursor: str | None = None
    pages_seen: int = 0
    items_seen: int = 0
    byte_count: int = 0
    observed_upper_bound: int | None = None
    complete: bool = False

    #: Keys seen so far, for cross-page duplicate detection.
    seen_keys: set[str] = field(default_factory=set)
    #: ``False`` once tracking stopped because the key budget was exhausted.
    global_uniqueness_verified: bool = True
    #: Recent cursor hashes, oldest first. A deque rather than a set so the
    #: window is bounded by count and a long scan cannot grow it.
    cursor_hashes: deque[str] = field(default_factory=lambda: deque(maxlen=4096))
    _cursor_hash_set: set[str] = field(default_factory=set)

    def pin(self, page: Page[Any]) -> None:
        """Capture the provenance every later page is held to."""
        self.run_id = page.provenance.run_id
        self.generation = page.provenance.generation
        self.snapshot_seq = page.snapshot_seq
        self.worker_epoch_at_start = page.identity.worker_epoch

    def expectation(self) -> ScanExpectation:
        return ScanExpectation(
            run_id=self.run_id,
            generation=self.generation,
            snapshot_seq=self.snapshot_seq,
            page_index=self.pages_seen,
            previous_upper_bound=self.observed_upper_bound,
            requested_after_seq=self.requested_after_seq,
        )

    def note_cursor(self, cursor: str) -> bool:
        """Record a cursor. Returns ``False`` if it has been seen before."""
        digest = cursor_fingerprint(cursor)
        if digest in self._cursor_hash_set:
            return False
        if len(self.cursor_hashes) == self.cursor_hashes.maxlen:
            # The deque evicted its oldest entry; drop it from the set too so
            # the two stay the same size.
            evicted = self.cursor_hashes[0]
            self._cursor_hash_set.discard(evicted)
        self.cursor_hashes.append(digest)
        self._cursor_hash_set.add(digest)
        return True

    def track_keys(self, keys: list[str]) -> str | None:
        """Record item keys. Returns the first duplicate found, if any."""
        if not self.global_uniqueness_verified:
            return None
        for key in keys:
            if key in self.seen_keys:
                return key
            self.seen_keys.add(key)
            if len(self.seen_keys) > self.max_tracked_keys:
                # Honesty over a false guarantee: the set is released and the
                # scan records that it stopped verifying uniqueness globally.
                self.global_uniqueness_verified = False
                self.seen_keys = set()
                logger.warning(
                    "duplicate tracking stopped: the scan exceeded its tracked-key "
                    "budget, so this scan no longer verifies global uniqueness",
                    extra={
                        "scan_id": self.scan_id,
                        "entity": self.entity.value,
                        "max_tracked_keys": self.max_tracked_keys,
                    },
                )
                return None
        return None

    def summary(self) -> dict[str, Any]:
        return {
            "scan_id": self.scan_id,
            "entity": self.entity.value,
            "filter_digest": self.filter_digest,
            "run_id": self.run_id,
            "generation": self.generation,
            "snapshot_seq": self.snapshot_seq,
            "worker_epoch_at_start": self.worker_epoch_at_start,
            "pages_seen": self.pages_seen,
            "items_seen": self.items_seen,
            "byte_count": self.byte_count,
            "observed_upper_bound": self.observed_upper_bound,
            "complete": self.complete,
            "global_uniqueness_verified": self.global_uniqueness_verified,
            "requested_after_seq": self.requested_after_seq,
        }


def _key_of(item: Any, entity: Entity) -> str:
    if entity is Entity.ACCOUNTS:
        return item.account_id
    if entity is Entity.POSITIONS:
        return item.position_id
    return item.deal_id


class Scanner:
    """Drives a paged scan over one entity."""

    def __init__(self, connector: Connector) -> None:
        self._connector = connector
        self._settings = connector.settings

    async def pages(
        self,
        plan: ReadPlan,
        *,
        deadline_ms: int | None = None,
        client_id: str = "default",
        context: ScanContext | None = None,
    ) -> AsyncIterator[Page[Any]]:
        """Yield validated pages until the snapshot is exhausted.

        The scan slot is held for the whole iteration and released in a
        ``finally``, so a consumer abandoning the generator does not leak it.
        """
        ctx = context or self.new_context(plan)
        slot = self._connector.admission.acquire_scan(scan_id=ctx.scan_id)
        current = plan
        try:
            while True:
                if ctx.pages_seen >= ctx.max_pages:
                    raise ScanAborted(
                        "the scan reached its page limit before the source reported a "
                        "terminal page, so the result is incomplete",
                        source_id=self._settings.source_id,
                        details={
                            "max_pages_per_scan": ctx.max_pages,
                            "pages_seen": ctx.pages_seen,
                            "entity": ctx.entity.value,
                        },
                    )
                page = await self._connector.read_page(
                    current,
                    deadline_ms=deadline_ms,
                    client_id=client_id,
                    expect=ctx.expectation(),
                )
                if ctx.pages_seen == 0:
                    ctx.pin(page)
                self._absorb(ctx, page)
                yield page

                if page.next_cursor is None:
                    ctx.complete = True
                    return
                if not ctx.note_cursor(page.next_cursor):
                    raise DataIntegrityError(
                        "the source returned a cursor this scan has already used, "
                        "which would paginate forever",
                        source_id=self._settings.source_id,
                        details={
                            "entity": ctx.entity.value,
                            "page_index": page.page_index,
                            "cursor_fingerprint": cursor_fingerprint(page.next_cursor),
                        },
                    )
                if not page.items and page.next_cursor is not None:
                    # Permitted - a filtered scan can produce an empty page -
                    # but only because the cursor advanced, which the check
                    # above has just proved. Without that proof this is an
                    # infinite loop (WF2-I-03).
                    logger.debug(
                        "empty page with a further cursor; continuing because the "
                        "cursor advanced",
                        extra={"scan_id": ctx.scan_id, "page_index": page.page_index},
                    )
                current = current.advance(page.next_cursor)
                ctx.cursor = page.next_cursor
        finally:
            self._connector.admission.release_scan(slot)

    def new_context(self, plan: ReadPlan) -> ScanContext:
        return ScanContext(
            scan_id=f"scan-{uuid.uuid4().hex[:12]}",
            entity=plan.entity,
            filter_digest=filter_digest(plan.filter_fields()),
            requested_after_seq=plan.after_seq,
            max_pages=self._settings.max_pages_per_scan,
            max_tracked_keys=self._settings.max_scan_tracked_keys,
        )

    def _absorb(self, ctx: ScanContext, page: Page[Any]) -> None:
        ctx.pages_seen += 1
        ctx.items_seen += len(page.items)
        ctx.byte_count += page.response_bytes
        if page.observed_upper_bound is not None:
            ctx.observed_upper_bound = (
                page.observed_upper_bound
                if ctx.observed_upper_bound is None
                else max(ctx.observed_upper_bound, page.observed_upper_bound)
            )
        duplicate = ctx.track_keys([_key_of(item, ctx.entity) for item in page.items])
        if duplicate is not None:
            raise DataIntegrityError(
                "the same identifier appeared on two pages of one snapshot scan",
                source_id=self._settings.source_id,
                details={
                    "entity": ctx.entity.value,
                    "page_index": page.page_index,
                    "field": f"{ctx.entity.value}_identifier",
                    # The key itself is omitted: it is a business identifier
                    # and this detail dict is logged (INV-15).
                    "duplicate_key_digest": cursor_fingerprint(duplicate),
                },
            )

    async def run(
        self,
        plan: ReadPlan,
        *,
        sink: Callable[[Page[Any]], Awaitable[None]] | None = None,
        deadline_ms: int | None = None,
        client_id: str = "default",
    ) -> ScanResult:
        """Run a whole scan and report how it ended.

        ``sink`` is awaited for each page *before* the next is fetched. That
        is the backpressure: a consumer taking 60 s to persist a page gets 60 s
        of pause, not 60 s of pages accumulating in memory.

        A sink failure aborts the scan and is reported as ``FAILED``. It is not
        retried here: the connector does not know whether the sink's write
        landed, and re-sending is the consumer's idempotency problem
        (ADR-207).
        """
        ctx = self.new_context(plan)
        identity = self._connector.supervisor.identity or SourceIdentity(
            source_kind=self._settings.source_kind,
            source_id=self._settings.source_id,
            worker_epoch=self._connector.supervisor.worker_epoch,
        )
        iterator = self.pages(plan, deadline_ms=deadline_ms, client_id=client_id, context=ctx)
        try:
            async for page in iterator:
                identity = page.identity
                if sink is not None:
                    await sink(page)
        except ConnectorError as error:
            await iterator.aclose()
            outcome = (
                ScanOutcome.RESYNC_REQUIRED if error.resync_required else ScanOutcome.FAILED
            )
            if error.code in _SESSION_LEVEL_RESYNC:
                # Only an identity change takes the *session* out of service.
                # A cursor that expired, or one page that failed validation,
                # means this scan must start again - not that the source has
                # stopped being usable. Escalating those would take a healthy
                # source offline because one long scan outlived its snapshot
                # lease (section 6.2, WF2-D-04 versus WF2-J-03).
                self._connector.supervisor.mark_resync_required(
                    f"{error.code}: {error.message}"
                )
            logger.warning(
                "scan aborted",
                extra={
                    "scan_id": ctx.scan_id,
                    "entity": ctx.entity.value,
                    "error_code": error.code,
                    "pages_seen": ctx.pages_seen,
                    "items_seen": ctx.items_seen,
                },
            )
            return ScanResult(
                outcome=outcome,
                entity=ctx.entity,
                identity=identity,
                pages_seen=ctx.pages_seen,
                items_seen=ctx.items_seen,
                snapshot_seq=ctx.snapshot_seq,
                checkpoint_candidate=None,
                error_code=error.code,
                reason=error.message,
            )
        except Exception:
            await iterator.aclose()
            raise

        if not ctx.complete or ctx.snapshot_seq is None:  # pragma: no cover - defensive
            return ScanResult(
                outcome=ScanOutcome.INCOMPLETE,
                entity=ctx.entity,
                identity=identity,
                pages_seen=ctx.pages_seen,
                items_seen=ctx.items_seen,
                snapshot_seq=ctx.snapshot_seq,
                reason="the scan ended without a terminal page",
            )

        return ScanResult(
            outcome=ScanOutcome.COMPLETE,
            entity=ctx.entity,
            identity=identity,
            pages_seen=ctx.pages_seen,
            items_seen=ctx.items_seen,
            snapshot_seq=ctx.snapshot_seq,
            checkpoint_candidate=CheckpointCandidate(
                source_id=self._settings.source_id,
                entity=ctx.entity,
                run_id=ctx.run_id,
                generation=ctx.generation,
                snapshot_seq=ctx.snapshot_seq,
                filter_digest=ctx.filter_digest,
                pages_committed_by_source=ctx.pages_seen,
                items_seen=ctx.items_seen,
            ),
        )

    async def collect(
        self,
        plan: ReadPlan,
        *,
        deadline_ms: int | None = None,
        limit_items: int | None = None,
    ) -> tuple[ScanResult, list[Any]]:
        """Run a scan and keep every item. For tests and small datasets only.

        ``limit_items`` is mandatory in spirit: holding a 1,000,000-deal scan
        in a list is exactly what section 7.4 forbids in production. The guard
        raises rather than silently truncating.
        """
        collected: list[Any] = []

        async def sink(page: Page[Any]) -> None:
            if limit_items is not None and len(collected) + len(page.items) > limit_items:
                raise SourceContractError(
                    "collect() exceeded its item budget; stream the scan instead of "
                    "materialising it",
                    source_id=self._settings.source_id,
                    details={"limit_items": limit_items, "collected": len(collected)},
                )
            collected.extend(page.items)

        result = await self.run(plan, sink=sink, deadline_ms=deadline_ms)
        return result, collected

"""Source identity resolution and epoch fencing.

The job is to answer one question before any scan starts: *is the source we
are about to read the same source whose watermark we hold?* If the answer is
no, continuing would apply a watermark from one dataset to a different one, and
because deal ids repeat across simulator runs, the first replayed id would
either collide or silently overwrite (WF3-013..WF3-015).

A detected change is **not** self-healing. The old epoch is fenced and the new
one is recorded as ``DISCOVERED``, then the guard raises. Bootstrapping the new
epoch from zero is a decision with consequences -- it means a full re-read and
a second copy of history -- so it needs an explicit approval rather than
happening inside a retry loop (design 8.3).

The one exception is the very first epoch for a source, where there is no
previous dataset to confuse it with and nothing to lose; that is the enrollment
path WF3-005 describes.

What this cannot promise: there is no transaction spanning the source's HTTP
response and our database. A source can reset microseconds after a successful
verification. Data already published stays correctly attributed to the epoch it
was read from, and the next poll detects the change (design 8.3 item 6).
"""

from __future__ import annotations

from trading_sync.application.ports import EpochHandle, SyncSink, SyncSource
from trading_sync.domain.snapshots import (
    Entity,
    EpochStatus,
    SourceIdentity,
    SourceKind,
    ValidatedPage,
)
from trading_sync.errors import (
    EpochBlockedError,
    SourceCapabilityError,
    SourceContractError,
    SourceEpochChangedError,
)

__all__ = ["SourceEpochGuard"]


class SourceEpochGuard:
    """Resolves the active epoch and verifies page provenance against it."""

    def __init__(
        self,
        sink: SyncSink,
        *,
        source_id: str,
        expected_kind: SourceKind,
        require_run_id_and_generation: bool = True,
        auto_approve_first_epoch: bool = True,
    ) -> None:
        self._sink = sink
        self._source_id = source_id
        self._expected_kind = expected_kind
        self._require_identity = require_run_id_and_generation
        self._auto_approve_first = auto_approve_first_epoch

    async def resolve(self, source: SyncSource, *, entity: Entity) -> tuple[EpochHandle, SourceIdentity]:
        """Return the epoch a scan may use, or raise.

        Raises :class:`SourceEpochChangedError` when the source moved on, after
        fencing the old epoch and recording the new one for approval.
        """
        identity = await source.read_meta(self._source_id)
        self._validate_identity(identity, entity=entity)

        active = await self._sink.get_active_epoch(self._source_id)
        if active is None:
            epoch = await self._sink.register_epoch(identity)
            if not self._auto_approve_first:
                raise EpochBlockedError(
                    "first epoch awaits approval before ingestion",
                    epoch=epoch.label,
                )
            approved = await self._sink.approve_epoch(
                epoch.epoch_id, reason="FIRST_ENROLLMENT"
            )
            return approved, identity

        if (active.run_id, active.generation) == (identity.run_id, identity.generation):
            return active, identity

        # The source is not the one we hold a watermark for.
        await self._sink.block_epoch(
            active.epoch_id, reason_code="SOURCE_EPOCH_CHANGED"
        )
        discovered = await self._sink.register_epoch(identity)
        await self._sink.audit(
            discovered.epoch_id,
            None,
            "SOURCE_EPOCH_DISCOVERED",
            reason_code="SOURCE_EPOCH_CHANGED",
            safe_details={
                "previous_epoch": active.label,
                "observed_epoch": discovered.label,
            },
        )
        raise SourceEpochChangedError(
            "source identity changed; the previous epoch is fenced and the new "
            "one needs an explicit approval",
            previous_epoch=active.label,
            observed_epoch=discovered.label,
        )

    def _validate_identity(self, identity: SourceIdentity, *, entity: Entity) -> None:
        if identity.source_id != self._source_id:
            raise SourceContractError(
                "source reported a different source_id than the configured one",
                configured=self._source_id,
                reported=identity.source_id,
            )
        if identity.source_kind is not self._expected_kind:
            raise SourceContractError(
                "source kind does not match configuration",
                configured=str(self._expected_kind),
                reported=str(identity.source_kind),
            )
        if self._require_identity:
            if not identity.run_id or not identity.run_id.strip():
                raise SourceContractError(
                    "source meta carries no run_id; refusing to synthesise one"
                )
            if identity.generation < 0:
                raise SourceContractError(
                    "source meta carries a negative generation",
                    generation=identity.generation,
                )
        if entity is Entity.DEALS and not identity.supports_incremental_deals:
            # No stable event sequence means no watermark can be defined over
            # it. Refusing beats inventing one (WF3-008, WF3-180).
            raise SourceCapabilityError(
                "source does not support sequence-based incremental deal sync",
                source_kind=str(identity.source_kind),
            )

    def verify_page(self, epoch: EpochHandle, page: ValidatedPage) -> None:
        """Re-check a validated page against the epoch the scan belongs to.

        The page validator already enforces that pages agree with *each other*.
        This checks they agree with the epoch we hold the watermark for, which
        catches a stale worker delivering a page from a previous run after a
        handshake with the new one (WF3-017).
        """
        if (page.run_id, page.generation) != (epoch.run_id, epoch.generation):
            raise SourceEpochChangedError(
                "page provenance does not match the scan's epoch",
                epoch=epoch.label,
                page_epoch=f"{page.run_id}:G{page.generation}",
            )

    @staticmethod
    def is_committable(epoch: EpochHandle) -> bool:
        return epoch.status in (EpochStatus.ACTIVE, EpochStatus.DEGRADED)

"""Capture a complete source manifest (SYSTEM_DESIGN section 7.4).

The protocol, step by step as the design specifies it:

1. Pin ``(run_id, generation, snapshot_seq)`` from page 1; every later page
   must match exactly.
2. Validate every record *before* it can contribute to a manifest: canonical
   types, declared scale, ``source_seq <= S``, and no duplicate key.
3. Commit the page receipt and its items in **one** transaction, and commit it
   **before** fetching the next page. A crash therefore never leaves items
   without the receipt that vouches for them (section 7.6).
4. Mark the scan terminal only when ``next_cursor`` was null *and* the
   receipt chain is contiguous, single-cut and internally consistent.
5. Promote ``source_complete`` through the validated transition in the
   repository, never by assignment.

The one subtlety worth stating: on resume after a lost commit ACK the worker
cannot know whether page N landed. It reads the stored receipt and compares
``page_hash``. Equal means the page is already durable and is skipped;
different means the source is serving different bytes for the same position in
the scan, which ends the run rather than overwriting evidence (IT04-01).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from .canonical import item_key as canonical_item_key
from .canonical import page_hash, payload_hash
from .clock import Clock
from .config import Settings
from .db.repository import ChainStats, ReconRepository
from .dto import CapturedItem, CapturedPage, SourceIdentity, SourcePage
from .errors import (
    CanonicalizationError,
    DataIntegrityError,
    ReasonCode,
    SourceEpochChangedError,
    SourceError,
)
from .ports.source import SourcePort, cursor_digest


@dataclass(frozen=True, slots=True)
class CaptureResult:
    """Outcome of one source capture."""

    complete: bool
    pages: int
    items: int
    snapshot_seq: int | None
    reason_code: ReasonCode | None
    stats: ChainStats | None
    resumed_pages: int = 0


class SourceCaptureWorker:
    """Turns a paged source listing into durable, provable evidence."""

    def __init__(
        self,
        *,
        repository: ReconRepository,
        source: SourcePort,
        settings: Settings,
        clock: Clock,
    ) -> None:
        self._repo = repository
        self._source = source
        self._settings = settings
        self._clock = clock

    def capture(
        self,
        run_id: str,
        *,
        entity: str,
        expected_identity: SourceIdentity,
        scope: str = "UNFILTERED",
    ) -> CaptureResult:
        """Run the capture protocol. Never raises for expected faults.

        Expected source faults (expiry, reset, bad payload, transport) are
        converted into a ``CaptureResult`` with ``complete=False`` and a
        reason code, because "the source misbehaved" is a normal outcome this
        service must survive, not an exception for the caller to interpret
        (section 5.6).
        """
        pages = 0
        items = 0
        resumed = 0
        snapshot_seq: int | None = None
        seen_keys: set[str] = set()

        try:
            for page in self._source.iter_pages(
                entity, scope=scope, page_size=self._settings.source_max_page_size
            ):
                self._repo.check_deadline(run_id)

                if pages == 0:
                    # Page 1 establishes the cut, and the cut must belong to
                    # the epoch we were asked to audit (section 7.2).
                    if (
                        page.run_id != expected_identity.run_id
                        or page.generation != expected_identity.generation
                    ):
                        raise SourceEpochChangedError(
                            "first page belongs to a different source run or generation",
                            expected_run_id=expected_identity.run_id,
                            expected_generation=expected_identity.generation,
                            actual_run_id=page.run_id,
                            actual_generation=page.generation,
                        )
                    snapshot_seq = page.snapshot_seq
                    self._repo.pin_source_identity(
                        run_id,
                        run_id_source=page.run_id,
                        generation=page.generation,
                        snapshot_seq=page.snapshot_seq,
                    )

                if pages + 1 > self._settings.max_pages_per_run:
                    raise SourceError(
                        "scan exceeded the permitted page budget",
                        reason=ReasonCode.SCAN_QUOTA_EXCEEDED,
                        pages=pages,
                        max_pages_per_run=self._settings.max_pages_per_run,
                    )

                captured = self._reduce_page(page, entity=entity, seen_keys=seen_keys)

                if items + len(captured.items) > self._settings.max_items_per_run:
                    raise SourceError(
                        "scan exceeded the permitted item budget",
                        reason=ReasonCode.SCAN_QUOTA_EXCEEDED,
                        items=items,
                        max_items_per_run=self._settings.max_items_per_run,
                    )

                if self._commit_or_resume(run_id, captured):
                    resumed += 1

                pages += 1
                items += len(captured.items)

        except (SourceError, DataIntegrityError, CanonicalizationError) as exc:
            reason = getattr(exc, "reason", ReasonCode.SOURCE_TRANSPORT_FAILED)
            return CaptureResult(
                complete=False,
                pages=pages,
                items=items,
                snapshot_seq=snapshot_seq,
                reason_code=reason,
                stats=None,
                resumed_pages=resumed,
            )

        ok, reason, stats = self._repo.validate_receipt_chain(run_id)
        if not ok:
            return CaptureResult(
                complete=False,
                pages=pages,
                items=items,
                snapshot_seq=snapshot_seq,
                reason_code=reason,
                stats=stats,
                resumed_pages=resumed,
            )

        try:
            stats = self._repo.mark_source_complete(run_id)
        except SourceError as exc:
            return CaptureResult(
                complete=False,
                pages=pages,
                items=items,
                snapshot_seq=snapshot_seq,
                reason_code=exc.reason,
                stats=stats,
                resumed_pages=resumed,
            )

        return CaptureResult(
            complete=True,
            pages=pages,
            items=items,
            snapshot_seq=snapshot_seq,
            reason_code=None,
            stats=stats,
            resumed_pages=resumed,
        )

    # ---- internals -------------------------------------------------------

    def _reduce_page(
        self, page: SourcePage, *, entity: str, seen_keys: set[str]
    ) -> CapturedPage:
        """Validate and reduce one page to manifest rows.

        Raises rather than skipping a bad row: a manifest missing a row the
        source actually sent would make the target look like it has an extra
        record, inverting the diagnosis (section 7.4 step 3).
        """
        captured: list[CapturedItem] = []
        previous_seq: int | None = None

        for record in page.items:
            key = canonical_item_key(entity, record)
            digest = payload_hash(entity, record)
            source_seq = self._validated_seq(record, page=page, entity=entity)

            if key in seen_keys:
                raise DataIntegrityError(
                    "source returned the same key twice within one scan",
                    reason=ReasonCode.SOURCE_DUPLICATE_KEY,
                    page_index=page.page_index,
                    item_key=key,
                )
            seen_keys.add(key)

            if source_seq is not None:
                if previous_seq is not None and source_seq < previous_seq:
                    # Deals are paged by ascending sequence. Out-of-order rows
                    # mean the cursor chain is not what it claims, so the scan
                    # cannot be proven complete (section 7.4 step 3).
                    raise DataIntegrityError(
                        "source returned a sequence out of order within a page",
                        reason=ReasonCode.SOURCE_ORDER_VIOLATED,
                        page_index=page.page_index,
                        item_key=key,
                        previous_seq=previous_seq,
                        source_seq=source_seq,
                    )
                previous_seq = source_seq

            captured.append(
                CapturedItem(item_key=key, payload_hash=digest, source_seq=source_seq)
            )

        return CapturedPage(
            page_index=page.page_index,
            run_id=page.run_id,
            generation=page.generation,
            snapshot_seq=page.snapshot_seq,
            items=captured,
            page_hash=page_hash(entity, [(i.item_key, i.payload_hash) for i in captured]),
            is_terminal=page.is_terminal,
            cursor_digest=cursor_digest(page.next_cursor),
        )

    def _validated_seq(
        self, record: Mapping[str, Any], *, page: SourcePage, entity: str
    ) -> int | None:
        """Extract ``source_seq`` and hold it to the pinned cut.

        A row whose sequence exceeds the snapshot it was served under did not
        exist at that snapshot, so including it would corrupt the comparison
        bound (section 5.2).
        """
        if "source_seq" not in record:
            return None
        raw = record["source_seq"]
        if isinstance(raw, bool) or not isinstance(raw, int):
            raise DataIntegrityError(
                "source_seq is not an integer",
                page_index=page.page_index,
                entity=entity,
                value=repr(raw),
            )
        if raw < 0:
            raise DataIntegrityError(
                "source_seq is negative", page_index=page.page_index, value=raw
            )
        if raw > page.snapshot_seq:
            raise DataIntegrityError(
                "source_seq is beyond the snapshot it was served under",
                reason=ReasonCode.SOURCE_SEQ_AFTER_SNAPSHOT,
                page_index=page.page_index,
                source_seq=raw,
                snapshot_seq=page.snapshot_seq,
            )
        return raw

    def _commit_or_resume(self, run_id: str, captured: CapturedPage) -> bool:
        """Commit the page, or recognise it as already durable.

        Returns True when the page was found already stored, which is the
        lost-ACK resume path (section 7.6 "after receipt COMMIT, ACK lost").
        """
        existing = self._repo.get_page_receipt(run_id, captured.page_index)
        if existing is not None:
            if existing["page_hash"] != captured.page_hash:
                raise DataIntegrityError(
                    "source served different content for a page already committed",
                    reason=ReasonCode.SOURCE_CUT_DRIFTED,
                    page_index=captured.page_index,
                    stored_page_hash=existing["page_hash"],
                    offered_page_hash=captured.page_hash,
                )
            if (
                existing["snapshot_seq"] != captured.snapshot_seq
                or existing["run_id"] != captured.run_id
                or existing["generation"] != captured.generation
            ):
                raise DataIntegrityError(
                    "stored receipt belongs to a different cut than the page offered",
                    reason=ReasonCode.SOURCE_CUT_DRIFTED,
                    page_index=captured.page_index,
                )
            return True

        self._repo.commit_page(run_id, captured)
        return False


def reduce_records_for_target(
    entity: str, records: Sequence[Mapping[str, Any]]
) -> list[CapturedItem]:
    """Canonicalise target rows with the *same* mapper used for the source.

    Section 6.4 warns that P03's own stored ``payload_hash`` must not be
    assumed comparable unless its encoder and field set are confirmed. This
    service therefore always recomputes the digest from business fields, so a
    difference in how P03 happened to hash a row can neither create a false
    mismatch nor mask a real one.
    """
    return [
        CapturedItem(
            item_key=canonical_item_key(entity, record),
            payload_hash=payload_hash(entity, record),
            source_seq=record.get("source_seq"),
        )
        for record in records
    ]

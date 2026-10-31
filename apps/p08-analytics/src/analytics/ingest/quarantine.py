"""The durable poison ledger. Section 6.1 step 3, INV08-21.

A quarantine receipt is the difference between isolating a bad event and
losing one. The rule is strict: a record may only be stepped over once its
receipt is *durable*. If the receipt cannot be written, the partition pauses
and the offset stays put, because a paused partition is a visible problem
while a skipped record is an invisible one.

The ledger also answers a question the build needs: does this cut contain
unresolved poison? ``READY`` is not available while it does (INV08-21), and
a Kafka lag of zero says nothing about it - a permanently rejected event
leaves no lag behind at all.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from typing import Sequence

from ..clock import Clock
from ..errors import CompletenessError, Reason
from ..ports.control import ControlPlanePort, QuarantineRecord
from .validator import ValidationFailure


@dataclass(frozen=True, slots=True)
class QuarantineOutcome:
    receipt: QuarantineRecord
    newly_recorded: bool


class QuarantineLedger:
    """Writes receipts and answers completeness questions about them."""

    def __init__(self, control: ControlPlanePort, *, clock: Clock) -> None:
        self._control = control
        self._clock = clock

    @staticmethod
    def receipt_id(topic: str, partition: int, offset: int) -> str:
        """Deterministic id from the Kafka coordinate.

        Derived rather than random so a replay of the same poison produces the
        same id. A random id would make the "idempotent receipt" requirement
        of P08-F-09 depend on a database lookup succeeding first.
        """
        digest = hashlib.sha256(f"{topic}/{partition}/{offset}".encode("utf-8")).hexdigest()
        return f"qr-{digest[:32]}"

    def record(self, failure: ValidationFailure, *, tenant_hint: str | None = None) -> QuarantineOutcome:
        """Persist one poison receipt, idempotently."""
        topic, partition, offset = failure.coordinate
        receipt = QuarantineRecord(
            receipt_id=self.receipt_id(topic, partition, offset),
            tenant_id=tenant_hint,
            source_id=_detail_str(failure, "source_id"),
            epoch_id=_detail_str(failure, "epoch_id"),
            topic=topic,
            partition=partition,
            offset=offset,
            event_id=failure.event_id,
            reason=str(failure.reason),
            detail={"message": failure.message, **{k: str(v) for k, v in failure.detail.items()}},
            payload_sha256=failure.payload_sha256,
            recorded_at=self._clock.now(),
        )
        stored = self._control.record_quarantine(receipt)
        return QuarantineOutcome(receipt=stored, newly_recorded=stored.receipt_id == receipt.receipt_id
                                 and stored.recorded_at == receipt.recorded_at)

    def pending_count(
        self,
        *,
        tenant_id: str | None = None,
        source_id: str | None = None,
        epoch_id: str | None = None,
    ) -> int:
        return len(
            self._control.list_quarantine(
                tenant_id=tenant_id,
                source_id=source_id,
                epoch_id=epoch_id,
                unresolved_only=True,
            )
        )

    def unresolved(
        self,
        *,
        tenant_id: str | None = None,
        source_id: str | None = None,
        epoch_id: str | None = None,
    ) -> Sequence[QuarantineRecord]:
        return self._control.list_quarantine(
            tenant_id=tenant_id, source_id=source_id, epoch_id=epoch_id, unresolved_only=True
        )

    def require_clean_for_cut(
        self, *, tenant_id: str, source_id: str, epoch_id: str
    ) -> None:
        """Block a build when its scope has unresolved poison. INV08-21.

        Scoped receipts are the ones that matter, but a receipt whose scope
        could not be determined (because the envelope was too broken to name
        its source) is also blocking: an event that might belong to this cut
        cannot be assumed not to.
        """
        pending = self._control.list_quarantine(unresolved_only=True)
        relevant = [
            record
            for record in pending
            if (record.tenant_id, record.source_id, record.epoch_id)
            == (tenant_id, source_id, epoch_id)
            or record.source_id is None
            or record.tenant_id is None
        ]
        if relevant:
            raise CompletenessError(
                Reason.QUARANTINE_PENDING_IN_CUT,
                f"{len(relevant)} unresolved quarantine receipts affect this cut",
                receipts=[record.receipt_id for record in relevant[:20]],
                total=len(relevant),
            )

    def resolve(self, receipt_id: str, note: str) -> QuarantineRecord:
        if not note.strip():
            # An unexplained resolution is how poison quietly disappears.
            raise CompletenessError(
                Reason.FIELD_INVALID, "resolving a quarantine receipt requires an operator note"
            )
        return self._control.resolve_quarantine(receipt_id=receipt_id, note=note)


def _detail_str(failure: ValidationFailure, key: str) -> str | None:
    value = failure.detail.get(key)
    return value if isinstance(value, str) else None

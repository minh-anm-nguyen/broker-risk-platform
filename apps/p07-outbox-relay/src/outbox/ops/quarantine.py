"""Quarantine inspection, and the sanitised incident record.

``QUARANTINED`` is not success -- design section 6.5 says so in bold, and this
module is built so that the state cannot quietly behave like success. There is
no "skip" operation. The only way out is :mod:`outbox.ops.replay`, which
requires a named approver and preserves the ``event_id``.

:func:`poison_notice` is the only thing that gets published about a quarantined
event, and it carries identity and classification, never content. Section 10.4
and the topic table in 5.2 both require "sanitized poison metadata only"; the
payload hash is enough for an operator to correlate the notice with the outbox
row they are authorised to read, without the private fields travelling to a
topic whose consumers were never scoped to them.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from ..canonical import canonical_json_bytes
from ..clock import to_wire
from ..events.routing import POISON_TOPIC
from ..persistence.ports import EventState


@dataclass(frozen=True, slots=True)
class QuarantineEntry:
    event_id: str
    event_type: str
    source_id: str
    epoch_id: str
    aggregate_key: str
    aggregate_version: int
    attempt_count: int
    error_code: str | None
    reason: str | None
    payload_sha256: str

    def blocks_versions_above(self) -> int:
        """Versions of this aggregate above this one cannot be applied.

        Per-key ordering means a parked event blocks its aggregate's later
        versions until it is repaired or explicitly waived. Returning the number
        rather than a boolean lets a runbook say exactly what is stuck.
        """
        return self.aggregate_version


def list_quarantined(store: Any) -> list[QuarantineEntry]:
    return [
        QuarantineEntry(
            event_id=str(record.event_id),
            event_type=record.event_type,
            source_id=record.source_id,
            epoch_id=record.epoch_id,
            aggregate_key=record.aggregate_key,
            aggregate_version=record.aggregate_version,
            attempt_count=record.attempt_count,
            error_code=record.last_error_code,
            reason=record.quarantine_reason,
            payload_sha256=record.payload_sha256,
        )
        for record in store.iter_records(state=EventState.QUARANTINED)
    ]


def blocked_aggregates(store: Any) -> dict[tuple[str, str, str], int]:
    """Aggregates whose progress is stopped, and the version doing the stopping.

    An operator needs this to answer "what is not flowing": each entry is an
    account (or snapshot stream) where every later event is waiting behind one
    parked row.
    """
    blocked: dict[tuple[str, str, str], int] = {}
    for entry in list_quarantined(store):
        key = (entry.source_id, entry.epoch_id, entry.aggregate_key)
        current = blocked.get(key)
        if current is None or entry.aggregate_version < current:
            blocked[key] = entry.aggregate_version
    return blocked


def poison_notice(entry: QuarantineEntry, *, now: datetime) -> dict[str, Any]:
    """Build the sanitised notice. Identity and classification only."""
    return {
        "event_id": entry.event_id,
        "event_type": entry.event_type,
        "source_id": entry.source_id,
        "epoch_id": entry.epoch_id,
        "aggregate_key": entry.aggregate_key,
        "aggregate_version": entry.aggregate_version,
        "error_code": entry.error_code or "UNKNOWN",
        # Truncated and already sanitised at quarantine time, but truncated
        # again here: a reason is a classification, not a place for detail.
        "reason": (entry.reason or "")[:200],
        "payload_sha256": entry.payload_sha256,
        "quarantined_at": to_wire(now),
    }


def publish_poison_notice(producer: Any, entry: QuarantineEntry, *, now: datetime) -> None:
    """Publish one notice to the poison topic.

    Fire and forget by design: the durable record of the incident is the
    ``QUARANTINED`` outbox row, and this notice is an alerting convenience. If
    it fails to deliver, nothing is lost that the database does not already
    hold -- which is why no delivery confirmation is recorded for it.
    """
    notice = poison_notice(entry, now=now)
    producer.produce(
        topic=POISON_TOPIC.name,
        key=entry.event_id.encode("utf-8"),
        value=canonical_json_bytes(notice),
        headers={"event_id": entry.event_id, "error_code": notice["error_code"]},
        on_delivery=lambda _result: None,
    )

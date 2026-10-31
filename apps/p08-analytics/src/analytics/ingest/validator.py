"""Decode and validate a polled batch into accepted rows plus poison.

A validation pass returns *both* outcomes rather than raising on the first bad
record, because the offset decision needs the whole picture: a valid record at
offset 11 may not be committed while offset 10 is unresolved (P08-E-05), and
that can only be decided once both are classified.

Decoding is deliberately strict. A JSON body that is not an object, a float
where money belongs, a 63-character hash - each is poison, and each is
recorded with the reason rather than skipped.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Mapping, Sequence

from ..canonical import canonical_json
from ..contracts.events import (
    ACCOUNT_SNAPSHOT_PUBLISHED,
    DEAL_RECORDED,
    EventEnvelope,
    TenantResolver,
    parse_envelope,
)
from ..errors import PoisonError, Reason
from ..ports.clickhouse import RawObservation
from ..ports.kafka import ConsumerRecord, PartitionKey


@dataclass(frozen=True, slots=True)
class ValidationFailure:
    """One poison record and why it can never be accepted."""

    record: ConsumerRecord
    reason: Reason
    message: str
    detail: Mapping[str, object] = field(default_factory=dict)
    event_id: str | None = None
    payload_sha256: str | None = None

    @property
    def coordinate(self) -> tuple[str, int, int]:
        return (self.record.topic, self.record.partition, self.record.offset)


@dataclass(frozen=True, slots=True)
class ValidatedBatch:
    """Classification of one polled batch."""

    accepted: tuple[EventEnvelope, ...]
    raw_rows: tuple[RawObservation, ...]
    markers: tuple[EventEnvelope, ...]
    failures: tuple[ValidationFailure, ...]

    def failures_by_partition(self) -> dict[PartitionKey, list[ValidationFailure]]:
        out: dict[PartitionKey, list[ValidationFailure]] = {}
        for failure in self.failures:
            out.setdefault(failure.record.partition_key, []).append(failure)
        for items in out.values():
            items.sort(key=lambda f: f.record.offset)
        return out

    @property
    def unique_event_ids(self) -> frozenset[str]:
        return frozenset(envelope.event_id for envelope in self.accepted)


def validate_records(
    records: Sequence[ConsumerRecord],
    *,
    tenant_resolver: TenantResolver,
    allowed_topics: frozenset[str],
    max_envelope_bytes: int,
    ingested_at: datetime | None = None,
) -> ValidatedBatch:
    """Classify every record. Never raises for a bad record."""
    accepted: list[EventEnvelope] = []
    raw_rows: list[RawObservation] = []
    markers: list[EventEnvelope] = []
    failures: list[ValidationFailure] = []

    for record in records:
        try:
            body = _decode(record, max_envelope_bytes)
            envelope = parse_envelope(
                body,
                topic=record.topic,
                partition=record.partition,
                offset=record.offset,
                tenant_resolver=tenant_resolver,
                allowed_topics=allowed_topics,
                max_bytes=max_envelope_bytes,
                raw_size=record.size_bytes,
            )
        except PoisonError as exc:
            failures.append(
                ValidationFailure(
                    record=record,
                    reason=exc.reason,
                    message=str(exc),
                    detail=dict(exc.details),
                    event_id=_peek(record, "event_id"),
                    payload_sha256=_peek(record, "payload_sha256"),
                )
            )
            continue

        accepted.append(envelope)
        if envelope.event_type == DEAL_RECORDED:
            raw_rows.append(_to_raw(envelope, record, ingested_at))
        elif envelope.event_type == ACCOUNT_SNAPSHOT_PUBLISHED:
            # INV08-16: a marker says P03 published a snapshot. It is not the
            # snapshot. It is kept in the control log, never in the Deal raw
            # table, so no account metric can be derived from its existence.
            markers.append(envelope)

    return ValidatedBatch(
        accepted=tuple(accepted),
        raw_rows=tuple(raw_rows),
        markers=tuple(markers),
        failures=tuple(failures),
    )


def _decode(record: ConsumerRecord, max_bytes: int) -> Mapping[str, object]:
    if record.size_bytes > max_bytes:
        raise PoisonError(
            Reason.ENVELOPE_TOO_LARGE,
            f"record of {record.size_bytes} bytes exceeds {max_bytes}",
            size=record.size_bytes,
        )
    try:
        text = record.value.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise PoisonError(Reason.ENVELOPE_MALFORMED, "record value is not valid UTF-8") from exc
    try:
        # No field in the envelope is legitimately a fractional JSON number:
        # money is a decimal string and every other numeric field is an
        # integer. So a JSON float is poison at the decode boundary, before
        # it can round-trip into something that merely looks like a valid
        # amount (section 7.2). Letting ``parse_float=str`` convert it would
        # make ``12.3`` indistinguishable from the string ``"12.3"``.
        body = json.loads(text, parse_float=_reject_float, parse_constant=_reject_constant)
    except ValueError as exc:
        raise PoisonError(Reason.ENVELOPE_MALFORMED, f"record value is not JSON: {exc}") from exc
    if not isinstance(body, dict):
        raise PoisonError(Reason.ENVELOPE_MALFORMED, "record value must be a JSON object")
    return body


def _reject_float(text: str) -> object:
    raise ValueError(
        f"JSON float {text!r} is not permitted; money must be sent as a decimal string"
    )


def _reject_constant(name: str) -> object:
    raise ValueError(f"JSON constant {name} is not permitted in a financial envelope")


def _peek(record: ConsumerRecord, key: str) -> str | None:
    """Best-effort field read for the quarantine receipt.

    A poison record still needs to be findable by whoever investigates it, so
    we try to recover its ``event_id`` - but a failure here must never mask
    the original validation failure.
    """
    try:
        body = json.loads(record.value.decode("utf-8"))
    except Exception:
        return None
    if not isinstance(body, dict):
        return None
    value = body.get(key)
    return value if isinstance(value, str) else None


def _to_raw(
    envelope: EventEnvelope, record: ConsumerRecord, ingested_at: datetime | None
) -> RawObservation:
    deal = envelope.deal
    return RawObservation(
        tenant_id=envelope.tenant_id,
        source_id=envelope.source_id,
        epoch_id=envelope.epoch_id,
        event_id=envelope.event_id,
        dedupe_key=envelope.dedupe_key,
        kafka_record_key=record.key or "",
        event_type=envelope.event_type,
        schema_version=envelope.schema_version,
        payload_sha256=envelope.payload_sha256,
        deal_payload_hash=envelope.deal_payload_hash,
        deal_id=envelope.deal_id,
        account_id=deal.account_id if deal else envelope.aggregate_id,
        source_seq=envelope.source_seq,
        aggregate_version=envelope.aggregate_version,
        event_time=envelope.created_at,
        executed_at=deal.executed_at if deal else None,
        kafka_topic=envelope.topic,
        kafka_partition=envelope.partition,
        kafka_offset=envelope.offset,
        # Stored canonically so the raw row is byte-reproducible, which is
        # what lets an auditor recompute ``payload_sha256`` from the table.
        payload_json=canonical_json(dict(envelope.payload)),
        ingested_at=ingested_at,
    )

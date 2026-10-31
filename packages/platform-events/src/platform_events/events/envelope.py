"""Envelope v1: the wire record, and the only way to build or parse one.

The envelope carries three things a consumer cannot reconstruct for itself: who
produced the fact (source and epoch), which ordering stream it belongs to
(aggregate key and version), and a hash over the canonical payload so that a
corrupted or tampered record is detectable rather than merely unusual.

``parse_wire`` recomputes ``payload_sha256`` and compares. That check is the
difference between "a duplicate" and "an integrity incident": design section
8.1 requires a second copy with a *different* hash to be quarantined rather
than applied, and that distinction is only available if the hash is verified on
arrival instead of trusted.

The envelope's ``aggregate_id`` is also the outbox table's ``aggregate_key``.
One value, two names across the design's wire example and ERD; keeping them
equal means the ordering stream a consumer sees is literally the one the
database serialised against.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from ..canonical import canonical_json_bytes, decode_wire_bytes, payload_hash
from ..clock import to_wire
from ..errors import ErrorCode, ValidationError
from .keys import EventType, build_dedupe_key, derive_event_id, normalize_uuid
from .schema import FieldSpec, Kind, get_spec, validate_payload, validate_scalar

#: Provenance is a closed set too: it is where "extra context" would otherwise
#: accumulate until it contained something that should never leave P03.
_PROVENANCE_FIELDS: tuple[FieldSpec, ...] = (
    FieldSpec("source_seq", Kind.COUNT, required=False),
    FieldSpec("deal_id", Kind.IDENTITY, required=False),
    FieldSpec("scan_id", Kind.IDENTITY, required=False),
    FieldSpec("deal_payload_hash", Kind.HASH, required=False),
    FieldSpec("reconciliation_revision", Kind.COUNT, required=False),
)

_REQUIRED_WIRE_FIELDS = (
    "event_id",
    "dedupe_key",
    "event_type",
    "schema_version",
    "source_id",
    "epoch_id",
    "aggregate_type",
    "aggregate_id",
    "aggregate_version",
    "event_time",
    "created_at",
    "payload_sha256",
    "provenance",
    "payload",
)


def validate_provenance(raw: object) -> dict[str, Any]:
    """Validate the provenance block, rejecting unknown keys."""
    if not isinstance(raw, dict):
        raise ValidationError(f"provenance must be an object, got {type(raw).__name__}")
    allowed = {spec.name: spec for spec in _PROVENANCE_FIELDS}
    unknown = set(raw) - set(allowed)
    if unknown:
        raise ValidationError(f"provenance has unknown fields {sorted(unknown)}")
    result: dict[str, Any] = {}
    for name, value in raw.items():
        if value is None:
            continue
        result[name] = validate_scalar(allowed[name], value)
    return result


@dataclass(frozen=True, slots=True)
class Envelope:
    """One validated event, ready to persist or to publish."""

    event_id: uuid.UUID
    dedupe_key: str
    event_type: EventType
    schema_version: int
    source_id: str
    epoch_id: str
    aggregate_type: str
    aggregate_id: str
    aggregate_version: int
    event_time: str
    created_at: str
    payload_sha256: str
    payload: dict[str, Any]
    provenance: dict[str, Any] = field(default_factory=dict)

    @property
    def aggregate_key(self) -> str:
        """The outbox column name for :attr:`aggregate_id`."""
        return self.aggregate_id

    @classmethod
    def build(
        cls,
        *,
        event_type: EventType,
        schema_version: int,
        source_id: str,
        epoch_id: str,
        aggregate_id: str,
        aggregate_version: int,
        payload: dict[str, Any],
        event_time: datetime | str,
        created_at: datetime | str,
        provenance: dict[str, Any] | None = None,
        identity: str,
    ) -> Envelope:
        """Validate everything and derive the identity fields.

        ``identity`` is the business identifier that makes the fact unique
        within its epoch -- a deal id, a scan id, a reconciliation revision.
        It is the caller's job to choose it, and the dedupe key and event id
        both follow from it, so a caller cannot accidentally produce a stable
        key with an unstable id or the reverse.
        """
        spec = get_spec(event_type, schema_version)
        if not isinstance(aggregate_version, int) or isinstance(aggregate_version, bool):
            raise ValidationError(f"aggregate_version must be an integer, got {aggregate_version!r}")
        if aggregate_version < 1:
            raise ValidationError("aggregate_version must be >= 1")
        if not isinstance(source_id, str) or not source_id:
            raise ValidationError("source_id must be a non-empty string")
        normalized_epoch = normalize_uuid(epoch_id, field="epoch_id")
        normalized_payload = validate_payload(event_type, schema_version, payload)
        dedupe_key = build_dedupe_key(spec.event_type, normalized_epoch, identity)
        return cls(
            event_id=derive_event_id(source_id, dedupe_key),
            dedupe_key=dedupe_key,
            event_type=spec.event_type,
            schema_version=spec.schema_version,
            source_id=source_id,
            epoch_id=normalized_epoch,
            aggregate_type=spec.aggregate_type,
            aggregate_id=_require_identity(aggregate_id),
            aggregate_version=aggregate_version,
            event_time=_as_wire_time(event_time, field_name="event_time"),
            created_at=_as_wire_time(created_at, field_name="created_at"),
            payload_sha256=payload_hash(normalized_payload),
            payload=normalized_payload,
            provenance=validate_provenance(provenance or {}),
        )

    def to_wire_dict(self) -> dict[str, Any]:
        return {
            "event_id": str(self.event_id),
            "dedupe_key": self.dedupe_key,
            "event_type": str(self.event_type),
            "schema_version": self.schema_version,
            "source_id": self.source_id,
            "epoch_id": self.epoch_id,
            "aggregate_type": self.aggregate_type,
            "aggregate_id": self.aggregate_id,
            "aggregate_version": self.aggregate_version,
            "event_time": self.event_time,
            "created_at": self.created_at,
            "payload_sha256": self.payload_sha256,
            "provenance": self.provenance,
            "payload": self.payload,
        }

    def to_wire_bytes(self, *, max_bytes: int | None = None) -> bytes:
        """The exact bytes that go to Kafka, under the canonical encoding."""
        return canonical_json_bytes(self.to_wire_dict(), max_bytes=max_bytes)

    @classmethod
    def parse_wire(cls, data: bytes, *, supported_schema_versions: tuple[int, ...] | None = None) -> Envelope:
        """Parse and fully re-validate a received record.

        Order matters here. The schema allowlist is checked before the payload
        is interpreted, so an unsupported version is quarantined as
        ``SCHEMA_UNSUPPORTED`` rather than producing a confusing field error
        from a validator that was never meant to see it.
        """
        raw = decode_wire_bytes(data)
        missing = [name for name in _REQUIRED_WIRE_FIELDS if name not in raw]
        if missing:
            raise ValidationError(f"envelope is missing required fields {missing}")
        unknown = set(raw) - set(_REQUIRED_WIRE_FIELDS)
        if unknown:
            raise ValidationError(f"envelope has unknown fields {sorted(unknown)}")

        schema_version = raw["schema_version"]
        if supported_schema_versions is not None and schema_version not in supported_schema_versions:
            raise ValidationError(
                f"schema_version {schema_version!r} is outside this consumer's allowlist "
                f"{supported_schema_versions}",
                code=ErrorCode.SCHEMA_UNSUPPORTED,
            )
        spec = get_spec(raw["event_type"], schema_version)

        event_id = _parse_uuid(raw["event_id"], field_name="event_id")
        epoch_id = normalize_uuid(raw["epoch_id"], field="epoch_id")
        source_id = raw["source_id"]
        if not isinstance(source_id, str) or not source_id:
            raise ValidationError("source_id must be a non-empty string")

        # Identity is legible even when the payload is not, so it travels with
        # any failure from here on. A consumer can then quarantine precisely the
        # affected aggregate rather than stalling everything on the partition.
        identity_context: dict[str, object] = {
            "event_id": str(event_id),
            "source_id": source_id,
            "epoch_id": epoch_id,
            "aggregate_key": raw.get("aggregate_id"),
            "aggregate_version": raw.get("aggregate_version"),
            "payload_sha256": raw.get("payload_sha256"),
        }

        payload = validate_payload(spec.event_type, spec.schema_version, raw["payload"])
        declared_hash = raw["payload_sha256"]
        computed = payload_hash(payload)
        if declared_hash != computed:
            # Either the payload was altered in flight, or the producer hashed
            # something other than the canonical form. Both are integrity
            # incidents, and neither may be applied.
            raise ValidationError(
                f"payload_sha256 mismatch: envelope declares {declared_hash!r}, "
                f"canonical payload hashes to {computed!r}",
                code=ErrorCode.INTEGRITY_CONFLICT,
                context=identity_context,
            )

        if raw["aggregate_type"] != spec.aggregate_type:
            raise ValidationError(
                f"aggregate_type {raw['aggregate_type']!r} does not match "
                f"{spec.event_type} contract {spec.aggregate_type!r}"
            )
        version = raw["aggregate_version"]
        if isinstance(version, bool) or not isinstance(version, int) or version < 1:
            raise ValidationError(f"aggregate_version {version!r} must be a positive integer")

        envelope = cls(
            event_id=event_id,
            dedupe_key=raw["dedupe_key"],
            event_type=spec.event_type,
            schema_version=spec.schema_version,
            source_id=source_id,
            epoch_id=epoch_id,
            aggregate_type=raw["aggregate_type"],
            aggregate_id=_require_identity(raw["aggregate_id"]),
            aggregate_version=version,
            event_time=_as_wire_time(raw["event_time"], field_name="event_time"),
            created_at=_as_wire_time(raw["created_at"], field_name="created_at"),
            payload_sha256=declared_hash,
            payload=payload,
            provenance=validate_provenance(raw["provenance"]),
        )
        # The dedupe key and event id are derivable, so a record whose
        # identity fields disagree with its own content was not produced by a
        # conforming writer.
        expected_key = build_dedupe_key(
            spec.event_type, epoch_id, _identity_from_dedupe_key(envelope.dedupe_key)
        )
        if envelope.dedupe_key != expected_key:
            raise ValidationError(f"dedupe_key {envelope.dedupe_key!r} is not canonical")
        if envelope.event_id != derive_event_id(source_id, envelope.dedupe_key):
            raise ValidationError(
                f"event_id {envelope.event_id} is not the derived id for "
                f"{source_id}/{envelope.dedupe_key}",
                code=ErrorCode.INTEGRITY_CONFLICT,
                context=identity_context,
            )
        return envelope


def _identity_from_dedupe_key(key: str) -> str:
    from .keys import parse_dedupe_key

    return parse_dedupe_key(key).identity


def _require_identity(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise ValidationError(f"aggregate_id must be a non-empty string, got {value!r}")
    return value


def _parse_uuid(value: object, *, field_name: str) -> uuid.UUID:
    if isinstance(value, uuid.UUID):
        return value
    return uuid.UUID(normalize_uuid(value, field=field_name))


def _as_wire_time(value: datetime | str, *, field_name: str) -> str:
    if isinstance(value, datetime):
        return to_wire(value)
    if not isinstance(value, str):
        raise ValidationError(f"{field_name}: expected a datetime or wire string")
    from ..clock import from_wire

    try:
        from_wire(value)
    except ValueError as exc:
        raise ValidationError(f"{field_name}: {exc}") from exc
    return value

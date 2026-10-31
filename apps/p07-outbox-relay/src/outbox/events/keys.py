"""Identity: dedupe keys, derived event ids, aggregate and partition keys.

Three separate jobs that are easy to conflate:

* **``dedupe_key``** names *the business fact*. Two attempts to record the same
  Deal must produce the same key, so the unique index on it is what makes a
  retry a no-op instead of a second event (INV07-03).
* **``event_id``** names *the message*. It is derived from the dedupe key rather
  than randomly generated, which is a stronger guarantee than the design asks
  for: even a writer that somehow bypassed the unique index cannot mint a
  second id for one fact.
* **``aggregate_key``/``partition_key``** name *the ordering stream*. These
  decide which events are serialised against each other and which Kafka
  partition they land on.

The encoding detail that matters: every source-supplied identifier is
hex-encoded before it goes into a composite key. Design section 14.2 warns
against a "naive string split when IDs contain separators", and this is the fix
rather than a convention to remember -- an account id of ``A:42`` cannot be
made to collide with account ``A`` in epoch ``42``, because neither one ever
appears raw inside a key.
"""

from __future__ import annotations

import enum
import hashlib
import uuid
from dataclasses import dataclass

from ..errors import ValidationError

#: Namespace label mixed into derived ids. Changing it would renumber every
#: event in the system, so it is versioned and pinned.
_EVENT_ID_NAMESPACE = b"outbox-event-id-v1"

#: Upper bound on a single source identifier. Long enough for any MT5 or
#: simulator id, short enough that a composite key stays indexable.
MAX_IDENTITY_LENGTH = 256


class EventType(enum.StrEnum):
    """The P0 event classes from design section 5.1."""

    DEAL_RECORDED = "DealRecorded"
    ACCOUNT_SNAPSHOT_PUBLISHED = "AccountSnapshotPublished"
    POSITION_SNAPSHOT_PUBLISHED = "PositionSnapshotPublished"
    SOURCE_EPOCH_CHANGED = "SourceEpochChanged"
    RECONCILIATION_VERIFIED = "ReconciliationVerified"


def hex_identity(value: str) -> str:
    """Hex-encode a source identifier for embedding in a composite key."""
    if not isinstance(value, str) or not value:
        raise ValidationError(f"identity must be a non-empty string, got {value!r}")
    if len(value) > MAX_IDENTITY_LENGTH:
        raise ValidationError(f"identity of {len(value)} chars exceeds {MAX_IDENTITY_LENGTH}")
    if "\x00" in value:
        raise ValidationError("identity must not contain NUL")
    return value.encode("utf-8").hex()


def unhex_identity(encoded: str) -> str:
    """Recover a source identifier, rejecting anything that is not clean hex."""
    if not isinstance(encoded, str) or not encoded or len(encoded) % 2:
        raise ValidationError(f"{encoded!r} is not a hex-encoded identity")
    try:
        raw = bytes.fromhex(encoded)
    except ValueError as exc:
        raise ValidationError(f"{encoded!r} is not a hex-encoded identity") from exc
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValidationError(f"{encoded!r} does not decode to UTF-8") from exc


def normalize_uuid(value: object, *, field: str) -> str:
    """Canonical lowercase hyphenated UUID text, or a rejection.

    Accepting ``{...}``, uppercase and unhyphenated spellings of one UUID would
    give one epoch several key spellings and therefore several ordering streams.
    """
    if isinstance(value, uuid.UUID):
        return str(value)
    if not isinstance(value, str):
        raise ValidationError(f"{field}: expected a UUID, got {type(value).__name__}")
    try:
        parsed = uuid.UUID(value)
    except ValueError as exc:
        raise ValidationError(f"{field}: {value!r} is not a UUID") from exc
    if str(parsed) != value:
        raise ValidationError(f"{field}: {value!r} is not canonical UUID text, expected {parsed}")
    return str(parsed)


@dataclass(frozen=True, slots=True)
class DedupeKey:
    """A parsed dedupe key. The typed mapping oracle O2 is built on."""

    event_type: EventType
    epoch_id: str
    identity: str

    def __str__(self) -> str:
        return f"{self.event_type}:{self.epoch_id}:{hex_identity(self.identity)}"


def build_dedupe_key(event_type: EventType, epoch_id: str, identity: str) -> str:
    """``<EventType>:<epoch uuid>:<hex identity>``.

    Matches the worked example in design section 5.3 and the SQL in section
    14.2, which reconstructs the same string with
    ``encode(convert_to(deal_id,'UTF8'),'hex')``.
    """
    return f"{EventType(event_type)}:{normalize_uuid(epoch_id, field='epoch_id')}:{hex_identity(identity)}"


def parse_dedupe_key(key: str) -> DedupeKey:
    """Parse a dedupe key back into its three typed parts.

    Safe to split on ``:`` precisely because the identity is hex: the only
    colons in the string are the two separators this function put there.
    """
    if not isinstance(key, str):
        raise ValidationError(f"dedupe key must be a string, got {type(key).__name__}")
    parts = key.split(":")
    if len(parts) != 3:
        raise ValidationError(f"dedupe key {key!r} does not have exactly three parts")
    raw_type, raw_epoch, raw_identity = parts
    try:
        event_type = EventType(raw_type)
    except ValueError as exc:
        raise ValidationError(f"dedupe key {key!r} names unknown event type {raw_type!r}") from exc
    return DedupeKey(
        event_type=event_type,
        epoch_id=normalize_uuid(raw_epoch, field="dedupe_key.epoch_id"),
        identity=unhex_identity(raw_identity),
    )


def derive_event_id(source_id: str, dedupe_key: str) -> uuid.UUID:
    """Deterministically derive the event id from the semantic key.

    Shaped as a version-4 UUID to match the envelope example in the design,
    while being a pure function of ``(source_id, dedupe_key)``. The variant and
    version bits are forced so that the result is a well-formed RFC 4122 UUID
    that any consumer's UUID parser will accept.
    """
    if not isinstance(source_id, str) or not source_id:
        raise ValidationError("source_id must be a non-empty string")
    digest = hashlib.sha256(
        _EVENT_ID_NAMESPACE + b"\x00" + source_id.encode("utf-8") + b"\x00" + dedupe_key.encode("utf-8")
    ).digest()
    raw = bytearray(digest[:16])
    raw[6] = (raw[6] & 0x0F) | 0x40  # version 4
    raw[8] = (raw[8] & 0x3F) | 0x80  # RFC 4122 variant
    return uuid.UUID(bytes=bytes(raw))


def partition_key(source_id: str, epoch_id: str, aggregate_key: str) -> str:
    """``<source>:<epoch>:<hex aggregate key>`` -- the Kafka record key.

    The aggregate part is hex-encoded for the same reason identities are: a
    partition key is also an ordering identity, and two distinct aggregates
    sharing one key would silently merge two ordering streams onto one
    partition and make the consumer's version check compare unrelated events.
    """
    if ":" in source_id:
        raise ValidationError(f"source_id {source_id!r} must not contain ':'")
    return f"{source_id}:{normalize_uuid(epoch_id, field='epoch_id')}:{hex_identity(aggregate_key)}"


#: Aggregate keys for the non-Deal streams. Fixed strings rather than derived
#: ones: all account scans in an epoch share one ordering stream, so their
#: markers must share one aggregate key.
AGGREGATE_ACCOUNTS = "accounts"
AGGREGATE_POSITIONS = "positions"
AGGREGATE_RECONCILIATION = "reconciliation"
AGGREGATE_SOURCE_LIFECYCLE = "source-lifecycle"

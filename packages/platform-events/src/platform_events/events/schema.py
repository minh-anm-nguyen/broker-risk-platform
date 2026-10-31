"""The pinned payload schemas, and validation that rejects rather than repairs.

The registry is data, not code paths, for two reasons. A test can enumerate
every event type and assert properties across all of them, and -- more
importantly -- adding a field is visibly a schema change in a reviewable table
rather than an extra ``if`` in a validator. Design section 5.4 forbids silently
changing a sign, unit, id meaning or currency; the way to honour that is to make
the schema the kind of thing you have to edit deliberately.

Validation is closed: unknown payload keys are an error, not forward
compatibility. A producer that sends an unexpected field has either a bug or an
unreviewed schema change, and a consumer that silently drops the field will
disagree with the hash. Additive evolution happens by registering a new spec
version, not by loosening this one.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import Any

from .. import money as money_mod
from ..clock import from_wire
from ..errors import ErrorCode, ValidationError
from .keys import MAX_IDENTITY_LENGTH, EventType, normalize_uuid

#: Longest free-text value accepted in a payload (symbol, scope label).
MAX_TEXT_LENGTH = 128


class Kind(enum.StrEnum):
    """How one payload field is validated and normalised."""

    IDENTITY = "identity"
    """A source-supplied identifier. Non-empty, bounded, no NUL."""

    UUID = "uuid"
    TEXT = "text"
    ENUM = "enum"
    MONEY = "money"
    PRICE = "price"
    VOLUME = "volume"
    TIMESTAMP = "timestamp"
    COUNT = "count"
    """A non-negative integer: record counts, sequence numbers, revisions."""

    HASH = "hash"
    """A 64-character lowercase hex SHA-256."""

    FLAG_TRUE = "flag_true"
    """A boolean that must be present and ``True``. Used for notification_only."""

    CURRENCY = "currency"
    """The ``money_currency`` half of the currency pair; may be null."""

    CURRENCY_STATUS = "currency_status"


@dataclass(frozen=True, slots=True)
class FieldSpec:
    name: str
    kind: Kind
    required: bool = True
    choices: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class EventSpec:
    """One ``(event_type, schema_version)`` contract."""

    event_type: EventType
    schema_version: int
    aggregate_type: str
    fields: tuple[FieldSpec, ...]
    money_bearing: bool
    notification_only: bool

    @property
    def field_names(self) -> frozenset[str]:
        return frozenset(field.name for field in self.fields)


_DEAL_FIELDS: tuple[FieldSpec, ...] = (
    FieldSpec("deal_id", Kind.IDENTITY),
    FieldSpec("account_id", Kind.IDENTITY),
    FieldSpec("position_id", Kind.IDENTITY, required=False),
    FieldSpec("symbol", Kind.TEXT),
    FieldSpec("side", Kind.ENUM, choices=("BUY", "SELL")),
    FieldSpec("entry_kind", Kind.ENUM, choices=("IN", "OUT", "INOUT", "BALANCE")),
    FieldSpec("volume_lots", Kind.VOLUME),
    FieldSpec("price", Kind.PRICE),
    FieldSpec("realized_pnl_gross", Kind.MONEY),
    FieldSpec("commission", Kind.MONEY),
    FieldSpec("swap", Kind.MONEY),
    FieldSpec("money_currency", Kind.CURRENCY, required=False),
    FieldSpec("money_currency_status", Kind.CURRENCY_STATUS),
    FieldSpec("executed_at", Kind.TIMESTAMP),
)

_SNAPSHOT_FIELDS: tuple[FieldSpec, ...] = (
    FieldSpec("entity", Kind.ENUM, choices=("accounts", "positions")),
    FieldSpec("scan_id", Kind.IDENTITY),
    FieldSpec("snapshot_seq", Kind.COUNT),
    FieldSpec("record_count", Kind.COUNT),
    FieldSpec("manifest_sha256", Kind.HASH),
    FieldSpec("published_at", Kind.TIMESTAMP),
    # ADR07-06: a marker must announce that it is only a notification, so a
    # consumer cannot mistake it for the position data itself.
    FieldSpec("notification_only", Kind.FLAG_TRUE),
)

REGISTRY: dict[tuple[EventType, int], EventSpec] = {
    (EventType.DEAL_RECORDED, 1): EventSpec(
        event_type=EventType.DEAL_RECORDED,
        schema_version=1,
        aggregate_type="account",
        fields=_DEAL_FIELDS,
        money_bearing=True,
        notification_only=False,
    ),
    (EventType.ACCOUNT_SNAPSHOT_PUBLISHED, 1): EventSpec(
        event_type=EventType.ACCOUNT_SNAPSHOT_PUBLISHED,
        schema_version=1,
        aggregate_type="snapshot_stream",
        fields=_SNAPSHOT_FIELDS,
        money_bearing=False,
        notification_only=True,
    ),
    (EventType.POSITION_SNAPSHOT_PUBLISHED, 1): EventSpec(
        event_type=EventType.POSITION_SNAPSHOT_PUBLISHED,
        schema_version=1,
        aggregate_type="snapshot_stream",
        fields=_SNAPSHOT_FIELDS,
        money_bearing=False,
        notification_only=True,
    ),
    (EventType.SOURCE_EPOCH_CHANGED, 1): EventSpec(
        event_type=EventType.SOURCE_EPOCH_CHANGED,
        schema_version=1,
        aggregate_type="source_lifecycle",
        fields=(
            FieldSpec("previous_epoch_id", Kind.UUID, required=False),
            FieldSpec("new_epoch_id", Kind.UUID),
            FieldSpec("reason", Kind.ENUM, choices=("RESET", "ADOPTED", "RETIRED")),
            FieldSpec("changed_at", Kind.TIMESTAMP),
            FieldSpec("notification_only", Kind.FLAG_TRUE),
        ),
        money_bearing=False,
        notification_only=True,
    ),
    (EventType.RECONCILIATION_VERIFIED, 1): EventSpec(
        event_type=EventType.RECONCILIATION_VERIFIED,
        schema_version=1,
        aggregate_type="reconciliation",
        fields=(
            FieldSpec("reconciliation_revision", Kind.COUNT),
            FieldSpec("scope", Kind.TEXT),
            FieldSpec("evidence_id", Kind.IDENTITY),
            FieldSpec("status", Kind.ENUM, choices=("VERIFIED", "BLOCKED")),
            FieldSpec("verified_at", Kind.TIMESTAMP),
            # P04 findings stay in P04. A verification summary travels; raw
            # findings would put reconciliation detail on an analytics topic.
            FieldSpec("notification_only", Kind.FLAG_TRUE),
        ),
        money_bearing=False,
        notification_only=True,
    ),
}


def get_spec(event_type: object, schema_version: object) -> EventSpec:
    """Look up a spec, or raise the error a consumer must quarantine on."""
    try:
        typed = EventType(event_type)
    except ValueError as exc:
        raise ValidationError(
            f"unknown event_type {event_type!r}", code=ErrorCode.SCHEMA_UNSUPPORTED
        ) from exc
    if not isinstance(schema_version, int) or isinstance(schema_version, bool) or schema_version < 1:
        raise ValidationError(f"schema_version {schema_version!r} must be a positive integer")
    spec = REGISTRY.get((typed, schema_version))
    if spec is None:
        raise ValidationError(
            f"{typed} schema_version {schema_version} is not in the supported registry",
            code=ErrorCode.SCHEMA_UNSUPPORTED,
        )
    return spec


def _validate_identity(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValidationError(f"{field}: expected a non-empty string")
    if len(value) > MAX_IDENTITY_LENGTH:
        raise ValidationError(f"{field}: {len(value)} chars exceeds {MAX_IDENTITY_LENGTH}")
    if "\x00" in value:
        raise ValidationError(f"{field}: must not contain NUL")
    return value


def _validate_field(spec: FieldSpec, value: object) -> Any:
    field = spec.name
    match spec.kind:
        case Kind.IDENTITY:
            return _validate_identity(value, field=field)
        case Kind.UUID:
            return normalize_uuid(value, field=field)
        case Kind.TEXT:
            text = _validate_identity(value, field=field)
            if len(text) > MAX_TEXT_LENGTH:
                raise ValidationError(f"{field}: {len(text)} chars exceeds {MAX_TEXT_LENGTH}")
            return text
        case Kind.ENUM:
            if value not in spec.choices:
                raise ValidationError(f"{field}: {value!r} not in {spec.choices}")
            return value
        case Kind.MONEY:
            return money_mod.normalize_money(value, field=field)
        case Kind.PRICE:
            return money_mod.normalize_price(value, field=field)
        case Kind.VOLUME:
            return money_mod.normalize_volume(value, field=field)
        case Kind.TIMESTAMP:
            if not isinstance(value, str):
                raise ValidationError(f"{field}: expected a timestamp string")
            try:
                from_wire(value)
            except ValueError as exc:
                raise ValidationError(f"{field}: {exc}") from exc
            return value
        case Kind.COUNT:
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValidationError(f"{field}: expected a non-negative integer, got {value!r}")
            return value
        case Kind.HASH:
            if not isinstance(value, str) or len(value) != 64:
                raise ValidationError(f"{field}: expected a 64-character hex digest")
            if any(char not in "0123456789abcdef" for char in value):
                raise ValidationError(f"{field}: expected lowercase hex")
            return value
        case Kind.FLAG_TRUE:
            if value is not True:
                raise ValidationError(f"{field}: must be present and true")
            return True
        case Kind.CURRENCY:
            if value is None or (isinstance(value, str) and len(value) == 3 and value.isupper()):
                return value
            raise ValidationError(f"{field}: {value!r} is not an ISO-4217-shaped code or null")
        case Kind.CURRENCY_STATUS:
            if value not in tuple(str(status) for status in money_mod.CurrencyStatus):
                raise ValidationError(f"{field}: {value!r} is not a known currency status")
            return value
    raise AssertionError(f"unhandled field kind {spec.kind}")  # pragma: no cover


def validate_scalar(spec: FieldSpec, value: object) -> Any:
    """Validate one field against its spec.

    Exposed because the envelope's provenance block is validated field by field
    with the same rules as a payload, without being a registered event schema.
    """
    return _validate_field(spec, value)


def validate_payload(event_type: object, schema_version: object, payload: object) -> dict[str, Any]:
    """Validate and normalise a payload against its pinned spec.

    Returns a fresh dict: callers must hash and store *this*, not what they
    passed in, because normalisation is part of the canonical form. Returning
    a copy rather than mutating in place is what keeps a caller from hashing
    the pre-normalisation version by accident.
    """
    spec = get_spec(event_type, schema_version)
    if not isinstance(payload, dict):
        raise ValidationError(f"payload must be an object, got {type(payload).__name__}")

    unknown = set(payload) - spec.field_names
    if unknown:
        raise ValidationError(
            f"{spec.event_type} v{spec.schema_version}: unknown payload fields "
            f"{sorted(unknown)}; register a new schema version instead"
        )

    normalized: dict[str, Any] = {}
    for field in spec.fields:
        if field.name not in payload:
            if field.required:
                raise ValidationError(f"{spec.event_type}: missing required field {field.name!r}")
            continue
        value = payload[field.name]
        if value is None and not field.required:
            # An explicit null for an optional field is kept, so that "absent"
            # and "known to be absent" hash the same way across producers.
            normalized[field.name] = None
            continue
        normalized[field.name] = _validate_field(field, value)

    if spec.money_bearing:
        # Cross-field rule: the currency pair must be coherent. Raises when a
        # payload claims PROVEN without a code, or carries a code while
        # claiming UNPROVEN.
        money_mod.MoneyCurrency.from_payload(normalized)
    return normalized

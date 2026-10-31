"""The P07 event envelope as P08 is allowed to understand it. Section 2.1.

Two things deserve attention before changing this file.

**Tenant is not a payload field.** P08 adds ``tenant_id`` to the data model,
but the design is explicit that an approved mapping must be configured rather
than trusted from the wire (section 6.1 step 1). So :func:`parse_envelope`
takes the tenant from a resolver the operator configured; a ``tenant_id``
appearing in the payload is treated as an attempted scope escalation.

**Two hashes, two different jobs.** ``payload_sha256`` covers the whole P07
payload and detects a changed envelope. ``deal_payload_hash`` covers only the
financially meaningful Deal fields, which is what lets P08 recognise the same
Deal arriving under a second ``event_id`` (INV08-03, P08-G-04). Checking only
one of them leaves a real conflict class invisible, so both are mandatory and
both are recomputed locally rather than believed.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Callable, Mapping

from ..canonical import digest_of, ensure_utc, is_sha256_hex, parse_timestamp
from ..errors import PoisonError, Reason
from ..money import MoneyError, parse_currency, parse_money

DEAL_RECORDED = "DealRecorded"
ACCOUNT_SNAPSHOT_PUBLISHED = "AccountSnapshotPublished"

#: Only these event types may reach the Deal raw table. A snapshot marker is
#: *accepted* by the consumer but routed to the lightweight control log,
#: because a marker is not a snapshot (INV08-16, section 5.2).
ALLOWED_EVENT_TYPES: frozenset[str] = frozenset({DEAL_RECORDED, ACCOUNT_SNAPSHOT_PUBLISHED})

#: ``DealRecorded.v1`` only. A v2 correction event must be rejected until the
#: protocol in section 7.3 is agreed, so that an unapproved correction cannot
#: silently rewrite money (P08-W).
SUPPORTED_SCHEMA_VERSIONS: frozenset[int] = frozenset({1})

#: Deal field set covered by ``deal_payload_hash``. Pinned and ordered: adding
#: a field changes every fingerprint, so it is a contract change.
CANONICAL_DEAL_FIELDS: tuple[str, ...] = (
    "account_id",
    "commission",
    "deal_id",
    "entry_kind",
    "executed_at",
    "money_currency",
    "realized_pnl_gross",
    "side",
    "swap",
    "symbol",
    "volume_lots",
)

ALLOWED_SIDES: frozenset[str] = frozenset({"BUY", "SELL"})
ALLOWED_ENTRY_KINDS: frozenset[str] = frozenset({"IN", "OUT", "INOUT", "OUT_BY"})

_UUID_RE = re.compile(
    r"\A[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z", re.IGNORECASE
)
_ID_RE = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._:\-]{0,127}\Z")

#: Guards against a hostile or broken producer sending an unbounded envelope
#: that would be parsed before any size limit applied (P08-AK).
MAX_ENVELOPE_BYTES = 256 * 1024

TenantResolver = Callable[[str, str], str | None]
"""``(topic, source_id) -> tenant_id``, from operator configuration only."""


def _require(mapping: Mapping[str, Any], key: str) -> Any:
    if key not in mapping:
        raise PoisonError(Reason.FIELD_MISSING, f"missing required field {key}", field=key)
    value = mapping[key]
    if value is None:
        raise PoisonError(Reason.FIELD_MISSING, f"field {key} must not be null", field=key)
    return value


def _require_identifier(mapping: Mapping[str, Any], key: str) -> str:
    value = _require(mapping, key)
    if not isinstance(value, str) or not _ID_RE.match(value):
        raise PoisonError(Reason.FIELD_INVALID, f"field {key} is not a valid identifier", field=key)
    return value


def _require_uuid(mapping: Mapping[str, Any], key: str) -> str:
    value = _require(mapping, key)
    if not isinstance(value, str) or not _UUID_RE.match(value):
        raise PoisonError(Reason.FIELD_INVALID, f"field {key} is not a UUID", field=key)
    return value.lower()


def _require_uint(mapping: Mapping[str, Any], key: str, *, limit: int = 2**63 - 1) -> int:
    value = _require(mapping, key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise PoisonError(Reason.FIELD_INVALID, f"field {key} must be an integer", field=key)
    if value < 0 or value > limit:
        # P08-E-12: a negative or wrapped coordinate is rejected at the type
        # boundary rather than truncated into range by the storage engine.
        raise PoisonError(Reason.FIELD_INVALID, f"field {key} is out of range", field=key)
    return value


def _require_hash(mapping: Mapping[str, Any], key: str) -> str:
    value = _require(mapping, key)
    if not is_sha256_hex(value):
        raise PoisonError(Reason.HASH_MALFORMED, f"field {key} is not 64 hex chars", field=key)
    return str(value)


@dataclass(frozen=True, slots=True)
class DealPayload:
    """Validated financial content of one ``DealRecorded.v1`` event."""

    deal_id: str
    account_id: str
    symbol: str
    side: str
    entry_kind: str
    executed_at: datetime
    volume_lots: Decimal
    realized_pnl_gross: Decimal
    commission: Decimal
    swap: Decimal
    money_currency: str
    symbol_catalog_version: str
    accounting_policy_version: str
    declared_fee_coverage: str

    def canonical_fields(self) -> dict[str, Any]:
        return {
            "account_id": self.account_id,
            "commission": self.commission,
            "deal_id": self.deal_id,
            "entry_kind": self.entry_kind,
            "executed_at": self.executed_at,
            "money_currency": self.money_currency,
            "realized_pnl_gross": self.realized_pnl_gross,
            "side": self.side,
            "swap": self.swap,
            "symbol": self.symbol,
            "volume_lots": self.volume_lots,
        }


def canonical_deal_fingerprint(payload: DealPayload) -> str:
    """SHA-256 over the pinned canonical Deal fields.

    Compared against P07's ``provenance.deal_payload_hash``. Recomputed rather
    than trusted so that a producer-side normalisation bug - the same money
    under a different ``account_id``, say - is caught here instead of becoming
    a "same hash, different account" conflict later (P08-R-03).
    """
    return digest_of(payload.canonical_fields())


def parse_deal_payload(payload: Mapping[str, Any]) -> DealPayload:
    """Validate the Deal payload, reporting every failure as poison.

    Money and currency helpers raise :class:`MoneyError`, which is *not* a
    :class:`PoisonError`. That distinction matters elsewhere - a money error
    inside the build is a policy failure, not a wire failure - but here it
    would be a hole: the ingest worker quarantines ``PoisonError`` and lets
    anything else propagate, so a Deal with a lowercase currency would crash
    the consumer instead of being isolated. Re-raising as poison keeps the
    classification honest: a malformed amount on the wire can never become
    valid by retrying.
    """
    if not isinstance(payload, Mapping):
        raise PoisonError(Reason.ENVELOPE_MALFORMED, "payload must be an object")
    try:
        return _parse_deal_payload_fields(payload)
    except MoneyError as exc:
        raise PoisonError(exc.reason, str(exc), **exc.details) from exc


def _parse_deal_payload_fields(payload: Mapping[str, Any]) -> DealPayload:

    side = _require(payload, "side")
    if side not in ALLOWED_SIDES:
        raise PoisonError(Reason.SIDE_UNKNOWN, f"unsupported side {side!r}", field="side")
    entry_kind = _require(payload, "entry_kind")
    if entry_kind not in ALLOWED_ENTRY_KINDS:
        raise PoisonError(
            Reason.ENTRY_KIND_UNKNOWN,
            f"unsupported entry_kind {entry_kind!r}",
            field="entry_kind",
        )

    symbol = _require(payload, "symbol")
    if not isinstance(symbol, str) or not symbol.strip():
        # P08-U-08: a null or blank symbol is never mapped to a default
        # instrument; the Deal is poison.
        raise PoisonError(Reason.SYMBOL_UNKNOWN, "symbol is required", field="symbol")

    executed_at_raw = _require(payload, "executed_at")
    if isinstance(executed_at_raw, datetime):
        executed_at = ensure_utc(executed_at_raw, "executed_at")
    else:
        try:
            executed_at = parse_timestamp(str(executed_at_raw), "executed_at")
        except Exception as exc:
            raise PoisonError(Reason.FIELD_INVALID, str(exc), field="executed_at") from exc

    volume = parse_money(_require(payload, "volume_lots"), "volume_lots", scale=8)
    if volume <= 0:
        raise PoisonError(Reason.FIELD_INVALID, "volume_lots must be positive", field="volume_lots")

    return DealPayload(
        deal_id=_require_identifier(payload, "deal_id"),
        account_id=_require_identifier(payload, "account_id"),
        symbol=symbol,
        side=side,
        entry_kind=entry_kind,
        executed_at=executed_at,
        volume_lots=volume,
        realized_pnl_gross=parse_money(_require(payload, "realized_pnl_gross"), "realized_pnl_gross"),
        commission=parse_money(_require(payload, "commission"), "commission"),
        swap=parse_money(_require(payload, "swap"), "swap"),
        money_currency=parse_currency(_require(payload, "money_currency")),
        symbol_catalog_version=_require_identifier(payload, "symbol_catalog_version"),
        accounting_policy_version=_require_identifier(payload, "accounting_policy_version"),
        declared_fee_coverage=_require_identifier(payload, "fee_coverage"),
    )


@dataclass(frozen=True, slots=True)
class EventEnvelope:
    """A P07 envelope that passed every structural and hash check."""

    tenant_id: str
    event_id: str
    dedupe_key: str
    event_type: str
    schema_version: int
    source_id: str
    epoch_id: str
    aggregate_type: str
    aggregate_id: str
    aggregate_version: int
    created_at: datetime
    payload_sha256: str
    source_seq: int
    deal_id: str | None
    deal_payload_hash: str | None
    deal: DealPayload | None
    payload: Mapping[str, Any]
    topic: str
    partition: int
    offset: int

    @property
    def scope(self) -> tuple[str, str, str]:
        """``(tenant_id, source_id, epoch_id)`` - the hard isolation boundary."""
        return (self.tenant_id, self.source_id, self.epoch_id)

    @property
    def deal_key(self) -> tuple[str, str, str, str]:
        """Semantic Deal identity. INV08-03/INV08-04."""
        if self.deal_id is None:
            raise ValueError("deal_key is only defined for DealRecorded events")
        return (self.tenant_id, self.source_id, self.epoch_id, self.deal_id)

    @property
    def is_deal(self) -> bool:
        return self.event_type == DEAL_RECORDED


def parse_envelope(
    record: Mapping[str, Any],
    *,
    topic: str,
    partition: int,
    offset: int,
    tenant_resolver: TenantResolver,
    allowed_topics: frozenset[str],
    allowed_event_types: frozenset[str] = ALLOWED_EVENT_TYPES,
    max_bytes: int = MAX_ENVELOPE_BYTES,
    raw_size: int | None = None,
) -> EventEnvelope:
    """Validate one wire record into an :class:`EventEnvelope`.

    Every failure is :class:`PoisonError`: retrying a malformed record cannot
    make it valid, and the caller must therefore quarantine it durably rather
    than skip it (section 6.1 step 3). Transient faults are raised by the
    transport, not here.
    """
    if raw_size is not None and raw_size > max_bytes:
        raise PoisonError(
            Reason.ENVELOPE_TOO_LARGE,
            f"envelope of {raw_size} bytes exceeds {max_bytes}",
            size=raw_size,
        )
    if topic not in allowed_topics:
        raise PoisonError(Reason.TOPIC_NOT_ALLOWED, f"topic {topic!r} is not allowlisted", topic=topic)
    if not isinstance(record, Mapping):
        raise PoisonError(Reason.ENVELOPE_MALFORMED, "record must be a JSON object")

    event_type = _require(record, "event_type")
    if event_type not in allowed_event_types:
        raise PoisonError(
            Reason.EVENT_TYPE_NOT_ALLOWED,
            f"event_type {event_type!r} is not accepted",
            event_type=str(event_type),
        )

    schema_version = _require_uint(record, "schema_version", limit=65535)
    if schema_version not in SUPPORTED_SCHEMA_VERSIONS:
        # Includes the v2 correction events of section 7.3: refusing them is
        # the correct MVP behaviour, and it must be visible, not silent.
        raise PoisonError(
            Reason.SCHEMA_VERSION_UNSUPPORTED,
            f"schema_version {schema_version} is not supported",
            schema_version=schema_version,
        )

    source_id = _require_identifier(record, "source_id")
    tenant_id = tenant_resolver(topic, source_id)
    if not tenant_id:
        # P08-B: an unmapped source is never given a default tenant, because a
        # default tenant is a cross-tenant leak waiting to happen.
        raise PoisonError(
            Reason.TENANT_UNMAPPED,
            f"no approved tenant mapping for source {source_id!r} on {topic!r}",
            source_id=source_id,
            topic=topic,
        )
    if "tenant_id" in record and record["tenant_id"] != tenant_id:
        raise PoisonError(
            Reason.TENANT_UNMAPPED,
            "payload tenant_id contradicts the approved mapping",
            source_id=source_id,
        )

    payload = _require(record, "payload")
    if not isinstance(payload, Mapping):
        raise PoisonError(Reason.ENVELOPE_MALFORMED, "payload must be an object")
    if "tenant_id" in payload:
        raise PoisonError(
            Reason.TENANT_UNMAPPED,
            "payload must not carry tenant_id; scope comes from configuration",
        )

    declared_payload_hash = _require_hash(record, "payload_sha256")
    computed_payload_hash = digest_of(_decimalize_money_fields(payload))
    if declared_payload_hash != computed_payload_hash:
        # INV08-02 territory: the producer's own digest disagrees with the
        # bytes it shipped, so the record is untrustworthy regardless of which
        # side is wrong.
        raise PoisonError(
            Reason.HASH_MISMATCH,
            "payload_sha256 does not match the canonical payload",
            declared=declared_payload_hash,
            computed=computed_payload_hash,
        )

    provenance = _require(record, "provenance")
    if not isinstance(provenance, Mapping):
        raise PoisonError(Reason.ENVELOPE_MALFORMED, "provenance must be an object")
    source_seq = _require_uint(provenance, "source_seq")

    deal: DealPayload | None = None
    deal_id: str | None = None
    deal_payload_hash: str | None = None
    if event_type == DEAL_RECORDED:
        deal = parse_deal_payload(payload)
        deal_id = _require_identifier(provenance, "deal_id")
        if deal_id != deal.deal_id:
            raise PoisonError(
                Reason.FIELD_INVALID,
                "provenance.deal_id does not match payload.deal_id",
                field="provenance.deal_id",
            )
        deal_payload_hash = _require_hash(provenance, "deal_payload_hash")
        computed_fingerprint = canonical_deal_fingerprint(deal)
        if deal_payload_hash != computed_fingerprint:
            raise PoisonError(
                Reason.HASH_MISMATCH,
                "deal_payload_hash does not match the canonical Deal fields",
                declared=deal_payload_hash,
                computed=computed_fingerprint,
            )

    return EventEnvelope(
        tenant_id=tenant_id,
        event_id=_require_uuid(record, "event_id"),
        dedupe_key=_require_identifier(record, "dedupe_key"),
        event_type=str(event_type),
        schema_version=schema_version,
        source_id=source_id,
        epoch_id=_require_uuid(record, "epoch_id"),
        aggregate_type=_require_identifier(record, "aggregate_type"),
        aggregate_id=_require_identifier(record, "aggregate_id"),
        aggregate_version=_require_uint(record, "aggregate_version"),
        created_at=parse_timestamp(str(_require(record, "created_at")), "created_at"),
        payload_sha256=declared_payload_hash,
        source_seq=source_seq,
        deal_id=deal_id,
        deal_payload_hash=deal_payload_hash,
        deal=deal,
        payload=dict(payload),
        topic=topic,
        partition=partition,
        offset=offset,
    )


#: Payload keys whose wire form is a decimal *string*. They are converted to
#: ``Decimal`` before hashing so that ``"12.30"`` and ``"12.300"`` - the same
#: amount written two ways - produce one digest. Without this, a producer that
#: trimmed a trailing zero would look like a payload conflict.
_MONEY_PAYLOAD_FIELDS = ("realized_pnl_gross", "commission", "swap", "volume_lots")


def _decimalize_money_fields(payload: Mapping[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = dict(payload)
    for field in _MONEY_PAYLOAD_FIELDS:
        if field in normalized:
            value = normalized[field]
            if isinstance(value, float):
                raise PoisonError(
                    Reason.MONEY_NOT_A_STRING,
                    f"{field} arrived as a JSON float",
                    field=field,
                )
            if isinstance(value, (str, int, Decimal)):
                try:
                    normalized[field] = Decimal(str(value))
                except Exception as exc:
                    raise PoisonError(
                        Reason.MONEY_MALFORMED, f"{field} is not a decimal", field=field
                    ) from exc
    executed = normalized.get("executed_at")
    if isinstance(executed, str):
        try:
            normalized["executed_at"] = parse_timestamp(executed, "executed_at")
        except Exception as exc:
            raise PoisonError(Reason.FIELD_INVALID, str(exc), field="executed_at") from exc
    return normalized

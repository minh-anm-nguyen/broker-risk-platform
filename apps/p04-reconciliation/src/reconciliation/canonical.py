"""Canonical field mapping and content hashing (SYSTEM_DESIGN section 6.4).

This module is the single place where "are these two records the same?" is
decided, so it is deliberately the strictest code in the service.

Design rules enforced here:

* **Decimals come from strings only.** A ``float`` can never enter a money
  comparison: ``0.1 + 0.2 != 0.3`` in binary floating point, and a hash built
  on that would report a mismatch that does not exist, or hide one that does
  (section 6.4, F-10).
* **Scale normalisation is lossless or it is refused.** ``Decimal("1.0")`` and
  ``Decimal("1.00")`` hash equal because the declared USD policy is scale 2
  and dropping a trailing zero loses nothing. ``Decimal("1.004")`` is
  *rejected* rather than rounded: silently turning it into ``1.00`` would
  invent a value the source never sent (section 6.4 "reject unsupported
  scale").
* **IDs are strings.** ``"000123"`` and ``"123"`` are different accounts. The
  mapper refuses a non-string ID instead of coercing one, because coercion is
  where leading zeros disappear (F-13).
* **Timestamps must be unambiguous.** A naive datetime has no instant, so it
  cannot be compared across two systems; it is refused (section 6.4).
* **The hash is domain-separated.** Entity type, schema version, hash version
  and the ordered canonical field list are all inside the hashed bytes. Two
  records from different schema versions therefore cannot collide into a false
  ``MATCHED``, and a field-list change cannot silently keep old hashes valid
  (section 6.4, property rule 5 in section 10.4).
* **``null`` and "absent" are different.** ``{"mark": null}`` does not hash the
  same as ``{}``: one says "the source told us there is no mark", the other
  says "we never asked". Collapsing them would erase evidence.

The functions here do not know what a diff is and never consult policy. They
turn a validated record into bytes and a digest, nothing more.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any, Final

from .errors import CanonicalizationError, ReasonCode

#: Bumping this invalidates every comparison against stored hashes. It is part
#: of the hash domain, so an old hash can never be mistaken for a new one.
HASH_VERSION: Final[str] = "sha256:canonical-json-v1"

#: The P04 canonical projection version. Independent of P01's wire version and
#: of P03's storage version: both must be mapped *into* this (section 6.4).
SCHEMA_VERSION: Final[str] = "p04-recon-v1"

#: Declared money scale per currency (section 6.4 "declared scale/currency
#: policy"). A currency absent from this table is not comparable, because we
#: have not stated how many minor units it has.
MONEY_SCALE: Final[Mapping[str, int]] = {"USD": 2}

#: Price and volume scales are instrument conventions, not money policy. P01
#: emits 5-dp FX prices and 2-dp lot volumes.
PRICE_SCALE: Final[int] = 5
VOLUME_SCALE: Final[int] = 2

#: Guard against absurd magnitudes before they reach the database. This is a
#: sanity bound, not a business rule: a balance with 30 integer digits is a
#: corrupted payload, not a rich client.
MAX_INTEGER_DIGITS: Final[int] = 18

_ID_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:\-]{0,63}$")

_SYMBOL_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[A-Z0-9._]{1,32}$")


class FieldKind(StrEnum):
    """How one canonical field is normalised before hashing."""

    ID = "ID"
    MONEY = "MONEY"
    PRICE = "PRICE"
    VOLUME = "VOLUME"
    CURRENCY = "CURRENCY"
    SYMBOL = "SYMBOL"
    ENUM = "ENUM"
    INT = "INT"
    TIME = "TIME"
    BOOL = "BOOL"


class FieldSpec:
    """One field in a versioned canonical projection.

    ``optional`` means the key may be *absent*. ``nullable`` means the key may
    be present with value ``None``. They are different states and hash
    differently on purpose.
    """

    __slots__ = ("name", "kind", "optional", "nullable", "allowed")

    def __init__(
        self,
        name: str,
        kind: FieldKind,
        *,
        optional: bool = False,
        nullable: bool = False,
        allowed: frozenset[str] | None = None,
    ) -> None:
        self.name = name
        self.kind = kind
        self.optional = optional
        self.nullable = nullable
        self.allowed = allowed

    def __repr__(self) -> str:  # pragma: no cover - diagnostic sugar
        return f"FieldSpec({self.name!r}, {self.kind})"


class EntitySpec:
    """The ordered, versioned canonical field list for one entity type.

    ``key_fields`` identify the row across source and target. ``money_fields``
    are the subset whose disagreement is a financial conflict that policy must
    never auto-correct (section 8.1).
    """

    __slots__ = ("entity", "fields", "key_fields", "money_fields", "_by_name")

    def __init__(
        self,
        entity: str,
        fields: Sequence[FieldSpec],
        *,
        key_fields: Sequence[str],
        money_fields: Sequence[str] = (),
    ) -> None:
        self.entity = entity
        self.fields = tuple(fields)
        self.key_fields = tuple(key_fields)
        self.money_fields = frozenset(money_fields)
        self._by_name = {spec.name: spec for spec in self.fields}
        missing = [name for name in key_fields if name not in self._by_name]
        if missing:
            raise CanonicalizationError(
                "key field is not part of the canonical field list",
                entity=entity,
                missing=missing,
            )

    @property
    def field_names(self) -> tuple[str, ...]:
        return tuple(spec.name for spec in self.fields)

    def spec_for(self, name: str) -> FieldSpec:
        try:
            return self._by_name[name]
        except KeyError:
            raise CanonicalizationError(
                "field is not in the canonical projection",
                entity=self.entity,
                field=name,
            ) from None


# ---------------------------------------------------------------------------
# Canonical projections
#
# These lists are the contract. Adding, removing or reordering a field changes
# every hash for that entity, which is why SCHEMA_VERSION is hashed alongside
# them. Transport-only values (request ids, cursors, scan timestamps, mark
# staleness flags) are excluded: they are not properties of the entity and
# would make two correct observations disagree (section 6.4).
# ---------------------------------------------------------------------------

ACCOUNT_SPEC: Final[EntitySpec] = EntitySpec(
    "accounts",
    [
        FieldSpec("account_id", FieldKind.ID),
        FieldSpec("currency", FieldKind.CURRENCY),
        FieldSpec("balance", FieldKind.MONEY),
        FieldSpec("credit", FieldKind.MONEY),
    ],
    key_fields=["account_id"],
    money_fields=["balance", "credit"],
)

POSITION_SPEC: Final[EntitySpec] = EntitySpec(
    "positions",
    [
        FieldSpec("position_id", FieldKind.ID),
        FieldSpec("account_id", FieldKind.ID),
        FieldSpec("symbol", FieldKind.SYMBOL),
        FieldSpec("side", FieldKind.ENUM, allowed=frozenset({"BUY", "SELL"})),
        FieldSpec("volume_lots", FieldKind.VOLUME),
        FieldSpec("open_price", FieldKind.PRICE),
        FieldSpec("status", FieldKind.ENUM, allowed=frozenset({"OPEN", "CLOSED"})),
    ],
    key_fields=["position_id"],
)

DEAL_SPEC: Final[EntitySpec] = EntitySpec(
    "deals",
    [
        FieldSpec("deal_id", FieldKind.ID),
        FieldSpec("account_id", FieldKind.ID),
        FieldSpec("position_id", FieldKind.ID),
        FieldSpec("symbol", FieldKind.SYMBOL),
        FieldSpec("entry", FieldKind.ENUM, allowed=frozenset({"IN", "OUT", "INOUT"})),
        FieldSpec("side", FieldKind.ENUM, allowed=frozenset({"BUY", "SELL"})),
        FieldSpec("volume_lots", FieldKind.VOLUME),
        FieldSpec("price", FieldKind.PRICE),
        FieldSpec("realized_pnl_gross", FieldKind.MONEY),
        FieldSpec("commission", FieldKind.MONEY),
        FieldSpec("swap", FieldKind.MONEY),
        FieldSpec("executed_at", FieldKind.TIME),
    ],
    key_fields=["deal_id"],
    money_fields=["realized_pnl_gross", "commission", "swap"],
)

ENTITY_SPECS: Final[Mapping[str, EntitySpec]] = {
    ACCOUNT_SPEC.entity: ACCOUNT_SPEC,
    POSITION_SPEC.entity: POSITION_SPEC,
    DEAL_SPEC.entity: DEAL_SPEC,
}

#: ``source_seq`` is carried beside the hash, never inside it. Two observations
#: of the same immutable deal must hash identically regardless of which scan
#: saw it; the sequence is provenance, not content (section 5.2 rule 7).
EXCLUDED_FROM_HASH: Final[frozenset[str]] = frozenset(
    {
        "source_seq",
        "snapshot_seq",
        "run_id",
        "generation",
        "request_id",
        "scan_id",
        "cursor",
        "next_cursor",
        "as_of_seq",
        "unrealized_pnl",
        "equity",
        "mark_market_time",
        "mark_stale",
        "data_quality",
        "open_position_count",
        "captured_at",
    }
)


def spec_for_entity(entity: str) -> EntitySpec:
    try:
        return ENTITY_SPECS[entity]
    except KeyError:
        raise CanonicalizationError(
            "unsupported entity for canonical hashing",
            reason=ReasonCode.INCOMPARABLE_SCHEMA,
            entity=entity,
        ) from None


# ---------------------------------------------------------------------------
# Scalar normalisation
# ---------------------------------------------------------------------------


def parse_decimal(raw: Any, *, field: str) -> Decimal:
    """Parse a Decimal from a string, refusing every lossy input.

    ``bool`` is checked before ``int`` because ``bool`` is a subclass of
    ``int`` in Python and ``True`` would otherwise become ``Decimal("1")``.
    """
    if isinstance(raw, Decimal):
        value = raw
    elif isinstance(raw, bool):
        raise CanonicalizationError(
            "boolean is not a numeric value", field=field, value=repr(raw)
        )
    elif isinstance(raw, float):
        # Refused rather than converted: the binary value is already not the
        # decimal the source meant (section 6.4).
        raise CanonicalizationError(
            "float is never accepted for a decimal field; send a string",
            field=field,
            value=repr(raw),
        )
    elif isinstance(raw, int):
        value = Decimal(raw)
    elif isinstance(raw, str):
        text = raw.strip()
        if not text:
            raise CanonicalizationError("empty string is not a decimal", field=field)
        if text != raw:
            raise CanonicalizationError(
                "decimal string carries surrounding whitespace", field=field, value=raw
            )
        try:
            value = Decimal(text)
        except InvalidOperation:
            raise CanonicalizationError(
                "value is not a valid decimal", field=field, value=raw
            ) from None
    else:
        raise CanonicalizationError(
            "unsupported type for a decimal field", field=field, type=type(raw).__name__
        )

    if value.is_nan() or value.is_infinite():
        raise CanonicalizationError(
            "NaN and Infinity are not comparable money values", field=field, value=str(raw)
        )
    sign, digits, exponent = value.as_tuple()
    integer_digits = len(digits) + int(exponent) if isinstance(exponent, int) else len(digits)
    if integer_digits > MAX_INTEGER_DIGITS:
        raise CanonicalizationError(
            "decimal magnitude exceeds the supported bound",
            field=field,
            value=str(value),
            max_integer_digits=MAX_INTEGER_DIGITS,
        )
    return value


def quantize_exact(value: Decimal, scale: int, *, field: str) -> Decimal:
    """Normalise to ``scale`` decimal places, but only if nothing is lost.

    This is what makes ``"1.0"`` and ``"1.00"`` the same canonical value while
    keeping ``"1.004"`` an error under a 2-dp policy. Rounding here would be
    the service inventing money (section 6.4, F-10).
    """
    target = Decimal(1).scaleb(-scale)
    normalised = value.quantize(target)
    if normalised != value:
        raise CanonicalizationError(
            "value carries more precision than the declared scale allows",
            reason=ReasonCode.INCOMPARABLE_SCHEMA,
            field=field,
            value=str(value),
            declared_scale=scale,
        )
    if normalised == 0:
        # Collapse -0.00 to 0.00: they are the same amount of money, and a
        # sign bit on zero would otherwise produce a phantom mismatch (F-10).
        normalised = Decimal(0).quantize(target)
    return normalised


def money_scale_for(currency: str) -> int:
    try:
        return MONEY_SCALE[currency]
    except KeyError:
        raise CanonicalizationError(
            "currency has no declared money scale; comparison is unsafe",
            reason=ReasonCode.INCOMPARABLE_SCHEMA,
            currency=currency,
        ) from None


def normalise_timestamp(raw: Any, *, field: str) -> str:
    """Return a fixed-width UTC instant, refusing anything ambiguous."""
    if isinstance(raw, datetime):
        moment = raw
    elif isinstance(raw, str):
        text = raw.strip()
        if text != raw or not text:
            raise CanonicalizationError("timestamp has stray whitespace", field=field, value=raw)
        candidate = text[:-1] + "+00:00" if text.endswith("Z") else text
        try:
            moment = datetime.fromisoformat(candidate)
        except ValueError:
            raise CanonicalizationError(
                "timestamp is not ISO-8601", field=field, value=raw
            ) from None
    else:
        raise CanonicalizationError(
            "unsupported type for a timestamp field", field=field, type=type(raw).__name__
        )

    if moment.tzinfo is None or moment.tzinfo.utcoffset(moment) is None:
        raise CanonicalizationError(
            "naive timestamp has no instant and cannot be compared",
            field=field,
            value=str(raw),
        )
    utc = moment.astimezone(timezone.utc)
    # Microsecond precision always rendered, so "…:09Z" and "…:09.000000Z"
    # -- the same instant -- produce the same canonical text.
    return utc.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def normalise_id(raw: Any, *, field: str) -> str:
    """Identifiers stay strings, with their leading zeros intact (F-13)."""
    if isinstance(raw, bool) or not isinstance(raw, str):
        raise CanonicalizationError(
            "identifier must be a string; numeric coercion loses leading zeros",
            field=field,
            type=type(raw).__name__,
        )
    if not _ID_PATTERN.match(raw):
        raise CanonicalizationError(
            "identifier contains characters outside the declared policy",
            field=field,
            value=raw,
        )
    return raw


def _normalise_field(spec: FieldSpec, raw: Any, *, currency: str | None) -> Any:
    kind = spec.kind
    if kind is FieldKind.ID:
        return normalise_id(raw, field=spec.name)
    if kind is FieldKind.MONEY:
        if currency is None:
            raise CanonicalizationError(
                "money field needs a currency to establish its scale", field=spec.name
            )
        scale = money_scale_for(currency)
        return str(quantize_exact(parse_decimal(raw, field=spec.name), scale, field=spec.name))
    if kind is FieldKind.PRICE:
        return str(
            quantize_exact(parse_decimal(raw, field=spec.name), PRICE_SCALE, field=spec.name)
        )
    if kind is FieldKind.VOLUME:
        return str(
            quantize_exact(parse_decimal(raw, field=spec.name), VOLUME_SCALE, field=spec.name)
        )
    if kind is FieldKind.CURRENCY:
        if not isinstance(raw, str) or raw not in MONEY_SCALE:
            raise CanonicalizationError(
                "currency is not in the declared policy table",
                reason=ReasonCode.INCOMPARABLE_SCHEMA,
                field=spec.name,
                value=repr(raw),
            )
        return raw
    if kind is FieldKind.SYMBOL:
        if not isinstance(raw, str) or not _SYMBOL_PATTERN.match(raw):
            raise CanonicalizationError(
                "symbol violates the declared catalog policy", field=spec.name, value=repr(raw)
            )
        return raw
    if kind is FieldKind.ENUM:
        if not isinstance(raw, str):
            raise CanonicalizationError(
                "enum field must be a string", field=spec.name, type=type(raw).__name__
            )
        if spec.allowed is not None and raw not in spec.allowed:
            raise CanonicalizationError(
                "enum value is outside the declared domain",
                field=spec.name,
                value=raw,
                allowed=sorted(spec.allowed),
            )
        return raw
    if kind is FieldKind.INT:
        if isinstance(raw, bool) or not isinstance(raw, int):
            raise CanonicalizationError(
                "integer field must be an int", field=spec.name, type=type(raw).__name__
            )
        return raw
    if kind is FieldKind.BOOL:
        if not isinstance(raw, bool):
            raise CanonicalizationError(
                "boolean field must be a bool", field=spec.name, type=type(raw).__name__
            )
        return raw
    if kind is FieldKind.TIME:
        return normalise_timestamp(raw, field=spec.name)
    raise CanonicalizationError("unhandled field kind", field=spec.name, kind=str(kind))


# ---------------------------------------------------------------------------
# Record canonicalisation and hashing
# ---------------------------------------------------------------------------


def canonical_record(entity: str, record: Mapping[str, Any]) -> dict[str, Any]:
    """Project a validated record onto its canonical, versioned field set.

    Unknown keys are ignored only when they are on the documented exclusion
    list; anything else is an error, because an unrecognised field may be the
    very value that differs between the two systems (section 6.4).
    """
    spec = spec_for_entity(entity)
    unexpected = sorted(
        key
        for key in record
        if key not in spec.field_names and key not in EXCLUDED_FROM_HASH
    )
    if unexpected:
        raise CanonicalizationError(
            "record carries fields outside the canonical projection",
            reason=ReasonCode.INCOMPARABLE_SCHEMA,
            entity=entity,
            unexpected=unexpected,
        )

    currency: str | None = None
    if "currency" in spec.field_names:
        raw_currency = record.get("currency")
        if isinstance(raw_currency, str):
            currency = raw_currency
    else:
        # Positions and deals carry no currency of their own; the declared
        # single-currency policy for this MVP is the account currency. Stated
        # here rather than assumed silently (section 1.4, open question).
        currency = "USD"

    canonical: dict[str, Any] = {}
    for field in spec.fields:
        if field.name not in record:
            if field.optional:
                continue
            raise CanonicalizationError(
                "canonical field is missing from the record",
                entity=entity,
                field=field.name,
            )
        raw = record[field.name]
        if raw is None:
            if not field.nullable:
                raise CanonicalizationError(
                    "canonical field is not nullable", entity=entity, field=field.name
                )
            canonical[field.name] = None
            continue
        canonical[field.name] = _normalise_field(field, raw, currency=currency)
    return canonical


def canonical_json_bytes(payload: Any) -> bytes:
    """Deterministic UTF-8 JSON: sorted keys, no insignificant whitespace.

    ``allow_nan=False`` makes a stray float NaN an error rather than the
    non-standard ``NaN`` token, which some parsers accept and some reject.
    """
    return json.dumps(
        payload,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def hash_domain(entity: str) -> dict[str, Any]:
    """The domain-separation header hashed with every record."""
    spec = spec_for_entity(entity)
    return {
        "entity": spec.entity,
        "schema_version": SCHEMA_VERSION,
        "hash_version": HASH_VERSION,
        "fields": list(spec.field_names),
    }


def payload_hash(entity: str, record: Mapping[str, Any]) -> str:
    """Return the canonical content digest for one record.

    Equality of two digests is evidence that the two canonical representations
    are identical. It is **not** proof that either side is correct, and not
    proof of origin (section 5.1 definition of ``H_v``).
    """
    envelope = {"domain": hash_domain(entity), "fields": canonical_record(entity, record)}
    return hashlib.sha256(canonical_json_bytes(envelope)).hexdigest()


def item_key(entity: str, record: Mapping[str, Any]) -> str:
    """The cross-system identity of a row, as a string."""
    spec = spec_for_entity(entity)
    parts = [normalise_id(record[name], field=name) for name in spec.key_fields]
    return "|".join(parts)


def page_hash(entity: str, items: Iterable[tuple[str, str]]) -> str:
    """Digest of one page's ``(item_key, payload_hash)`` pairs, in order.

    Page hashes are order-sensitive on purpose: they prove *this page* was
    received as transmitted, which is what a resumed scan re-validates after a
    lost commit ACK (section 7.6, IT04-01).
    """
    envelope = {
        "domain": {"entity": entity, "schema_version": SCHEMA_VERSION, "hash_version": HASH_VERSION},
        "items": [[key, digest] for key, digest in items],
    }
    return hashlib.sha256(canonical_json_bytes(envelope)).hexdigest()


def root_hash(entity: str, items: Iterable[tuple[str, str]]) -> str:
    """Order-independent digest of a whole manifest.

    Sorted before hashing so that two scans which read the same set in a
    different order produce the same root. That is the metamorphic property in
    section 10.4 rule 1, and it is why this is safe to compare across a source
    listing and a SQL result set.
    """
    ordered = sorted(set(items))
    envelope = {
        "domain": {"entity": entity, "schema_version": SCHEMA_VERSION, "hash_version": HASH_VERSION},
        "count": len(ordered),
        "items": [[key, digest] for key, digest in ordered],
    }
    return hashlib.sha256(canonical_json_bytes(envelope)).hexdigest()


def content_hash(payload: Any) -> str:
    """General-purpose digest for policy, scope and plan fingerprints."""
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def chain_hash(previous: str | None, event: Mapping[str, Any]) -> str:
    """Next link of the tamper-evident audit chain (section 6.2 caveat 3).

    Detects in-place edits of the audit table by a process that does not
    recompute the whole chain. It is not a substitute for append-only external
    storage: a privileged DBA who rewrites every row can still produce a
    consistent chain.
    """
    return hashlib.sha256(
        canonical_json_bytes({"prev": previous, "event": event})
    ).hexdigest()

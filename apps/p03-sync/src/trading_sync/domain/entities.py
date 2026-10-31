"""Normalized records, built by validating a raw source mapping.

Each ``from_payload`` is the single door between untrusted source JSON and the
rest of the service. It enforces three things beyond per-field validation:

*Every declared field must be present.* A missing ``commission`` is a
validation failure, not a zero. Defaulting an absent money field to zero
invents a number and makes the record look complete (WF3-092).

*Unknown fields are ignored, not stored.* The canonical hash covers the
declared field set for the pinned mapper version, so a backward-compatible
addition upstream does not change the hash of existing rows and does not look
like an immutability conflict (WF3-009, WF3-176).

*The hash is computed here.* It travels with the record, so the sink compares
content without re-deriving it and the comparison cannot drift between the
write path and the conflict check.
"""

from __future__ import annotations

from dataclasses import dataclass, field as dataclass_field
from datetime import datetime
from decimal import Decimal
from typing import Any, Collection, Mapping

from trading_sync.domain.canonical import payload_hash
from trading_sync.domain.identifiers import normalize_identifier
from trading_sync.domain.money import parse_money, parse_positive_money
from trading_sync.domain.snapshots import Side, parse_side
from trading_sync.domain.timestamps import parse_utc_timestamp
from trading_sync.errors import InvalidIdentifierError, InvalidMoneyError, SchemaError

__all__ = ["AccountRecord", "DealRecord", "PositionRecord", "MAX_SYMBOL_LENGTH"]

MAX_SYMBOL_LENGTH = 32
_MAX_ENTRY_KIND_LENGTH = 16


def _require(payload: Mapping[str, Any], key: str) -> Any:
    """Fetch a declared field, failing if it is absent.

    ``None`` is returned as-is so that nullable fields can distinguish an
    explicit null from an omission.
    """
    if key not in payload:
        raise SchemaError("required field is missing", field=key)
    return payload[key]


def _parse_symbol(raw: object) -> str:
    return normalize_identifier(raw, field="symbol", max_length=MAX_SYMBOL_LENGTH)


def _parse_currency(raw: object, allowed: Collection[str] | None) -> str:
    """Validate a currency code and, when configured, check it against a list.

    The format check alone cannot catch a payload that swaps ``USD`` for
    ``JPY`` while keeping the digits, which would restate a balance by two
    orders of magnitude. That needs the allowlist, so a source configured with
    one rejects any code outside it rather than converting silently (WF3-187).
    """
    if isinstance(raw, bool) or not isinstance(raw, str):
        raise SchemaError(
            "currency must be a string", field="currency", actual_type=type(raw).__name__
        )
    if len(raw) != 3 or not raw.isascii() or not raw.isalpha() or raw != raw.upper():
        raise SchemaError(
            "currency is not a three-letter uppercase code", field="currency"
        )
    if allowed is not None and raw not in allowed:
        raise SchemaError(
            "currency is outside the configured allowlist",
            field="currency",
            currency=raw,
        )
    return raw


def _parse_entry_kind(raw: object) -> str:
    """Validate the deal entry kind without pinning an enum.

    The simulator's vocabulary is not frozen in this design, so the shape is
    checked and the value preserved rather than mapped onto a guessed enum.
    """
    if isinstance(raw, bool) or not isinstance(raw, str):
        raise SchemaError(
            "entry kind must be a string",
            field="entry_kind",
            actual_type=type(raw).__name__,
        )
    if not raw or len(raw) > _MAX_ENTRY_KIND_LENGTH:
        raise SchemaError("entry kind has an unusable length", field="entry_kind")
    if not all(char.isascii() and (char.isalpha() or char == "_") for char in raw):
        raise SchemaError("entry kind has unexpected characters", field="entry_kind")
    if raw != raw.upper():
        raise SchemaError("entry kind must be uppercase", field="entry_kind")
    return raw


def _parse_source_seq(raw: object) -> int:
    """Parse ``source_seq`` as a positive integer.

    Rejects ``bool`` and any non-``int``: a float sequence number would mean
    the value had already lost precision, and ``True`` would read as 1.
    """
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise SchemaError(
            "source_seq must be an integer",
            field="source_seq",
            actual_type=type(raw).__name__,
        )
    if raw <= 0:
        raise SchemaError("source_seq must be positive", field="source_seq", value=raw)
    return raw


@dataclass(frozen=True, slots=True)
class AccountRecord:
    """A current-state account as of one snapshot.

    Balance and credit are stored exactly as the source reported them. This
    service never recomputes a balance from deals: the source is the authority
    on its own money, and a derived balance would be a second, conflicting
    answer (WF3-220, section 10.12).
    """

    account_id: str
    currency: str
    balance: Decimal
    credit: Decimal
    payload_hash: str = dataclass_field(compare=False)

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any],
        *,
        allowed_currencies: Collection[str] | None = None,
    ) -> AccountRecord:
        account_id = normalize_identifier(_require(payload, "account_id"), field="account_id")
        currency = _parse_currency(_require(payload, "currency"), allowed_currencies)
        balance = parse_money(_require(payload, "balance"), field="balance")
        credit = parse_money(_require(payload, "credit"), field="credit")
        digest = payload_hash(
            "account",
            {
                "account_id": account_id,
                "currency": currency,
                "balance": balance,
                "credit": credit,
            },
        )
        return cls(
            account_id=account_id,
            currency=currency,
            balance=balance,
            credit=credit,
            payload_hash=digest,
        )

    @property
    def key(self) -> str:
        return self.account_id


@dataclass(frozen=True, slots=True)
class PositionRecord:
    """A current-state position as of one snapshot.

    ``source_status`` is whatever the source called it, kept verbatim. It is
    deliberately not an enum and deliberately not derived from presence: the
    difference between "we did not see it" and "the source says it is closed"
    is the whole point of INV-08.
    """

    position_id: str
    account_id: str
    symbol: str
    side: Side
    volume_lots: Decimal
    open_price: Decimal
    source_status: str
    payload_hash: str = dataclass_field(compare=False)

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> PositionRecord:
        position_id = normalize_identifier(
            _require(payload, "position_id"), field="position_id"
        )
        account_id = normalize_identifier(
            _require(payload, "account_id"), field="account_id"
        )
        symbol = _parse_symbol(_require(payload, "symbol"))
        side = parse_side(_require(payload, "side"))
        # Non-negative, not strictly positive. This class keeps
        # ``source_status`` verbatim precisely so a source may report a
        # position that is no longer open, and a closed position is flat: its
        # volume is zero. Requiring a positive volume contradicted that, and
        # the two conditions could not both hold - a positions scan failed
        # INVALID_MONEY on the first closed position the simulator returned.
        #
        # Zero is allowed; negative is not. A negative lot count is not a
        # short position, it is a corrupt one: direction lives in ``side``.
        volume_lots = parse_money(_require(payload, "volume_lots"), field="volume_lots")
        if volume_lots < 0:
            raise InvalidMoneyError(
                "position volume cannot be negative; direction is carried by side",
                field="volume_lots",
            )
        # Still strictly positive: a position opened at zero has no price, and
        # nothing downstream could value it.
        open_price = parse_positive_money(
            _require(payload, "open_price"), field="open_price"
        )
        source_status = _parse_source_status(_require(payload, "source_status"))
        digest = payload_hash(
            "position",
            {
                "position_id": position_id,
                "account_id": account_id,
                "symbol": symbol,
                "side": str(side),
                "volume_lots": volume_lots,
                "open_price": open_price,
                "source_status": source_status,
            },
        )
        return cls(
            position_id=position_id,
            account_id=account_id,
            symbol=symbol,
            side=side,
            volume_lots=volume_lots,
            open_price=open_price,
            source_status=source_status,
            payload_hash=digest,
        )

    @property
    def key(self) -> str:
        return self.position_id


def _parse_source_status(raw: object) -> str:
    if isinstance(raw, bool) or not isinstance(raw, str):
        raise SchemaError(
            "source_status must be a string",
            field="source_status",
            actual_type=type(raw).__name__,
        )
    try:
        return normalize_identifier(raw, field="source_status", max_length=32)
    except InvalidIdentifierError as exc:
        raise SchemaError("source_status is unusable", **exc.safe_details) from None


@dataclass(frozen=True, slots=True)
class DealRecord:
    """An immutable executed deal.

    ``position_id`` is nullable and carries no foreign key. A historical deal
    may reference a position that has long since left the current open set, and
    failing the insert for that would discard real history to satisfy a
    constraint the source never promised (ADR-014, WF3-077).

    ``executed_at`` is business time and ``source_seq`` is event order; they can
    disagree. A correction posted today for last week's fill has a new sequence
    and an old timestamp, and incremental sync follows the sequence (WF3-035,
    fixture G05).
    """

    deal_id: str
    source_seq: int
    account_id: str
    position_id: str | None
    symbol: str
    entry_kind: str
    side: Side
    volume_lots: Decimal
    price: Decimal
    realized_pnl_gross: Decimal
    commission: Decimal
    swap: Decimal
    executed_at: datetime
    payload_hash: str = dataclass_field(compare=False)

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> DealRecord:
        deal_id = normalize_identifier(_require(payload, "deal_id"), field="deal_id")
        source_seq = _parse_source_seq(_require(payload, "source_seq"))
        account_id = normalize_identifier(
            _require(payload, "account_id"), field="account_id"
        )
        raw_position = _require(payload, "position_id")
        position_id = (
            None
            if raw_position is None
            else normalize_identifier(raw_position, field="position_id")
        )
        symbol = _parse_symbol(_require(payload, "symbol"))
        entry_kind = _parse_entry_kind(_require(payload, "entry_kind"))
        side = parse_side(_require(payload, "side"))
        volume_lots = parse_positive_money(
            _require(payload, "volume_lots"), field="volume_lots"
        )
        price = parse_positive_money(_require(payload, "price"), field="price")
        realized_pnl_gross = parse_money(
            _require(payload, "realized_pnl_gross"), field="realized_pnl_gross"
        )
        commission = parse_money(_require(payload, "commission"), field="commission")
        swap = parse_money(_require(payload, "swap"), field="swap")
        executed_at = parse_utc_timestamp(
            _require(payload, "executed_at"), field="executed_at"
        )
        digest = payload_hash(
            "deal",
            {
                "deal_id": deal_id,
                "source_seq": source_seq,
                "account_id": account_id,
                "position_id": position_id,
                "symbol": symbol,
                "entry_kind": entry_kind,
                "side": str(side),
                "volume_lots": volume_lots,
                "price": price,
                "realized_pnl_gross": realized_pnl_gross,
                "commission": commission,
                "swap": swap,
                "executed_at": executed_at,
            },
        )
        return cls(
            deal_id=deal_id,
            source_seq=source_seq,
            account_id=account_id,
            position_id=position_id,
            symbol=symbol,
            entry_kind=entry_kind,
            side=side,
            volume_lots=volume_lots,
            price=price,
            realized_pnl_gross=realized_pnl_gross,
            commission=commission,
            swap=swap,
            executed_at=executed_at,
            payload_hash=digest,
        )

    @property
    def key(self) -> str:
        return self.deal_id

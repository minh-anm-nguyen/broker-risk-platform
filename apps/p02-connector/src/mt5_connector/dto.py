"""Normalised, validated records handed to a consumer (design section 4.3).

A DTO in this module is a promise: every field has been strictly decoded, and
no value was repaired, rounded or defaulted on the way through. Construction
is the validation - there is no way to build one of these from a payload that
failed a check, which is what makes "we never emitted an invalid DTO" a
property the type system helps enforce rather than a claim in a comment
(INV-10).

Domain invariants that the *source* could satisfy syntactically but not
sensibly are checked here too: a position that is OPEN with zero volume
(WF2-L-11), a negative volume (WF2-L-04), a non-positive price. These are
rejected rather than clamped; a clamped number is a wrong number with the
error removed from view.

What is deliberately **not** here: any arithmetic. The connector does not
compute equity, P&L or exposure. Deriving a figure would mean owning its
correctness, and that belongs to Project 05.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Final

from .scalars import (
    ScalarError,
    parse_enum,
    parse_identifier,
    parse_money,
    parse_sequence,
    parse_timestamp,
)

__all__ = [
    "SIDES",
    "DEAL_ENTRIES",
    "POSITION_STATUSES",
    "AccountDTO",
    "PositionDTO",
    "DealDTO",
    "build_account",
    "build_position",
    "build_deal",
]

SIDES: Final[frozenset[str]] = frozenset({"BUY", "SELL"})
DEAL_ENTRIES: Final[frozenset[str]] = frozenset({"IN", "OUT"})
POSITION_STATUSES: Final[frozenset[str]] = frozenset({"OPEN", "CLOSED"})

#: Currency codes are checked for *shape* only. This build does not carry an
#: ISO 4217 table, and inventing a partial allowlist would reject a broker's
#: legitimate currency while still not proving the ones it accepts are real.
#: The code is preserved exactly as received and never converted (WF2-L-09).
_CURRENCY_LENGTH: Final[int] = 3


@dataclass(frozen=True, slots=True)
class AccountDTO:
    account_id: str
    currency: str
    balance: Decimal
    credit: Decimal


@dataclass(frozen=True, slots=True)
class PositionDTO:
    position_id: str
    account_id: str
    symbol: str
    side: str
    volume_lots: Decimal
    open_price: Decimal
    status: str
    #: Present only when the source supplied a mark inside the snapshot.
    #: ``None`` means "not known at this snapshot", which is not zero: a
    #: missing quote reported as 0.00 is a confidently wrong number.
    unrealized_pnl: Decimal | None = None
    mark_market_time: datetime | None = None
    mark_stale: bool | None = None


@dataclass(frozen=True, slots=True)
class DealDTO:
    deal_id: str
    account_id: str
    position_id: str
    symbol: str
    entry: str
    side: str
    volume_lots: Decimal
    price: Decimal
    realized_pnl_gross: Decimal
    commission: Decimal
    swap: Decimal
    executed_at: datetime
    #: Simulator semantics. ``None`` for any source that does not declare the
    #: ``incremental_deals_after_seq`` capability - the connector never
    #: fabricates one, because a synthetic sequence would make a gap-free
    #: claim that nothing verified (design section 15.5, WF2-R-07).
    source_seq: int | None = None

    @property
    def dedup_key(self) -> str:
        """The key duplicate detection uses inside one scan.

        ``deal_id`` alone. Including the amounts would turn "the same deal
        reported twice with different money" into two distinct records, which
        is exactly the conflict that must surface (WF2-K-09).
        """
        return self.deal_id


def _require(payload: dict[str, Any], field: str) -> Any:
    if field not in payload:
        raise ScalarError(field, "is required but absent")
    return payload[field]


def _optional(payload: dict[str, Any], field: str) -> Any:
    return payload.get(field)


def _parse_currency(value: Any) -> str:
    code = parse_identifier(value, field="currency", max_length=8)
    if len(code) != _CURRENCY_LENGTH or not code.isalpha() or not code.isupper():
        raise ScalarError(
            "currency", "must be a three-letter uppercase currency code", observed_type="str"
        )
    return code


def build_account(payload: Any) -> AccountDTO:
    """Build an :class:`AccountDTO` or raise :class:`ScalarError`."""
    if not isinstance(payload, dict):
        raise ScalarError("account", "must be a JSON object", observed_type=type(payload).__name__)
    return AccountDTO(
        account_id=parse_identifier(_require(payload, "account_id"), field="account_id"),
        currency=_parse_currency(_require(payload, "currency")),
        balance=parse_money(_require(payload, "balance"), field="balance"),
        credit=parse_money(_require(payload, "credit"), field="credit"),
    )


def build_position(payload: Any) -> PositionDTO:
    """Build a :class:`PositionDTO` or raise :class:`ScalarError`."""
    if not isinstance(payload, dict):
        raise ScalarError("position", "must be a JSON object", observed_type=type(payload).__name__)
    status = parse_enum(_require(payload, "status"), field="status", allowed=POSITION_STATUSES)
    volume = parse_money(_require(payload, "volume_lots"), field="volume_lots")
    open_price = parse_money(_require(payload, "open_price"), field="open_price")

    if volume < 0:
        # Negative volume is not a direction - ``side`` carries the direction.
        raise ScalarError("volume_lots", "must not be negative")
    if status == "OPEN" and volume == 0:
        # An open position holding nothing is a contradiction. Passing it on
        # would give Project 05 a position to value with no size (WF2-L-11).
        raise ScalarError("volume_lots", "must be greater than zero while status is OPEN")
    if open_price <= 0:
        raise ScalarError("open_price", "must be greater than zero")

    mark_time_raw = _optional(payload, "mark_market_time")
    mark_stale_raw = _optional(payload, "mark_stale")
    if mark_stale_raw is not None and not isinstance(mark_stale_raw, bool):
        raise ScalarError(
            "mark_stale", "must be a boolean", observed_type=type(mark_stale_raw).__name__
        )
    unrealized_raw = _optional(payload, "unrealized_pnl")

    return PositionDTO(
        position_id=parse_identifier(_require(payload, "position_id"), field="position_id"),
        account_id=parse_identifier(_require(payload, "account_id"), field="account_id"),
        symbol=parse_identifier(_require(payload, "symbol"), field="symbol", max_length=32),
        side=parse_enum(_require(payload, "side"), field="side", allowed=SIDES),
        volume_lots=volume,
        open_price=open_price,
        status=status,
        unrealized_pnl=(
            None if unrealized_raw is None else parse_money(unrealized_raw, field="unrealized_pnl")
        ),
        mark_market_time=(
            None if mark_time_raw is None else parse_timestamp(mark_time_raw, field="mark_market_time")
        ),
        mark_stale=mark_stale_raw,
    )


def build_deal(payload: Any, *, expect_source_seq: bool) -> DealDTO:
    """Build a :class:`DealDTO` or raise :class:`ScalarError`.

    ``expect_source_seq`` comes from the negotiated capability set, not from
    whether the field happens to be present. A source that declared
    ``incremental_deals_after_seq`` and then omitted ``source_seq`` has broken
    its contract, and defaulting the field to ``None`` would hide that while
    quietly disabling incremental sync (WF2-A-09).
    """
    if not isinstance(payload, dict):
        raise ScalarError("deal", "must be a JSON object", observed_type=type(payload).__name__)

    volume = parse_money(_require(payload, "volume_lots"), field="volume_lots")
    price = parse_money(_require(payload, "price"), field="price")
    if volume <= 0:
        raise ScalarError("volume_lots", "must be greater than zero")
    if price <= 0:
        raise ScalarError("price", "must be greater than zero")

    if expect_source_seq:
        source_seq = parse_sequence(_require(payload, "source_seq"), field="source_seq", minimum=1)
    else:
        if "source_seq" in payload:
            # Present without having been declared: the source may support it,
            # but this build negotiated without it and will not start relying
            # on an undeclared field mid-scan.
            raise ScalarError(
                "source_seq",
                "was supplied although the source did not declare the "
                "incremental_deals_after_seq capability",
            )
        source_seq = None

    return DealDTO(
        deal_id=parse_identifier(_require(payload, "deal_id"), field="deal_id"),
        account_id=parse_identifier(_require(payload, "account_id"), field="account_id"),
        position_id=parse_identifier(_require(payload, "position_id"), field="position_id"),
        symbol=parse_identifier(_require(payload, "symbol"), field="symbol", max_length=32),
        entry=parse_enum(_require(payload, "entry"), field="entry", allowed=DEAL_ENTRIES),
        side=parse_enum(_require(payload, "side"), field="side", allowed=SIDES),
        volume_lots=volume,
        price=price,
        # Signed fees and a signed gross P&L are preserved exactly. Flipping a
        # sign to make a figure "look right" is the single most dangerous
        # repair available here (WF2-L-03).
        realized_pnl_gross=parse_money(
            _require(payload, "realized_pnl_gross"), field="realized_pnl_gross"
        ),
        commission=parse_money(_require(payload, "commission"), field="commission"),
        swap=parse_money(_require(payload, "swap"), field="swap"),
        executed_at=parse_timestamp(_require(payload, "executed_at"), field="executed_at"),
        source_seq=source_seq,
    )

"""The pure event state machine.

``apply_intent`` is the single place where a business rule is enforced. It is
deliberately free of SQLite, FastAPI and asyncio so that exactly the same code
runs in three places:

1. inside the writer's SQLite transaction (production path),
2. against an in-memory reference model in the property-based tests, and
3. in the offline invariant/replay tooling.

A second implementation of these rules anywhere else would be a second source
of truth, and the two would drift.

Validation order matters: an intent is fully validated *before* any identifier
is allocated. A rejected intent must therefore consume no deal id, no position
id and no sequence number, which is what keeps WF-015/WF-016 ("seq không nhảy")
and the seed-determinism guarantee compatible with each other.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Protocol

from . import accounting as acc
from .models import (
    Account,
    AdjustAccount,
    ClosePosition,
    ConflictError,
    CreateAccount,
    Deal,
    DealEntry,
    EventType,
    IncreasePosition,
    Intent,
    Mark,
    NotFoundError,
    OpenPosition,
    Position,
    PositionStatus,
    Side,
    UnsupportedOperationError,
    UpdateMark,
    ValidationError,
    deal_id as make_deal_id,
    position_id as make_position_id,
    symbol_spec,
)

ACCOUNT_CURRENCY = "USD"


# ---------------------------------------------------------------------------
# Additional contract error codes
#
# Section 5.3 lists representative codes; these extend it for domain-specific
# rejections. They are documented in the OpenAPI error enum so a connector can
# branch on them instead of parsing messages.
# ---------------------------------------------------------------------------


class AccountAlreadyExistsError(ConflictError):
    code = "ACCOUNT_ALREADY_EXISTS"


class PositionAlreadyOpenError(ConflictError):
    code = "POSITION_ALREADY_OPEN"


class CloseVolumeExceedsPositionError(ValidationError):
    code = "CLOSE_VOLUME_EXCEEDS_POSITION"


class OpenPositionLimitReachedError(ValidationError):
    code = "OPEN_POSITION_LIMIT_REACHED"


class DealLimitReachedError(ValidationError):
    code = "DEAL_LIMIT_REACHED"


# ---------------------------------------------------------------------------
# Ports
# ---------------------------------------------------------------------------


class StateView(Protocol):
    """The read/allocate surface the engine needs.

    Implemented by the writer against an open transaction (so a rollback
    discards every allocation) and by the test reference model in memory.
    """

    def get_account(self, account_id: str) -> Account | None: ...

    def get_open_position(self, account_id: str, symbol: str) -> Position | None: ...

    def get_mark(self, symbol: str) -> Mark | None: ...

    def open_position_count(self) -> int: ...

    def deal_count(self) -> int: ...

    def allocate_position_ordinal(self) -> int: ...

    def allocate_deal_ordinal(self) -> int: ...


@dataclass(frozen=True, slots=True)
class EngineLimits:
    """Run-scoped bounds. ``0`` means "no additional limit beyond settings"."""

    max_open_positions: int = 0
    max_deals: int = 0


# ---------------------------------------------------------------------------
# Mutations the caller must persist atomically with the event
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class AccountMutation:
    account: Account
    #: True when this event creates the account.
    #:
    #: The engine already knows this, so it says so. Letting the writer
    #: rediscover it with an ``INSERT OR IGNORE`` costs one statement on every
    #: trading event, and a 100k-deal run has ~140k of those.
    is_new: bool = False


@dataclass(frozen=True, slots=True)
class PositionMutation:
    position: Position
    is_new: bool = False
    #: Status before this event, so the writer can maintain the open-position
    #: counter without re-reading the row it is about to replace.
    previous_status: PositionStatus | None = None


@dataclass(frozen=True, slots=True)
class MarkMutation:
    mark: Mark


Mutation = AccountMutation | PositionMutation | MarkMutation


@dataclass(frozen=True, slots=True)
class Applied:
    """Result of applying one intent.

    The caller must persist ``payload`` as the event body, apply every
    ``mutation`` as a new projection version and insert ``deal`` if present -
    all in one transaction. Partially applying this result would create exactly
    the half-written state INV-01/INV-03 exist to detect.
    """

    event_type: EventType
    payload: dict[str, Any]
    mutations: tuple[Mutation, ...]
    deal: Deal | None = None


def event_id_for(seq: int) -> str:
    """Deterministic event identifier.

    Derived from ``seq`` rather than a UUID so a replay of the same seed
    produces identical event ids and the canonical digest stays comparable.
    """
    return f"E-{seq:012d}"


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def apply_intent(
    view: StateView,
    intent: Intent,
    *,
    seq: int,
    virtual_time: datetime,
    limits: EngineLimits | None = None,
) -> Applied:
    """Validate ``intent`` against ``view`` and return what to persist.

    Raises a :class:`~simulator.domain.models.SimulatorError` subclass on
    rejection, leaving ``view`` logically unchanged.
    """
    bounds = limits or EngineLimits()
    if isinstance(intent, CreateAccount):
        return _apply_create_account(view, intent)
    if isinstance(intent, AdjustAccount):
        return _apply_adjust_account(view, intent)
    if isinstance(intent, UpdateMark):
        return _apply_update_mark(view, intent, virtual_time)
    if isinstance(intent, OpenPosition):
        return _apply_open_position(view, intent, seq, virtual_time, bounds)
    if isinstance(intent, IncreasePosition):
        return _apply_increase_position(view, intent, seq, virtual_time, bounds)
    if isinstance(intent, ClosePosition):
        return _apply_close_position(view, intent, seq, virtual_time, bounds)
    raise ValidationError(  # pragma: no cover - guarded by typing and schemas
        f"Unsupported intent type {type(intent).__name__}",
        details={"intent": type(intent).__name__},
    )


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------


def _require_account(view: StateView, account_id: str) -> Account:
    account = view.get_account(account_id)
    if account is None:
        # WF-013: an operation referencing an unknown account is rejected
        # whole, so no orphan deal or position can be created.
        raise NotFoundError(
            f"Account {account_id!r} does not exist",
            details={"account_id": account_id},
        )
    return account


def _require_account_id(account_id: str) -> str:
    if not account_id or not account_id.strip():
        raise ValidationError("account_id must not be empty", details={"field": "account_id"})
    if len(account_id) > 64:
        raise ValidationError("account_id is too long", details={"field": "account_id", "max": 64})
    return account_id


def _check_deal_budget(view: StateView, limits: EngineLimits) -> None:
    if limits.max_deals and view.deal_count() >= limits.max_deals:
        raise DealLimitReachedError(
            "run has reached its configured deal budget",
            details={"max_deals": limits.max_deals},
        )


def _apply_create_account(view: StateView, intent: CreateAccount) -> Applied:
    _require_account_id(intent.account_id)
    if intent.currency != ACCOUNT_CURRENCY:
        # Multi-currency accounting is WF-031 (P2). Accepting the field and
        # then applying USD rules would produce a confidently wrong balance.
        raise UnsupportedOperationError(
            f"Only {ACCOUNT_CURRENCY} accounts are supported in this version",
            details={"currency": intent.currency, "supported": [ACCOUNT_CURRENCY]},
        )
    balance = acc.validate_money(intent.balance, field="balance", allow_negative=False)
    credit = acc.validate_money(intent.credit, field="credit", allow_negative=False)

    if view.get_account(intent.account_id) is not None:
        raise AccountAlreadyExistsError(
            f"Account {intent.account_id!r} already exists",
            details={"account_id": intent.account_id},
        )

    account = Account(
        account_id=intent.account_id,
        currency=ACCOUNT_CURRENCY,
        balance=acc.quantize_money(balance, field="balance"),
        credit=acc.quantize_money(credit, field="credit"),
    )
    return Applied(
        event_type=EventType.ACCOUNT_CREATED,
        payload={
            "account_id": account.account_id,
            "currency": account.currency,
            "balance": account.balance,
            "credit": account.credit,
        },
        mutations=(AccountMutation(account, is_new=True),),
    )


def _apply_adjust_account(view: StateView, intent: AdjustAccount) -> Applied:
    account = _require_account(view, _require_account_id(intent.account_id))
    amount = acc.validate_money(intent.amount, field="amount")
    reason = intent.reason.strip()
    if not reason:
        # An unexplained balance adjustment is indistinguishable from
        # corruption during an audit, so the reason is mandatory.
        raise ValidationError("reason is required for a balance adjustment", details={"field": "reason"})
    if len(reason) > 256:
        raise ValidationError("reason is too long", details={"field": "reason", "max": 256})

    updated = Account(
        account_id=account.account_id,
        currency=account.currency,
        balance=acc.quantize_money(account.balance + amount, field="balance"),
        credit=account.credit,
    )
    return Applied(
        event_type=EventType.ACCOUNT_ADJUSTED,
        payload={
            "account_id": account.account_id,
            "amount": amount,
            "reason": reason,
            "balance_after": updated.balance,
        },
        mutations=(AccountMutation(updated),),
    )


def _apply_update_mark(view: StateView, intent: UpdateMark, virtual_time: datetime) -> Applied:
    spec = symbol_spec(intent.symbol)
    bid, ask = acc.validate_quote(intent.bid, intent.ask, spec)
    mark = Mark(
        symbol=spec.symbol,
        bid=acc.quantize_price(bid, spec, field="bid"),
        ask=acc.quantize_price(ask, spec, field="ask"),
        market_time=virtual_time,
    )
    # WF-024: a price tick never touches a balance and never creates a deal,
    # even when no position references the symbol.
    return Applied(
        event_type=EventType.MARK_PRICE_UPDATED,
        payload={
            "symbol": mark.symbol,
            "bid": mark.bid,
            "ask": mark.ask,
            "market_time": mark.market_time,
        },
        mutations=(MarkMutation(mark),),
    )


def _apply_open_position(
    view: StateView,
    intent: OpenPosition,
    seq: int,
    virtual_time: datetime,
    limits: EngineLimits,
) -> Applied:
    account = _require_account(view, _require_account_id(intent.account_id))
    spec = symbol_spec(intent.symbol)
    volume = acc.validate_volume(intent.volume_lots, spec)
    price = acc.validate_price(intent.price, spec)
    commission = acc.validate_money(intent.commission, field="commission")
    swap = acc.validate_money(intent.swap, field="swap")

    if view.get_open_position(intent.account_id, spec.symbol) is not None:
        # MVP netting: one open position per (account, symbol). The caller must
        # say explicitly whether it meant to increase or to close.
        raise PositionAlreadyOpenError(
            f"An open position already exists for {intent.account_id}/{spec.symbol}",
            details={"account_id": intent.account_id, "symbol": spec.symbol},
        )
    if limits.max_open_positions and view.open_position_count() >= limits.max_open_positions:
        raise OpenPositionLimitReachedError(
            "run has reached its configured open-position limit",
            details={"max_open_positions": limits.max_open_positions},
        )
    _check_deal_budget(view, limits)

    position = Position(
        position_id=make_position_id(view.allocate_position_ordinal()),
        account_id=account.account_id,
        symbol=spec.symbol,
        side=intent.side,
        volume_lots=volume,
        open_price=price,
        status=PositionStatus.OPEN,
    )
    deal = Deal(
        deal_id=make_deal_id(view.allocate_deal_ordinal()),
        source_seq=seq,
        position_id=position.position_id,
        account_id=account.account_id,
        symbol=spec.symbol,
        entry=DealEntry.IN,
        side=intent.side,
        volume_lots=volume,
        price=price,
        # Opening realizes nothing; only fees move the balance.
        realized_pnl_gross=acc.quantize_money(Decimal(0), field="realized_pnl_gross"),
        commission=commission,
        swap=swap,
        executed_at=virtual_time,
    )
    updated_account = _book_deal(account, deal)
    return Applied(
        event_type=EventType.POSITION_OPENED,
        payload=_position_event_payload(position, deal, updated_account),
        mutations=(
            PositionMutation(position, is_new=True, previous_status=None),
            AccountMutation(updated_account),
        ),
        deal=deal,
    )


def _apply_increase_position(
    view: StateView,
    intent: IncreasePosition,
    seq: int,
    virtual_time: datetime,
    limits: EngineLimits,
) -> Applied:
    account = _require_account(view, _require_account_id(intent.account_id))
    spec = symbol_spec(intent.symbol)
    added_volume = acc.validate_volume(intent.volume_lots, spec)
    price = acc.validate_price(intent.price, spec)
    commission = acc.validate_money(intent.commission, field="commission")
    swap = acc.validate_money(intent.swap, field="swap")

    position = view.get_open_position(intent.account_id, spec.symbol)
    if position is None:
        raise NotFoundError(
            f"No open position for {intent.account_id}/{spec.symbol} to increase",
            details={"account_id": intent.account_id, "symbol": spec.symbol},
        )
    if intent.side is not position.side:
        # WF-028: reversal and hedging are explicitly out of scope. Netting an
        # opposite-side increase would silently change the position's meaning.
        raise UnsupportedOperationError(
            "Opposite-side increase (reversal/hedging) is not supported in this version",
            details={
                "position_side": position.side.value,
                "requested_side": intent.side.value,
                "hint": "close the position first, then open the other direction",
            },
        )
    _check_deal_budget(view, limits)

    new_volume = position.volume_lots + added_volume
    acc.validate_volume(new_volume, spec, field="volume_lots_after_increase")
    new_open_price = acc.weighted_average_price(
        position.open_price, position.volume_lots, price, added_volume, spec
    )

    updated_position = Position(
        position_id=position.position_id,
        account_id=position.account_id,
        symbol=position.symbol,
        side=position.side,
        volume_lots=new_volume,
        open_price=new_open_price,
        status=PositionStatus.OPEN,
    )
    deal = Deal(
        deal_id=make_deal_id(view.allocate_deal_ordinal()),
        source_seq=seq,
        position_id=position.position_id,
        account_id=account.account_id,
        symbol=spec.symbol,
        entry=DealEntry.IN,
        side=position.side,
        volume_lots=added_volume,
        price=price,
        realized_pnl_gross=acc.quantize_money(Decimal(0), field="realized_pnl_gross"),
        commission=commission,
        swap=swap,
        executed_at=virtual_time,
    )
    updated_account = _book_deal(account, deal)
    payload = _position_event_payload(updated_position, deal, updated_account)
    payload["previous_open_price"] = position.open_price
    payload["previous_volume_lots"] = position.volume_lots
    return Applied(
        event_type=EventType.POSITION_INCREASED,
        payload=payload,
        mutations=(
            PositionMutation(updated_position, previous_status=position.status),
            AccountMutation(updated_account),
        ),
        deal=deal,
    )


def _apply_close_position(
    view: StateView,
    intent: ClosePosition,
    seq: int,
    virtual_time: datetime,
    limits: EngineLimits,
) -> Applied:
    account = _require_account(view, _require_account_id(intent.account_id))
    spec = symbol_spec(intent.symbol)
    close_volume = acc.validate_volume(intent.volume_lots, spec)
    price = acc.validate_price(intent.price, spec)
    commission = acc.validate_money(intent.commission, field="commission")
    swap = acc.validate_money(intent.swap, field="swap")

    position = view.get_open_position(intent.account_id, spec.symbol)
    if position is None:
        # WF-015: closing a non-existent or already closed position is a
        # domain rejection, not a no-op success.
        raise NotFoundError(
            f"No open position for {intent.account_id}/{spec.symbol} to close",
            details={"account_id": intent.account_id, "symbol": spec.symbol},
        )
    if close_volume > position.volume_lots:
        # WF-016: over-closing must change nothing at all.
        raise CloseVolumeExceedsPositionError(
            "close volume exceeds the remaining position volume",
            details={
                "requested": acc.format_decimal(close_volume),
                "remaining": acc.format_decimal(position.volume_lots),
                "position_id": position.position_id,
            },
        )
    _check_deal_budget(view, limits)

    remaining = position.volume_lots - close_volume
    gross = acc.gross_pnl(position.side, position.open_price, price, close_volume, spec)
    fully_closed = remaining == 0

    updated_position = Position(
        position_id=position.position_id,
        account_id=position.account_id,
        symbol=position.symbol,
        side=position.side,
        volume_lots=remaining,
        # The open price of the remainder is untouched by a partial close: it
        # is the cost basis, not a running average of exits.
        open_price=position.open_price,
        status=PositionStatus.CLOSED if fully_closed else PositionStatus.OPEN,
    )
    deal = Deal(
        deal_id=make_deal_id(view.allocate_deal_ordinal()),
        source_seq=seq,
        position_id=position.position_id,
        account_id=account.account_id,
        symbol=spec.symbol,
        entry=DealEntry.OUT,
        # A deal that closes a long is itself a sell, and vice versa.
        side=position.side.opposite,
        volume_lots=close_volume,
        price=price,
        realized_pnl_gross=gross,
        commission=commission,
        swap=swap,
        executed_at=virtual_time,
    )
    updated_account = _book_deal(account, deal)
    payload = _position_event_payload(updated_position, deal, updated_account)
    payload["remaining_volume_lots"] = remaining
    payload["open_price"] = position.open_price
    return Applied(
        event_type=EventType.POSITION_CLOSED if fully_closed else EventType.POSITION_PARTIALLY_CLOSED,
        payload=payload,
        mutations=(
            PositionMutation(updated_position, previous_status=position.status),
            AccountMutation(updated_account),
        ),
        deal=deal,
    )


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _book_deal(account: Account, deal: Deal) -> Account:
    """Apply a deal's balance effect. The only place a balance changes."""
    delta = acc.balance_delta(deal.realized_pnl_gross, deal.commission, deal.swap)
    return Account(
        account_id=account.account_id,
        currency=account.currency,
        # WF-026: a balance may legitimately go negative. It is reported as
        # such rather than floored at zero, which would hide the loss.
        balance=acc.quantize_money(account.balance + delta, field="balance"),
        credit=account.credit,
    )


def _position_event_payload(position: Position, deal: Deal, account: Account) -> dict[str, Any]:
    return {
        "account_id": account.account_id,
        "position_id": position.position_id,
        "symbol": position.symbol,
        "side": position.side,
        "status": position.status,
        "volume_lots": position.volume_lots,
        "open_price": position.open_price,
        "deal_id": deal.deal_id,
        "deal_entry": deal.entry,
        "deal_side": deal.side,
        "deal_volume_lots": deal.volume_lots,
        "deal_price": deal.price,
        "realized_pnl_gross": deal.realized_pnl_gross,
        "commission": deal.commission,
        "swap": deal.swap,
        "balance_after": account.balance,
    }


def describe_intent(intent: Intent) -> dict[str, Any]:
    """Canonical description of an intent, for request hashing and audit logs."""
    if isinstance(intent, CreateAccount):
        return {
            "kind": "CREATE_ACCOUNT",
            "account_id": intent.account_id,
            "currency": intent.currency,
            "balance": intent.balance,
            "credit": intent.credit,
        }
    if isinstance(intent, AdjustAccount):
        return {
            "kind": "ADJUST_ACCOUNT",
            "account_id": intent.account_id,
            "amount": intent.amount,
            "reason": intent.reason,
        }
    if isinstance(intent, UpdateMark):
        return {"kind": "UPDATE_MARK", "symbol": intent.symbol, "bid": intent.bid, "ask": intent.ask}
    if isinstance(intent, OpenPosition):
        return {
            "kind": "OPEN_POSITION",
            "account_id": intent.account_id,
            "symbol": intent.symbol,
            "side": intent.side,
            "volume_lots": intent.volume_lots,
            "price": intent.price,
            "commission": intent.commission,
            "swap": intent.swap,
        }
    if isinstance(intent, IncreasePosition):
        return {
            "kind": "INCREASE_POSITION",
            "account_id": intent.account_id,
            "symbol": intent.symbol,
            "side": intent.side,
            "volume_lots": intent.volume_lots,
            "price": intent.price,
            "commission": intent.commission,
            "swap": intent.swap,
        }
    if isinstance(intent, ClosePosition):
        return {
            "kind": "CLOSE_POSITION",
            "account_id": intent.account_id,
            "symbol": intent.symbol,
            "volume_lots": intent.volume_lots,
            "price": intent.price,
            "commission": intent.commission,
            "swap": intent.swap,
        }
    raise ValidationError(  # pragma: no cover
        f"Unsupported intent type {type(intent).__name__}"
    )


def unrealized_for(position: Position, mark: Mark | None) -> Decimal | None:
    """Mark-to-market a position, or ``None`` when no mark is available.

    Returning ``None`` rather than ``0`` is the point: a missing quote is not a
    zero P&L, and the API surfaces it as ``null`` with a staleness flag instead
    of inventing a number (section 3.4).
    """
    if mark is None or position.status is not PositionStatus.OPEN:
        return None
    spec = symbol_spec(position.symbol)
    return acc.unrealized_pnl(position.side, position.open_price, position.volume_lots, spec, mark)

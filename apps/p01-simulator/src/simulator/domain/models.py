"""Domain entities, enums, command intents and the error taxonomy.

Every value that represents money, a price or a volume is a ``Decimal``. The
only place a ``float`` is allowed to exist in this service is a latency metric.

The error ``code`` strings here are part of the public contract (SYSTEM_DESIGN
section 5.3). Downstream retry logic branches on them, so renaming one is a
breaking API change, not a refactor.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

# ---------------------------------------------------------------------------
# Error taxonomy
# ---------------------------------------------------------------------------


class SimulatorError(Exception):
    """Base class for every error this service raises deliberately.

    An unexpected exception is a bug and maps to ``INTERNAL_ERROR``; anything
    that subclasses this is a contract outcome the client can reason about.
    """

    code: str = "INTERNAL_ERROR"
    retryable: bool = False

    def __init__(self, message: str, *, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details: dict[str, Any] = details or {}

    def to_payload(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "details": self.details}


class DomainError(SimulatorError):
    """A rejected business operation. The canonical state must be unchanged."""

    code = "VALIDATION_FAILED"


class ValidationError(DomainError):
    code = "VALIDATION_FAILED"


class NotFoundError(DomainError):
    code = "NOT_FOUND"


class UnsupportedOperationError(DomainError):
    """A legal-looking request the MVP deliberately refuses (WF-028, WF-029).

    Refusing is the whole point: silently approximating an unsupported
    instrument would produce a plausible wrong number.
    """

    code = "UNSUPPORTED_OPERATION"


class ConflictError(SimulatorError):
    code = "RUN_CONFLICT"


class IdempotencyConflictError(SimulatorError):
    code = "IDEMPOTENCY_CONFLICT"


class InvalidCursorError(SimulatorError):
    code = "INVALID_CURSOR"


class SnapshotExpiredError(SimulatorError):
    code = "SNAPSHOT_EXPIRED"


class RunReplacedError(SimulatorError):
    code = "RUN_REPLACED"


class WriterOverloadedError(SimulatorError):
    code = "WRITER_OVERLOADED"
    retryable = True


class ServiceNotReadyError(SimulatorError):
    code = "SERVICE_NOT_READY"
    retryable = True


class RateLimitedError(SimulatorError):
    code = "RATE_LIMITED"
    retryable = True


class StorageError(SimulatorError):
    """Storage refused the write (disk full, read-only mount, lock timeout).

    Retryable, but the caller must not assume the write did or did not happen:
    that is what the idempotency key is for.
    """

    code = "STORAGE_ERROR"
    retryable = True


class PayloadTooLargeError(SimulatorError):
    code = "PAYLOAD_TOO_LARGE"


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"

    @property
    def opposite(self) -> "Side":
        return Side.SELL if self is Side.BUY else Side.BUY


class DealEntry(StrEnum):
    """Whether a deal increased (``IN``) or reduced (``OUT``) a position."""

    IN = "IN"
    OUT = "OUT"


class PositionStatus(StrEnum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"


class EventType(StrEnum):
    ACCOUNT_CREATED = "ACCOUNT_CREATED"
    ACCOUNT_ADJUSTED = "ACCOUNT_ADJUSTED"
    POSITION_OPENED = "POSITION_OPENED"
    POSITION_INCREASED = "POSITION_INCREASED"
    POSITION_PARTIALLY_CLOSED = "POSITION_PARTIALLY_CLOSED"
    POSITION_CLOSED = "POSITION_CLOSED"
    MARK_PRICE_UPDATED = "MARK_PRICE_UPDATED"


class RunStatus(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    FAILED = "FAILED"
    REPLACED = "REPLACED"
    # Projections disagree with the event log. The service stays up for
    # forensics but must not report itself ready (section 4.3).
    READ_ONLY_REPAIR_REQUIRED = "READ_ONLY_REPAIR_REQUIRED"

    @property
    def is_terminal(self) -> bool:
        return self in {RunStatus.STOPPED, RunStatus.FAILED, RunStatus.REPLACED}

    @property
    def accepts_writes(self) -> bool:
        return self in {RunStatus.PENDING, RunStatus.RUNNING}


class JobStatus(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

    @property
    def is_terminal(self) -> bool:
        return self in {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED}


# ---------------------------------------------------------------------------
# Symbol catalog
# ---------------------------------------------------------------------------

SYMBOL_CATALOG_VERSION = "1.0"


@dataclass(frozen=True, slots=True)
class SymbolSpec:
    symbol: str
    digits: int
    contract_size: Decimal
    lot_step: Decimal
    min_lot: Decimal
    max_lot: Decimal
    base_currency: str
    quote_currency: str

    @property
    def profit_currency(self) -> str:
        """Currency a linear P&L figure comes out in.

        For the MVP profile this is always the quote currency, which is why the
        catalog only contains USD-quoted pairs.
        """
        return self.quote_currency


def _spec(symbol: str, base: str, quote: str) -> SymbolSpec:
    return SymbolSpec(
        symbol=symbol,
        digits=5,
        contract_size=Decimal("100000"),
        lot_step=Decimal("0.01"),
        min_lot=Decimal("0.01"),
        max_lot=Decimal("100"),
        base_currency=base,
        quote_currency=quote,
    )


# MVP profile: USD accounts and USD-quoted FX pairs only, so a linear P&L
# formula is exact and no FX conversion step is needed (section 1.4).
SYMBOL_SPECS: dict[str, SymbolSpec] = {
    "EURUSD": _spec("EURUSD", "EUR", "USD"),
    "GBPUSD": _spec("GBPUSD", "GBP", "USD"),
    "AUDUSD": _spec("AUDUSD", "AUD", "USD"),
}

DEFAULT_SYMBOLS: tuple[str, ...] = ("EURUSD", "GBPUSD", "AUDUSD")

# Instruments whose profit currency is not the account currency. They are
# refused with UNSUPPORTED_OPERATION rather than computed as if they were
# USD-quoted, which is the failure mode WF-029 exists to catch.
FX_CONVERSION_SYMBOLS: frozenset[str] = frozenset(
    {"USDJPY", "USDCHF", "USDCAD", "EURGBP", "EURJPY", "GBPJPY", "AUDJPY", "EURCHF"}
)

ACCOUNT_CURRENCY = "USD"


def symbol_spec(symbol: str) -> SymbolSpec:
    """Look up a symbol, distinguishing "unknown" from "knowingly unsupported"."""
    spec = SYMBOL_SPECS.get(symbol)
    if spec is not None:
        return spec
    if symbol in FX_CONVERSION_SYMBOLS:
        raise UnsupportedOperationError(
            f"{symbol} requires FX conversion to {ACCOUNT_CURRENCY}, which the MVP does not implement",
            details={"symbol": symbol, "reason": "FX_CONVERSION_REQUIRED"},
        )
    raise ValidationError(
        f"Unknown symbol {symbol!r}",
        details={"symbol": symbol, "supported": list(SYMBOL_SPECS)},
    )


# ---------------------------------------------------------------------------
# Entities (immutable snapshots of one version)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Account:
    account_id: str
    currency: str
    balance: Decimal
    credit: Decimal


@dataclass(frozen=True, slots=True)
class Position:
    position_id: str
    account_id: str
    symbol: str
    side: Side
    volume_lots: Decimal
    open_price: Decimal
    status: PositionStatus


@dataclass(frozen=True, slots=True)
class Deal:
    """An execution record. Immutable once committed.

    ``source_seq`` is the simulator event sequence that produced this deal. It
    is a simulator-defined ordering aid for incremental sync tests and carries
    no claim about any real MT5 Manager API field.
    """

    deal_id: str
    source_seq: int
    position_id: str
    account_id: str
    symbol: str
    entry: DealEntry
    side: Side
    volume_lots: Decimal
    price: Decimal
    realized_pnl_gross: Decimal
    commission: Decimal
    swap: Decimal
    executed_at: datetime


@dataclass(frozen=True, slots=True)
class Mark:
    symbol: str
    bid: Decimal
    ask: Decimal
    market_time: datetime


# ---------------------------------------------------------------------------
# Command intents
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CreateAccount:
    account_id: str
    currency: str
    balance: Decimal
    credit: Decimal


@dataclass(frozen=True, slots=True)
class AdjustAccount:
    account_id: str
    amount: Decimal
    reason: str


@dataclass(frozen=True, slots=True)
class UpdateMark:
    symbol: str
    bid: Decimal
    ask: Decimal


@dataclass(frozen=True, slots=True)
class OpenPosition:
    account_id: str
    symbol: str
    side: Side
    volume_lots: Decimal
    price: Decimal
    commission: Decimal
    swap: Decimal


@dataclass(frozen=True, slots=True)
class IncreasePosition:
    account_id: str
    symbol: str
    side: Side
    volume_lots: Decimal
    price: Decimal
    commission: Decimal
    swap: Decimal


@dataclass(frozen=True, slots=True)
class ClosePosition:
    account_id: str
    symbol: str
    volume_lots: Decimal
    price: Decimal
    commission: Decimal
    swap: Decimal


Intent = (
    CreateAccount | AdjustAccount | UpdateMark | OpenPosition | IncreasePosition | ClosePosition
)

INTENT_KINDS: dict[str, type] = {
    "CREATE_ACCOUNT": CreateAccount,
    "ADJUST_ACCOUNT": AdjustAccount,
    "UPDATE_MARK": UpdateMark,
    "OPEN_POSITION": OpenPosition,
    "INCREASE_POSITION": IncreasePosition,
    "CLOSE_POSITION": ClosePosition,
}

INTENT_KIND_BY_TYPE: dict[type, str] = {v: k for k, v in INTENT_KINDS.items()}


# ---------------------------------------------------------------------------
# Deterministic identifiers
#
# Identifiers are a pure function of an ordinal, never of wall time or a UUID,
# so that two runs of the same seed produce byte-identical ids and the
# canonical digest comparison in WF-001 is meaningful.
# ---------------------------------------------------------------------------


def account_id(ordinal: int) -> str:
    return f"A-{ordinal:06d}"


def position_id(ordinal: int) -> str:
    return f"P-{ordinal:08d}"


def deal_id(ordinal: int) -> str:
    return f"D-{ordinal:08d}"

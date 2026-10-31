"""Unrealized P&L: mark open positions to a pinned, side-correct quote.

For the one supported mode, ``FOREX_LINEAR_P0``::

    units = lots * contract_size
    BUY   -> (bid   - open_price) * units
    SELL  -> (open_price - ask  ) * units

Three details in that formula carry most of the correctness risk.

*Side chooses the price.* A long is closed into the bid, a short out of the ask.
Swap those and every open position gains the spread instead of paying it, which
on a large book is a steady, invisible overstatement (WF5-061, WF5-062).

*The snapshot is pinned.* Every position in one calculation is valued from the
same :class:`~risk.domain.facts.QuoteSnapshot`. Re-reading a live feed per
position yields a portfolio that existed at no single instant (WF5-129).

*Freshness is a status, not a filter.* A quote older than the policy threshold
still produces a number -- marked ``STALE`` -- because the alternative that
tempts implementers is to substitute a zero (WF5-065, section 9.1).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from risk.domain.facts import PositionFact, QuoteFact, QuoteSnapshot
from risk.domain.instruments import InstrumentSpec, SymbolCatalog
from risk.domain.money import CALC_PRECISION, Money, MoneySemantic
from risk.errors import ContractError, QualityGateError, ReasonCode
from risk.status import Side

__all__ = [
    "PositionValuation",
    "UnrealizedResult",
    "compute_unrealized",
    "unrealized_for_position",
    "quote_age_seconds",
]


def quote_age_seconds(quote: QuoteFact, *, as_of: datetime) -> Decimal:
    """Age of a quote at a reference instant, in seconds.

    A negative age means the mark is dated after the valuation instant. That is
    look-ahead pricing and is refused rather than clamped to zero: used for a
    historical report it reveals the future, and used live it hides a clock
    problem (WF5-066, WF5-183).
    """
    delta = as_of - quote.market_time
    seconds = Decimal(delta.total_seconds()).quantize(Decimal("0.000001"))
    return seconds


@dataclass(frozen=True, slots=True)
class PositionValuation:
    """Mark-to-market result for a single position."""

    position_id: str
    symbol: str
    side: Side
    lots: Decimal
    units: Decimal
    open_price: Decimal
    close_price: Decimal
    unrealized: Money
    quote_id: str
    quote_age_seconds: Decimal
    stale: bool

    def as_payload(self) -> dict[str, object]:
        return {
            "position_id": self.position_id,
            "symbol": self.symbol,
            "side": str(self.side),
            "lots": f"{self.lots:f}",
            "units": f"{self.units:f}",
            "open_price": f"{self.open_price:f}",
            "close_price": f"{self.close_price:f}",
            "unrealized": self.unrealized.canonical(),
            "currency": self.unrealized.currency,
            "quote_id": self.quote_id,
            "quote_age_seconds": f"{self.quote_age_seconds:f}",
            "stale": self.stale,
        }


@dataclass(frozen=True)
class UnrealizedResult:
    """Portfolio unrealized total plus the per-position detail behind it."""

    currency: str
    total: Money
    by_position: tuple[PositionValuation, ...]
    quote_snapshot_id: str
    catalog_version: str
    stale_symbols: frozenset[str]
    max_quote_age_seconds: Decimal

    @property
    def is_stale(self) -> bool:
        return bool(self.stale_symbols)

    def as_payload(self) -> dict[str, object]:
        return {
            "currency": self.currency,
            "total": self.total.canonical(),
            "position_count": len(self.by_position),
            "quote_snapshot_id": self.quote_snapshot_id,
            "catalog_version": self.catalog_version,
            "stale_symbols": sorted(self.stale_symbols),
            "max_quote_age_seconds": f"{self.max_quote_age_seconds:f}",
            "by_position": [v.as_payload() for v in self.by_position],
        }


def unrealized_for_position(
    position: PositionFact,
    spec: InstrumentSpec,
    quote: QuoteFact,
) -> tuple[Decimal, Decimal, Decimal]:
    """Return ``(close_price, units, unrealized_amount)`` for one position.

    Raises:
        UnsupportedError: when the instrument's calc mode is not whitelisted.
        ContractError: when the quote or the position is malformed.
    """
    spec.require_supported()
    if quote.symbol != position.symbol:
        raise ContractError(
            ReasonCode.QUOTE_SYMBOL_MISMATCH,
            "quote does not belong to the position symbol",
            position_symbol=position.symbol,
            quote_symbol=quote.symbol,
        )

    close_price = quote.closing_price_for(position.side)
    units = spec.units_for(position.lots)

    from decimal import localcontext

    with localcontext() as ctx:
        ctx.prec = CALC_PRECISION
        if position.side is Side.BUY:
            amount = (close_price - position.open_price) * units
        else:
            amount = (position.open_price - close_price) * units
    return close_price, units, amount


def compute_unrealized(
    positions: tuple[PositionFact, ...],
    catalog: SymbolCatalog,
    quotes: QuoteSnapshot,
    *,
    valuation_as_of: datetime,
    max_quote_age_seconds: Decimal | None,
    reporting_currency: str,
    allow_lookahead: bool = False,
) -> UnrealizedResult:
    """Value every open position from one pinned quote snapshot.

    Args:
        positions: Validated positions from one published scan.
        catalog: Approved instrument specs.
        quotes: Pinned, immutable marks. A missing symbol blocks.
        valuation_as_of: Instant the valuation claims to describe.
        max_quote_age_seconds: Freshness threshold, or ``None`` to skip the
            check (historical valuation under its own policy).
        reporting_currency: Currency every position's profit must land in.
        allow_lookahead: Escape hatch for fixtures that deliberately exercise
            the look-ahead guard. Never set in production paths.

    Raises:
        QualityGateError: for a missing quote (``QUOTE_MISSING``) or a mark
            dated after the valuation instant (``QUOTE_LOOKAHEAD``).
        ContractError / UnsupportedError: for malformed or unsupported inputs.

    Note:
        A symbol whose profit currency differs from ``reporting_currency``
        raises ``CURRENCY_MISMATCH``. No FX leg is applied implicitly, because
        a conversion with an unapproved rate is worse than no conversion
        (WF5-097, WF5-098).
    """
    valuations: list[PositionValuation] = []
    stale_symbols: set[str] = set()
    worst_age = Decimal(0)
    total = Money.zero(reporting_currency, MoneySemantic.UNREALIZED_GROSS)
    seen: set[str] = set()

    for position in positions:
        if position.position_id in seen:
            raise ContractError(
                ReasonCode.DATA_INTEGRITY_CONFLICT,
                "duplicate position_id in one captured scan",
                position_id=position.position_id,
            )
        seen.add(position.position_id)

        position.require_known_status()
        if not position.is_open:
            continue

        spec = catalog.get(position.symbol)
        spec.require_supported()
        if spec.profit_currency != reporting_currency:
            raise ContractError(
                ReasonCode.CURRENCY_MISMATCH,
                "instrument profit currency needs an approved FX conversion",
                symbol=position.symbol,
                profit_currency=spec.profit_currency,
                reporting_currency=reporting_currency,
            )

        quote = quotes.get(position.symbol)
        age = quote_age_seconds(quote, as_of=valuation_as_of)
        if age < 0 and not allow_lookahead:
            raise QualityGateError(
                ReasonCode.QUOTE_LOOKAHEAD,
                "quote is dated after the valuation instant",
                symbol=position.symbol,
                quote_id=quote.quote_id,
            )
        if quote.feed_status != "ACTIVE":
            # A reachable feed that reports itself halted is not a fresh feed
            # merely because the HTTP call succeeded (WF5-190).
            stale_symbols.add(position.symbol)
        if max_quote_age_seconds is not None and age > max_quote_age_seconds:
            stale_symbols.add(position.symbol)
        worst_age = max(worst_age, age)

        close_price, units, amount = unrealized_for_position(position, spec, quote)
        valuation = PositionValuation(
            position_id=position.position_id,
            symbol=position.symbol,
            side=position.side,
            lots=position.lots,
            units=units,
            open_price=position.open_price,
            close_price=close_price,
            unrealized=Money(amount, spec.profit_currency, MoneySemantic.UNREALIZED_GROSS),
            quote_id=quote.quote_id,
            quote_age_seconds=age,
            stale=position.symbol in stale_symbols,
        )
        valuations.append(valuation)
        total = total + valuation.unrealized

    # Sorted so the digest is independent of reader order (WF5-254, WF5-303).
    valuations.sort(key=lambda v: (v.symbol, v.position_id))

    return UnrealizedResult(
        currency=reporting_currency,
        total=total,
        by_position=tuple(valuations),
        quote_snapshot_id=quotes.snapshot_id,
        catalog_version=catalog.catalog_version,
        stale_symbols=frozenset(stale_symbols),
        max_quote_age_seconds=worst_age,
    )


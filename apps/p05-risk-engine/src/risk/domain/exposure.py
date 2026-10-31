"""Exposure: signed, gross and net quantities per symbol, in declared units.

Two rules make this module longer than a one-line sum would be.

*Gross survives netting.* A long lot and a short lot of the same symbol net to
zero, but the broker still carries two positions and two spreads of risk.
Reporting ``gross = 0`` there is the classic way to make a hedged book look
risk-free (WF5-051, WF5-147).

*Units are not interchangeable.* 100 000 EUR and 100 000 GBP are different
amounts of risk, so they are kept in separate :class:`~risk.domain.money.Quantity`
buckets and never added. A consolidated figure requires an approved valuation
policy, which is a separate, labelled output (WF5-148).
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from risk.domain.facts import PositionFact, QuoteSnapshot
from risk.domain.instruments import InstrumentSpec, SymbolCatalog
from risk.domain.money import Money, Quantity
from risk.errors import ContractError, ReasonCode
from risk.status import Side

__all__ = [
    "SymbolExposure",
    "ExposureResult",
    "compute_exposure",
    "ValuationBasis",
]

#: Label attached to any informational notional. ``MID`` is not a price anyone
#: can trade at; it is only ever used for size reporting, never for P&L.
ValuationBasis = str


@dataclass(frozen=True, slots=True)
class SymbolExposure:
    """Exposure for one symbol under one instrument spec version."""

    symbol: str
    unit_currency: str
    long_units: Quantity
    short_units: Quantity
    position_count: int
    spec_version: str
    #: Whether this build has a verified money formula for the instrument. Unit
    #: quantities are meaningful either way -- they come from the contract size
    #: alone -- but a notional or a P&L does not, so the flag travels with the
    #: numbers rather than being inferred downstream (section 4.2).
    calc_mode_supported: bool = True
    quote_notional: Money | None = None
    valuation_basis: ValuationBasis | None = None
    quote_id: str | None = None

    @property
    def gross_units(self) -> Quantity:
        """Total size at risk, both directions added as magnitudes."""
        return self.long_units + self.short_units

    @property
    def net_units(self) -> Quantity:
        """Directional size: long minus short."""
        return self.long_units - self.short_units

    def as_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "unit_currency": self.unit_currency,
            "long_units": self.long_units.canonical(),
            "short_units": self.short_units.canonical(),
            "gross_units": self.gross_units.canonical(),
            "net_units": self.net_units.canonical(),
            "position_count": self.position_count,
            "spec_version": self.spec_version,
            "calc_mode_supported": self.calc_mode_supported,
        }
        if self.quote_notional is not None:
            payload["quote_notional"] = self.quote_notional.canonical()
            payload["quote_notional_currency"] = self.quote_notional.currency
            payload["valuation_basis"] = self.valuation_basis
            payload["quote_id"] = self.quote_id
        return payload


@dataclass(frozen=True)
class ExposureResult:
    by_symbol: dict[str, SymbolExposure]
    included_position_ids: frozenset[str]
    skipped_closed_positions: int
    catalog_version: str

    def as_payload(self) -> dict[str, object]:
        return {
            "by_symbol": {s: e.as_payload() for s, e in sorted(self.by_symbol.items())},
            "position_count": sum(e.position_count for e in self.by_symbol.values()),
            "catalog_version": self.catalog_version,
        }


def _accumulate(
    position: PositionFact,
    spec: InstrumentSpec,
    longs: dict[str, Decimal],
    shorts: dict[str, Decimal],
    counts: dict[str, int],
) -> None:
    units = spec.units_for(position.lots)
    if position.side is Side.BUY:
        longs[position.symbol] = longs.get(position.symbol, Decimal(0)) + units
    elif position.side is Side.SELL:
        # Held as a positive magnitude. The direction lives in which bucket the
        # value is in, so that gross can add and net can subtract.
        shorts[position.symbol] = shorts.get(position.symbol, Decimal(0)) + units
    else:  # pragma: no cover - PositionFact validates the enum
        raise ContractError(ReasonCode.UNSUPPORTED_POSITION_SIDE, "unknown position side")
    counts[position.symbol] = counts.get(position.symbol, 0) + 1


def compute_exposure(
    positions: tuple[PositionFact, ...],
    catalog: SymbolCatalog,
    *,
    quotes: QuoteSnapshot | None = None,
    include_quote_notional: bool = False,
    validate_volume_grid: bool = True,
) -> ExposureResult:
    """Aggregate open positions into per-symbol exposure.

    Args:
        positions: Validated position facts from one published scan.
        catalog: Approved instrument specs; a missing symbol is an error.
        quotes: Pinned marks, required only for ``include_quote_notional``.
        include_quote_notional: Add an informational MID-based notional.
        validate_volume_grid: Enforce volume step/maximum. Off only for
            diagnostic replays of historical data whose grid has since changed.

    Raises:
        ContractError: for an unknown position status, a duplicate position id
            or an off-grid volume.
        UnsupportedError: for a symbol with no catalog entry.

    Note:
        Positions are *not* silently skipped on an unrecognised status. A
        status this build cannot interpret means the open/closed question is
        unanswered, and an unanswered position must not be dropped out of a
        risk figure (WF5-053).
    """
    longs: dict[str, Decimal] = {}
    shorts: dict[str, Decimal] = {}
    counts: dict[str, int] = {}
    included: set[str] = set()
    skipped = 0
    seen: set[str] = set()

    for position in positions:
        if position.position_id in seen:
            # Two rows with one id inside a single scan is snapshot corruption;
            # counting both would double the book (WF5-057, WF5-256).
            raise ContractError(
                ReasonCode.DATA_INTEGRITY_CONFLICT,
                "duplicate position_id in one captured scan",
                position_id=position.position_id,
            )
        seen.add(position.position_id)

        position.require_known_status()
        if not position.is_open:
            skipped += 1
            continue

        spec = catalog.get(position.symbol)
        if validate_volume_grid:
            spec.validate_volume(position.lots)
        _accumulate(position, spec, longs, shorts, counts)
        included.add(position.position_id)

    by_symbol: dict[str, SymbolExposure] = {}
    for symbol in sorted(set(longs) | set(shorts)):
        spec = catalog.get(symbol)
        long_q = Quantity(longs.get(symbol, Decimal(0)), spec.base_currency)
        short_q = Quantity(shorts.get(symbol, Decimal(0)), spec.base_currency)

        notional: Money | None = None
        basis: str | None = None
        quote_id: str | None = None
        if include_quote_notional:
            # Unit quantities survive an unsupported calc mode; a money value
            # does not. Multiplying gold lots by a forex contract size would
            # produce a number with no defensible meaning (WF5-156).
            spec.require_supported()
            if quotes is None:
                raise ContractError(
                    ReasonCode.QUOTE_MISSING, "quote notional requested without a pinned snapshot"
                )
            quote = quotes.get(symbol)
            # MID, explicitly labelled: a size report, not a liquidation value.
            notional = (long_q - short_q).valued_at(quote.mid, spec.quote_currency)
            basis = "MID"
            quote_id = quote.quote_id

        by_symbol[symbol] = SymbolExposure(
            symbol=symbol,
            unit_currency=spec.base_currency,
            long_units=long_q,
            short_units=short_q,
            position_count=counts.get(symbol, 0),
            spec_version=spec.spec_version,
            calc_mode_supported=spec.supported and spec.profit_currency == spec.quote_currency,
            quote_notional=notional,
            valuation_basis=basis,
            quote_id=quote_id,
        )

    return ExposureResult(
        by_symbol=by_symbol,
        included_position_ids=frozenset(included),
        skipped_closed_positions=skipped,
        catalog_version=catalog.catalog_version,
    )

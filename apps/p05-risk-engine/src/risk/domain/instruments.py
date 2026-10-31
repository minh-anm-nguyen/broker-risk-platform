"""Versioned instrument specifications and the supported-profile whitelist.

A price alone cannot value a position. The arithmetic needs the contract size,
the unit of the quantity, the currency the profit lands in, and the formula
family that applies to the instrument. None of that is derivable from the
string ``EURUSD``: it comes from an approved, versioned catalog or the
calculation does not happen (WF5-081).

The whitelist is the other half of that discipline. ``FOREX_LINEAR_P0`` is the
only mode whose formula this build has verified against an independent oracle,
so every other mode -- including modes that *look* like simple multiplication,
such as a CFD with a multiplier -- resolves to ``UNSUPPORTED_CALC_MODE`` rather
than being pushed through the forex formula (WF5-079, WF5-080).
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, localcontext
from enum import StrEnum, unique
from typing import Final, Mapping

from risk.domain.money import CALC_PRECISION, parse_decimal, validate_currency
from risk.errors import ContractError, ReasonCode, UnsupportedError

__all__ = [
    "CalcMode",
    "SUPPORTED_CALC_MODES",
    "InstrumentSpec",
    "SymbolCatalog",
]


@unique
class CalcMode(StrEnum):
    """Formula family for an instrument.

    Names mirror the intent of MetaTrader's ``SYMBOL_TRADE_CALC_MODE`` without
    claiming parity with it: these are this project's own identifiers for
    formula families, and only the ``_P0`` member has a verified implementation.
    """

    #: profit = (close - open) * lots * contract_size, profit currency == quote
    #: currency == USD. Verified against the golden vectors of section 5.6.
    FOREX_LINEAR_P0 = "FOREX_LINEAR_P0"
    #: Same shape but needs an FX leg to reach the account currency. Declared
    #: so that catalogs can describe it; blocked until the FX provider is
    #: approved.
    FOREX_CROSS = "FOREX_CROSS"
    #: Contract-for-difference with a broker-specific multiplier.
    CFD = "CFD"
    #: Futures priced off tick value / tick size rather than contract size.
    FUTURES = "FUTURES"
    #: Exchange-style stock pricing.
    EXCHANGE_STOCKS = "EXCHANGE_STOCKS"


#: The only modes this build may compute money for.
SUPPORTED_CALC_MODES: Final[frozenset[CalcMode]] = frozenset({CalcMode.FOREX_LINEAR_P0})


@dataclass(frozen=True, slots=True)
class InstrumentSpec:
    """Immutable, approved contract metadata for one symbol at one version.

    Invalid specs are rejected at construction, not at use. A catalog row with
    ``contract_size = 0`` would otherwise turn every exposure into a silent
    zero, which is precisely the failure class this project exists to prevent
    (WF5-074).
    """

    symbol: str
    base_currency: str
    quote_currency: str
    profit_currency: str
    calc_mode: CalcMode
    contract_size: Decimal
    tick_size: Decimal
    volume_step: Decimal
    volume_max: Decimal
    spec_version: str
    approved_hash: str = ""

    def __post_init__(self) -> None:
        if not self.symbol or not isinstance(self.symbol, str):
            raise ContractError(ReasonCode.INVALID_INSTRUMENT_SPEC, "symbol must be a non-empty string")
        if len(self.symbol) > 32:
            raise ContractError(
                ReasonCode.INVALID_INSTRUMENT_SPEC, "symbol too long", length=len(self.symbol)
            )
        for field_name in ("base_currency", "quote_currency", "profit_currency"):
            validate_currency(getattr(self, field_name))
        if not isinstance(self.calc_mode, CalcMode):
            raise ContractError(
                ReasonCode.UNSUPPORTED_CALC_MODE, "calc_mode must be a known CalcMode member"
            )
        if not self.spec_version:
            raise ContractError(ReasonCode.INVALID_INSTRUMENT_SPEC, "spec_version is required")

        for field_name in ("contract_size", "tick_size", "volume_step", "volume_max"):
            value = getattr(self, field_name)
            if not isinstance(value, Decimal) or not value.is_finite():
                raise ContractError(
                    ReasonCode.INVALID_INSTRUMENT_SPEC,
                    "spec numeric must be a finite Decimal",
                    field=field_name,
                )
            if value <= 0:
                # Covers contract_size 0 (WF5-074) and non-positive tick values
                # that would otherwise reach a division (WF5-084).
                raise ContractError(
                    ReasonCode.INVALID_INSTRUMENT_SPEC,
                    "spec numeric must be strictly positive",
                    field=field_name,
                )

    @property
    def supported(self) -> bool:
        """Whether money may be computed for this instrument."""
        return self.calc_mode in SUPPORTED_CALC_MODES

    def require_supported(self) -> None:
        """Raise unless this build has a verified formula for the instrument."""
        if self.calc_mode not in SUPPORTED_CALC_MODES:
            raise UnsupportedError(
                ReasonCode.UNSUPPORTED_CALC_MODE,
                "no verified formula for this calculation mode",
                symbol=self.symbol,
                calc_mode=str(self.calc_mode),
            )
        # The P0 formula multiplies a price difference by base units and calls
        # the product profit currency. That identity only holds when the quote
        # currency *is* the profit currency; otherwise an FX leg is missing.
        if self.profit_currency != self.quote_currency:
            raise UnsupportedError(
                ReasonCode.UNSUPPORTED_CALC_MODE,
                "linear P0 formula requires profit_currency == quote_currency",
                symbol=self.symbol,
                profit_currency=self.profit_currency,
                quote_currency=self.quote_currency,
            )

    def units_for(self, lots: Decimal) -> Decimal:
        """Base-currency units represented by a lot volume."""
        if not isinstance(lots, Decimal) or not lots.is_finite():
            raise ContractError(ReasonCode.INVALID_POSITION_VOLUME, "lots must be a finite Decimal")
        with localcontext() as ctx:
            ctx.prec = CALC_PRECISION
            return lots * self.contract_size

    def validate_volume(self, lots: Decimal) -> None:
        """Check a lot volume against the step grid and the maximum.

        Off-grid volumes are rejected, never rounded. Rounding here would
        silently change a client's position size (WF5-075).
        """
        if not isinstance(lots, Decimal) or not lots.is_finite():
            raise ContractError(ReasonCode.INVALID_POSITION_VOLUME, "lots must be a finite Decimal")
        if lots <= 0:
            raise ContractError(
                ReasonCode.INVALID_POSITION_VOLUME,
                "position volume must be strictly positive; direction comes from side",
                symbol=self.symbol,
            )
        if lots > self.volume_max:
            raise ContractError(
                ReasonCode.VOLUME_LIMIT_EXCEEDED,
                "position volume above the approved maximum",
                symbol=self.symbol,
            )
        with localcontext() as ctx:
            ctx.prec = CALC_PRECISION
            remainder = lots % self.volume_step
        if remainder != 0:
            raise ContractError(
                ReasonCode.VOLUME_STEP_VIOLATION,
                "position volume is not a multiple of the approved volume step",
                symbol=self.symbol,
            )

    def validate_price_on_tick(self, price: Decimal, *, field: str) -> None:
        """Check that a price sits on the instrument's tick grid."""
        if not isinstance(price, Decimal) or not price.is_finite():
            raise ContractError(ReasonCode.INVALID_POSITION_PRICE, "price must be a finite Decimal")
        if price <= 0:
            raise ContractError(
                ReasonCode.INVALID_POSITION_PRICE,
                "price must be strictly positive",
                symbol=self.symbol,
                field=field,
            )
        with localcontext() as ctx:
            ctx.prec = CALC_PRECISION
            remainder = price % self.tick_size
        if remainder != 0:
            raise ContractError(
                ReasonCode.PRICE_OFF_TICK,
                "price is not a multiple of the instrument tick size",
                symbol=self.symbol,
                field=field,
            )


@dataclass(frozen=True)
class SymbolCatalog:
    """An immutable set of specs identified by one catalog version.

    Lookups fail loudly. An unknown symbol is ``UNSUPPORTED_SYMBOL``, never a
    default spec: a generic ``contract_size = 100000`` applied to an unknown
    instrument produces a plausible, auditable, wrong number (WF5-267).
    """

    catalog_version: str
    specs: Mapping[str, InstrumentSpec]

    def __post_init__(self) -> None:
        if not self.catalog_version:
            raise ContractError(ReasonCode.INVALID_INSTRUMENT_SPEC, "catalog_version is required")
        for symbol, spec in self.specs.items():
            if symbol != spec.symbol:
                raise ContractError(
                    ReasonCode.INVALID_INSTRUMENT_SPEC,
                    "catalog key does not match the spec symbol",
                    key=symbol,
                )
            if spec.spec_version != self.catalog_version:
                # A row carrying a different version inside one catalog means
                # two approval decisions got merged (WF5-293).
                raise ContractError(
                    ReasonCode.SPEC_VERSION_MISMATCH,
                    "spec version differs from its catalog version",
                    symbol=symbol,
                    spec_version=spec.spec_version,
                    catalog_version=self.catalog_version,
                )

    def get(self, symbol: str) -> InstrumentSpec:
        try:
            return self.specs[symbol]
        except KeyError:
            raise UnsupportedError(
                ReasonCode.UNSUPPORTED_SYMBOL,
                "symbol has no approved catalog entry",
                symbol=symbol,
                catalog_version=self.catalog_version,
            ) from None

    def __contains__(self, symbol: object) -> bool:
        return symbol in self.specs

    @property
    def symbols(self) -> tuple[str, ...]:
        return tuple(sorted(self.specs))

    @classmethod
    def from_rows(cls, catalog_version: str, rows: list[Mapping[str, object]]) -> SymbolCatalog:
        """Build a catalog from untrusted rows (YAML, SQL, fixture)."""
        specs: dict[str, InstrumentSpec] = {}
        for row in rows:
            symbol = row.get("symbol")
            if not isinstance(symbol, str) or not symbol:
                raise ContractError(ReasonCode.INVALID_INSTRUMENT_SPEC, "catalog row lacks a symbol")
            if symbol in specs:
                raise ContractError(
                    ReasonCode.DATA_INTEGRITY_CONFLICT, "duplicate catalog symbol", symbol=symbol
                )
            raw_mode = row.get("calc_mode")
            try:
                calc_mode = CalcMode(raw_mode)
            except ValueError:
                # An unmapped mode is not a parse convenience to be papered
                # over: it means the catalog describes something this build has
                # never seen (WF5-271 applied to instruments).
                raise ContractError(
                    ReasonCode.UNSUPPORTED_CALC_MODE,
                    "catalog row carries an unknown calc_mode",
                    symbol=symbol,
                ) from None
            specs[symbol] = InstrumentSpec(
                symbol=symbol,
                base_currency=str(row.get("base_currency", "")),
                quote_currency=str(row.get("quote_currency", "")),
                profit_currency=str(row.get("profit_currency", "")),
                calc_mode=calc_mode,
                contract_size=parse_decimal(row.get("contract_size"), field="contract_size"),
                tick_size=parse_decimal(row.get("tick_size"), field="tick_size"),
                volume_step=parse_decimal(row.get("volume_step"), field="volume_step"),
                volume_max=parse_decimal(row.get("volume_max"), field="volume_max"),
                spec_version=catalog_version,
                approved_hash=str(row.get("approved_hash", "")),
            )
        return cls(catalog_version=catalog_version, specs=specs)

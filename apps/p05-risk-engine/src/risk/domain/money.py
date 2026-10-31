"""Decimal ingress policy, typed money and typed quantities.

Everything monetary in this engine is a :class:`Money`: an exact
:class:`~decimal.Decimal` welded to a currency and to an aggregation semantic.
The welding is the point. Adding a Decimal holding USD gross profit to one
holding a JPY fee is always legal to Python; the same expression on
:class:`Money` raises.

Ingress rules (design section 5.1):

* Money arrives as ``str`` (or ``int``). A ``float`` is rejected rather than
  converted, because by the time a price has been through binary floating point
  the precision is already gone and no later conversion recovers it.
* ``NaN`` / ``Infinity`` are rejected before any arithmetic.
* Digit count and exponent are bounded *before* the value reaches the decimal
  arithmetic unit, so a hostile ``1E+999999999`` cannot buy CPU time.
* Rounding happens once, at the reporting boundary -- never per row. Summing
  1000 values of ``0.005`` must yield ``5.00``, not ``0.00``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, localcontext
from enum import StrEnum, unique
from typing import Final, Self

from risk.errors import ContractError, ReasonCode

__all__ = [
    "MoneySemantic",
    "Money",
    "Quantity",
    "parse_decimal",
    "parse_money",
    "quantize_display",
    "canonical_decimal_str",
    "validate_currency",
    "CALC_PRECISION",
    "MAX_MONEY_DIGITS",
    "MAX_MONEY_SCALE",
    "MAX_MONEY_EXPONENT",
    "MAX_DECIMAL_INPUT_CHARS",
    "CURRENCY_MINOR_UNITS",
]

#: Working precision for intermediate products and sums. Generous enough that a
#: 1M-row aggregation of 12-scale values never loses a digit, bounded enough
#: that a single operation cannot become a denial of service.
CALC_PRECISION: Final[int] = 50

#: Matches the ``NUMERIC(38,12)`` storage contract declared by Project 03.
MAX_MONEY_DIGITS: Final[int] = 38
MAX_MONEY_SCALE: Final[int] = 12

#: Guards the cheap pre-parse screen. ``1E+99999999`` is only 11 characters, so
#: a length check alone is not enough -- the exponent is bounded separately.
MAX_MONEY_EXPONENT: Final[int] = 38
MAX_DECIMAL_INPUT_CHARS: Final[int] = 64

#: ISO-4217 minor unit counts for the currencies this build knows about. A
#: currency absent from this table cannot be used as a reporting currency: the
#: engine will not guess that "two decimals" is right (JPY has zero).
CURRENCY_MINOR_UNITS: Final[dict[str, int]] = {
    "USD": 2,
    "EUR": 2,
    "GBP": 2,
    "AUD": 2,
    "CHF": 2,
    "CAD": 2,
    "JPY": 0,
}

_CURRENCY_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Z]{3}$")
# The exponent is allowed up to nine digits here so that a hostile value like
# 1E+99999999 reaches the magnitude guard below and is rejected with a reason
# that says what was wrong. Decimal() does not expand the digits, so letting it
# construct the value costs nothing; anything longer than nine exponent digits
# is refused by shape.
_DECIMAL_RE: Final[re.Pattern[str]] = re.compile(r"^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d{1,9})?$")


@unique
class MoneySemantic(StrEnum):
    """What a monetary quantity *means*, enforced at addition time.

    Two USD amounts are still not freely addable: a realized gross profit and a
    commission expense belong to different lines of the report, and silently
    folding one into the other is how a "net" figure stops meaning anything.
    Crossing semantics requires an explicit :meth:`Money.reinterpret`, which is
    visible in a code review.
    """

    REALIZED_GROSS = "REALIZED_GROSS"
    COMMISSION = "COMMISSION"
    SWAP = "SWAP"
    FEE = "FEE"
    NET_EXCLUDING_FEE = "NET_EXCLUDING_FEE"
    UNREALIZED_GROSS = "UNREALIZED_GROSS"
    NOTIONAL = "NOTIONAL"
    FX_RATE = "FX_RATE"


def validate_currency(raw: object) -> str:
    """Return a validated ISO-4217 alphabetic code."""
    if not isinstance(raw, str):
        raise ContractError(
            ReasonCode.INVALID_CURRENCY, "currency must be a string", got=type(raw).__name__
        )
    if not _CURRENCY_RE.match(raw):
        # Only the length is echoed, never the value: bounds log injection.
        raise ContractError(
            ReasonCode.INVALID_CURRENCY, "currency must match three upper-case letters", length=len(raw)
        )
    return raw


def parse_decimal(
    raw: object,
    *,
    field: str,
    max_scale: int = MAX_MONEY_SCALE,
    max_digits: int = MAX_MONEY_DIGITS,
    allow_int: bool = True,
) -> Decimal:
    """Parse untrusted input into an exact bounded :class:`Decimal`.

    Args:
        raw: ``str`` always accepted; ``int`` and ``Decimal`` accepted when they
            are already exact. ``float`` is refused.
        field: Field name used for the error detail, never the value itself.
        max_scale: Maximum number of fractional digits permitted.
        max_digits: Maximum total significant digits permitted.
        allow_int: Set ``False`` on wire formats where an integer would mask a
            serializer that dropped the decimal point.

    Raises:
        ContractError: with ``DECIMAL_STRING_REQUIRED``, ``INVALID_DECIMAL``,
            ``NONFINITE_MONEY``, ``DECIMAL_SCALE_EXCEEDED`` or
            ``DECIMAL_MAGNITUDE_EXCEEDED``.
    """
    if isinstance(raw, bool):  # bool is an int subclass; never a money value
        raise ContractError(ReasonCode.DECIMAL_STRING_REQUIRED, "bool is not a numeric", field=field)
    if isinstance(raw, float):
        raise ContractError(
            ReasonCode.DECIMAL_STRING_REQUIRED,
            "float transport loses precision; send a decimal string",
            field=field,
        )
    if isinstance(raw, Decimal):
        value = raw
    elif isinstance(raw, int):
        if not allow_int:
            raise ContractError(
                ReasonCode.DECIMAL_STRING_REQUIRED, "integer not accepted here", field=field
            )
        value = Decimal(raw)
    elif isinstance(raw, str):
        text = raw.strip()
        if len(text) > MAX_DECIMAL_INPUT_CHARS:
            raise ContractError(
                ReasonCode.DECIMAL_MAGNITUDE_EXCEEDED,
                "decimal literal too long",
                field=field,
                length=len(text),
            )
        # Reject NaN/Infinity/underscores by shape before Decimal() sees them,
        # so the exponent guard below is the only path that costs arithmetic.
        if not _DECIMAL_RE.match(text):
            raise ContractError(ReasonCode.INVALID_DECIMAL, "malformed decimal literal", field=field)
        try:
            value = Decimal(text)
        except InvalidOperation as exc:  # pragma: no cover - shape check precedes
            raise ContractError(ReasonCode.INVALID_DECIMAL, "undecodable decimal", field=field) from exc
    else:
        raise ContractError(
            ReasonCode.DECIMAL_STRING_REQUIRED,
            "money must arrive as str, int or Decimal",
            field=field,
            got=type(raw).__name__,
        )

    if not value.is_finite():
        raise ContractError(ReasonCode.NONFINITE_MONEY, "NaN/Infinity rejected", field=field)

    _sign, digits, exponent = value.as_tuple()
    assert isinstance(exponent, int)  # finite checked above

    # adjusted() is the base-10 exponent of the leading digit: it bounds
    # magnitude without materialising the expanded form of 1E+99999999.
    if value != 0 and abs(value.adjusted()) > MAX_MONEY_EXPONENT:
        raise ContractError(
            ReasonCode.DECIMAL_MAGNITUDE_EXCEEDED,
            "exponent outside supported range",
            field=field,
            adjusted=value.adjusted(),
        )
    if exponent < 0 and -exponent > max_scale:
        raise ContractError(
            ReasonCode.DECIMAL_SCALE_EXCEEDED,
            "more fractional digits than the storage contract allows",
            field=field,
            scale=-exponent,
            max_scale=max_scale,
        )
    if len(digits) > max_digits:
        raise ContractError(
            ReasonCode.DECIMAL_MAGNITUDE_EXCEEDED,
            "more significant digits than the storage contract allows",
            field=field,
            digits=len(digits),
            max_digits=max_digits,
        )
    return value


def canonical_decimal_str(value: Decimal) -> str:
    """Render a Decimal for hashing and for the wire.

    Canonical means: no exponent notation, and no negative zero. Two hosts that
    computed the same amount must produce the same string, or the output digest
    stops being a cross-host oracle (WF5-282, WF5-330).
    """
    if not value.is_finite():  # defence in depth; ingress should have caught it
        raise ContractError(ReasonCode.NONFINITE_MONEY, "cannot canonicalise nonfinite")
    if value == 0:
        # Preserve scale (0.00 stays 0.00) but erase the sign of negative zero.
        exponent = value.as_tuple()[2]
        assert isinstance(exponent, int)
        return f"{Decimal((0, (0,), exponent)):f}"
    return f"{value:f}"


@dataclass(frozen=True, slots=True)
class Money:
    """An exact amount bound to a currency and an aggregation semantic."""

    amount: Decimal
    currency: str
    semantic: MoneySemantic

    def __post_init__(self) -> None:
        if not isinstance(self.amount, Decimal):
            raise ContractError(
                ReasonCode.DECIMAL_STRING_REQUIRED,
                "Money.amount must be Decimal",
                got=type(self.amount).__name__,
            )
        if not self.amount.is_finite():
            raise ContractError(ReasonCode.NONFINITE_MONEY, "Money.amount must be finite")
        validate_currency(self.currency)
        if not isinstance(self.semantic, MoneySemantic):
            raise ContractError(
                ReasonCode.CONTRACT_MISSING_FIELD, "Money.semantic must be a MoneySemantic"
            )

    @classmethod
    def zero(cls, currency: str, semantic: MoneySemantic) -> Self:
        return cls(Decimal(0), currency, semantic)

    @classmethod
    def of(cls, raw: object, currency: str, semantic: MoneySemantic, *, field: str = "amount") -> Self:
        return cls(parse_decimal(raw, field=field), currency, semantic)

    def _require_compatible(self, other: Money) -> None:
        if self.currency != other.currency:
            raise ContractError(
                ReasonCode.CURRENCY_MISMATCH,
                "refusing to add different currencies without an FX proof",
                left=self.currency,
                right=other.currency,
            )
        if self.semantic is not other.semantic:
            raise ContractError(
                ReasonCode.UNIT_MISMATCH,
                "refusing to add different money semantics",
                left=str(self.semantic),
                right=str(other.semantic),
            )

    def __add__(self, other: Money) -> Money:
        self._require_compatible(other)
        with localcontext() as ctx:
            ctx.prec = CALC_PRECISION
            return Money(self.amount + other.amount, self.currency, self.semantic)

    def __sub__(self, other: Money) -> Money:
        self._require_compatible(other)
        with localcontext() as ctx:
            ctx.prec = CALC_PRECISION
            return Money(self.amount - other.amount, self.currency, self.semantic)

    def __neg__(self) -> Money:
        return Money(-self.amount, self.currency, self.semantic)

    def scaled_by(self, factor: Decimal) -> Money:
        """Multiply by a dimensionless factor, keeping currency and semantic."""
        if not isinstance(factor, Decimal):
            raise ContractError(ReasonCode.DECIMAL_STRING_REQUIRED, "factor must be Decimal")
        with localcontext() as ctx:
            ctx.prec = CALC_PRECISION
            return Money(self.amount * factor, self.currency, self.semantic)

    def reinterpret(self, semantic: MoneySemantic) -> Money:
        """Relabel the semantic deliberately (e.g. gross -> net component)."""
        return Money(self.amount, self.currency, semantic)

    def converted(self, rate: Decimal, target_currency: str) -> Money:
        """Apply an already-approved FX rate.

        The rate's provenance is not checked here; that is the FX provider's
        job. This method exists so the currency label can never fail to change
        together with the amount.
        """
        if not isinstance(rate, Decimal):
            raise ContractError(ReasonCode.DECIMAL_STRING_REQUIRED, "rate must be Decimal")
        if not rate.is_finite() or rate <= 0:
            raise ContractError(ReasonCode.FX_RATE_INVALID, "rate must be finite and positive")
        validate_currency(target_currency)
        with localcontext() as ctx:
            ctx.prec = CALC_PRECISION
            return Money(self.amount * rate, target_currency, self.semantic)

    def display(self, rounding: str) -> Decimal:
        """Quantize to the currency's minor unit at the reporting boundary."""
        scale = CURRENCY_MINOR_UNITS.get(self.currency)
        if scale is None:
            raise ContractError(
                ReasonCode.INVALID_CURRENCY,
                "no approved minor-unit scale for this currency",
                currency=self.currency,
            )
        return quantize_display(self.amount, scale, rounding=rounding)

    def canonical(self) -> str:
        return canonical_decimal_str(self.amount)

    def __str__(self) -> str:
        return f"{self.canonical()} {self.currency}"


@dataclass(frozen=True, slots=True)
class Quantity:
    """A signed instrument quantity bound to its unit of measure.

    Exposure is counted in units of a *base currency* (100000 EUR, not 100000
    "units"). Keeping the unit on the value is what makes ``eur + gbp`` raise
    instead of producing a meaningless scalar (WF5-148).
    """

    amount: Decimal
    unit: str

    def __post_init__(self) -> None:
        if not isinstance(self.amount, Decimal):
            raise ContractError(ReasonCode.DECIMAL_STRING_REQUIRED, "Quantity.amount must be Decimal")
        if not self.amount.is_finite():
            raise ContractError(ReasonCode.NONFINITE_MONEY, "Quantity.amount must be finite")
        validate_currency(self.unit)

    @classmethod
    def zero(cls, unit: str) -> Self:
        return cls(Decimal(0), unit)

    def _require_same_unit(self, other: Quantity) -> None:
        if self.unit != other.unit:
            raise ContractError(
                ReasonCode.UNIT_MISMATCH,
                "refusing to add quantities of different units",
                left=self.unit,
                right=other.unit,
            )

    def __add__(self, other: Quantity) -> Quantity:
        self._require_same_unit(other)
        with localcontext() as ctx:
            ctx.prec = CALC_PRECISION
            return Quantity(self.amount + other.amount, self.unit)

    def __sub__(self, other: Quantity) -> Quantity:
        self._require_same_unit(other)
        with localcontext() as ctx:
            ctx.prec = CALC_PRECISION
            return Quantity(self.amount - other.amount, self.unit)

    def __abs__(self) -> Quantity:
        return Quantity(abs(self.amount), self.unit)

    def __neg__(self) -> Quantity:
        return Quantity(-self.amount, self.unit)

    def valued_at(self, price: Decimal, quote_currency: str) -> Money:
        """Convert a base-currency quantity into a quote-currency notional."""
        if not isinstance(price, Decimal):
            raise ContractError(ReasonCode.DECIMAL_STRING_REQUIRED, "price must be Decimal")
        with localcontext() as ctx:
            ctx.prec = CALC_PRECISION
            return Money(self.amount * price, quote_currency, MoneySemantic.NOTIONAL)

    def canonical(self) -> str:
        return canonical_decimal_str(self.amount)

    def __str__(self) -> str:
        return f"{self.canonical()} {self.unit}"


def quantize_display(value: Decimal, scale: int, *, rounding: str) -> Decimal:
    """Round once, at the reporting boundary, under an explicit policy."""
    if not isinstance(value, Decimal):
        raise ContractError(ReasonCode.DECIMAL_STRING_REQUIRED, "value must be Decimal")
    if not value.is_finite():
        raise ContractError(ReasonCode.NONFINITE_MONEY, "cannot quantize nonfinite")
    if not 0 <= scale <= MAX_MONEY_SCALE:
        raise ContractError(ReasonCode.DECIMAL_SCALE_EXCEEDED, "display scale out of range", scale=scale)
    with localcontext() as ctx:
        ctx.prec = CALC_PRECISION
        ctx.rounding = rounding
        out = value.quantize(Decimal(1).scaleb(-scale))
    # -0.00 and 0.00 are the same amount; only one of them may reach a digest.
    return Decimal(0).scaleb(-scale) if out == 0 else out


def parse_money(
    raw: object,
    currency: str,
    semantic: MoneySemantic,
    *,
    field: str,
    max_scale: int = MAX_MONEY_SCALE,
) -> Money:
    """Ingress helper: validate a raw value and brand it with its meaning."""
    return Money(parse_decimal(raw, field=field, max_scale=max_scale), currency, semantic)

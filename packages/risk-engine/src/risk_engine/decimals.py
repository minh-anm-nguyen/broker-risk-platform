"""Decimal policy for the reference risk engine.

Rules this module enforces (design doc section 5.1):

* Money and lot sizes never travel through ``float``.
* Non-finite values (``NaN``, ``Infinity``) are rejected, never coerced to zero.
* Serialization is an explicit, plain-string projection with no exponent form.
* Negative zero is canonicalised to positive zero.
"""

from __future__ import annotations

import re
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation, localcontext
from typing import Final

from .errors import InvalidDecimalError

__all__ = [
    "DECIMAL_STRING_RE",
    "CALC_PRECISION",
    "CURRENCY_SCALE",
    "UNIT_MAX_SCALE",
    "parse_decimal",
    "format_money",
    "format_units",
    "quantize_money",
    "calc_context",
    "is_canonical_decimal_string",
]

# Wire shape: optional sign, bounded integer part, optional bounded fraction.
# Deliberately rejects exponents, leading ``+``, bare ``.5`` and ``5.``.
DECIMAL_STRING_RE: Final = re.compile(r"-?(?:0|[1-9][0-9]{0,17})(?:\.[0-9]{1,10})?\Z")

CALC_PRECISION: Final = 50
CURRENCY_SCALE: Final[dict[str, int]] = {"USD": 2, "EUR": 2, "JPY": 0}
UNIT_MAX_SCALE: Final = 8


def calc_context():
    """Return a localcontext manager with the engine's arithmetic precision."""
    ctx = localcontext()
    return ctx


def parse_decimal(value: object, *, field: str) -> Decimal:
    """Parse an input value into a finite ``Decimal``.

    ``float`` input is refused outright: accepting it would silently import
    binary rounding error into money.
    """
    if isinstance(value, bool) or isinstance(value, float):
        raise InvalidDecimalError(f"{field}: float/bool is not an accepted money input")
    if isinstance(value, Decimal):
        parsed = value
    elif isinstance(value, int):
        parsed = Decimal(value)
    elif isinstance(value, str):
        if DECIMAL_STRING_RE.fullmatch(value) is None:
            raise InvalidDecimalError(f"{field}: not a canonical decimal string")
        try:
            parsed = Decimal(value)
        except InvalidOperation as exc:  # pragma: no cover - regex already guards
            raise InvalidDecimalError(f"{field}: undecodable decimal") from exc
    else:
        raise InvalidDecimalError(f"{field}: unsupported decimal input type")
    if not parsed.is_finite():
        raise InvalidDecimalError(f"{field}: non-finite decimal rejected")
    return parsed


def quantize_money(amount: Decimal, currency: str) -> Decimal:
    """Quantize to the currency's minor unit with banker's rounding."""
    if not amount.is_finite():
        raise InvalidDecimalError("non-finite money amount")
    scale = CURRENCY_SCALE.get(currency)
    if scale is None:
        raise InvalidDecimalError(f"unknown currency scale for {currency}")
    with localcontext() as ctx:
        ctx.prec = CALC_PRECISION
        quantized = amount.quantize(Decimal(1).scaleb(-scale), rounding=ROUND_HALF_EVEN)
    return quantized + Decimal(0) if quantized == 0 else quantized


def format_money(amount: Decimal, currency: str) -> str:
    """Project a money amount to its canonical wire string."""
    quantized = quantize_money(amount, currency)
    if quantized == 0:
        # Collapse -0.00 to 0.00 so digests and goldens stay stable.
        quantized = Decimal(0).quantize(Decimal(1).scaleb(-CURRENCY_SCALE[currency]))
    text = format(quantized, "f")
    if DECIMAL_STRING_RE.fullmatch(text) is None:
        raise InvalidDecimalError("money amount outside representable wire bounds")
    return text


def format_units(units: Decimal) -> str:
    """Project a quantity to a plain string, trimming only trailing fraction zeros."""
    if not units.is_finite():
        raise InvalidDecimalError("non-finite unit quantity")
    with localcontext() as ctx:
        ctx.prec = CALC_PRECISION
        value = units.quantize(Decimal(1).scaleb(-UNIT_MAX_SCALE), rounding=ROUND_HALF_EVEN)
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    if text in ("", "-0", "-"):
        text = "0"
    if DECIMAL_STRING_RE.fullmatch(text) is None:
        raise InvalidDecimalError("unit quantity outside representable wire bounds")
    return text


def is_canonical_decimal_string(value: object) -> bool:
    """True only for ``str`` values matching the canonical wire shape."""
    return type(value) is str and DECIMAL_STRING_RE.fullmatch(value) is not None

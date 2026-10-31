"""Decimal money, price and volume arithmetic.

Every financial value in Project 01 passes through this module. The rules it
enforces, and why:

* ``float`` is rejected at the boundary rather than coerced. Accepting it would
  import binary rounding error into an accounting value, and the resulting
  error is invisible until it is a cent off in a report (ADR-005).
* Intermediate arithmetic runs at :data:`CALC_PRECISION` significant digits;
  only the final result is quantized. Quantizing intermediates accumulates
  rounding error that no golden vector would catch at small volumes.
* Rounding is explicitly ``ROUND_HALF_UP`` and versioned. Changing it changes
  generated data, so it is part of the generator version contract.
* Negative zero is normalized away. ``-0.00`` as a balance is a formatting
  artefact that downstream string comparisons would treat as a real difference.
* Magnitudes are bounded. ``Decimal("1e9999")`` is finite, parses happily, and
  will exhaust memory somewhere far away from where it entered (WF-067).
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation, localcontext
from typing import Any

from .models import Mark, Side, SymbolSpec, ValidationError

#: Account currency scale for the MVP (USD).
MONEY_SCALE = 2
MONEY_QUANT = Decimal("0.01")
ROUNDING = ROUND_HALF_UP

#: Significant digits for intermediate arithmetic (IEEE 754 decimal128).
CALC_PRECISION = 34

#: Bumped whenever a formula or rounding rule changes. Part of the reproducible
#: result contract alongside ``GENERATOR_VERSION``.
ACCOUNTING_POLICY_VERSION = "1.0"

#: Sanity bounds. These are far outside any legitimate synthetic dataset and
#: exist to turn an absurd input into a 422 instead of a memory event.
MAX_ABS_MONEY = Decimal("1e15")
MAX_ABS_PRICE = Decimal("1e9")
MAX_ABS_VOLUME = Decimal("1e9")


@contextlib.contextmanager
def calc_context() -> Iterator[None]:
    """Run a block at high precision without leaking context globally."""
    with localcontext() as ctx:
        ctx.prec = CALC_PRECISION
        ctx.rounding = ROUNDING
        yield


# ---------------------------------------------------------------------------
# Parsing and normalization
# ---------------------------------------------------------------------------


def ensure_finite(value: Decimal, *, field: str) -> Decimal:
    if value.is_nan() or value.is_infinite():
        raise ValidationError(
            f"{field} must be a finite number",
            details={"field": field, "value": str(value)},
        )
    return value


def ensure_magnitude(value: Decimal, *, field: str, limit: Decimal) -> Decimal:
    """Reject values whose magnitude is outside the supported range.

    Decimal comparison is exact and does not materialize the digits, so this is
    safe to call on an absurd exponent such as ``1e9999``.
    """
    if value.copy_abs() > limit:
        raise ValidationError(
            f"{field} magnitude exceeds the supported range",
            details={"field": field, "limit": format_decimal(limit)},
        )
    return value


def normalize_zero(value: Decimal) -> Decimal:
    """Map ``-0`` to ``0`` while preserving the exponent."""
    if value == 0:
        return value + Decimal(0)
    return value


def parse_decimal(raw: Any, *, field: str) -> Decimal:
    """Convert API or storage input into a finite, bounded ``Decimal``."""
    if isinstance(raw, bool):
        # bool is an int subclass; accepting it would silently mean 0/1.
        raise ValidationError(
            f"{field} must be a decimal string or integer, not a boolean",
            details={"field": field},
        )
    if isinstance(raw, Decimal):
        value = raw
    elif isinstance(raw, int):
        value = Decimal(raw)
    elif isinstance(raw, str):
        text = raw.strip()
        if not text:
            raise ValidationError(f"{field} must not be empty", details={"field": field})
        try:
            value = Decimal(text)
        except InvalidOperation as exc:
            raise ValidationError(
                f"{field} is not a valid decimal",
                details={"field": field, "value": raw[:64]},
            ) from exc
    else:
        raise ValidationError(
            f"{field} must be a decimal string or integer, not {type(raw).__name__}",
            details={"field": field, "type": type(raw).__name__},
        )
    return ensure_finite(value, field=field)


def format_decimal(value: Decimal) -> str:
    """Canonical JSON form: plain notation, no exponent, no negative zero.

    Scientific notation would be valid JSON but would make two equal amounts
    compare unequal as strings, which breaks the digest and every golden
    fixture that stores an expected payload.
    """
    ensure_finite(value, field="decimal")
    return format(normalize_zero(value), "f")


# ---------------------------------------------------------------------------
# Quantization
# ---------------------------------------------------------------------------


# Quantization deliberately runs in the *default* decimal context rather than
# under `calc_context()`. `localcontext()` copies the whole context on every
# call, and in the writer profile that copy was the largest cost outside
# SQLite itself (18k calls on a 4k-deal run). It is also unnecessary here:
# `quantize` needs only enough precision to hold the result, and the bounds
# above cap that at 18 significant digits for money and 15 for a price - well
# inside the 28-digit default. `calc_context()` still wraps the multiplications
# and divisions below, where the extra headroom does matter.


def quantize_money(value: Decimal, *, field: str = "amount") -> Decimal:
    ensure_finite(value, field=field)
    ensure_magnitude(value, field=field, limit=MAX_ABS_MONEY)
    return normalize_zero(value.quantize(MONEY_QUANT, rounding=ROUNDING))


def quantize_price(value: Decimal, spec: SymbolSpec, *, field: str = "price") -> Decimal:
    ensure_finite(value, field=field)
    ensure_magnitude(value, field=field, limit=MAX_ABS_PRICE)
    quant = Decimal(1).scaleb(-spec.digits)
    return normalize_zero(value.quantize(quant, rounding=ROUNDING))


# ---------------------------------------------------------------------------
# Field validation
# ---------------------------------------------------------------------------


def validate_money(value: Decimal, *, field: str, allow_negative: bool = True) -> Decimal:
    """Validate a money input without rounding it.

    Over-precise input is rejected rather than quietly rounded: a commission of
    ``-2.505`` is a caller bug, and rounding it hides which direction the cent
    went.
    """
    ensure_finite(value, field=field)
    ensure_magnitude(value, field=field, limit=MAX_ABS_MONEY)
    if not allow_negative and value < 0:
        raise ValidationError(f"{field} must not be negative", details={"field": field})
    exponent = value.as_tuple().exponent
    if isinstance(exponent, int) and -exponent > MONEY_SCALE:
        raise ValidationError(
            f"{field} must not have more than {MONEY_SCALE} decimal places",
            details={"field": field, "scale": MONEY_SCALE},
        )
    return normalize_zero(value)


def validate_price(value: Decimal, spec: SymbolSpec, *, field: str = "price") -> Decimal:
    ensure_finite(value, field=field)
    ensure_magnitude(value, field=field, limit=MAX_ABS_PRICE)
    if value <= 0:
        raise ValidationError(
            f"{field} must be strictly positive",
            details={"field": field, "value": format_decimal(value)},
        )
    exponent = value.as_tuple().exponent
    if isinstance(exponent, int) and -exponent > spec.digits:
        raise ValidationError(
            f"{field} must not have more than {spec.digits} decimal places for {spec.symbol}",
            details={"field": field, "symbol": spec.symbol, "digits": spec.digits},
        )
    return value


def validate_volume(value: Decimal, spec: SymbolSpec, *, field: str = "volume_lots") -> Decimal:
    """Validate a tradable volume against the symbol's lot grid (WF-018)."""
    ensure_finite(value, field=field)
    ensure_magnitude(value, field=field, limit=MAX_ABS_VOLUME)
    if value <= 0:
        raise ValidationError(
            f"{field} must be strictly positive",
            details={"field": field, "value": format_decimal(value)},
        )
    with calc_context():
        steps = value / spec.lot_step
    if steps != steps.to_integral_value():
        raise ValidationError(
            f"{field} must be a multiple of the {spec.symbol} lot step",
            details={
                "field": field,
                "lot_step": format_decimal(spec.lot_step),
                "value": format_decimal(value),
            },
        )
    if value < spec.min_lot:
        raise ValidationError(
            f"{field} is below the minimum lot for {spec.symbol}",
            details={"field": field, "min_lot": format_decimal(spec.min_lot)},
        )
    if value > spec.max_lot:
        raise ValidationError(
            f"{field} exceeds the maximum lot for {spec.symbol}",
            details={"field": field, "max_lot": format_decimal(spec.max_lot)},
        )
    return value


def validate_quote(bid: Decimal, ask: Decimal, spec: SymbolSpec) -> tuple[Decimal, Decimal]:
    """Validate a two-sided quote in normal mode.

    An inverted quote (``ask < bid``) is only ever produced by an explicitly
    named fault scenario, never by the generator, so it is rejected here.
    """
    validate_price(bid, spec, field="bid")
    validate_price(ask, spec, field="ask")
    if ask < bid:
        raise ValidationError(
            "ask must be greater than or equal to bid in normal mode",
            details={"symbol": spec.symbol, "bid": format_decimal(bid), "ask": format_decimal(ask)},
        )
    return bid, ask


# ---------------------------------------------------------------------------
# Core formulas
#
#   contract_units = volume_lots * contract_size
#   BUY  gross_pnl = (close_price - open_price) * contract_units
#   SELL gross_pnl = (open_price - close_price) * contract_units
#   balance_delta  = realized_pnl_gross + commission + swap
#   equity         = balance + sum(unrealized_pnl of open positions)
# ---------------------------------------------------------------------------


def contract_units(volume_lots: Decimal, spec: SymbolSpec) -> Decimal:
    with calc_context():
        return volume_lots * spec.contract_size


def gross_pnl(
    side: Side,
    open_price: Decimal,
    close_price: Decimal,
    volume_lots: Decimal,
    spec: SymbolSpec,
) -> Decimal:
    """Realized gross P&L for ``volume_lots`` closed at ``close_price``.

    Quantized once, at the end, in the account currency.
    """
    with calc_context():
        units = volume_lots * spec.contract_size
        diff = (close_price - open_price) if side is Side.BUY else (open_price - close_price)
        return quantize_money(diff * units, field="realized_pnl_gross")


def unrealized_pnl(
    side: Side,
    open_price: Decimal,
    volume_lots: Decimal,
    spec: SymbolSpec,
    mark: Mark,
) -> Decimal:
    """Mark-to-market P&L of an open position.

    A long is valued against the bid and a short against the ask, because that
    is the side it would actually be closed on. Using the mid, or using the
    wrong side, flatters the result by exactly the spread.
    """
    close_price = mark.bid if side is Side.BUY else mark.ask
    return gross_pnl(side, open_price, close_price, volume_lots, spec)


def balance_delta(gross: Decimal, commission: Decimal, swap: Decimal) -> Decimal:
    """Balance movement booked by one deal.

    ``commission`` and ``swap`` are signed: a fee is negative, and a rebate is
    positive when a scenario declares one. Nothing here flips a sign.

    Adding three values that are already at the currency scale needs no extra
    precision, so this runs in the default context. It is called once per deal
    on the write path and once per deal again by the INV-07 recomputation, and
    a context copy on each was measurable at 100k deals.
    """
    return quantize_money(gross + commission + swap, field="balance_delta")


def weighted_average_price(
    old_price: Decimal,
    old_volume: Decimal,
    add_price: Decimal,
    add_volume: Decimal,
    spec: SymbolSpec,
) -> Decimal:
    """Volume-weighted open price after a same-side increase (WF-027)."""
    with calc_context():
        total_volume = old_volume + add_volume
        if total_volume <= 0:
            raise ValidationError(
                "total volume must be positive to compute a weighted average price",
                details={"symbol": spec.symbol},
            )
        blended = (old_price * old_volume + add_price * add_volume) / total_volume
    return quantize_price(blended, spec, field="open_price")


def equity(balance: Decimal, unrealized_total: Decimal) -> Decimal:
    """Derived figure only.

    This is *not* a margin model: it is ``balance + unrealized`` and nothing
    more. Project 05 owns real risk arithmetic.
    """
    with calc_context():
        return quantize_money(balance + unrealized_total, field="equity")

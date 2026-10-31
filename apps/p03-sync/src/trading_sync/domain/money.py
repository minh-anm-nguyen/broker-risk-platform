"""Monetary and quantity parsing under a declared exact-precision policy.

Two decisions carry this module.

**Strings only.** A monetary field must arrive as a JSON string. A JSON number
has already passed through a binary float by the time Python sees it, so
``0.1`` is no longer ``0.1``; accepting one would mean storing a value the
source never sent (AS-08, WF3-086, INV-12).

**Reject, never round.** The storage policy is ``NUMERIC(38,12)``. A value with
13 fractional digits, or one too large for the column, is refused here. Were it
to reach PostgreSQL it would be rounded or would overflow, and a rounded
commission is a wrong commission that nothing downstream can detect
(WF3-088, WF3-089).

``decimal.Decimal`` is lenient in ways this module is not: it strips
surrounding whitespace and underscores, and it happily parses ``'NaN'``,
``'Infinity'`` and exponent notation. Each of those is rejected explicitly
below rather than inherited.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_DOWN

from trading_sync.errors import InvalidMoneyError

__all__ = [
    "MAX_INTEGER_DIGITS",
    "MONEY_PRECISION",
    "MONEY_SCALE",
    "parse_money",
    "parse_positive_money",
]

#: Mirrors the NUMERIC(38,12) column policy. An MVP decision, not a universal
#: MT5 precision (design 5.3 note 1).
MONEY_PRECISION = 38
MONEY_SCALE = 12
MAX_INTEGER_DIGITS = MONEY_PRECISION - MONEY_SCALE


def parse_money(raw: object, *, field: str) -> Decimal:
    """Parse an exact decimal from a plain decimal string.

    Zero and negative values pass: a balance or a realized P&L may legitimately
    be either, and inventing a positivity rule here would reject real source
    data (WF3-070, WF3-093).
    """
    if isinstance(raw, bool) or not isinstance(raw, str):
        raise InvalidMoneyError(
            "monetary value must be a JSON string, not a number",
            field=field,
            actual_type=type(raw).__name__,
        )
    if raw == "":
        raise InvalidMoneyError("monetary value is empty", field=field)
    if raw != raw.strip():
        # Decimal() would strip these for us, which is the problem.
        raise InvalidMoneyError("monetary value has surrounding whitespace", field=field)
    if "_" in raw:
        raise InvalidMoneyError("monetary value contains an underscore", field=field)
    if "e" in raw or "E" in raw:
        raise InvalidMoneyError(
            "exponent notation is not accepted for monetary values", field=field
        )
    try:
        value = Decimal(raw)
    except InvalidOperation:
        raise InvalidMoneyError("monetary value is not a decimal", field=field) from None
    if not value.is_finite():
        # Decimal('NaN') and Decimal('Infinity') parse; neither is money.
        raise InvalidMoneyError("monetary value is not finite", field=field)

    exponent = value.as_tuple().exponent
    assert isinstance(exponent, int)  # guaranteed by the is_finite() check above
    scale = -exponent if exponent < 0 else 0
    if scale > MONEY_SCALE:
        raise InvalidMoneyError(
            "monetary value has more fractional digits than policy allows",
            field=field,
            scale=scale,
            max_scale=MONEY_SCALE,
        )
    integer_digits = _integer_digit_count(value)
    if integer_digits > MAX_INTEGER_DIGITS:
        raise InvalidMoneyError(
            "monetary value exceeds the storable range",
            field=field,
            integer_digits=integer_digits,
            max_integer_digits=MAX_INTEGER_DIGITS,
        )
    return value


def parse_positive_money(raw: object, *, field: str) -> Decimal:
    """As :func:`parse_money`, but the value must be strictly positive.

    Used for prices and volumes, where a non-positive number is not a value we
    could faithfully store -- it is a malformed record (WF3-083).
    """
    value = parse_money(raw, field=field)
    if value <= 0:
        raise InvalidMoneyError(
            "value must be strictly positive",
            field=field,
            sign="zero" if value == 0 else "negative",
        )
    return value


def _integer_digit_count(value: Decimal) -> int:
    """Digits left of the point, ignoring sign. ``0.5`` counts as zero."""
    integral = value.copy_abs().to_integral_value(rounding=ROUND_DOWN)
    if integral == 0:
        return 0
    return len(format(integral, "f"))

"""Decimal money: parsing, bounds, signs and currency-scoped sums.

Every rule here exists because of a specific way a financial pipeline lies to
its operators:

* A JSON float ``0.1`` is not 0.1, so money arrives and leaves as a *string*
  and a float input is an error, never a convenience (section 7.2, P08-L).
* ``Decimal("nan")`` parses happily and poisons every later comparison, so
  non-finite values are rejected at the boundary.
* ``Decimal("-0.00") == 0`` is true but ``str()`` differs, so negative zero is
  normalised; otherwise two equal totals produce two different digests.
* Adding USD to EUR is always a bug, so sums are keyed by currency and the
  accumulator refuses a second currency rather than coercing one (INV08-07).
* ``Decimal(38, 12)`` is a chosen bound, not a law of nature, so the bound is
  checked explicitly instead of being discovered by a ClickHouse overflow
  (section 5.2, P08-L-09).
"""

from __future__ import annotations

import re
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, getcontext, localcontext
from typing import Iterable

from .errors import AnalyticsError, Reason

#: Mirrors ``Decimal(38, 12)`` in the ClickHouse DDL (section 5.2): at most 38
#: significant digits with 12 after the point.
MONEY_PRECISION = 38
MONEY_SCALE = 12

#: Working precision for money arithmetic, with headroom for intermediate
#: cancellation.
MONEY_CONTEXT_PRECISION = MONEY_PRECISION + MONEY_SCALE + 10


def _raise_default_precision() -> None:
    """Widen the process decimal context to cover ``Decimal(38, 12)``.

    Python's default context carries 28 significant digits, which is *fewer*
    than this project's own money bound. That gap bites outside the guarded
    helpers below: negating or subtracting a 32-digit amount silently rounds
    it before :func:`checked_sum` ever sees the value, so an exact
    cancellation stops being exact and the error is invisible at the call
    site.

    Raising the default here rather than wrapping every expression means code
    that does ordinary arithmetic on parsed money cannot quietly lose digits.
    It only ever widens the context, so a caller that deliberately set a
    larger precision keeps it.
    """
    context = getcontext()
    if context.prec < MONEY_CONTEXT_PRECISION:
        context.prec = MONEY_CONTEXT_PRECISION


_raise_default_precision()
MONEY_MAX = Decimal(10) ** (MONEY_PRECISION - MONEY_SCALE) - Decimal(10) ** -MONEY_SCALE
MONEY_MIN = -MONEY_MAX

#: Strict grammar: optional sign, digits, optional fraction. No exponent, no
#: leading ``+.``, no whitespace, no thousands separators, no bare ``.``. A
#: looser pattern would accept ``1e3`` and ``Infinity``, both of which
#: ``Decimal`` is happy to parse.
_MONEY_RE = re.compile(r"\A-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?\Z")

#: ISO-4217-shaped: exactly three uppercase letters. P08 never infers currency
#: from a symbol, so an unparseable currency is fatal rather than defaulted
#: (INV08-15).
_CURRENCY_RE = re.compile(r"\A[A-Z]{3}\Z")


class MoneyError(AnalyticsError):
    """Money input violates the decimal contract."""

    status_code = 422


def parse_currency(value: object, field: str = "money_currency") -> str:
    if value is None or (isinstance(value, str) and not value.strip()):
        raise MoneyError(Reason.CURRENCY_MISSING, f"{field} is required", field=field)
    if not isinstance(value, str) or not _CURRENCY_RE.match(value):
        raise MoneyError(
            Reason.CURRENCY_UNSUPPORTED,
            f"{field} must be three uppercase letters",
            field=field,
        )
    return value


def parse_money(value: object, field: str = "amount", *, scale: int = MONEY_SCALE) -> Decimal:
    """Parse a decimal money string into a bounded, normalised ``Decimal``.

    Raises rather than coercing in every doubtful case. The caller that wanted
    a default should state it; code that guesses here would turn a contract
    break into a plausible number.
    """
    if isinstance(value, bool) or isinstance(value, float):
        # bool is an int subclass and would otherwise sail through as 0/1.
        raise MoneyError(
            Reason.MONEY_NOT_A_STRING,
            f"{field} must be a decimal string, not {type(value).__name__}",
            field=field,
        )
    if isinstance(value, Decimal):
        text = format(value, "f") if value.is_finite() else str(value)
    elif isinstance(value, int):
        text = str(value)
    elif isinstance(value, str):
        text = value.strip()
    else:
        raise MoneyError(
            Reason.MONEY_NOT_A_STRING,
            f"{field} must be a decimal string, not {type(value).__name__}",
            field=field,
        )

    if not _MONEY_RE.match(text):
        raise MoneyError(Reason.MONEY_MALFORMED, f"{field} is not a plain decimal", field=field)

    fraction = text.partition(".")[2]
    if len(fraction) > scale:
        raise MoneyError(
            Reason.MONEY_SCALE_EXCEEDED,
            f"{field} has {len(fraction)} fractional digits, limit {scale}",
            field=field,
        )

    try:
        parsed = Decimal(text)
    except InvalidOperation as exc:  # pragma: no cover - grammar already rejects these
        raise MoneyError(Reason.MONEY_MALFORMED, f"{field} is not a valid decimal", field=field) from exc

    if not parsed.is_finite():  # pragma: no cover - grammar rejects nan/inf
        raise MoneyError(Reason.MONEY_MALFORMED, f"{field} must be finite", field=field)
    if parsed > MONEY_MAX or parsed < MONEY_MIN:
        raise MoneyError(
            Reason.MONEY_OUT_OF_BOUNDS,
            f"{field} exceeds Decimal({MONEY_PRECISION},{MONEY_SCALE}) bounds",
            field=field,
        )
    return normalize_money(parsed)


def normalize_money(value: Decimal) -> Decimal:
    """Collapse negative zero so equal amounts share one representation.

    ``Decimal("-0.00")`` and ``Decimal("0.00")`` compare equal but format
    differently, which is enough to break a digest comparison between two
    runs that are financially identical (P08-L).
    """
    if value == 0:
        return Decimal(0).scaleb(value.as_tuple().exponent)
    return value


def format_money(value: Decimal) -> str:
    """Render money for the wire: plain decimal string, never a JSON number."""
    if not value.is_finite():
        raise MoneyError(Reason.MONEY_MALFORMED, "cannot format a non-finite amount")
    return format(normalize_money(value), "f")


def quantize_money(value: Decimal, scale: int = MONEY_SCALE) -> Decimal:
    """Quantize to a fixed scale, refusing to lose non-zero digits.

    Rounding mode is not a parameter on purpose: a sum that needs rounding to
    fit the storage scale is a policy question for P05, not something this
    module should answer silently.
    """
    quantum = Decimal(1).scaleb(-scale)
    reduced = value.quantize(quantum)
    if reduced != value:
        raise MoneyError(
            Reason.MONEY_SCALE_EXCEEDED,
            "quantization would discard significant digits",
            requested_scale=scale,
        )
    return normalize_money(reduced)


def checked_sum(values: Iterable[Decimal], *, field: str = "total") -> Decimal:
    """Sum with an explicit overflow guard and no intermediate rounding.

    The wide local context keeps intermediate cancellation exact (P08-U-09),
    while the bounds check afterwards ensures a total that cannot be stored is
    reported as an error instead of being truncated by the database.
    """
    with localcontext() as ctx:
        ctx.prec = MONEY_CONTEXT_PRECISION
        total = Decimal(0)
        for value in values:
            if not value.is_finite():
                raise MoneyError(Reason.MONEY_MALFORMED, f"{field} contains a non-finite term")
            total += value
    if total > MONEY_MAX or total < MONEY_MIN:
        raise MoneyError(
            Reason.MONEY_OUT_OF_BOUNDS,
            f"{field} overflows Decimal({MONEY_PRECISION},{MONEY_SCALE})",
            field=field,
        )
    return normalize_money(total)


@dataclass(frozen=True, slots=True)
class Money:
    """An amount paired with its currency. Never arithmetic across currencies."""

    amount: Decimal
    currency: str

    @staticmethod
    def of(amount: object, currency: object, field: str = "amount") -> "Money":
        return Money(parse_money(amount, field), parse_currency(currency))

    def __add__(self, other: "Money") -> "Money":
        if self.currency != other.currency:
            raise MoneyError(
                Reason.CURRENCY_MIXED,
                f"refusing to add {self.currency} to {other.currency}",
                left=self.currency,
                right=other.currency,
            )
        return Money(checked_sum((self.amount, other.amount)), self.currency)

    def __str__(self) -> str:
        return f"{format_money(self.amount)} {self.currency}"


class CurrencyBuckets:
    """Per-currency accumulator used by the independent oracle and rollups.

    The whole point is that there is no API to get a single grand total across
    currencies. INV08-07 is enforced by the shape of the type, not by a
    reviewer noticing a missing ``GROUP BY``.
    """

    __slots__ = ("_totals",)

    def __init__(self) -> None:
        self._totals: dict[str, dict[str, Decimal]] = {}

    def add(self, currency: str, measure: str, amount: Decimal) -> None:
        bucket = self._totals.setdefault(parse_currency(currency), {})
        current = bucket.get(measure, Decimal(0))
        bucket[measure] = checked_sum((current, amount), field=f"{currency}/{measure}")

    def get(self, currency: str, measure: str) -> Decimal:
        return self._totals.get(currency, {}).get(measure, Decimal(0))

    def currencies(self) -> list[str]:
        return sorted(self._totals)

    def items(self) -> Iterator[tuple[str, Mapping[str, Decimal]]]:
        for currency in sorted(self._totals):
            yield currency, dict(sorted(self._totals[currency].items()))

    def as_wire(self) -> dict[str, dict[str, str]]:
        """Serialize for evidence bundles: strings, so no float ever appears."""
        return {
            currency: {measure: format_money(amount) for measure, amount in measures.items()}
            for currency, measures in self.items()
        }

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, CurrencyBuckets):
            return NotImplemented
        return self.as_wire() == other.as_wire()

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"CurrencyBuckets({self.as_wire()!r})"

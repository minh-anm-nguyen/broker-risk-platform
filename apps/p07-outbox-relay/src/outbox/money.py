"""Decimal-string money, and the refusal to guess a currency.

Two independent rules from the design meet in this module.

**Scale is pinned per field, and rounding is forbidden.** Section 5.3 requires
money on the wire as decimal strings with explicit units. Pinning a scale is
what makes ``"0.1"`` and ``"0.10"`` hash identically (the metamorphic property
in section 14.1), but normalising is only safe when it loses nothing: an input
with more fractional digits than the field allows is an integrity error, never
a rounding opportunity. ``"0.105"`` into a scale-2 field raises.

**An unknown currency stays unknown.** P03's Deal table is not guaranteed to
carry a currency, and the design is blunt about the failure mode: never invent
USD because the simulator happens to use USD. So a money-bearing payload
carries ``money_currency_status``; ``UNPROVEN`` means the amounts are still
exact and still verifiable by hash, but no consumer may aggregate them into a
currency total. :class:`MoneyCurrency` makes the two states unforgeable --
there is no way to spell "proven" without supplying a code.
"""

from __future__ import annotations

import enum
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from .errors import MoneyPolicyError

#: Scales pinned by the event schema. Money at 2, price at 5 and lots at 2 match
#: the worked example in design section 5.3 (commission "-0.50", price
#: "1.10500", volume "0.10").
MONEY_SCALE = 2
PRICE_SCALE = 5
VOLUME_SCALE = 2

#: Total significant digits accepted before a value is treated as out of range.
#: Guards against a source field that is wrong by orders of magnitude rather
#: than wrong in the last place.
MAX_PRECISION = 20

#: Deliberately stricter than Decimal's own parser: no exponent notation, no
#: underscores, no leading ``+``, no whitespace. The wire form must be the form
#: a human reads in an incident review.
_DECIMAL_RE = re.compile(r"^-?(0|[1-9][0-9]*)(\.[0-9]+)?$")

#: ISO 4217 shape only. Membership in a real code list belongs to P05, which
#: owns the accounting/symbol currency classification.
_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")


class CurrencyStatus(enum.StrEnum):
    """Whether the currency of the amounts in a payload is established."""

    PROVEN = "PROVEN"
    """P05 supplied and validated the calculation currency."""

    UNPROVEN = "UNPROVEN"
    """Amounts are exact, but their unit is unknown. Do not aggregate."""


@dataclass(frozen=True, slots=True)
class MoneyCurrency:
    """The currency classification of a payload's monetary fields.

    Constructed only through :meth:`proven` or :meth:`unproven`, so a payload
    cannot claim ``PROVEN`` without carrying a code, and cannot carry a code
    while claiming ``UNPROVEN``.
    """

    status: CurrencyStatus
    code: str | None

    @classmethod
    def proven(cls, code: str) -> MoneyCurrency:
        if not isinstance(code, str) or not _CURRENCY_RE.match(code):
            raise MoneyPolicyError(f"currency code {code!r} is not an ISO-4217-shaped code")
        return cls(CurrencyStatus.PROVEN, code)

    @classmethod
    def unproven(cls) -> MoneyCurrency:
        return cls(CurrencyStatus.UNPROVEN, None)

    @property
    def aggregatable(self) -> bool:
        """True only when a consumer may add these amounts into a total."""
        return self.status is CurrencyStatus.PROVEN

    def as_payload_fields(self) -> dict[str, str | None]:
        """The two wire fields that always travel together."""
        return {"money_currency": self.code, "money_currency_status": str(self.status)}

    @classmethod
    def from_payload(cls, payload: dict[str, object]) -> MoneyCurrency:
        """Rebuild the classification from a received payload, strictly.

        A mismatched pair (``PROVEN`` with no code, ``UNPROVEN`` with one) is
        rejected rather than resolved in the payload's favour: either half could
        be the lie, and applying money under a guessed unit is the exact failure
        this type exists to prevent.
        """
        raw_status = payload.get("money_currency_status")
        raw_code = payload.get("money_currency")
        try:
            status = CurrencyStatus(raw_status)
        except ValueError as exc:
            raise MoneyPolicyError(f"money_currency_status {raw_status!r} is not a known status") from exc
        if status is CurrencyStatus.PROVEN:
            if not isinstance(raw_code, str):
                raise MoneyPolicyError("money_currency_status=PROVEN requires a money_currency code")
            return cls.proven(raw_code)
        if raw_code is not None:
            raise MoneyPolicyError("money_currency_status=UNPROVEN must not carry a money_currency code")
        return cls.unproven()


def parse_decimal(raw: object, *, field: str) -> Decimal:
    """Parse a wire decimal string into an exact ``Decimal``.

    ``float`` is rejected outright rather than converted: by the time a value is
    a float the damage is already done, and accepting it here would hide the
    place upstream where precision was lost.
    """
    if isinstance(raw, bool) or not isinstance(raw, str):
        raise MoneyPolicyError(f"{field}: expected a decimal string, got {type(raw).__name__}")
    if not _DECIMAL_RE.match(raw):
        raise MoneyPolicyError(f"{field}: {raw!r} is not a plain decimal string")
    try:
        value = Decimal(raw)
    except InvalidOperation as exc:  # pragma: no cover - regex already filtered
        raise MoneyPolicyError(f"{field}: {raw!r} is not a decimal") from exc
    if not value.is_finite():  # pragma: no cover - regex rejects NaN/Infinity
        raise MoneyPolicyError(f"{field}: {raw!r} is not finite")
    digits = len(value.as_tuple().digits)
    if digits > MAX_PRECISION:
        raise MoneyPolicyError(f"{field}: {digits} significant digits exceeds {MAX_PRECISION}")
    return value


def normalize(raw: object, *, field: str, scale: int) -> str:
    """Return the canonical string for ``raw`` at exactly ``scale`` decimals.

    Padding is allowed (``"0.1"`` -> ``"0.10"``); truncation never is. The
    caller gets back a string, not a ``Decimal``, because the string is the
    thing that gets hashed and the two must not drift.
    """
    value = parse_decimal(raw, field=field)
    exponent = value.as_tuple().exponent
    assert isinstance(exponent, int)  # finite, enforced by parse_decimal
    if -exponent > scale:
        raise MoneyPolicyError(
            f"{field}: {raw!r} has {-exponent} decimal places, field allows {scale}; "
            "rounding a financial value is not permitted"
        )
    quantized = value.quantize(Decimal(1).scaleb(-scale))
    # Decimal renders negative zero as "-0.00"; collapse it so that a zero
    # amount has exactly one representation and therefore one hash.
    if quantized == 0:
        quantized = quantized.copy_abs()
    return f"{quantized:f}"


def normalize_money(raw: object, *, field: str) -> str:
    return normalize(raw, field=field, scale=MONEY_SCALE)


def normalize_price(raw: object, *, field: str) -> str:
    return normalize(raw, field=field, scale=PRICE_SCALE)


def normalize_volume(raw: object, *, field: str) -> str:
    """Lot volume, which is additionally required to be positive."""
    text = normalize(raw, field=field, scale=VOLUME_SCALE)
    if Decimal(text) <= 0:
        raise MoneyPolicyError(f"{field}: volume {raw!r} must be greater than zero")
    return text


def add_money(left: str, right: str, *, field: str = "sum") -> str:
    """Exact addition of two canonical money strings, staying at MONEY_SCALE.

    Used by the reference consumer's projection. Doing this in ``Decimal`` and
    storing the result as text keeps the projection exactly comparable with the
    source NUMERIC values (oracle O6); accumulating in float would make the
    oracle fail for reasons that have nothing to do with the pipeline.
    """
    total = parse_decimal(left, field=field) + parse_decimal(right, field=field)
    return normalize(f"{total:f}", field=field, scale=MONEY_SCALE)

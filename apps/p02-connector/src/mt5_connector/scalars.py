"""Strict scalar decoding for values that arrive from a source.

This module is the only place a string becomes a number or a timestamp, and it
is deliberately unforgiving. Every rule here exists because the lenient
version of it silently produces a wrong financial record:

* **A JSON float is rejected, never coerced** (WF2-L-01). ``1.10000`` parsed as
  an IEEE 754 double is not ``1.10000``, and ``Decimal(1.1)`` preserves the
  error rather than removing it. Rounding it to two places would import the
  source's representation error and hide it. The only safe response is to
  refuse the page and say which field was wrong.
* **NaN and Infinity are rejected** (WF2-L-02). ``Decimal("NaN")`` constructs
  happily and then poisons every sum it touches, usually in a different
  service hours later.
* **A naive timestamp is rejected** (WF2-L-08). "No offset" is not "UTC"; it is
  an unknown offset, and guessing one shifts an execution time by hours.
* **Digit counts are bounded** (WF2-L-10). ``Decimal`` handles arbitrary
  precision, which means a hostile ``1e999999999`` is an allocation attack.

``bool`` is checked before ``int`` throughout: ``True`` is an ``int`` in Python
and would otherwise decode as the amount 1.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Final

__all__ = [
    "MAX_DECIMAL_DIGITS",
    "MAX_DECIMAL_TEXT",
    "MAX_ID_LENGTH",
    "ScalarError",
    "parse_money",
    "parse_timestamp",
    "parse_identifier",
    "parse_sequence",
    "parse_enum",
    "format_money",
]

#: Total significant digits accepted in one amount. Broker money and volumes
#: sit far below this; the cap exists to bound work, not to express a domain
#: rule (WF2-L-10).
MAX_DECIMAL_DIGITS: Final[int] = 34

#: Characters accepted in the raw text of an amount, checked before parsing so
#: an enormous literal is refused without being converted first.
MAX_DECIMAL_TEXT: Final[int] = 64

#: Identifier length cap. Long enough for any broker key, short enough that an
#: identifier can never be a payload.
MAX_ID_LENGTH: Final[int] = 128

#: Plain decimal notation only. Exponent form is refused: it is legal JSON but
#: it is not how this contract transmits money, and accepting ``1e3`` here
#: would mean accepting ``1e999999`` too.
_DECIMAL_TEXT = re.compile(r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$")

#: Control characters have no place in an identifier and would break both log
#: framing and metric labels if one reached them (WF2-N-06).
_CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f]")

#: RFC 3339 with a mandatory offset. ``Z`` or a numeric offset; nothing else.
_RFC3339 = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})[Tt ](\d{2}):(\d{2}):(\d{2})(\.\d{1,9})?"
    r"([Zz]|[+-]\d{2}:\d{2})$"
)

#: Business timestamps outside this window are a source bug, not data. The
#: bound keeps a corrupt year like 0001 or 9999 out of downstream date maths.
_MIN_YEAR: Final[int] = 1990
_MAX_YEAR: Final[int] = 2100


class ScalarError(ValueError):
    """A single field failed strict decoding.

    Carries the field name and a *reason*, never the offending value: the
    value may be part of a financial record and this error is logged
    (INV-15). ``observed_type`` is safe because a type name is not data.
    """

    def __init__(self, field: str, reason: str, *, observed_type: str | None = None) -> None:
        super().__init__(f"{field}: {reason}")
        self.field = field
        self.reason = reason
        self.observed_type = observed_type

    def as_details(self) -> dict[str, Any]:
        details: dict[str, Any] = {"field": self.field, "reason": self.reason}
        if self.observed_type is not None:
            details["observed_type"] = self.observed_type
        return details


def parse_money(value: Any, *, field: str) -> Decimal:
    """Decode an amount, price or volume.

    Accepts a decimal **string** or an ``int``. Everything else - most
    importantly ``float`` - is refused.
    """
    if isinstance(value, bool):
        raise ScalarError(field, "must be a decimal string, not a boolean", observed_type="bool")
    if isinstance(value, float):
        # The one rule this module exists for. A float has already lost
        # precision by the time it reaches here; there is nothing to recover.
        raise ScalarError(
            field,
            "financial values must be transmitted as JSON strings; a JSON number "
            "has already been through a double and cannot be trusted",
            observed_type="float",
        )
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, Decimal):
        # Already decoded upstream in this process (a fake driver, a replayed
        # fixture). Still subject to the finiteness and digit checks below.
        decoded = value
        if not decoded.is_finite():
            raise ScalarError(field, "must be a finite amount", observed_type="Decimal")
        _check_digits(decoded, field)
        return decoded
    if not isinstance(value, str):
        raise ScalarError(
            field, "must be a decimal string", observed_type=type(value).__name__
        )

    text = value.strip()
    if not text:
        raise ScalarError(field, "must not be blank", observed_type="str")
    if len(text) > MAX_DECIMAL_TEXT:
        raise ScalarError(
            field, f"decimal text exceeds {MAX_DECIMAL_TEXT} characters", observed_type="str"
        )
    if not _DECIMAL_TEXT.match(text):
        # Covers NaN, Infinity, exponent notation, thousands separators,
        # locale commas, leading plus and leading zeros in one check.
        raise ScalarError(
            field,
            "must be plain decimal notation with no exponent, sign prefix, "
            "separator or non-finite literal",
            observed_type="str",
        )
    try:
        decoded = Decimal(text)
    except InvalidOperation as exc:  # pragma: no cover - the regex precedes this
        raise ScalarError(field, "is not a valid decimal", observed_type="str") from exc
    if not decoded.is_finite():  # pragma: no cover - the regex precedes this
        raise ScalarError(field, "must be a finite amount", observed_type="str")
    _check_digits(decoded, field)
    return decoded


def _check_digits(value: Decimal, field: str) -> None:
    digits = len(value.as_tuple().digits)
    if digits > MAX_DECIMAL_DIGITS:
        raise ScalarError(
            field, f"has more than {MAX_DECIMAL_DIGITS} significant digits"
        )


def format_money(value: Decimal) -> str:
    """Canonical text for an amount: plain notation, no exponent, no -0.

    Used for evidence files and digests, where two equal amounts must also be
    equal as strings.
    """
    if value == 0:
        # Preserve the scale but drop the sign: "-0.00" and "0.00" are the
        # same amount and must not produce two different digests.
        value = value.copy_abs()
    text = format(value, "f")
    return text


def parse_timestamp(value: Any, *, field: str) -> datetime:
    """Decode an RFC 3339 timestamp and normalise it to UTC.

    A value without an offset is refused rather than assumed to be UTC: the
    difference between the two policies is a trade booked in the wrong hour.
    """
    if not isinstance(value, str):
        raise ScalarError(
            field, "must be an RFC 3339 timestamp string", observed_type=type(value).__name__
        )
    text = value.strip()
    if not _RFC3339.match(text):
        raise ScalarError(
            field,
            "must be RFC 3339 with an explicit UTC offset (for example "
            "2026-10-30T00:00:00Z); a timestamp without an offset is an "
            "unknown offset, not UTC",
            observed_type="str",
        )
    normalised = text.replace(" ", "T").replace("z", "Z")
    if normalised.endswith("Z"):
        normalised = normalised[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalised)
    except ValueError as exc:
        raise ScalarError(field, "is not a valid calendar timestamp", observed_type="str") from exc
    if parsed.tzinfo is None:  # pragma: no cover - the regex guarantees an offset
        raise ScalarError(field, "must carry a UTC offset", observed_type="str")
    if not _MIN_YEAR <= parsed.year <= _MAX_YEAR:
        raise ScalarError(
            field, f"year is outside the plausible range {_MIN_YEAR}-{_MAX_YEAR}"
        )
    return parsed.astimezone(UTC)


def format_timestamp(value: datetime) -> str:
    """Canonical UTC text with millisecond precision."""
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.") + f"{value.microsecond // 1000:03d}Z"


def parse_identifier(value: Any, *, field: str, max_length: int = MAX_ID_LENGTH) -> str:
    """Decode an identifier: non-blank, bounded, free of control characters."""
    if not isinstance(value, str):
        raise ScalarError(field, "must be a string", observed_type=type(value).__name__)
    if not value:
        raise ScalarError(field, "must not be empty", observed_type="str")
    if value != value.strip() or not value.strip():
        # A key that differs from its own trimmed form would compare unequal to
        # the same key written without the padding, which is a silent
        # duplicate downstream (WF2-L-07).
        raise ScalarError(field, "must not be blank or carry surrounding whitespace", observed_type="str")
    if len(value) > max_length:
        raise ScalarError(field, f"exceeds {max_length} characters", observed_type="str")
    if _CONTROL_CHARS.search(value):
        raise ScalarError(field, "must not contain control characters", observed_type="str")
    return value


def parse_sequence(value: Any, *, field: str, minimum: int = 0) -> int:
    """Decode a monotonic sequence number."""
    if isinstance(value, bool):
        raise ScalarError(field, "must be an integer, not a boolean", observed_type="bool")
    if not isinstance(value, int):
        raise ScalarError(field, "must be an integer", observed_type=type(value).__name__)
    if value < minimum:
        raise ScalarError(field, f"must be at least {minimum}")
    if value > 2**63 - 1:
        raise ScalarError(field, "exceeds a 64-bit sequence")
    return value


def parse_enum(value: Any, *, field: str, allowed: frozenset[str]) -> str:
    """Decode a closed enumeration.

    An unknown member is an error rather than a pass-through: forwarding a
    value this build does not understand means a downstream consumer branches
    on something nobody mapped (WF2-E-12).
    """
    if not isinstance(value, str):
        raise ScalarError(field, "must be a string", observed_type=type(value).__name__)
    if value not in allowed:
        raise ScalarError(
            field, f"is not one of the supported values {sorted(allowed)}", observed_type="str"
        )
    return value

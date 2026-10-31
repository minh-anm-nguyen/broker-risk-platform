"""Wire money types.

Money leaves this service as a JSON *string*, always. The helpers here are the
only sanctioned way to put a ``Decimal`` on the wire, and ``assert_no_float_in``
is the guard tests and the response validator use to prove nothing slipped
through as a JSON number.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Annotated, Any, Final

from pydantic import BaseModel, ConfigDict, StringConstraints, field_validator
from risk_engine import DECIMAL_STRING_RE, format_money, format_units, is_canonical_decimal_string

__all__ = [
    "DecimalString",
    "CurrencyCode",
    "MoneyAmount",
    "money_string",
    "units_string",
    "assert_no_float_in",
    "FloatInPayloadError",
    "MAX_MONEY_SCALE",
]

MAX_MONEY_SCALE: Final = 10

DecimalString = Annotated[str, StringConstraints(strip_whitespace=False, min_length=1, max_length=40)]
CurrencyCode = Annotated[str, StringConstraints(pattern=r"^[A-Z]{3}$")]


class FloatInPayloadError(TypeError):
    """Raised when a float reached a place where only decimal strings belong."""


class MoneyAmount(BaseModel):
    """A currency-tagged decimal string.

    ``strict=True`` plus the explicit validator means an accidental
    ``amount=1.5`` (a float) is a hard error, not a silent coercion.
    """

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    currency: CurrencyCode
    amount: DecimalString

    @field_validator("amount")
    @classmethod
    def _canonical_amount(cls, value: str) -> str:
        if not is_canonical_decimal_string(value):
            raise ValueError("amount must be a canonical decimal string")
        return value

    def as_decimal(self) -> Decimal:
        return Decimal(self.amount)


def money_string(value: Decimal | None, currency: str) -> str | None:
    """Project a money ``Decimal`` to its wire string, or ``None`` if absent.

    ``None`` means *not available* and stays ``None``; it never becomes "0.00".
    """
    if value is None:
        return None
    return format_money(value, currency)


def units_string(value: Decimal | None) -> str | None:
    """Project a quantity to its wire string, preserving absence."""
    if value is None:
        return None
    return format_units(value)


def assert_no_float_in(payload: Any, *, path: str = "$") -> None:
    """Walk a serializable tree and refuse any float token.

    Pydantic would happily emit ``1.5`` for a float field; this walk is what
    turns "we believe it is a string" into "we checked".
    """
    if isinstance(payload, bool) or payload is None or isinstance(payload, (int, str)):
        return
    if isinstance(payload, float):
        raise FloatInPayloadError(f"float found at {path}")
    if isinstance(payload, Decimal):
        raise FloatInPayloadError(f"unserialized Decimal found at {path}")
    if isinstance(payload, dict):
        for key, value in payload.items():
            assert_no_float_in(value, path=f"{path}.{key}")
        return
    if isinstance(payload, (list, tuple)):
        for index, value in enumerate(payload):
            assert_no_float_in(value, path=f"{path}[{index}]")
        return
    raise FloatInPayloadError(f"non-JSON value of type {type(payload).__name__} at {path}")


def _decimal_regex_source() -> str:  # pragma: no cover - documentation helper
    return DECIMAL_STRING_RE.pattern

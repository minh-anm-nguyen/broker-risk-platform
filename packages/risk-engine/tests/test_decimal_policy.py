"""The decimal policy, tested directly.

This library is what Project 06 serves every financial response from, so a
regression here reaches an API response without passing through anything that
would notice. Contract gap CG-06 recorded that it had no suite of its own; this
is that suite.

Each expected value below was taken from running the code, not from reading it.
Where a behaviour looks surprising -- banker's rounding sending ``0.125`` down
and ``0.135`` up, or ``-0.005`` formatting as ``"0.00"`` -- the test says so, so
that a future change cannot quietly decide it was a bug.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from risk_engine.decimals import (
    CURRENCY_SCALE,
    DECIMAL_STRING_RE,
    UNIT_MAX_SCALE,
    format_money,
    format_units,
    is_canonical_decimal_string,
    parse_decimal,
    quantize_money,
)
from risk_engine.errors import InvalidDecimalError

pytestmark = pytest.mark.unit


# --------------------------------------------------------------- parse_decimal


def test_float_is_refused_rather_than_converted() -> None:
    """The single most expensive accident this library exists to prevent.

    ``Decimal(0.1)`` is ``0.1000000000000000055511151231257827``. Accepting a
    float would import that error into money, and no later rounding step could
    tell it apart from a real value.
    """
    with pytest.raises(InvalidDecimalError, match="float/bool"):
        parse_decimal(1.5, field="gross_profit")


def test_bool_is_refused_even_though_it_is_an_int() -> None:
    """``isinstance(True, int)`` is True, so bool has to be rejected first or
    ``True`` would silently become ``Decimal(1)``."""
    with pytest.raises(InvalidDecimalError, match="float/bool"):
        parse_decimal(True, field="commission")


@pytest.mark.parametrize("value", [0, 1, -1, 10**18])
def test_int_is_accepted_exactly(value: int) -> None:
    assert parse_decimal(value, field="x") == Decimal(value)


def test_decimal_keeps_its_declared_scale() -> None:
    """Not merely equal: the exponent survives, so a caller's declared scale is
    not silently normalised away."""
    original = Decimal("-12.3400")
    assert parse_decimal(original, field="x").as_tuple() == original.as_tuple()


@pytest.mark.parametrize(
    "value",
    [
        "0",
        "-0",
        "1",
        "-1",
        "0.5",
        "-0.5",
        "123456789012345678",
        "1.0123456789",
    ],
)
def test_canonical_strings_are_accepted(value: str) -> None:
    assert parse_decimal(value, field="x") == Decimal(value)


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("+1", id="leading-plus"),
        pytest.param(".5", id="bare-fraction"),
        pytest.param("5.", id="trailing-point"),
        pytest.param("01", id="leading-zero"),
        pytest.param("1e5", id="exponent-lower"),
        pytest.param("1E5", id="exponent-upper"),
        pytest.param("NaN", id="nan-word"),
        pytest.param("Infinity", id="infinity-word"),
        pytest.param("1,5", id="comma-separator"),
        pytest.param(" 1", id="leading-space"),
        pytest.param("1 ", id="trailing-space"),
        pytest.param("", id="empty-is-not-zero"),
        pytest.param("1.01234567890", id="eleven-fraction-digits"),
        pytest.param("1234567890123456789", id="nineteen-integer-digits"),
    ],
)
def test_non_canonical_strings_are_refused(value: str) -> None:
    with pytest.raises(InvalidDecimalError, match="not a canonical decimal string"):
        parse_decimal(value, field="x")


@pytest.mark.parametrize("value", [Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity")])
def test_non_finite_decimals_are_refused_not_coerced(value: Decimal) -> None:
    """A NaN total must not become zero. Zero is a number someone would act on."""
    with pytest.raises(InvalidDecimalError, match="non-finite"):
        parse_decimal(value, field="x")


@pytest.mark.parametrize("value", [None, [], {}, object()])
def test_unsupported_types_are_refused(value: object) -> None:
    with pytest.raises(InvalidDecimalError):
        parse_decimal(value, field="x")


# ------------------------------------------------------------- quantize_money


@pytest.mark.parametrize(
    ("amount", "currency", "expected"),
    [
        # ROUND_HALF_EVEN, not ROUND_HALF_UP: an exact half goes to the even
        # digit, so 0.125 rounds DOWN and 0.135 rounds UP. Both directions are
        # pinned, because a switch to ROUND_HALF_UP would still pass a test
        # that only checked one of them.
        ("0.125", "USD", "0.12"),
        ("0.135", "USD", "0.14"),
        ("0.124", "USD", "0.12"),
        ("0.126", "USD", "0.13"),
        ("-0.125", "USD", "-0.12"),
        ("-0.135", "USD", "-0.14"),
        # JPY has no minor unit at all.
        ("0.5", "JPY", "0"),
        ("1.5", "JPY", "2"),
        ("2.5", "JPY", "2"),
        ("100", "JPY", "100"),
    ],
)
def test_quantization_uses_bankers_rounding_at_the_currency_scale(
    amount: str, currency: str, expected: str
) -> None:
    assert quantize_money(Decimal(amount), currency) == Decimal(expected)


def test_an_unknown_currency_scale_is_an_error_not_a_guess() -> None:
    """Defaulting an unknown currency to two decimal places would be a silent
    financial assumption about a currency nobody declared."""
    with pytest.raises(InvalidDecimalError, match="unknown currency scale for GBP"):
        quantize_money(Decimal("1.005"), "GBP")


def test_quantizing_a_non_finite_amount_is_refused() -> None:
    with pytest.raises(InvalidDecimalError, match="non-finite"):
        quantize_money(Decimal("NaN"), "USD")


# ---------------------------------------------------------------- format_money


@pytest.mark.parametrize(
    ("amount", "currency", "expected"),
    [
        ("0", "USD", "0.00"),
        ("1", "USD", "1.00"),
        ("-1.005", "USD", "-1.00"),
        ("1234.567", "USD", "1234.57"),
        ("100", "JPY", "100"),
        ("-100", "JPY", "-100"),
    ],
)
def test_money_is_projected_at_full_currency_scale(
    amount: str, currency: str, expected: str
) -> None:
    assert format_money(Decimal(amount), currency) == expected


@pytest.mark.parametrize("amount", ["-0", "-0.001", "-0.004", "-0.005"])
def test_negative_zero_collapses_to_positive_zero(amount: str) -> None:
    """``-0.00`` and ``0.00`` are the same amount of money but different bytes,
    and different bytes mean a different digest and a failed golden comparison.
    """
    formatted = format_money(Decimal(amount), "USD")
    assert formatted == "0.00"
    assert not formatted.startswith("-")


def test_formatted_money_never_uses_exponent_notation() -> None:
    """``str(Decimal("1E+2"))`` is ``"1E+2"``, which no parser downstream of
    here accepts. The projection must be plain."""
    assert format_money(Decimal("1E+2"), "USD") == "100.00"


def test_an_amount_too_large_for_the_wire_shape_is_refused() -> None:
    with pytest.raises(InvalidDecimalError, match="representable wire bounds"):
        format_money(Decimal("1" * 19), "USD")


@pytest.mark.parametrize("currency", sorted(CURRENCY_SCALE))
def test_every_known_currency_round_trips_through_its_own_parser(currency: str) -> None:
    """The output of ``format_money`` must be valid input again. If it is not, a
    value can leave this library and be rejected by the next component."""
    text = format_money(Decimal("12.345"), currency)
    assert is_canonical_decimal_string(text)
    assert parse_decimal(text, field="round_trip") == Decimal(text)


# ---------------------------------------------------------------- format_units


@pytest.mark.parametrize(
    ("units", "expected"),
    [
        ("1.5000", "1.5"),
        ("1.00000000", "1"),
        ("0", "0"),
        ("-0", "0"),
        ("-0.000000001", "0"),
        ("1.000000004", "1"),
        ("2.5", "2.5"),
        ("-3.25", "-3.25"),
        ("0.00000001", "0.00000001"),
    ],
)
def test_units_trim_only_trailing_fraction_zeros(units: str, expected: str) -> None:
    assert format_units(Decimal(units)) == expected


def test_unit_scale_is_bounded_and_rounds_half_even() -> None:
    """Beyond ``UNIT_MAX_SCALE`` digits the quantity is rounded, not truncated,
    and the boundary is pinned so the constant cannot drift unnoticed."""
    assert UNIT_MAX_SCALE == 8
    assert format_units(Decimal("0.000000005")) == "0"
    assert format_units(Decimal("0.000000015")) == "0.00000002"


def test_non_finite_units_are_refused() -> None:
    with pytest.raises(InvalidDecimalError, match="non-finite"):
        format_units(Decimal("Infinity"))


# -------------------------------------------------- is_canonical_decimal_string


@pytest.mark.parametrize("value", ["0", "-1.5", "123"])
def test_canonical_predicate_accepts_canonical_strings(value: str) -> None:
    assert is_canonical_decimal_string(value) is True


@pytest.mark.parametrize("value", [Decimal("1"), 1, 1.0, True, None, b"1", "+1", "1e3"])
def test_canonical_predicate_is_false_for_everything_else(value: object) -> None:
    """It answers one question: is this already a canonical wire string? A
    ``Decimal`` is a correct amount but not a wire string, and answering True
    for it would let an unprojected value reach a JSON body."""
    assert is_canonical_decimal_string(value) is False


def test_canonical_predicate_rejects_str_subclasses() -> None:
    """``type(value) is str``, deliberately. A subclass can override ``__eq__``
    or carry state, and a money field should be a plain string by the time it is
    checked."""

    class Tainted(str):
        pass

    assert is_canonical_decimal_string(Tainted("1.00")) is False


def test_the_wire_regex_is_fully_anchored() -> None:
    """A partial match would accept ``"1.00; DROP"`` as a money value."""
    assert DECIMAL_STRING_RE.fullmatch("1.00") is not None
    assert DECIMAL_STRING_RE.fullmatch("1.00\n") is None
    assert DECIMAL_STRING_RE.fullmatch("1.00 trailing") is None

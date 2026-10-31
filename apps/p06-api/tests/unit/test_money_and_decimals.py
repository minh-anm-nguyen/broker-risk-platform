"""Decimal policy: the rules that stop money becoming a float.

WF06-K. These are the cheapest tests in the suite and they guard the most
expensive class of bug, so they check boundaries rather than happy paths:
non-finite values, float inputs, negative zero, scale limits and the exact
wire spelling.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from risk_api.contracts.money import (
    FloatInPayloadError,
    MoneyAmount,
    assert_no_float_in,
    money_string,
    units_string,
)
from risk_engine import (
    InvalidDecimalError,
    format_money,
    format_units,
    is_canonical_decimal_string,
    parse_decimal,
    quantize_money,
)

pytestmark = pytest.mark.unit


class TestParsing:
    def test_float_input_is_refused_outright(self):
        """Accepting a float would import binary rounding error into money."""
        with pytest.raises(InvalidDecimalError):
            parse_decimal(1.1, field="gross")

    def test_bool_is_not_a_number_here(self):
        with pytest.raises(InvalidDecimalError):
            parse_decimal(True, field="gross")

    @pytest.mark.parametrize("value", ["NaN", "nan", "Infinity", "-Infinity", "inf"])
    def test_non_finite_strings_are_refused(self, value: str):
        with pytest.raises(InvalidDecimalError):
            parse_decimal(value, field="gross")

    def test_non_finite_decimal_is_refused(self):
        with pytest.raises(InvalidDecimalError):
            parse_decimal(Decimal("NaN"), field="gross")

    @pytest.mark.parametrize("value", ["1e5", "+1.5", ".5", "5.", "1,5", "0x10", " 1.5", "1.5 "])
    def test_non_canonical_spellings_are_refused(self, value: str):
        with pytest.raises(InvalidDecimalError):
            parse_decimal(value, field="gross")

    @pytest.mark.parametrize(
        ("value", "expected"),
        [("0", Decimal(0)), ("-0", Decimal(0)), ("56.00", Decimal("56.00")), ("0.1000000000", Decimal("0.1"))],
    )
    def test_canonical_strings_parse_exactly(self, value: str, expected: Decimal):
        assert parse_decimal(value, field="gross") == expected

    def test_high_precision_source_value_survives(self):
        """FX-DECIMAL-009: precision inside the allowed scale is preserved."""
        parsed = parse_decimal("0.1000000001", field="price")
        assert parsed == Decimal("0.1000000001")
        assert parsed != Decimal("0.1")

    def test_scale_beyond_policy_is_refused(self):
        with pytest.raises(InvalidDecimalError):
            parse_decimal("0.12345678901", field="price")


class TestFormatting:
    @pytest.mark.parametrize(
        ("amount", "expected"),
        [
            (Decimal("56"), "56.00"),
            (Decimal("-3.5"), "-3.50"),
            (Decimal("0"), "0.00"),
            (Decimal("-0"), "0.00"),
            (Decimal("-0.00"), "0.00"),
            (Decimal("1234567.891"), "1234567.89"),
        ],
    )
    def test_money_uses_the_currency_scale(self, amount: Decimal, expected: str):
        assert format_money(amount, "USD") == expected

    def test_negative_zero_never_reaches_the_wire(self):
        """A "-0.00" in a report reads as a loss that is not there."""
        assert not format_money(Decimal("-0.000"), "USD").startswith("-")

    def test_banker_rounding_is_the_declared_policy(self):
        assert format_money(Decimal("0.125"), "USD") == "0.12"
        assert format_money(Decimal("0.135"), "USD") == "0.14"

    def test_zero_scale_currency_has_no_fraction(self):
        assert format_money(Decimal("1200"), "JPY") == "1200"

    def test_unknown_currency_scale_is_refused(self):
        with pytest.raises(InvalidDecimalError):
            quantize_money(Decimal("1"), "ZZZ")

    @pytest.mark.parametrize(
        ("units", "expected"),
        [
            (Decimal("50000"), "50000"),
            (Decimal("50000.00"), "50000"),
            (Decimal("0.5"), "0.5"),
            (Decimal("0"), "0"),
            (Decimal("-0.0"), "0"),
            (Decimal("1.10000000"), "1.1"),
        ],
    )
    def test_units_are_plain_with_no_exponent(self, units: Decimal, expected: str):
        assert format_units(units) == expected

    def test_exponent_notation_never_appears(self):
        assert "E" not in format_units(Decimal("1E+10"))
        assert format_units(Decimal("1E+10")) == "10000000000"


class TestCanonicalRecognition:
    @pytest.mark.parametrize("value", ["0", "-1", "56.00", "0.0000000001"])
    def test_accepts_canonical_strings(self, value: str):
        assert is_canonical_decimal_string(value)

    @pytest.mark.parametrize("value", [56.0, 56, Decimal("56"), None, True, "1e5", "", "01.5"])
    def test_rejects_everything_else(self, value: object):
        assert not is_canonical_decimal_string(value)


class TestMoneyAmountModel:
    def test_strings_are_accepted(self):
        amount = MoneyAmount(currency="USD", amount="56.00")
        assert amount.as_decimal() == Decimal("56.00")

    def test_float_amount_is_a_validation_error(self):
        with pytest.raises(Exception):
            MoneyAmount(currency="USD", amount=56.0)  # type: ignore[arg-type]

    def test_extra_members_are_forbidden(self):
        with pytest.raises(Exception):
            MoneyAmount(currency="USD", amount="56.00", note="x")  # type: ignore[call-arg]

    def test_currency_shape_is_enforced(self):
        with pytest.raises(Exception):
            MoneyAmount(currency="usd", amount="56.00")


class TestPayloadGuard:
    def test_a_float_anywhere_in_the_tree_is_caught(self):
        payload = {"components": {"realized": {"net": 56.0}}}
        with pytest.raises(FloatInPayloadError) as caught:
            assert_no_float_in(payload)
        assert "components.realized.net" in str(caught.value)

    def test_a_float_inside_a_list_is_caught(self):
        with pytest.raises(FloatInPayloadError) as caught:
            assert_no_float_in({"items": [{"lots": "0.5"}, {"lots": 0.5}]})
        assert "items[1].lots" in str(caught.value)

    def test_an_unserialized_decimal_is_caught(self):
        with pytest.raises(FloatInPayloadError):
            assert_no_float_in({"total": Decimal("1.00")})

    def test_integers_booleans_and_none_are_fine(self):
        assert_no_float_in({"count": 3, "ok": True, "missing": None, "id": "x"})


class TestAbsenceIsNotZero:
    def test_absent_money_stays_absent(self):
        """A missing fee is unknown, not zero. Projecting it as 0.00 is a lie."""
        assert money_string(None, "USD") is None
        assert units_string(None) is None

    def test_present_money_is_projected(self):
        assert money_string(Decimal("1.5"), "USD") == "1.50"
        assert units_string(Decimal("1.50")) == "1.5"

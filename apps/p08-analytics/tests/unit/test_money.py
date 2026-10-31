"""Decimal money boundary. Section 7.2, what-if domain L."""

from __future__ import annotations

from decimal import Decimal, getcontext

import pytest

from analytics.errors import Reason
from analytics.money import (
    MONEY_CONTEXT_PRECISION,
    MONEY_MAX,
    CurrencyBuckets,
    Money,
    MoneyError,
    checked_sum,
    format_money,
    normalize_money,
    parse_currency,
    parse_money,
    quantize_money,
)

pytestmark = pytest.mark.unit


class TestParseMoney:
    @pytest.mark.parametrize(
        "text,expected",
        [
            ("0", Decimal("0")),
            ("12.30", Decimal("12.30")),
            ("-0.50", Decimal("-0.50")),
            ("-1234567890.123456789012", Decimal("-1234567890.123456789012")),
            ("0.000000000001", Decimal("0.000000000001")),
        ],
    )
    def test_accepts_plain_decimal_strings(self, text, expected):
        assert parse_money(text) == expected

    @pytest.mark.parametrize(
        "value,reason",
        [
            # A binary float is the defect the whole rule exists for: 0.1 is
            # not 0.1, and accepting it would make every later sum wrong by an
            # amount nobody can see.
            (0.1, Reason.MONEY_NOT_A_STRING),
            (12.0, Reason.MONEY_NOT_A_STRING),
            (True, Reason.MONEY_NOT_A_STRING),
            (None, Reason.MONEY_NOT_A_STRING),
            ([], Reason.MONEY_NOT_A_STRING),
            ("1e3", Reason.MONEY_MALFORMED),
            ("1E-3", Reason.MONEY_MALFORMED),
            ("nan", Reason.MONEY_MALFORMED),
            ("inf", Reason.MONEY_MALFORMED),
            ("Infinity", Reason.MONEY_MALFORMED),
            ("", Reason.MONEY_MALFORMED),
            ("+1", Reason.MONEY_MALFORMED),
            (".5", Reason.MONEY_MALFORMED),
            ("5.", Reason.MONEY_MALFORMED),
            ("1,000.00", Reason.MONEY_MALFORMED),
            ("1 000", Reason.MONEY_MALFORMED),
            ("0x10", Reason.MONEY_MALFORMED),
            ("--1", Reason.MONEY_MALFORMED),
            ("01.5", Reason.MONEY_MALFORMED),
        ],
    )
    def test_rejects_everything_else(self, value, reason):
        with pytest.raises(MoneyError) as excinfo:
            parse_money(value)
        assert excinfo.value.reason is reason

    def test_rejects_excess_scale(self):
        with pytest.raises(MoneyError) as excinfo:
            parse_money("0.1234567890123")
        assert excinfo.value.reason is Reason.MONEY_SCALE_EXCEEDED

    def test_rejects_out_of_bounds(self):
        too_big = str(MONEY_MAX + Decimal(1)).split(".")[0]
        with pytest.raises(MoneyError) as excinfo:
            parse_money(too_big)
        assert excinfo.value.reason is Reason.MONEY_OUT_OF_BOUNDS

    def test_negative_zero_is_normalised(self):
        """``-0.00`` and ``0.00`` must share one representation.

        They compare equal but format differently, which is enough to make two
        financially identical builds produce different digests.
        """
        assert format_money(parse_money("-0.00")) == format_money(parse_money("0.00"))
        assert "-" not in format_money(parse_money("-0.0"))

    def test_decimal_input_round_trips(self):
        assert parse_money(Decimal("12.30")) == Decimal("12.30")

    def test_int_input_allowed(self):
        assert parse_money(7) == Decimal(7)


class TestCurrency:
    @pytest.mark.parametrize("code", ["USD", "EUR", "JPY"])
    def test_accepts_iso_shaped_codes(self, code):
        assert parse_currency(code) == code

    @pytest.mark.parametrize(
        "code,reason",
        [
            (None, Reason.CURRENCY_MISSING),
            ("", Reason.CURRENCY_MISSING),
            ("  ", Reason.CURRENCY_MISSING),
            ("usd", Reason.CURRENCY_UNSUPPORTED),
            ("US", Reason.CURRENCY_UNSUPPORTED),
            ("USDT", Reason.CURRENCY_UNSUPPORTED),
            (840, Reason.CURRENCY_UNSUPPORTED),
        ],
    )
    def test_rejects_bad_codes(self, code, reason):
        with pytest.raises(MoneyError) as excinfo:
            parse_currency(code)
        assert excinfo.value.reason is reason


class TestDecimalContext:
    def test_process_precision_covers_the_money_bound(self):
        """The default 28-digit context is narrower than Decimal(38,12).

        Importing :mod:`analytics.money` widens it. Without that, ordinary
        arithmetic *outside* the guarded helpers - a negation, a subtraction -
        rounds a 32-digit amount before any guard can see it, and the loss is
        invisible at the call site.
        """
        assert getcontext().prec >= MONEY_CONTEXT_PRECISION

    def test_negation_of_a_wide_amount_is_exact(self):
        big = parse_money("9" * 20 + ".999999999999")
        assert -(-big) == big
        assert (-big).as_tuple().digits == big.as_tuple().digits


class TestCheckedSum:
    def test_exact_cancellation_without_rounding(self):
        """Huge opposite terms must cancel exactly. P08-U-09."""
        big = Decimal("9" * 20 + ".999999999999")
        assert checked_sum([big, -big, Decimal("0.01")]) == Decimal("0.01")

    def test_overflow_is_reported_not_truncated(self):
        with pytest.raises(MoneyError) as excinfo:
            checked_sum([MONEY_MAX, MONEY_MAX])
        assert excinfo.value.reason is Reason.MONEY_OUT_OF_BOUNDS

    def test_empty_sum_is_zero(self):
        assert checked_sum([]) == Decimal(0)

    def test_many_small_terms_stay_exact(self):
        terms = [Decimal("0.01")] * 1000
        assert checked_sum(terms) == Decimal("10.00")


class TestQuantize:
    def test_refuses_to_discard_significant_digits(self):
        with pytest.raises(MoneyError) as excinfo:
            quantize_money(Decimal("0.005"), scale=2)
        assert excinfo.value.reason is Reason.MONEY_SCALE_EXCEEDED

    def test_lossless_quantize_succeeds(self):
        assert quantize_money(Decimal("1.5"), scale=2) == Decimal("1.50")


class TestMoneyType:
    def test_refuses_cross_currency_addition(self):
        """INV08-07 enforced by the type, not by reviewer vigilance."""
        with pytest.raises(MoneyError) as excinfo:
            Money.of("1.00", "USD") + Money.of("1.00", "EUR")
        assert excinfo.value.reason is Reason.CURRENCY_MIXED

    def test_same_currency_addition_works(self):
        total = Money.of("1.50", "USD") + Money.of("2.25", "USD")
        assert total.amount == Decimal("3.75")
        assert total.currency == "USD"


class TestCurrencyBuckets:
    def test_keeps_currencies_separate(self):
        buckets = CurrencyBuckets()
        buckets.add("USD", "gross", Decimal("10.00"))
        buckets.add("EUR", "gross", Decimal("5.00"))
        assert buckets.get("USD", "gross") == Decimal("10.00")
        assert buckets.get("EUR", "gross") == Decimal("5.00")
        assert buckets.currencies() == ["EUR", "USD"]

    def test_offers_no_grand_total_api(self):
        """There is deliberately no method that sums across currencies."""
        buckets = CurrencyBuckets()
        for name in dir(buckets):
            assert name not in ("total", "grand_total", "sum_all")

    def test_wire_form_is_strings(self):
        buckets = CurrencyBuckets()
        buckets.add("USD", "gross", Decimal("10.5"))
        wire = buckets.as_wire()
        assert wire == {"USD": {"gross": "10.5"}}
        assert isinstance(wire["USD"]["gross"], str)

    def test_rejects_unknown_currency_shape(self):
        buckets = CurrencyBuckets()
        with pytest.raises(MoneyError):
            buckets.add("usd", "gross", Decimal("1"))


def test_normalize_money_preserves_nonzero():
    assert normalize_money(Decimal("1.2300")) == Decimal("1.2300")


def test_format_money_rejects_non_finite():
    with pytest.raises(MoneyError):
        format_money(Decimal("NaN"))

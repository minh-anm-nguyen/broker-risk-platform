"""Strict scalar decoding.

Covers WF2-L-01, WF2-L-02, WF2-L-07, WF2-L-08, WF2-L-10, WF2-A-10,
WF2-E-07, WF2-E-10 and WF2-N-06, plus the first half of INV-10.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from mt5_connector.scalars import (
    MAX_DECIMAL_DIGITS,
    ScalarError,
    format_money,
    format_timestamp,
    parse_enum,
    parse_identifier,
    parse_money,
    parse_sequence,
    parse_timestamp,
)

pytestmark = pytest.mark.unit


class TestMoney:
    def test_decimal_string_round_trips_exactly(self) -> None:
        assert parse_money("1.10000", field="price") == Decimal("1.10000")
        # The scale survives: "1.10" and "1.1" are the same number but not the
        # same canonical text, and digests depend on the text.
        assert format_money(parse_money("1.10", field="price")) == "1.10"

    def test_a_json_float_is_refused_not_coerced(self) -> None:
        """WF2-L-01, WF2-E-07: the central rule of this module."""
        with pytest.raises(ScalarError) as excinfo:
            parse_money(1.10000, field="price")
        assert excinfo.value.observed_type == "float"
        assert "JSON strings" in excinfo.value.reason

    def test_a_float_is_refused_even_when_it_looks_exact(self) -> None:
        # 0.5 is exactly representable; the rule is about the *channel*, not
        # about whether this particular value happened to survive.
        with pytest.raises(ScalarError):
            parse_money(0.5, field="price")

    def test_rounding_a_float_is_never_offered_as_a_fallback(self) -> None:
        with pytest.raises(ScalarError):
            parse_money(0.1 + 0.2, field="balance")

    @pytest.mark.parametrize("literal", ["NaN", "nan", "Infinity", "-Infinity", "inf", "-inf"])
    def test_non_finite_literals_are_refused(self, literal: str) -> None:
        """WF2-L-02: Decimal('NaN') constructs happily and poisons every sum."""
        with pytest.raises(ScalarError):
            parse_money(literal, field="commission")

    def test_decimal_nan_instance_is_refused(self) -> None:
        with pytest.raises(ScalarError):
            parse_money(Decimal("NaN"), field="commission")

    @pytest.mark.parametrize(
        "literal",
        ["1e3", "1E3", "1_000", "1,000", "+1.5", "01.5", " ", "", "0x10", "1.2.3", "--1"],
    )
    def test_exponent_separator_and_sign_forms_are_refused(self, literal: str) -> None:
        with pytest.raises(ScalarError):
            parse_money(literal, field="balance")

    def test_booleans_are_refused_before_integers(self) -> None:
        # True is an int in Python and would otherwise decode as the amount 1.
        with pytest.raises(ScalarError) as excinfo:
            parse_money(True, field="balance")
        assert excinfo.value.observed_type == "bool"

    def test_integers_are_accepted(self) -> None:
        assert parse_money(10, field="balance") == Decimal(10)
        assert parse_money(-3, field="commission") == Decimal(-3)

    def test_negative_amounts_are_preserved(self) -> None:
        """WF2-L-03: a fee is money leaving; its sign is data, not a defect."""
        assert parse_money("-0.01", field="commission") == Decimal("-0.01")

    def test_excessive_digits_are_bounded(self) -> None:
        """WF2-L-10: arbitrary precision makes a huge literal an attack."""
        huge = "1" * (MAX_DECIMAL_DIGITS + 5)
        with pytest.raises(ScalarError):
            parse_money(huge, field="balance")

    def test_overlong_text_is_refused_before_parsing(self) -> None:
        with pytest.raises(ScalarError):
            parse_money("1." + "0" * 200, field="balance")

    def test_format_money_normalises_negative_zero(self) -> None:
        # "-0.00" and "0.00" are the same amount; two digests for one value
        # would make every golden comparison flaky.
        assert format_money(Decimal("-0.00")) == "0.00"
        assert format_money(Decimal("0.00")) == "0.00"

    def test_format_money_never_uses_exponent_notation(self) -> None:
        assert "E" not in format_money(Decimal("1E+10"))
        assert "e" not in format_money(Decimal("0.00000001"))


class TestTimestamps:
    def test_utc_timestamp_parses_to_utc(self) -> None:
        parsed = parse_timestamp("2026-10-30T05:32:01.123Z", field="executed_at")
        assert parsed == datetime(2026, 10, 30, 5, 32, 1, 123_000, tzinfo=UTC)
        assert parsed.tzinfo is UTC

    def test_offset_is_normalised_without_changing_the_instant(self) -> None:
        """WF2-A-10: normalise to UTC, never rewrite the source instant."""
        parsed = parse_timestamp("2026-10-30T12:32:01+07:00", field="executed_at")
        assert parsed == datetime(2026, 10, 30, 5, 32, 1, tzinfo=UTC)
        assert format_timestamp(parsed) == "2026-10-30T05:32:01.000Z"

    def test_a_naive_timestamp_is_refused(self) -> None:
        """WF2-L-08: no offset is an unknown offset, not UTC."""
        with pytest.raises(ScalarError) as excinfo:
            parse_timestamp("2026-10-30T05:32:01", field="executed_at")
        assert "unknown offset" in excinfo.value.reason

    @pytest.mark.parametrize(
        "value",
        [
            "2026-10-30",
            "30/10/2026 05:32:01Z",
            "2026-13-01T00:00:00Z",
            "2026-10-32T00:00:00Z",
            "not a time",
            "",
        ],
    )
    def test_invalid_calendar_values_are_refused(self, value: str) -> None:
        with pytest.raises(ScalarError):
            parse_timestamp(value, field="executed_at")

    def test_implausible_years_are_refused(self) -> None:
        for value in ("0001-01-01T00:00:00Z", "9999-12-31T23:59:59Z"):
            with pytest.raises(ScalarError):
                parse_timestamp(value, field="executed_at")

    def test_a_non_string_is_refused(self) -> None:
        with pytest.raises(ScalarError):
            parse_timestamp(1762000000, field="executed_at")


class TestIdentifiers:
    def test_blank_identifiers_are_refused(self) -> None:
        """WF2-L-07: an empty key propagates as a silent join failure."""
        for value in ("", "   ", "\t"):
            with pytest.raises(ScalarError):
                parse_identifier(value, field="account_id")

    def test_surrounding_whitespace_is_refused_not_trimmed(self) -> None:
        # Trimming would make " acct-1" and "acct-1" the same key here and
        # two different keys downstream.
        with pytest.raises(ScalarError):
            parse_identifier(" acct-0001", field="account_id")

    def test_control_characters_are_refused(self) -> None:
        """WF2-N-06: a newline in an identifier is a log-injection primitive."""
        for value in ("acct\n0001", "acct\x000001", "acct\r\n0001"):
            with pytest.raises(ScalarError):
                parse_identifier(value, field="account_id")

    def test_length_is_bounded(self) -> None:
        with pytest.raises(ScalarError):
            parse_identifier("a" * 200, field="account_id")

    def test_non_ascii_identifiers_are_accepted(self) -> None:
        """WF2-E-10: a key is bytes we must not mangle, not English."""
        assert parse_identifier("acct-日本-01", field="account_id") == "acct-日本-01"


class TestSequencesAndEnums:
    def test_sequence_must_be_an_integer(self) -> None:
        assert parse_sequence(7, field="source_seq") == 7
        for bad in ("7", 7.0, True, None):
            with pytest.raises(ScalarError):
                parse_sequence(bad, field="source_seq")

    def test_sequence_minimum_is_enforced(self) -> None:
        with pytest.raises(ScalarError):
            parse_sequence(0, field="source_seq", minimum=1)

    def test_sequence_is_bounded_to_64_bits(self) -> None:
        with pytest.raises(ScalarError):
            parse_sequence(2**64, field="source_seq")

    def test_unknown_enum_member_is_refused_not_passed_through(self) -> None:
        """WF2-E-12: forwarding an unmapped value makes downstream guess."""
        allowed = frozenset({"BUY", "SELL"})
        assert parse_enum("BUY", field="side", allowed=allowed) == "BUY"
        for bad in ("buy", "LONG", "", 1):
            with pytest.raises(ScalarError):
                parse_enum(bad, field="side", allowed=allowed)


class TestErrorPayloadSafety:
    def test_error_details_never_carry_the_offending_value(self) -> None:
        """INV-15: this error is logged, so the value must not travel in it."""
        try:
            parse_money("98765.4321", field="balance")
        except ScalarError:  # pragma: no cover - valid value
            pytest.fail("a valid amount must not raise")
        with pytest.raises(ScalarError) as excinfo:
            parse_money("98765.4321e9", field="balance")
        rendered = str(excinfo.value.as_details())
        assert "98765" not in rendered
        assert excinfo.value.as_details()["field"] == "balance"

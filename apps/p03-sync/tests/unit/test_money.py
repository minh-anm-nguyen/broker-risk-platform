"""Money parsing: exactness, and refusal rather than repair.

Covers design group H and INV-12. The through-line of every case here is that
a monetary value must survive unchanged or be rejected -- there is no third
acceptable outcome, because a quietly adjusted number is a wrong number that
reconciliation downstream cannot distinguish from a real one.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from trading_sync.domain.money import MONEY_SCALE, parse_money, parse_positive_money
from trading_sync.errors import InvalidMoneyError

pytestmark = pytest.mark.unit


@pytest.mark.whatif("WF3-085", "WF3-093")
@pytest.mark.parametrize(
    "raw",
    ["-0.01", "0.01", "1.00001", "-1234.567890123456"[:19], "0", "-0", "1000000.000001"],
)
def test_exact_decimal_strings_round_trip(raw: str) -> None:
    """A valid value parses to exactly itself, with sign and scale intact."""
    assert parse_money(raw, field="commission") == Decimal(raw)


@pytest.mark.whatif("WF3-086")
@pytest.mark.parametrize("raw", [0.1, 1.0, -2.5, 3])
def test_json_numbers_are_rejected(raw: object) -> None:
    """A JSON number has already been through a binary float.

    ``0.1`` is not 0.1 by the time Python sees it, so accepting one would store
    a value the source never sent (AS-08, INV-12).
    """
    with pytest.raises(InvalidMoneyError) as caught:
        parse_money(raw, field="commission")
    assert caught.value.reason_code == "INVALID_MONEY"


@pytest.mark.whatif("WF3-087")
@pytest.mark.parametrize("raw", ["NaN", "-NaN", "Infinity", "-Infinity", "inf", "sNaN"])
def test_non_finite_values_are_rejected(raw: str) -> None:
    """``Decimal`` parses these happily; none of them is money."""
    with pytest.raises(InvalidMoneyError):
        parse_money(raw, field="balance")


@pytest.mark.whatif("WF3-089")
def test_thirteenth_fractional_digit_is_rejected_not_rounded() -> None:
    """Scale beyond policy is refused here rather than rounded by PostgreSQL."""
    with pytest.raises(InvalidMoneyError) as caught:
        parse_money("-0.0000000000001", field="commission")
    assert caught.value.safe_details["scale"] == 13
    assert caught.value.safe_details["max_scale"] == MONEY_SCALE


@pytest.mark.whatif("WF3-089")
def test_twelfth_fractional_digit_is_accepted() -> None:
    """The boundary itself is valid; only beyond it is refused."""
    assert parse_money("-0.000000000001", field="commission") == Decimal("-0.000000000001")


@pytest.mark.whatif("WF3-088")
def test_value_beyond_numeric_range_is_rejected() -> None:
    """27 integer digits will not fit NUMERIC(38,12); overflow is not an option."""
    with pytest.raises(InvalidMoneyError) as caught:
        parse_money("1" + "0" * 26, field="balance")
    assert caught.value.safe_details["integer_digits"] == 27


@pytest.mark.whatif("WF3-088")
def test_largest_in_range_value_is_accepted() -> None:
    assert parse_money("9" * 26 + ".000000000001", field="balance") is not None


@pytest.mark.whatif("WF3-086")
@pytest.mark.parametrize("raw", ["1e5", "1E5", "1.5e-3"])
def test_exponent_notation_is_rejected(raw: str) -> None:
    """Plain decimal strings only: exponent forms are outside the contract."""
    with pytest.raises(InvalidMoneyError):
        parse_money(raw, field="price")


@pytest.mark.whatif("WF3-086")
@pytest.mark.parametrize("raw", [" 1.00", "1.00 ", "1_000.00", "", "abc", "--1", "1.2.3"])
def test_malformed_strings_are_rejected(raw: str) -> None:
    """``Decimal`` silently strips whitespace and underscores; we do not."""
    with pytest.raises(InvalidMoneyError):
        parse_money(raw, field="balance")


@pytest.mark.whatif("WF3-070")
@pytest.mark.parametrize("raw", ["0", "-5000.00", "0.00"])
def test_zero_and_negative_balances_are_preserved(raw: str) -> None:
    """A balance may legitimately be zero or negative.

    Inventing a positivity rule would reject real source data (WF3-070).
    """
    assert parse_money(raw, field="balance") == Decimal(raw)


@pytest.mark.whatif("WF3-083")
@pytest.mark.parametrize("raw", ["0", "-0.01", "0.000"])
def test_prices_and_volumes_must_be_positive(raw: str) -> None:
    """A non-positive volume is not a value to store faithfully; it is malformed."""
    with pytest.raises(InvalidMoneyError):
        parse_positive_money(raw, field="volume_lots")


@pytest.mark.whatif("WF3-092")
def test_error_details_name_the_field_without_leaking_the_value() -> None:
    """Diagnostics must identify where, not restate the payload (design 11.6)."""
    with pytest.raises(InvalidMoneyError) as caught:
        parse_money("not-a-number", field="swap")
    assert caught.value.safe_details == {"field": "swap"}
    assert "not-a-number" not in str(caught.value)

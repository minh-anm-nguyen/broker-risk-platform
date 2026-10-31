"""Decimal accounting: the golden vector and the rejection rules.

The golden vector in :func:`test_golden_vector_from_system_design` is the one
in SYSTEM_DESIGN section 3.4, reproduced here with the expected values written
as literals. It is deliberately *not* computed from the implementation: an
oracle derived from the code under test proves only that the code agrees with
itself.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from simulator.domain import accounting as acc
from simulator.domain.models import (
    Mark,
    Side,
    UnsupportedOperationError,
    ValidationError,
    symbol_spec,
)

pytestmark = pytest.mark.unit

EURUSD = symbol_spec("EURUSD")
T0 = datetime(2026, 10, 30, 5, 0, 0, tzinfo=UTC)


def mark(bid: str, ask: str) -> Mark:
    return Mark(symbol="EURUSD", bid=Decimal(bid), ask=Decimal(ask), market_time=T0)


def test_golden_vector_from_system_design() -> None:
    """Section 3.4, step by step, to the cent."""
    balance = Decimal("10000.00")
    assert acc.format_decimal(balance) == "10000.00"

    # Step 1: OPEN BUY 0.50 lot EURUSD @ 1.10000, commission -2.50.
    balance = acc.quantize_money(
        balance + acc.balance_delta(Decimal("0.00"), Decimal("-2.50"), Decimal("0.00"))
    )
    assert acc.format_decimal(balance) == "9997.50"

    # Step 2: mark bid 1.10200 -> unrealized +100.00, equity 10097.50.
    unrealized = acc.unrealized_pnl(
        Side.BUY, Decimal("1.10000"), Decimal("0.50"), EURUSD, mark("1.10200", "1.10210")
    )
    assert acc.format_decimal(unrealized) == "100.00"
    assert acc.format_decimal(acc.equity(balance, unrealized)) == "10097.50"

    # Step 3: CLOSE 0.20 lot @ 1.10300, commission -1.00, swap -0.50.
    gross = acc.gross_pnl(
        Side.BUY, Decimal("1.10000"), Decimal("1.10300"), Decimal("0.20"), EURUSD
    )
    assert acc.format_decimal(gross) == "60.00"
    balance = acc.quantize_money(
        balance + acc.balance_delta(gross, Decimal("-1.00"), Decimal("-0.50"))
    )
    assert acc.format_decimal(balance) == "10056.00"

    # Step 4: mark bid 1.10300, remaining 0.30 -> unrealized +90.00.
    unrealized = acc.unrealized_pnl(
        Side.BUY, Decimal("1.10000"), Decimal("0.30"), EURUSD, mark("1.10300", "1.10310")
    )
    assert acc.format_decimal(unrealized) == "90.00"
    assert acc.format_decimal(acc.equity(balance, unrealized)) == "10146.00"


def test_sell_side_pnl_signs() -> None:
    """WF-021: a short profits when the price falls, and vice versa."""
    falling = acc.gross_pnl(
        Side.SELL, Decimal("1.10000"), Decimal("1.09000"), Decimal("1.00"), EURUSD
    )
    rising = acc.gross_pnl(
        Side.SELL, Decimal("1.10000"), Decimal("1.11000"), Decimal("1.00"), EURUSD
    )
    assert acc.format_decimal(falling) == "1000.00"
    assert acc.format_decimal(rising) == "-1000.00"
    # And the long is the exact mirror.
    assert acc.gross_pnl(
        Side.BUY, Decimal("1.10000"), Decimal("1.09000"), Decimal("1.00"), EURUSD
    ) == -falling


def test_long_values_at_bid_short_at_ask() -> None:
    """Using one side of the quote for both would gift every position a spread."""
    quote = mark("1.10000", "1.10050")
    long_pnl = acc.unrealized_pnl(
        Side.BUY, Decimal("1.10000"), Decimal("1.00"), EURUSD, quote
    )
    short_pnl = acc.unrealized_pnl(
        Side.SELL, Decimal("1.10000"), Decimal("1.00"), EURUSD, quote
    )
    assert acc.format_decimal(long_pnl) == "0.00"  # closed at the bid, where it opened
    assert acc.format_decimal(short_pnl) == "-50.00"  # closed at the ask, one spread away


@pytest.mark.parametrize(
    ("commission", "swap", "expected"),
    [
        ("-2.50", "0.00", "-2.50"),
        ("0.00", "0.00", "0.00"),
        # WF-022: a positive swap is a legal rebate and the sign is carried
        # through rather than normalized away.
        ("-1.00", "0.40", "-0.60"),
        ("1.50", "0.50", "2.00"),
    ],
)
def test_balance_delta_carries_signed_fees(commission: str, swap: str, expected: str) -> None:
    delta = acc.balance_delta(Decimal("0.00"), Decimal(commission), Decimal(swap))
    assert acc.format_decimal(delta) == expected


def test_decimal_arithmetic_is_exact() -> None:
    """WF-020: the classic float trap must not appear."""
    assert acc.format_decimal(acc.quantize_money(Decimal("0.1") + Decimal("0.2"))) == "0.30"
    assert 0.1 + 0.2 != 0.3  # the float version, for contrast
    total = Decimal("0.00")
    for _ in range(1000):
        total += acc.quantize_money(Decimal("0.01"))
    assert acc.format_decimal(total) == "10.00"


def test_negative_zero_is_normalized() -> None:
    """A balance of -0.00 is a formatting artefact, not a fact."""
    assert acc.format_decimal(Decimal("-0.00")) == "0.00"
    assert acc.format_decimal(acc.quantize_money(Decimal("-0.001"))) == "0.00"
    assert acc.format_decimal(acc.balance_delta(Decimal("1.00"), Decimal("-1.00"), Decimal("0.00"))) == "0.00"


def test_format_decimal_never_uses_exponent_notation() -> None:
    """Scientific notation is valid JSON but breaks string equality."""
    assert acc.format_decimal(Decimal("1E+6")) == "1000000"
    assert acc.format_decimal(Decimal("1e-5")) == "0.00001"
    assert "E" not in acc.format_decimal(Decimal("123456789012.34"))


def test_rounding_is_half_up_and_documented() -> None:
    assert acc.format_decimal(acc.quantize_money(Decimal("1.005"))) == "1.01"
    assert acc.format_decimal(acc.quantize_money(Decimal("1.015"))) == "1.02"
    assert acc.format_decimal(acc.quantize_money(Decimal("-1.005"))) == "-1.01"
    assert acc.ACCOUNTING_POLICY_VERSION == "1.0"


@pytest.mark.parametrize("raw", ["NaN", "-NaN", "Infinity", "-Infinity", "sNaN"])
def test_non_finite_decimals_are_rejected(raw: str) -> None:
    with pytest.raises(ValidationError):
        acc.parse_decimal(raw, field="amount")


@pytest.mark.parametrize("raw", ["abc", "", "   ", "1.2.3", "1,5", "0x10"])
def test_unparseable_strings_are_rejected(raw: str) -> None:
    with pytest.raises(ValidationError):
        acc.parse_decimal(raw, field="amount")


@pytest.mark.parametrize("raw", [1.5, 0.1, True, False, None, [], {}])
def test_floats_and_other_types_are_rejected_not_coerced(raw: object) -> None:
    """A float price would import the caller's rounding error into storage."""
    with pytest.raises(ValidationError):
        acc.parse_decimal(raw, field="price")


def test_absurd_magnitudes_are_rejected_before_allocation() -> None:
    """WF-067: 1e9999 parses as a finite Decimal, so finiteness is not enough."""
    huge = acc.parse_decimal("1e9999", field="amount")
    assert huge.is_finite()
    with pytest.raises(ValidationError) as excinfo:
        acc.validate_money(huge, field="amount")
    assert "magnitude" in excinfo.value.message


@pytest.mark.parametrize("raw", ["0", "-0.01", "0.001", "1000", "100.01"])
def test_invalid_volumes_are_rejected(raw: str) -> None:
    """WF-018: zero, negative, off-grid and over-maximum volumes."""
    with pytest.raises(ValidationError):
        acc.validate_volume(Decimal(raw), EURUSD)


@pytest.mark.parametrize("raw", ["0.01", "0.50", "1.00", "100"])
def test_valid_volumes_are_accepted(raw: str) -> None:
    assert acc.validate_volume(Decimal(raw), EURUSD) == Decimal(raw)


@pytest.mark.parametrize("raw", ["0", "-1.10000", "1.100005", "1e9999"])
def test_invalid_prices_are_rejected(raw: str) -> None:
    """WF-019: zero, negative, too many digits, absurd magnitude."""
    with pytest.raises(ValidationError):
        acc.validate_price(Decimal(raw), EURUSD)


def test_money_input_precision_is_rejected_not_rounded() -> None:
    """Rounding a caller's over-precise fee hides which way the cent went."""
    with pytest.raises(ValidationError):
        acc.validate_money(Decimal("-2.505"), field="commission")
    assert acc.validate_money(Decimal("-2.50"), field="commission") == Decimal("-2.50")


def test_inverted_quote_is_rejected_in_normal_mode() -> None:
    with pytest.raises(ValidationError):
        acc.validate_quote(Decimal("1.10200"), Decimal("1.10100"), EURUSD)
    assert acc.validate_quote(Decimal("1.10100"), Decimal("1.10100"), EURUSD) == (
        Decimal("1.10100"),
        Decimal("1.10100"),
    )


def test_weighted_average_open_price() -> None:
    """WF-027: a same-side increase blends the cost basis by volume."""
    blended = acc.weighted_average_price(
        Decimal("1.10000"), Decimal("1.00"), Decimal("1.20000"), Decimal("1.00"), EURUSD
    )
    assert acc.format_decimal(blended) == "1.15000"
    skewed = acc.weighted_average_price(
        Decimal("1.10000"), Decimal("3.00"), Decimal("1.20000"), Decimal("1.00"), EURUSD
    )
    assert acc.format_decimal(skewed) == "1.12500"


def test_fx_conversion_symbols_are_refused_not_approximated() -> None:
    """WF-029: computing USDJPY with USD-quote rules yields a plausible lie."""
    with pytest.raises(UnsupportedOperationError) as excinfo:
        symbol_spec("USDJPY")
    assert excinfo.value.details["reason"] == "FX_CONVERSION_REQUIRED"


def test_unknown_symbol_is_a_validation_error() -> None:
    with pytest.raises(ValidationError):
        symbol_spec("NOT-A-SYMBOL")


def test_intermediate_precision_is_not_lost_at_small_volumes() -> None:
    """Quantizing intermediates would round each step towards zero.

    0.01 lots of a 0.00001 price move is 0.01 USD; 100 such positions must sum
    to 1.00 and not to 0.00.
    """
    single = acc.gross_pnl(
        Side.BUY, Decimal("1.10000"), Decimal("1.10001"), Decimal("0.01"), EURUSD
    )
    assert acc.format_decimal(single) == "0.01"
    total = acc.quantize_money(single * 100)
    assert acc.format_decimal(total) == "1.00"

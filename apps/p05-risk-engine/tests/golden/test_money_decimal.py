"""Domain H -- decimal precision, rounding, overflow and canonical money.

Covers WF5-085 through WF5-096.
"""

from __future__ import annotations

from decimal import ROUND_HALF_EVEN, ROUND_HALF_UP, Decimal

import pytest

from risk.domain.money import (
    CURRENCY_MINOR_UNITS,
    MAX_MONEY_SCALE,
    Money,
    MoneySemantic,
    Quantity,
    canonical_decimal_str,
    parse_decimal,
    quantize_display,
)
from risk.errors import ContractError, ReasonCode
from risk.manifest import digest
from tests.reference.oracle import D, reference_quantize, reference_realized

pytestmark = [pytest.mark.golden, pytest.mark.p0]

USD = "USD"


def _money(amount: str, semantic: MoneySemantic = MoneySemantic.REALIZED_GROSS) -> Money:
    return Money(D(amount), USD, semantic)


@pytest.mark.wf5("WF5-085")
def test_decimal_addition_is_exact(evidence):
    """0.1 + 0.2 is 0.3 exactly, with no binary floating-point residue."""
    total = _money("0.1") + _money("0.2")
    assert total.amount == D("0.3")
    # Belt and braces: the float version of the same sum is *not* 0.3, so this
    # assertion would fail on any implementation that routed through float.
    assert float("0.1") + float("0.2") != float("0.3")
    evidence(
        expected_values={"sum": "0.3"},
        observed_values={"sum": total.canonical()},
        expected_status="READY",
        observed_status="READY",
    )


@pytest.mark.wf5("WF5-086")
def test_half_even_tie_at_display_boundary(evidence):
    """A -0.005 commission ties; HALF_EVEN takes it to 0.00 and keeps the source."""
    commission = _money("-0.005", MoneySemantic.COMMISSION)
    displayed = commission.display(ROUND_HALF_EVEN)

    assert displayed == D("0.00")
    assert str(displayed) == "0.00"  # never "-0.00"
    # The internal value is untouched: display is a presentation step, not a
    # mutation of the amount the audit trail refers to.
    assert commission.amount == D("-0.005")
    # The same tie under the v2 policy goes the other way, which is why the two
    # policies cannot share a cache entry.
    assert commission.display(ROUND_HALF_UP) == D("-0.01")
    evidence(
        expected_values={"display_half_even": "0.00", "internal": "-0.005"},
        observed_values={"display_half_even": str(displayed), "internal": commission.canonical()},
    )


@pytest.mark.wf5("WF5-087", "WF5-137", oracle="GOLDEN_REFERENCE")
def test_thousand_small_commissions_sum_before_rounding(evidence):
    """1000 x 0.005 is 5.00, not the 0.00 that per-row rounding would give."""
    deals = [{"gross": "0.00", "commission": "0.005", "swap": "0.00"} for _ in range(1000)]
    expected = reference_realized(deals)

    total = Money.zero(USD, MoneySemantic.COMMISSION)
    for _ in range(1000):
        total = total + _money("0.005", MoneySemantic.COMMISSION)

    assert total.amount == expected["commission"] == D("5.000")
    assert total.display(ROUND_HALF_EVEN) == D("5.00")

    # The failure mode, made explicit: rounding each row first loses everything.
    rounded_per_row = sum(
        (quantize_display(D("0.005"), 2, rounding=ROUND_HALF_EVEN) for _ in range(1000)),
        D(0),
    )
    assert rounded_per_row == D("0.00")
    evidence(
        expected_values={"sum": "5.000", "naive_per_row": "0.00"},
        observed_values={"sum": total.canonical(), "naive_per_row": str(rounded_per_row)},
    )


@pytest.mark.wf5("WF5-088")
@pytest.mark.parametrize("raw", [0.1, 1.0, -2.5, 0.0, True, False])
def test_float_and_bool_transport_rejected(raw, evidence):
    """A JSON float never becomes money, however convenient that would be."""
    with pytest.raises(ContractError) as exc:
        parse_decimal(raw, field="commission")
    assert exc.value.reason is ReasonCode.DECIMAL_STRING_REQUIRED
    evidence(expected_reason_codes=("DECIMAL_STRING_REQUIRED",),
             observed_reason_codes=(str(exc.value.reason),))


@pytest.mark.wf5("WF5-089", "WF5-090")
@pytest.mark.parametrize(
    "raw,expected_reason",
    [
        ("NaN", ReasonCode.INVALID_DECIMAL),
        ("nan", ReasonCode.INVALID_DECIMAL),
        ("Infinity", ReasonCode.INVALID_DECIMAL),
        ("-Infinity", ReasonCode.INVALID_DECIMAL),
        ("inf", ReasonCode.INVALID_DECIMAL),
        ("sNaN", ReasonCode.INVALID_DECIMAL),
    ],
)
def test_nonfinite_tokens_rejected(raw, expected_reason, evidence):
    """NaN and the infinities are refused at the shape check, before arithmetic."""
    with pytest.raises(ContractError) as exc:
        parse_decimal(raw, field="swap")
    assert exc.value.reason is expected_reason
    evidence(observed_reason_codes=(str(exc.value.reason),))


@pytest.mark.wf5("WF5-089")
def test_nonfinite_decimal_object_rejected():
    """A Decimal('NaN') constructed elsewhere still cannot become Money."""
    with pytest.raises(ContractError) as exc:
        parse_decimal(Decimal("NaN"), field="gross")
    assert exc.value.reason is ReasonCode.NONFINITE_MONEY


@pytest.mark.wf5("WF5-091")
def test_huge_exponent_bounded_before_arithmetic(evidence):
    """1E+99999999 is rejected on magnitude, not expanded into memory."""
    import time

    start = time.monotonic()
    with pytest.raises(ContractError) as exc:
        parse_decimal("1E+99999999", field="gross")
    elapsed = time.monotonic() - start

    assert exc.value.reason is ReasonCode.DECIMAL_MAGNITUDE_EXCEEDED
    # The guard is a comparison on the adjusted exponent, so rejection is
    # effectively instant. A multi-second rejection would mean the digits were
    # materialised first.
    assert elapsed < 1.0
    evidence(
        expected_reason_codes=("DECIMAL_MAGNITUDE_EXCEEDED",),
        observed_reason_codes=(str(exc.value.reason),),
        observed_values={"elapsed_s": f"{elapsed:.4f}"},
    )


@pytest.mark.wf5("WF5-246")
def test_adversarial_long_literal_rejected_by_length(evidence):
    """A 10 MB numeric literal is refused by the length guard, not parsed."""
    payload = "9" * 10_000_000
    with pytest.raises(ContractError) as exc:
        parse_decimal(payload, field="gross")
    assert exc.value.reason is ReasonCode.DECIMAL_MAGNITUDE_EXCEEDED
    # The error carries the length, never the value: a 10 MB log line is its own
    # denial of service.
    assert exc.value.detail["length"] == 10_000_000
    assert payload not in str(exc.value)
    evidence(observed_reason_codes=(str(exc.value.reason),))


@pytest.mark.wf5("WF5-092")
def test_negative_zero_canonicalised_but_scale_preserved(evidence):
    """-0.00 and 0.00 hash identically; 0.00 and 0.0 do not pretend to be equal strings."""
    assert canonical_decimal_str(D("-0.00")) == "0.00"
    assert canonical_decimal_str(D("0.00")) == "0.00"
    assert canonical_decimal_str(D("-0")) == "0"

    # The digest is what matters: two hosts computing a zero with different
    # signs must agree (cross-host oracle of WF5-282).
    assert digest({"v": D("-0.00")}) == digest({"v": D("0.00")})
    assert quantize_display(D("-0.0001"), 2, rounding=ROUND_HALF_EVEN) == D("0.00")
    assert str(quantize_display(D("-0.0001"), 2, rounding=ROUND_HALF_EVEN)) == "0.00"
    evidence(expected_values={"canonical": "0.00"},
             observed_values={"canonical": canonical_decimal_str(D("-0.00"))})


@pytest.mark.wf5("WF5-093")
@pytest.mark.p1
def test_scale_beyond_storage_contract_rejected(evidence):
    """A 13-decimal amount is refused, not silently truncated to the column's 12."""
    with pytest.raises(ContractError) as exc:
        parse_decimal("1.9999999999999", field="gross")  # 13 fractional digits
    assert exc.value.reason is ReasonCode.DECIMAL_SCALE_EXCEEDED
    assert exc.value.detail["max_scale"] == MAX_MONEY_SCALE

    # Exactly at the limit is accepted, so the boundary is tested from both sides.
    assert parse_decimal("1.999999999999", field="gross") == D("1.999999999999")
    evidence(observed_reason_codes=(str(exc.value.reason),))


@pytest.mark.wf5("WF5-094")
@pytest.mark.p1
def test_rounding_policy_change_yields_distinct_output(evidence):
    """HALF_EVEN and HALF_UP disagree on ties, so results cannot share a cache key."""
    ties = ["0.005", "0.015", "0.025", "-0.005", "2.675"]
    half_even = [str(reference_quantize(D(t), 2, ROUND_HALF_EVEN)) for t in ties]
    half_up = [str(reference_quantize(D(t), 2, ROUND_HALF_UP)) for t in ties]

    engine_even = [quantize_display(D(t), 2, rounding=ROUND_HALF_EVEN) for t in ties]
    engine_up = [quantize_display(D(t), 2, rounding=ROUND_HALF_UP) for t in ties]

    # Compared as values, because the engine canonicalises a rounded-away
    # negative into 0.00 while the naive reference still prints -0.00. Equal
    # amount, different rendering -- and the canonical form is the engine's.
    assert engine_even == [D(v) for v in half_even]
    assert engine_up == [D(v) for v in half_up]
    assert engine_even != engine_up
    assert [str(v) for v in engine_even][3] == "0.00"
    evidence(
        expected_values={"half_even": ",".join(half_even), "half_up": ",".join(half_up)},
        observed_values={
            "half_even": ",".join(str(v) for v in engine_even),
            "half_up": ",".join(str(v) for v in engine_up),
        },
    )


@pytest.mark.wf5("WF5-095")
@pytest.mark.p1
def test_sum_order_does_not_change_the_total(evidence):
    """Exact decimal addition is associative, so delivery order is irrelevant."""
    amounts = [D(f"{i}.{i:02d}") for i in range(1, 500)]
    forward = sum(amounts, D(0))
    backward = sum(reversed(amounts), D(0))
    interleaved = sum(amounts[::2] + amounts[1::2], D(0))

    assert forward == backward == interleaved
    # And the digest agrees, which is the property the output hash depends on.
    assert digest({"t": forward}) == digest({"t": interleaved})
    evidence(observed_values={"total": canonical_decimal_str(forward)})


@pytest.mark.wf5("WF5-096")
@pytest.mark.p2
def test_zero_minor_unit_currency_is_not_forced_into_cents(evidence):
    """JPY has no minor unit; the display scale comes from the table, not a default of 2."""
    assert CURRENCY_MINOR_UNITS["JPY"] == 0
    yen = Money(D("1234.56"), "JPY", MoneySemantic.REALIZED_GROSS)
    assert yen.display(ROUND_HALF_EVEN) == D("1235")

    # A currency with no approved scale cannot be displayed at all; guessing
    # would produce a figure with fabricated precision.
    exotic = Money(D("1.00"), "ZZZ", MoneySemantic.REALIZED_GROSS)
    with pytest.raises(ContractError) as exc:
        exotic.display(ROUND_HALF_EVEN)
    assert exc.value.reason is ReasonCode.INVALID_CURRENCY
    evidence(observed_values={"jpy_display": "1235"})


@pytest.mark.wf5("WF5-119", "WF5-098")
def test_money_refuses_cross_currency_addition(evidence):
    """USD + EUR raises. There is no FX rate in scope, so there is no sum."""
    usd = Money(D("10.00"), "USD", MoneySemantic.REALIZED_GROSS)
    eur = Money(D("10.00"), "EUR", MoneySemantic.REALIZED_GROSS)
    with pytest.raises(ContractError) as exc:
        usd + eur
    assert exc.value.reason is ReasonCode.CURRENCY_MISMATCH
    evidence(expected_reason_codes=("CURRENCY_MISMATCH",),
             observed_reason_codes=(str(exc.value.reason),))


def test_money_refuses_cross_semantic_addition():
    """A gross and a commission are both USD and still not addable."""
    gross = Money(D("60.00"), "USD", MoneySemantic.REALIZED_GROSS)
    commission = Money(D("-3.50"), "USD", MoneySemantic.COMMISSION)
    with pytest.raises(ContractError) as exc:
        gross + commission
    assert exc.value.reason is ReasonCode.UNIT_MISMATCH


@pytest.mark.wf5("WF5-148")
def test_quantity_refuses_cross_unit_addition(evidence):
    """100 000 EUR + 100 000 GBP is not 200 000 of anything."""
    eur = Quantity(D("100000"), "EUR")
    gbp = Quantity(D("100000"), "GBP")
    with pytest.raises(ContractError) as exc:
        eur + gbp
    assert exc.value.reason is ReasonCode.UNIT_MISMATCH
    evidence(observed_reason_codes=(str(exc.value.reason),))


def test_money_rejects_non_decimal_amount():
    with pytest.raises(ContractError):
        Money(0.5, "USD", MoneySemantic.REALIZED_GROSS)  # type: ignore[arg-type]


@pytest.mark.parametrize("bad", ["", "   ", "1_000", "1,000", "0x10", "1.2.3", "--1", "1e", "abc"])
def test_malformed_literals_rejected(bad):
    with pytest.raises(ContractError) as exc:
        parse_decimal(bad, field="gross")
    assert exc.value.reason in (ReasonCode.INVALID_DECIMAL, ReasonCode.DECIMAL_MAGNITUDE_EXCEEDED)


def test_digest_refuses_float_input():
    """A float cannot be hashed into permanence."""
    with pytest.raises(ContractError) as exc:
        digest({"amount": 0.1})
    assert exc.value.reason is ReasonCode.DECIMAL_STRING_REQUIRED

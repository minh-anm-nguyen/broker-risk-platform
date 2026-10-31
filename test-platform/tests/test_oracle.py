"""The oracle, checked.

A broken oracle is worse than none: it turns every run into either a false
alarm people learn to ignore or a green light nobody should trust. So the
comparisons get the same treatment they are meant to apply to everything else.

The two cases that matter most are the ones a careless implementation gets
wrong in opposite directions:

* counts agreeing while the sets differ, which a count-based check passes;
* ``"0.00"`` versus ``Decimal("0E-12")``, which a text-based check fails even
  though they are the same amount.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from oracle.identity import SAMPLE_LIMIT, compare_identity_sets
from oracle.money import MoneyOracleError, compare_money, parse_amount, sum_amounts

pytestmark = pytest.mark.unit


# ------------------------------------------------------------------ identity


def test_identical_sets_pass() -> None:
    verdict = compare_identity_sets("deals", {"A", "B"}, {"A", "B"})
    assert verdict.ok
    assert verdict.matched == 2


def test_equal_counts_with_different_members_is_a_failure() -> None:
    """The whole reason this module compares sets.

    One record lost on the way in and one invented on the way out cancel out
    in a count. P09 section 2.3 names this as the insufficient check, and a
    platform reporting it as a pass would hide two defects at once.
    """
    verdict = compare_identity_sets("deals", {"A", "B"}, {"A", "C"})
    assert verdict.source_total == verdict.target_total == 2
    assert not verdict.ok
    assert verdict.missing_sample == ("B",)
    assert verdict.unexpected_sample == ("C",)


def test_a_missing_record_is_reported_by_name() -> None:
    """An operator needs the id, not a tally."""
    verdict = compare_identity_sets("deals", {"A", "B", "C"}, {"A", "C"})
    assert not verdict.ok
    assert "B" in verdict.describe()


def test_an_unexpected_record_is_also_a_failure() -> None:
    """A target holding something the source never had is as wrong as a loss:
    it means a record was invented somewhere."""
    verdict = compare_identity_sets("deals", {"A"}, {"A", "ghost"})
    assert not verdict.ok
    assert verdict.unexpected_total == 1


def test_both_empty_is_a_pass_and_stays_visible_as_one() -> None:
    """Vacuously true. It must stay legible as such: a run that compared
    nothing has proven nothing about data."""
    verdict = compare_identity_sets("deals", set(), set())
    assert verdict.ok
    assert verdict.matched == 0


def test_the_listing_is_capped_but_the_counts_are_not() -> None:
    """A diff of ten thousand ids makes an evidence bundle unreadable; a wrong
    count makes it untrue."""
    source = {f"D-{i:05d}" for i in range(SAMPLE_LIMIT + 10)}
    verdict = compare_identity_sets("deals", source, set())
    assert verdict.missing_total == SAMPLE_LIMIT + 10
    assert len(verdict.missing_sample) == SAMPLE_LIMIT


# --------------------------------------------------------------------- money


def test_zero_written_three_ways_is_one_amount() -> None:
    """The false-alarm case.

    PostgreSQL hands back ``Decimal('0E-12')`` for a column a JSON payload
    rendered as ``"0.00"``. A text comparison would fail on every zero in the
    database and the oracle would be switched off within a day.
    """
    verdict = compare_money("swap", ["0.00", "0", "0.000"], [Decimal("0E-12"), 0, "0"])
    assert verdict.ok
    assert verdict.actual == Decimal(0)


def test_a_real_difference_is_caught_and_quantified() -> None:
    verdict = compare_money("pnl", ["10.00", "5.00"], ["10.00", "4.99"])
    assert not verdict.ok
    assert verdict.difference == Decimal("-0.01")
    assert "-0.01" in verdict.describe()


def test_signs_are_preserved_in_the_sum() -> None:
    """Commission and swap are legitimately negative; a sum over absolute
    values would agree with a sign bug."""
    verdict = compare_money("commission", ["-0.65", "-1.60"], ["-0.65", "-1.60"])
    assert verdict.ok
    assert verdict.expected == Decimal("-2.25")


def test_a_float_is_refused_rather_than_summed() -> None:
    """By the time a value is a float the exact amount is gone, so comparing
    it would be checking the corruption against itself."""
    with pytest.raises(MoneyOracleError, match="float"):
        sum_amounts([1.5], field="pnl")


@pytest.mark.parametrize("value", [True, False])
def test_a_bool_is_refused(value: bool) -> None:
    """``isinstance(True, int)`` is True, so bool must be refused explicitly or
    it would silently count as 1."""
    with pytest.raises(MoneyOracleError):
        parse_amount(value, field="pnl")


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity"])
def test_non_finite_amounts_are_refused(value: str) -> None:
    """A NaN total must not become zero. Zero is a number someone would act
    on."""
    with pytest.raises(MoneyOracleError, match="finite"):
        parse_amount(value, field="pnl")


def test_an_unreadable_string_is_refused_not_skipped() -> None:
    with pytest.raises(MoneyOracleError, match="not a decimal"):
        parse_amount("one dollar", field="pnl")


def test_the_offending_field_is_named() -> None:
    """A refusal over a thousand rows is useless without the index."""
    with pytest.raises(MoneyOracleError, match=r"pnl\[2\]"):
        sum_amounts(["1.00", "2.00", "bad"], field="pnl")


def test_exactness_survives_a_sum_a_float_would_not() -> None:
    """Ten cents, ten times. In binary floating point that is not one dollar,
    which is the entire reason the platform forbids floats for money."""
    verdict = compare_money("pnl", ["0.10"] * 10, [Decimal("1.00")])
    assert verdict.ok
    assert verdict.expected == Decimal("1.00")


def test_no_tolerance_is_applied() -> None:
    """These are sums of exact decimals, not measurements. The smallest
    representable difference is a defect, not noise."""
    verdict = compare_money("pnl", ["1.00"], ["1.0000000001"])
    assert not verdict.ok

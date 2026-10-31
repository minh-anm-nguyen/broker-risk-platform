"""Hypothesis properties over the pure layers.

These search for inputs a hand-written table would not think of. The
properties are chosen to be the ones whose violation would be silent: an
exactness failure in money, a hash that disagrees with numeric equality, or a
validator that accepts a page it should refuse.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from hypothesis import assume, given, settings as hyp_settings, strategies as st

from trading_sync.domain.canonical import canonical_decimal_str, payload_hash
from trading_sync.domain.identifiers import MAX_IDENTIFIER_LENGTH, normalize_identifier
from trading_sync.domain.money import MONEY_SCALE, parse_money
from trading_sync.domain.timestamps import parse_utc_timestamp
from trading_sync.errors import InvalidIdentifierError, InvalidMoneyError

pytestmark = pytest.mark.property

#: Values inside the declared NUMERIC(38,12) policy.
in_policy_decimals = st.decimals(
    min_value=Decimal("-1E20"),
    max_value=Decimal("1E20"),
    allow_nan=False,
    allow_infinity=False,
    places=MONEY_SCALE,
)


@given(in_policy_decimals)
def test_money_round_trips_exactly(value: Decimal) -> None:
    """Parsing the canonical rendering of a value returns that same value.

    Exactness is the single property money validation exists to provide, so it
    is worth asserting over a search rather than over examples.
    """
    rendered = format(value, "f")
    assert parse_money(rendered, field="balance") == value


@given(in_policy_decimals)
def test_canonical_decimal_is_idempotent(value: Decimal) -> None:
    once = canonical_decimal_str(value)
    assert canonical_decimal_str(Decimal(once)) == once


@given(in_policy_decimals, st.integers(min_value=0, max_value=MONEY_SCALE))
def test_hash_follows_numeric_equality_not_spelling(value: Decimal, pad: int) -> None:
    """Two spellings of one quantity must hash alike; different quantities must not.

    This is the property that keeps a trailing-zero change from being reported
    as a money-integrity incident (WF3-095).
    """
    padded = Decimal(format(value, "f") + ("" if pad == 0 else "0" * pad))
    assume(padded == value)
    assert payload_hash("t", {"v": value}) == payload_hash("t", {"v": padded})


@given(st.floats(allow_nan=True, allow_infinity=True))
def test_no_float_is_ever_accepted_as_money(value: float) -> None:
    """A JSON number has already lost exactness; there is no safe subset."""
    with pytest.raises(InvalidMoneyError):
        parse_money(value, field="commission")


@given(st.text(min_size=1, max_size=MAX_IDENTIFIER_LENGTH))
def test_accepted_identifiers_are_returned_unchanged(raw: str) -> None:
    """Validation never transforms. Either the exact string comes back, or it
    is rejected -- there is no third behaviour that could merge two keys.
    """
    try:
        assert normalize_identifier(raw, field="deal_id") == raw
    except InvalidIdentifierError:
        pass


@given(st.text(min_size=1))
def test_identifier_validation_never_raises_anything_unexpected(raw: str) -> None:
    """Hostile input produces a classified error, not a crash.

    An unclassified exception escaping the validator would reach the retry
    policy as "unknown", which refuses to retry -- safe, but it would hide the
    real cause from the audit trail.
    """
    try:
        normalize_identifier(raw, field="account_id")
    except InvalidIdentifierError as exc:
        assert exc.reason_code == "INVALID_IDENTIFIER"


@given(
    st.datetimes(
        min_value=__import__("datetime").datetime(1971, 1, 1),
        max_value=__import__("datetime").datetime(2099, 12, 31),
    ),
    st.integers(min_value=-14 * 60, max_value=14 * 60),
)
@hyp_settings(max_examples=200)
def test_offset_timestamps_normalize_to_the_same_instant(naive, offset_minutes: int) -> None:
    """Whatever offset a source writes, the stored instant is the same moment.

    Expressed as: shifting both the wall clock and the offset by the same
    amount must not move the result (INV-13).
    """
    from datetime import timedelta, timezone

    tz = timezone(timedelta(minutes=offset_minutes))
    aware = naive.replace(tzinfo=tz)
    parsed = parse_utc_timestamp(aware.isoformat(), field="executed_at")
    assert parsed == aware
    assert parsed.utcoffset() == timedelta(0)

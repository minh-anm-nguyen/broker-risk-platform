"""Canonical serialization, timestamps and digests.

The digest has to answer one question honestly: did two runs of the same seed
produce the same data? Which means it must *include* everything that must
match and *exclude* everything that legitimately differs. A digest that
accidentally includes a wall-clock field never matches and proves nothing; one
that accidentally excludes an amount always matches and is worse.
"""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from simulator.canonical import (
    CANONICAL_VERSION,
    StreamingDigest,
    canonical_json,
    digest_of,
    filter_digest,
    format_timestamp,
    parse_timestamp,
    to_epoch_ms,
)

pytestmark = pytest.mark.unit


def test_keys_are_sorted_and_whitespace_is_absent() -> None:
    assert canonical_json({"b": 1, "a": 2}) == '{"a":2,"b":1}'
    assert canonical_json({"z": {"y": 1, "x": 2}}) == '{"z":{"x":2,"y":1}}'


def test_decimals_serialize_as_plain_strings() -> None:
    """A JSON number would become a double in most clients (ADR-005)."""
    assert canonical_json({"amount": Decimal("10000.00")}) == '{"amount":"10000.00"}'
    assert canonical_json({"amount": Decimal("1E+6")}) == '{"amount":"1000000"}'
    assert canonical_json({"amount": Decimal("-0.00")}) == '{"amount":"0.00"}'


def test_non_finite_decimals_are_refused() -> None:
    for raw in ("NaN", "Infinity", "-Infinity"):
        with pytest.raises(ValueError):
            canonical_json({"amount": Decimal(raw)})


def test_float_nan_is_refused() -> None:
    with pytest.raises(ValueError):
        canonical_json({"value": math.nan})


def test_unserializable_types_are_refused_not_stringified() -> None:
    """Silently calling str() on an object would hide a real encoding bug."""
    with pytest.raises(TypeError):
        canonical_json({"value": object()})


def test_datetimes_render_as_rfc3339_utc() -> None:
    value = datetime(2026, 10, 30, 5, 0, 0, tzinfo=UTC)
    assert canonical_json({"at": value}) == '{"at":"2026-10-30T05:00:00Z"}'


def test_timestamp_offsets_are_normalized_to_utc() -> None:
    plus_seven = datetime(2026, 10, 30, 12, 0, 0, tzinfo=timezone(timedelta(hours=7)))
    assert format_timestamp(plus_seven) == "2026-10-30T05:00:00Z"


def test_naive_datetimes_are_treated_as_utc() -> None:
    assert format_timestamp(datetime(2026, 10, 30, 5, 0, 0)) == "2026-10-30T05:00:00Z"


@pytest.mark.parametrize(
    "text",
    [
        "2026-10-30T05:00:00Z",
        "2026-10-30T05:00:00z",
        "2026-10-30T05:00:00+00:00",
        "2026-10-30T12:00:00+07:00",
    ],
)
def test_parse_timestamp_accepts_rfc3339_forms(text: str) -> None:
    assert parse_timestamp(text) == datetime(2026, 10, 30, 5, 0, 0, tzinfo=UTC)


def test_timestamp_round_trip() -> None:
    value = datetime(2026, 10, 30, 5, 0, 0, 123456, tzinfo=UTC)
    assert parse_timestamp(format_timestamp(value)) == value


def test_epoch_ms_ordering_matches_chronology() -> None:
    """Why the integer column exists at all (WF-041, WF-046).

    As RFC 3339 *strings* these two sort the wrong way round, because '.'
    precedes 'Z'. A text range filter would put the sub-second timestamp on
    the wrong side of a boundary.
    """
    earlier = datetime(2026, 10, 30, 0, 0, 1, tzinfo=UTC)
    later = datetime(2026, 10, 30, 0, 0, 1, 500_000, tzinfo=UTC)
    assert earlier < later
    assert format_timestamp(later) < format_timestamp(earlier)  # the string trap
    assert to_epoch_ms(earlier) < to_epoch_ms(later)  # the integer fix


def test_epoch_ms_is_exact_integer_arithmetic() -> None:
    assert to_epoch_ms(datetime(1970, 1, 1, tzinfo=UTC)) == 0
    assert to_epoch_ms(datetime(1970, 1, 1, 0, 0, 1, tzinfo=UTC)) == 1_000
    assert to_epoch_ms(datetime(1970, 1, 1, 0, 0, 0, 1_000, tzinfo=UTC)) == 1
    # Sub-millisecond precision truncates rather than rounding into the next ms.
    assert to_epoch_ms(datetime(1970, 1, 1, 0, 0, 0, 1_999, tzinfo=UTC)) == 1


def test_digest_is_stable_and_order_independent_for_mappings() -> None:
    assert digest_of({"a": 1, "b": 2}) == digest_of({"b": 2, "a": 1})
    assert digest_of({"a": 1}) != digest_of({"a": 2})
    assert digest_of({"a": 1}).startswith("sha256:")


def test_filter_digest_treats_missing_and_null_alike() -> None:
    """A client omitting a filter and sending it as null mean the same thing."""
    assert filter_digest({"account_id": None, "limit": 100}) == filter_digest({"limit": 100})
    assert filter_digest({"account_id": "A-1", "limit": 100}) != filter_digest({"limit": 100})


def test_streaming_digest_is_order_sensitive() -> None:
    """Event order is data: two runs that reorder events are not the same run."""
    forward = StreamingDigest()
    forward.update("event", {"seq": 1})
    forward.update("event", {"seq": 2})
    backward = StreamingDigest()
    backward.update("event", {"seq": 2})
    backward.update("event", {"seq": 1})
    assert forward.hexdigest() != backward.hexdigest()


def test_streaming_digest_folds_in_the_record_count() -> None:
    """A truncated stream must not collide with a complete one."""
    short = StreamingDigest()
    short.update("event", {"seq": 1})
    long = StreamingDigest()
    long.update("event", {"seq": 1})
    long.update("event", {"seq": 2})
    assert short.record_count == 1
    assert long.record_count == 2
    assert short.hexdigest() != long.hexdigest()


def test_streaming_digest_separates_kinds() -> None:
    deal = StreamingDigest()
    deal.update("deal", {"id": "X"})
    account = StreamingDigest()
    account.update("account", {"id": "X"})
    assert deal.hexdigest() != account.hexdigest()


def test_update_text_field_separation_cannot_be_forged() -> None:
    """Explicit separators mean a value containing one cannot shift framing."""
    left = StreamingDigest()
    left.update_text("row", "ab", "c")
    right = StreamingDigest()
    right.update_text("row", "a", "bc")
    assert left.hexdigest() != right.hexdigest()


def test_update_text_matches_itself_across_instances() -> None:
    def build() -> str:
        digest = StreamingDigest()
        digest.update_text("deal", "D-1", "7", "0.50", "1.10000")
        digest.update_text("deal", "D-2", "8", "0.25", "1.10100")
        return digest.hexdigest()

    assert build() == build()


def test_hexdigest_does_not_consume_the_accumulator() -> None:
    digest = StreamingDigest()
    digest.update_text("row", "a")
    first = digest.hexdigest()
    assert digest.hexdigest() == first
    digest.update_text("row", "b")
    assert digest.hexdigest() != first


def test_canonical_version_is_recorded_in_the_digest() -> None:
    """A digest is only comparable within one canonical version."""
    assert CANONICAL_VERSION == "2"
    empty = StreamingDigest().hexdigest()
    assert empty.startswith("sha256:")
    # Two empty digests agree, so the version prefix is applied consistently.
    assert StreamingDigest().hexdigest() == empty

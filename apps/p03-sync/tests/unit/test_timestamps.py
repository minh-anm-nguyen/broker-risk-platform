"""Timestamp handling: tz-aware in, UTC out, instant preserved (INV-13)."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from trading_sync.domain.timestamps import MAX_YEAR, parse_utc_timestamp
from trading_sync.errors import InvalidTimestampError

pytestmark = pytest.mark.unit


@pytest.mark.whatif("WF3-090")
def test_offset_timestamp_converts_without_moving_the_instant() -> None:
    """Conversion changes representation, not the moment described."""
    parsed = parse_utc_timestamp("2026-03-01T09:30:00+07:00", field="executed_at")
    assert parsed == datetime(2026, 3, 1, 2, 30, tzinfo=timezone.utc)
    assert parsed.tzinfo is timezone.utc


@pytest.mark.whatif("WF3-090")
@pytest.mark.parametrize(
    "raw",
    [
        "2026-03-01T02:30:00Z",
        "2026-03-01T02:30:00+00:00",
        "2026-03-01T09:30:00+07:00",
        "2024-02-29T02:30:00Z",
    ],
)
def test_equivalent_spellings_agree(raw: str) -> None:
    """Every accepted spelling, including a leap day, normalizes to UTC."""
    assert parse_utc_timestamp(raw, field="executed_at").tzinfo is timezone.utc


@pytest.mark.whatif("WF3-091")
@pytest.mark.parametrize("raw", ["2026-03-01T02:30:00", "2026-03-01 02:30:00", "2026-03-01"])
def test_naive_timestamps_are_rejected(raw: str) -> None:
    """Both ways of resolving the ambiguity are wrong often enough to refuse.

    Assuming UTC silently shifts a Jakarta fill by seven hours; assuming server
    time makes the stored instant depend on where the process runs.
    """
    with pytest.raises(InvalidTimestampError) as caught:
        parse_utc_timestamp(raw, field="executed_at")
    assert caught.value.reason_code == "INVALID_TIMESTAMP"


@pytest.mark.whatif("WF3-094")
@pytest.mark.parametrize("raw", ["9999-12-31T00:00:00Z", "1900-01-01T00:00:00Z"])
def test_timestamps_outside_the_window_are_rejected(raw: str) -> None:
    with pytest.raises(InvalidTimestampError) as caught:
        parse_utc_timestamp(raw, field="executed_at")
    assert caught.value.safe_details["max_year"] == MAX_YEAR


@pytest.mark.whatif("WF3-096")
def test_dst_boundary_keeps_the_utc_instant() -> None:
    """Across a DST change the local wall clock jumps; the instant does not.

    01:30 and 03:30 local on a spring-forward night are one hour apart, not
    two, and the UTC values must show exactly that (fixture G05 companion).
    """
    before = parse_utc_timestamp("2026-03-29T01:30:00+01:00", field="executed_at")
    after = parse_utc_timestamp("2026-03-29T03:30:00+02:00", field="executed_at")
    assert (after - before).total_seconds() == 3600


@pytest.mark.whatif("WF3-091")
@pytest.mark.parametrize("raw", [1767225600, None, True, ""])
def test_non_string_timestamps_are_rejected(raw: object) -> None:
    with pytest.raises(InvalidTimestampError):
        parse_utc_timestamp(raw, field="executed_at")

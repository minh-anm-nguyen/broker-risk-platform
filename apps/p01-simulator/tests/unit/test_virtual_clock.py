"""Virtual clock and deadline semantics.

Business time comes from the scenario, which is what lets a test advance a day
in a millisecond and still get the same data every run. The alternative - a
``sleep`` to let time pass - makes a suite slow when it works and flaky when it
does not (WF-118).
"""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta, timezone

import pytest

from simulator.generation.virtual_clock import Deadline, VirtualClock, wall_expired

pytestmark = pytest.mark.unit

START = datetime(2026, 10, 30, 0, 0, 0, tzinfo=UTC)


def test_event_time_is_a_pure_function_of_its_index() -> None:
    clock = VirtualClock.from_millis(START, 1_000)
    assert clock.at_index(0) == START
    assert clock.at_index(1) == START + timedelta(seconds=1)
    assert clock.at_index(86_400) == START + timedelta(days=1)
    # Repeated calls agree: nothing here reads the host clock.
    assert clock.at_index(42) == clock.at_index(42)


def test_sub_second_steps_are_supported() -> None:
    clock = VirtualClock.from_millis(START, 250)
    assert clock.at_index(4) == START + timedelta(seconds=1)


def test_offsets_are_normalized_to_utc() -> None:
    local = datetime(2026, 10, 30, 7, 0, 0, tzinfo=timezone(timedelta(hours=7)))
    clock = VirtualClock.from_millis(local, 1_000)
    assert clock.start == datetime(2026, 10, 30, 0, 0, 0, tzinfo=UTC)


def test_advance_moves_by_whole_steps() -> None:
    clock = VirtualClock.from_millis(START, 1_000)
    assert clock.advance(START, 5) == START + timedelta(seconds=5)
    assert clock.advance(START) == START + timedelta(seconds=1)


def test_a_non_positive_step_is_refused() -> None:
    """A zero step would stop business time without saying so."""
    with pytest.raises(ValueError):
        VirtualClock.from_millis(START, 0)
    with pytest.raises(ValueError):
        VirtualClock.from_millis(START, -1_000)


def test_negative_index_is_refused() -> None:
    clock = VirtualClock.from_millis(START, 1_000)
    with pytest.raises(ValueError):
        clock.at_index(-1)


def test_virtual_time_never_depends_on_the_host_clock() -> None:
    """Two clocks built seconds apart describe the same timeline."""
    first = VirtualClock.from_millis(START, 1_000)
    time.sleep(0.01)
    second = VirtualClock.from_millis(START, 1_000)
    assert [first.at_index(i) for i in range(10)] == [second.at_index(i) for i in range(10)]


def test_deadline_carries_both_a_monotonic_and_a_wall_reference() -> None:
    """Monotonic rules in-process; wall time is what a cursor can serialize."""
    deadline = Deadline.in_seconds(60)
    assert deadline.monotonic_at > time.monotonic()
    assert deadline.wall_at > datetime.now(UTC)


def test_deadline_expiry_uses_the_monotonic_clock() -> None:
    deadline = Deadline.in_seconds(0.0)
    assert deadline.expired()
    assert deadline.remaining_seconds() == 0.0


def test_extend_to_only_pushes_a_deadline_outwards() -> None:
    """Reusing a cursor must not be able to shorten its own lease."""
    deadline = Deadline.in_seconds(100)
    original = deadline.monotonic_at
    deadline.extend_to(1)
    assert deadline.monotonic_at == original
    deadline.extend_to(1_000)
    assert deadline.monotonic_at > original


def test_wall_expiry_allows_a_bounded_skew() -> None:
    """WF-086: accepting a slightly stale cursor is a decision, not drift."""
    just_past = datetime.now(UTC) - timedelta(seconds=10)
    assert not wall_expired(just_past, skew_tolerance_seconds=60)
    assert wall_expired(just_past, skew_tolerance_seconds=5)


def test_wall_expiry_with_zero_tolerance_is_strict() -> None:
    assert wall_expired(datetime.now(UTC) - timedelta(seconds=1), skew_tolerance_seconds=0)
    assert not wall_expired(datetime.now(UTC) + timedelta(seconds=30), skew_tolerance_seconds=0)

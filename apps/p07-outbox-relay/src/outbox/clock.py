"""Time, made injectable, and the one timestamp format on the wire.

Lease expiry, backoff and retention are all time predicates, and a test that
proves "a lease expires and a standby fences the old owner" must be able to
make a lease expire without sleeping for thirty seconds. So nothing in this
package calls ``datetime.now`` directly; it takes a :class:`Clock`.

A caveat the design is careful about (section 5.3): these timestamps are *row
creation* and *observation* times. None of them is the PostgreSQL COMMIT
instant. ``clock_timestamp()`` inside a transaction fires before the commit
record is durable, so a "COMMIT to ACK" figure derived from it is an estimate
with the commit window missing. The metric names in
:mod:`outbox.observability.metrics` say ``created_to_ack`` for that reason.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from typing import Protocol, runtime_checkable


@runtime_checkable
class Clock(Protocol):
    def now(self) -> datetime:
        """Current time as a timezone-aware UTC datetime."""

    def monotonic(self) -> float:
        """Seconds from an arbitrary origin, for measuring durations only."""


class RealClock:
    """Wall clock plus the monotonic clock, each used for what it is good at."""

    __slots__ = ()

    def now(self) -> datetime:
        return datetime.now(UTC)

    def monotonic(self) -> float:
        return time.monotonic()


class ManualClock:
    """A clock the test moves by hand.

    Both readings advance together so that a test cannot accidentally expire a
    lease (wall clock) without also aging a backoff deadline (monotonic).
    """

    __slots__ = ("_now", "_origin")

    def __init__(self, start: datetime | None = None) -> None:
        self._now = start or datetime(2026, 10, 30, 6, 0, 0, tzinfo=UTC)
        self._origin = self._now

    def now(self) -> datetime:
        return self._now

    def monotonic(self) -> float:
        return (self._now - self._origin).total_seconds()

    def advance(self, seconds: float) -> datetime:
        self._now += timedelta(seconds=seconds)
        return self._now

    def set(self, moment: datetime) -> None:
        if moment.tzinfo is None:
            raise ValueError("ManualClock requires timezone-aware datetimes")
        self._now = moment.astimezone(UTC)


def to_wire(moment: datetime) -> str:
    """Format as ``2026-10-30T06:00:00.000000Z``.

    Fixed six-digit microseconds and a literal ``Z``: a variable-width
    timestamp would make two equal instants hash differently.
    """
    if moment.tzinfo is None:
        raise ValueError("refusing to serialise a naive datetime")
    utc = moment.astimezone(UTC)
    return f"{utc.strftime('%Y-%m-%dT%H:%M:%S')}.{utc.microsecond:06d}Z"


def from_wire(text: str) -> datetime:
    """Parse the wire format back, rejecting anything else."""
    if not isinstance(text, str) or not text.endswith("Z"):
        raise ValueError(f"timestamp {text!r} is not in the pinned wire format")
    try:
        parsed = datetime.strptime(text, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError as exc:
        raise ValueError(f"timestamp {text!r} is not in the pinned wire format") from exc
    return parsed.replace(tzinfo=UTC)

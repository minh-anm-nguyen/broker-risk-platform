"""Clocks.

Leases, freshness and ``STALE`` all depend on time, and all three are things
the test suite needs to assert rather than wait for. A :class:`ManualClock`
makes "the lease expired during the aggregation" (P08-S-08) a two-line setup
instead of a sleep.

The control plane deliberately uses its *own* clock rather than a timestamp
passed in by a worker. Section 5.3 calls for the database clock, because a
worker with a skewed clock must not be able to argue that somebody else's
live lease has lapsed.
"""

from __future__ import annotations

import threading
from datetime import UTC, datetime, timedelta
from typing import Protocol, runtime_checkable


@runtime_checkable
class Clock(Protocol):
    def now(self) -> datetime:
        """Current time as aware UTC."""


class SystemClock:
    """Wall clock, always UTC-aware."""

    __slots__ = ()

    def now(self) -> datetime:
        return datetime.now(UTC)


class ManualClock:
    """A clock that only moves when a test says so."""

    __slots__ = ("_now", "_lock")

    def __init__(self, start: datetime | None = None) -> None:
        base = start or datetime(2026, 10, 30, 12, 0, 0, tzinfo=UTC)
        if base.tzinfo is None:
            raise ValueError("ManualClock requires an aware datetime")
        self._now = base.astimezone(UTC)
        self._lock = threading.Lock()

    def now(self) -> datetime:
        with self._lock:
            return self._now

    def advance(self, seconds: float = 0.0, **kwargs: float) -> datetime:
        delta = timedelta(seconds=seconds, **kwargs)
        with self._lock:
            self._now = self._now + delta
            return self._now

    def set(self, moment: datetime) -> None:
        if moment.tzinfo is None:
            raise ValueError("ManualClock requires an aware datetime")
        with self._lock:
            self._now = moment.astimezone(UTC)

"""Absolute deadlines on a monotonic clock (design section 5.2).

A deadline here is an *absolute* instant, computed once when the request is
admitted, not a per-hop timeout. The difference matters: with per-hop
timeouts, a request that waits 2.9 s in the queue can still be handed a 1.5 s
upstream timeout and answer the caller at 4.4 s against a 3 s budget. With an
absolute deadline the dispatcher sees 0.1 s remaining, declines to start the
call and fails fast (WF2-H-01).

The clock is :func:`time.monotonic`, never the wall clock. A wall clock can
step backwards - NTP correction, a VM resuming, a host whose time differs from
its neighbour by five minutes - and a duration computed across such a step is
either negative or enormous. Negative becomes an immortal request; enormous
becomes an instant timeout (WF2-H-10, WF2-O-08). Wall-clock time appears in
this package only in log lines and in business timestamps that came from the
source.

:class:`Clock` is a seam so tests can advance time deterministically instead
of sleeping. Everything that measures or waits takes one.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Protocol

from .errors import DeadlineExceededError

__all__ = ["Clock", "SystemClock", "Deadline", "ManualClock"]


class Clock(Protocol):
    """Monotonic time plus the two waits this package performs."""

    def monotonic(self) -> float: ...

    async def sleep(self, seconds: float) -> None: ...

    def thread_sleep(self, seconds: float) -> None: ...


class SystemClock:
    """The real clock."""

    __slots__ = ()

    def monotonic(self) -> float:
        return time.monotonic()

    async def sleep(self, seconds: float) -> None:
        if seconds > 0:
            await asyncio.sleep(seconds)

    def thread_sleep(self, seconds: float) -> None:
        if seconds > 0:
            time.sleep(seconds)


class ManualClock:
    """A clock a test advances by hand.

    Async sleeps resolve immediately and record the duration requested, so a
    test can assert *how long* a retry would have waited without waiting
    (WF2-H-04, WF2-H-09). ``advance`` is the only thing that moves time.
    """

    def __init__(self, start: float = 1_000.0) -> None:
        self._now = start
        #: Every async sleep duration requested, in order.
        self.sleeps: list[float] = []
        #: Every blocking sleep duration requested, in order.
        self.thread_sleeps: list[float] = []
        #: When set, an async sleep also advances the clock by its duration.
        self.auto_advance = True

    def monotonic(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += seconds

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        if self.auto_advance and seconds > 0:
            self._now += seconds
        # Yield once so other tasks make progress, without real elapsed time.
        await asyncio.sleep(0)

    def thread_sleep(self, seconds: float) -> None:
        self.thread_sleeps.append(seconds)
        if self.auto_advance and seconds > 0:
            self._now += seconds


@dataclass(frozen=True, slots=True)
class Deadline:
    """An absolute instant on a monotonic clock, with a budget check.

    Immutable: a deadline that could be extended would not be a deadline. To
    express a shorter sub-budget - a queue-wait cap, for instance - use
        :meth:`capped`, which can only move the instant earlier.
    """

    at: float
    clock: Clock
    total_ms: int

    @classmethod
    def after(cls, total_ms: int, *, clock: Clock) -> "Deadline":
        if total_ms < 0:
            raise ValueError("deadline must not be negative")
        return cls(at=clock.monotonic() + total_ms / 1000.0, clock=clock, total_ms=total_ms)

    @property
    def remaining_s(self) -> float:
        """Seconds left, clamped at zero. Never negative."""
        return max(0.0, self.at - self.clock.monotonic())

    @property
    def remaining_ms(self) -> float:
        return self.remaining_s * 1000.0

    @property
    def expired(self) -> bool:
        return self.clock.monotonic() >= self.at

    def elapsed_ms(self) -> float:
        return max(0.0, self.total_ms - self.remaining_ms)

    def capped(self, cap_ms: int) -> "Deadline":
        """A sub-deadline no later than this one.

        Used for the queue-wait budget: waiting for admission may consume at
        most ``cap_ms``, and never more than the request has left.
        """
        candidate = self.clock.monotonic() + cap_ms / 1000.0
        return Deadline(at=min(self.at, candidate), clock=self.clock, total_ms=self.total_ms)

    def check(self, *, operation: str | None = None, request_id: str | None = None) -> None:
        """Raise if the budget is gone.

        Called before admission, before dispatch, before each retry and
        before a completion is accepted. The last of those is what stops a
        slow success from being delivered after the caller gave up
        (WF2-H-03, INV-04).
        """
        if self.expired:
            raise DeadlineExceededError(
                "the request budget was exhausted",
                operation=operation,
                request_id=request_id,
                details={"total_ms": self.total_ms},
            )

    def budget_for_attempt(self, minimum_ms: int) -> float | None:
        """Seconds available for one more attempt, or ``None`` if too little.

        A retry that cannot plausibly finish is not attempted: starting it
        guarantees a second failure and wastes upstream capacity during an
        incident, which is how a retry storm begins (section 6.3).
        """
        remaining = self.remaining_s
        if remaining * 1000.0 < minimum_ms:
            return None
        return remaining

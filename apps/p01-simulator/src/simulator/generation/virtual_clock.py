"""Virtual clock and monotonic deadlines.

Business time comes from the scenario, not from the host clock. That is what
makes a dataset reproducible and what lets a test advance a day in a
millisecond without a ``sleep`` anywhere (WF-118).

Deadlines are the mirror image of the same concern. A TTL measured against the
wall clock silently changes meaning when the host clock steps backwards, so an
in-process deadline is measured on the monotonic clock and only falls back to
wall time when the deadline has to outlive the process (WF-086).
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta


@dataclass(frozen=True, slots=True)
class VirtualClock:
    """Maps an event index onto business time.

    Deliberately a pure function of the index: event *n* has the same
    timestamp on every replay of the same spec, no matter how long generation
    actually took or how often it was interrupted.
    """

    start: datetime
    step: timedelta

    @classmethod
    def from_millis(cls, start: datetime, step_ms: int) -> "VirtualClock":
        if step_ms <= 0:
            raise ValueError("virtual clock step must be positive")
        return cls(start=start.astimezone(UTC), step=timedelta(milliseconds=step_ms))

    def at_index(self, index: int) -> datetime:
        """Timestamp of the ``index``-th event, counting from zero."""
        if index < 0:
            raise ValueError("event index must not be negative")
        return self.start + self.step * index

    def advance(self, current: datetime, steps: int = 1) -> datetime:
        return current + self.step * steps


@dataclass(slots=True)
class Deadline:
    """A deadline that survives a wall-clock jump.

    ``monotonic_at`` is authoritative while this process lives. ``wall_at``
    exists so a deadline can be serialized into a cursor and still mean
    something after a restart, where no monotonic reference is comparable.
    """

    monotonic_at: float
    wall_at: datetime

    @classmethod
    def in_seconds(cls, seconds: float) -> "Deadline":
        return cls(
            monotonic_at=time.monotonic() + seconds,
            wall_at=datetime.now(UTC) + timedelta(seconds=seconds),
        )

    def expired(self) -> bool:
        return time.monotonic() >= self.monotonic_at

    def remaining_seconds(self) -> float:
        return max(0.0, self.monotonic_at - time.monotonic())

    def extend_to(self, seconds: float) -> None:
        """Push the deadline out to at least ``seconds`` from now.

        Used when a cursor is reused: an active scan should keep its snapshot
        alive rather than lose it mid-pagination.
        """
        candidate = Deadline.in_seconds(seconds)
        if candidate.monotonic_at > self.monotonic_at:
            self.monotonic_at = candidate.monotonic_at
            self.wall_at = candidate.wall_at


def wall_expired(wall_at: datetime, *, skew_tolerance_seconds: int) -> bool:
    """Wall-clock expiry check with a bounded skew allowance.

    Only used for a deadline whose issuing process is gone. The tolerance is
    explicit so that "we accepted a slightly stale cursor" is a documented
    decision rather than an accident of clock drift.
    """
    now = datetime.now(UTC)
    return now > wall_at + timedelta(seconds=skew_tolerance_seconds)

"""Backoff with jitter, and a circuit breaker that stops the claim loop.

Both exist to answer the same FMEA row: "unbounded retries during outage ->
DB/Kafka storm". Two separate mechanisms because they solve different halves.

Backoff spaces out retries of *one* event. Jitter is not decoration: without it,
ten thousand rows that failed in the same broker outage all become due in the
same millisecond, and the recovery attempt is itself the next outage. The jitter
here is subtractive (``delay * (1 - r * random)``), so a delay never exceeds the
cap -- an additive jitter on top of a capped delay quietly raises the cap.

The circuit breaker stops claiming *anything*. When the broker is unreachable or
the ACL is rejecting us, per-event backoff still produces a steady stream of
doomed attempts, each one a claim transaction and a produce. Opening the circuit
turns that into one probe per cooldown. Design section 7.5 is the reason this
matters beyond efficiency: while the circuit is open the relay must stop
claiming so that rows stay ``PENDING`` and visible, rather than cycling through
``CLAIMED`` and inflating the attempt counter toward a quarantine threshold the
event did nothing to deserve.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field


def compute_delay(
    *,
    attempt: int,
    base_seconds: float,
    max_seconds: float,
    jitter_ratio: float = 0.25,
    rng: random.Random | None = None,
) -> float:
    """Exponential backoff with subtractive jitter, capped at ``max_seconds``.

    ``attempt`` is 1-based, so the first retry waits ``base_seconds``. The
    exponent is clamped before the shift so that a row with a few hundred
    attempts cannot compute ``2**300`` on its way to being capped anyway.
    """
    if attempt < 1:
        raise ValueError("attempt is 1-based")
    if base_seconds <= 0 or max_seconds < base_seconds:
        raise ValueError("require 0 < base_seconds <= max_seconds")
    if not 0.0 <= jitter_ratio <= 1.0:
        raise ValueError("jitter_ratio must be within [0, 1]")

    exponent = min(attempt - 1, 32)
    uncapped = base_seconds * (2**exponent)
    delay = min(uncapped, max_seconds)
    if jitter_ratio == 0.0:
        return delay
    source = rng or random
    return delay * (1.0 - jitter_ratio * source.random())


@dataclass(slots=True)
class CircuitBreaker:
    """Open after consecutive failures, half-open after a cooldown."""

    failure_threshold: int
    cooldown_seconds: float
    _consecutive_failures: int = 0
    _opened_at: float | None = None
    opened_count: int = field(default=0)

    def record_failure(self, *, monotonic: float) -> bool:
        """Count a failure. Returns True if this opened the circuit."""
        self._consecutive_failures += 1
        if self._opened_at is None and self._consecutive_failures >= self.failure_threshold:
            self._opened_at = monotonic
            self.opened_count += 1
            return True
        if self._opened_at is not None:
            # A failed probe re-arms the cooldown instead of letting the circuit
            # flap open and closed once per cycle.
            self._opened_at = monotonic
        return False

    def record_success(self) -> None:
        self._consecutive_failures = 0
        self._opened_at = None

    def is_open(self, *, monotonic: float) -> bool:
        if self._opened_at is None:
            return False
        if monotonic - self._opened_at >= self.cooldown_seconds:
            # Half-open: allow one cycle through. The next failure re-arms, a
            # success closes it.
            return False
        return True

    @property
    def consecutive_failures(self) -> int:
        return self._consecutive_failures

    def seconds_until_probe(self, *, monotonic: float) -> float:
        if self._opened_at is None:
            return 0.0
        return max(0.0, self.cooldown_seconds - (monotonic - self._opened_at))

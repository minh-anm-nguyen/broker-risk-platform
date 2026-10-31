"""Retry policy, backoff and the circuit breaker (design section 6.2, 6.3).

What may be retried is decided by the *error class*, never by the status code
alone and never by hope. Three rules carry the design:

* **Invalid data is not retried.** A page whose ``commission`` is ``NaN`` will
  be exactly as invalid on the second read. Retrying it converts one source
  bug into N requests during an incident while still producing nothing usable
  (WF2-H-05, ADR-205).
* **Authentication failure is not retried.** 100 callers against a bad
  credential must not become 300 upstream auth attempts. The session is
  disabled centrally and pending callers get a typed error (WF2-H-06,
  section 15.4).
* **A retry that cannot finish is not started.** Before sleeping, the
  controller checks that the delay *plus* a minimum attempt budget still fits
  inside the caller's absolute deadline. Sleeping past a deadline is pure
  waste: the result is discarded on arrival (WF2-D-02).

Backoff is **full jitter** - ``uniform(0, min(cap, base * 2**n))`` - rather
than exponential-plus-small-jitter. After an upstream restart, 100 clients
with the same base delay retry in near lock-step and knock it over again; full
jitter spreads them across the whole window (WF2-H-12).
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from enum import StrEnum
from typing import Awaitable, Callable, Final, TypeVar

from .deadline import Clock, Deadline
from .errors import (
    CircuitOpenError,
    ConnectorError,
    DeadlineExceededError,
    RateLimitedError,
    TransportError,
    UpstreamTimeoutError,
    UpstreamUnavailableError,
)

T = TypeVar("T")

__all__ = [
    "BreakerState",
    "CircuitBreaker",
    "RetryController",
    "RetryStats",
    "parse_retry_after",
    "BREAKER_COUNTED_CODES",
]

#: Only these codes move the breaker. A contract error, a validation failure
#: or a caller's bad page size says nothing about upstream health, and opening
#: the breaker on one would take a working source out of service because a
#: caller sent a malformed request (section 6.1).
BREAKER_COUNTED_CODES: Final[frozenset[str]] = frozenset(
    {
        TransportError.code,
        UpstreamUnavailableError.code,
        UpstreamTimeoutError.code,
    }
)


def parse_retry_after(
    value: str | None, *, cap_s: float, now: datetime | None = None
) -> float | None:
    """Decode a ``Retry-After`` header into bounded seconds.

    Accepts both forms RFC 9110 permits: delta-seconds and an HTTP-date. The
    result is clamped into ``[0, cap_s]``:

    * A header of ``999999`` is honoured only up to the cap. Obeying it
      literally would park the request for eleven days (WF2-D-02).
    * An HTTP-date already in the past yields ``0``, not a negative sleep. A
      negative delay passed to a sleep is either an immediate retry or an
      exception depending on the call site, and neither should depend on the
      source's clock (WF2-H-09).
    * A malformed or absent header yields ``None``, which tells the caller to
      fall back to its own backoff rather than to assume zero.
    """
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    if len(text) > 64:
        return None
    try:
        seconds = float(int(text))
    except ValueError:
        try:
            when = parsedate_to_datetime(text)
        except (TypeError, ValueError, IndexError):
            return None
        if when is None:
            return None
        if when.tzinfo is None:
            when = when.replace(tzinfo=UTC)
        reference = now or datetime.now(UTC)
        seconds = (when - reference).total_seconds()
    if seconds != seconds or seconds in (float("inf"), float("-inf")):  # NaN / Inf
        return None
    return max(0.0, min(cap_s, seconds))


class BreakerState(StrEnum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """One breaker per source, counting transport failures only.

    ``HALF_OPEN`` admits **exactly one** probe. Every other caller fast-fails
    while that probe is outstanding, so a recovering source receives one
    request rather than the fifty that were waiting (WF2-H-08).
    """

    def __init__(self, *, failure_threshold: int, cooldown_ms: int, clock: Clock) -> None:
        self._threshold = failure_threshold
        self._cooldown_s = cooldown_ms / 1000.0
        self._clock = clock
        self._state = BreakerState.CLOSED
        self._consecutive_failures = 0
        self._opened_at: float | None = None
        self._probe_in_flight = False
        self.opened_total = 0
        self.rejected_total = 0
        self.probes_total = 0

    @property
    def state(self) -> BreakerState:
        """Current state, after accounting for an elapsed cooldown."""
        if self._state is BreakerState.OPEN and self._cooldown_elapsed():
            return BreakerState.HALF_OPEN
        return self._state

    def _cooldown_elapsed(self) -> bool:
        return (
            self._opened_at is not None
            and self._clock.monotonic() - self._opened_at >= self._cooldown_s
        )

    def allow(self, *, operation: str, request_id: str | None = None) -> bool:
        """Admit a call, or raise. Returns ``True`` if this call is the probe.

        The return value matters to the caller only for metrics; the refusal
        is the raise.
        """
        state = self.state
        if state is BreakerState.CLOSED:
            return False
        if state is BreakerState.HALF_OPEN:
            if self._probe_in_flight:
                self.rejected_total += 1
                raise CircuitOpenError(
                    "a recovery probe is already in flight for this source",
                    operation=operation,
                    request_id=request_id,
                    details={"breaker_state": BreakerState.HALF_OPEN.value},
                )
            self._state = BreakerState.HALF_OPEN
            self._probe_in_flight = True
            self.probes_total += 1
            return True
        self.rejected_total += 1
        raise CircuitOpenError(
            "the circuit is open for this source; waiting for the cooldown",
            operation=operation,
            request_id=request_id,
            details={
                "breaker_state": BreakerState.OPEN.value,
                "cooldown_remaining_ms": max(
                    0.0,
                    (self._cooldown_s - (self._clock.monotonic() - (self._opened_at or 0.0)))
                    * 1000.0,
                ),
            },
        )

    def record_success(self) -> None:
        self._consecutive_failures = 0
        self._probe_in_flight = False
        self._opened_at = None
        self._state = BreakerState.CLOSED

    def record_failure(self, error: ConnectorError) -> None:
        if error.code not in BREAKER_COUNTED_CODES:
            # A contract or data failure is not an availability signal. It
            # does not reset the counter either: doing so would let alternating
            # transport and data errors keep the breaker closed forever.
            self._probe_in_flight = False
            return
        was_probe = self._probe_in_flight
        self._probe_in_flight = False
        self._consecutive_failures += 1
        if was_probe or self._consecutive_failures >= self._threshold:
            # A failed probe reopens immediately and restarts the cooldown,
            # rather than letting the next caller probe straight away.
            if self._state is not BreakerState.OPEN or was_probe:
                self.opened_total += 1
            self._state = BreakerState.OPEN
            self._opened_at = self._clock.monotonic()

    def snapshot(self) -> dict[str, object]:
        return {
            "state": self.state.value,
            "consecutive_failures": self._consecutive_failures,
            "opened_total": self.opened_total,
            "rejected_total": self.rejected_total,
            "probes_total": self.probes_total,
        }


@dataclass(slots=True)
class RetryStats:
    """What one :meth:`RetryController.run` actually did.

    Recorded per operation so a test can assert the *exact* attempt count and
    the delays that were requested, rather than inferring them from elapsed
    time (WF2-H-04, WF2-P-06).
    """

    attempts: int = 0
    delays_s: list[float] = None  # type: ignore[assignment]
    retry_after_honoured_s: list[float] = None  # type: ignore[assignment]
    final_code: str | None = None
    gave_up_reason: str | None = None

    def __post_init__(self) -> None:
        if self.delays_s is None:
            self.delays_s = []
        if self.retry_after_honoured_s is None:
            self.retry_after_honoured_s = []


class RetryController:
    """Runs one operation with bounded attempts inside an absolute deadline."""

    def __init__(
        self,
        *,
        max_attempts: int,
        base_backoff_ms: int,
        max_backoff_ms: int,
        retry_after_cap_ms: int,
        minimum_attempt_budget_ms: int,
        clock: Clock,
        breaker: CircuitBreaker,
        rng: random.Random | None = None,
    ) -> None:
        self._max_attempts = max_attempts
        self._base_s = base_backoff_ms / 1000.0
        self._max_s = max_backoff_ms / 1000.0
        self._retry_after_cap_s = retry_after_cap_ms / 1000.0
        self._minimum_budget_ms = minimum_attempt_budget_ms
        self._clock = clock
        self._breaker = breaker
        #: Seeded in tests so a jitter distribution assertion is reproducible.
        self._rng = rng or random.Random()
        self.attempts_total = 0
        self.retries_total = 0

    @property
    def breaker(self) -> CircuitBreaker:
        return self._breaker

    def backoff_for(self, attempt: int) -> float:
        """Full-jitter delay before attempt ``attempt + 1``."""
        window = min(self._max_s, self._base_s * (2 ** max(0, attempt - 1)))
        if window <= 0:
            return 0.0
        return self._rng.uniform(0.0, window)

    async def run(
        self,
        attempt_fn: Callable[[int, float], Awaitable[T]],
        *,
        operation: str,
        deadline: Deadline,
        request_id: str | None = None,
        stats: RetryStats | None = None,
    ) -> T:
        """Call ``attempt_fn(attempt, budget_s)`` until it succeeds or stops.

        ``attempt`` is 1-based and ``budget_s`` is what remains of the
        caller's deadline at the moment of the call, recomputed every time -
        never the original budget handed out again (section 5.2).
        """
        stats = stats if stats is not None else RetryStats()
        last_error: ConnectorError | None = None

        for attempt in range(1, self._max_attempts + 1):
            budget = deadline.budget_for_attempt(self._minimum_budget_ms)
            if budget is None:
                stats.gave_up_reason = "deadline"
                raise self._deadline_error(
                    operation, request_id, stats, last_error, deadline
                )

            is_probe = self._breaker.allow(operation=operation, request_id=request_id)
            stats.attempts = attempt
            self.attempts_total += 1
            try:
                result = await attempt_fn(attempt, budget)
            except ConnectorError as error:
                error.with_context(
                    operation=operation, request_id=request_id, attempt=attempt
                )
                self._breaker.record_failure(error)
                last_error = error
                stats.final_code = error.code

                if not error.retryable:
                    # Includes every data and contract failure, every identity
                    # failure, and the local refusals. Nothing is gained by a
                    # second look.
                    stats.gave_up_reason = "not_retryable"
                    raise
                if attempt >= self._max_attempts:
                    stats.gave_up_reason = "attempts"
                    error.details.setdefault("attempts", attempt)
                    error.details.setdefault("max_attempts", self._max_attempts)
                    raise
                if is_probe:
                    # A failed half-open probe must not be followed by a second
                    # probe from the same operation; the breaker is open again.
                    stats.gave_up_reason = "probe_failed"
                    error.details.setdefault("attempts", attempt)
                    raise

                delay = self._delay_for(error, attempt, stats)
                remaining = deadline.remaining_s
                if (remaining - delay) * 1000.0 < self._minimum_budget_ms:
                    # The sleep itself would consume the budget, leaving no
                    # time for the attempt it precedes. Fail now rather than
                    # sleep into a certain timeout.
                    stats.gave_up_reason = "deadline"
                    raise self._deadline_error(
                        operation, request_id, stats, error, deadline
                    )
                stats.delays_s.append(delay)
                self.retries_total += 1
                await self._clock.sleep(delay)
            else:
                self._breaker.record_success()
                stats.final_code = None
                return result

        # Unreachable: the loop either returns or raises.
        raise AssertionError("retry loop exited without a result")  # pragma: no cover

    def _delay_for(self, error: ConnectorError, attempt: int, stats: RetryStats) -> float:
        if isinstance(error, RateLimitedError) and error.retry_after_s is not None:
            honoured = max(0.0, min(self._retry_after_cap_s, error.retry_after_s))
            stats.retry_after_honoured_s.append(honoured)
            # The source asked for a specific wait; honour it up to the cap,
            # and never shorten it with jitter, which would defeat its purpose.
            return honoured
        return self.backoff_for(attempt)

    @staticmethod
    def _deadline_error(
        operation: str,
        request_id: str | None,
        stats: RetryStats,
        last_error: ConnectorError | None,
        deadline: Deadline,
    ) -> DeadlineExceededError:
        details: dict[str, object] = {
            "attempts": stats.attempts,
            "total_ms": deadline.total_ms,
        }
        if last_error is not None:
            # The transport code that preceded the timeout is the actionable
            # part of this error, so it travels with it instead of being lost.
            details["last_error_code"] = last_error.code
            if last_error.upstream_status is not None:
                details["last_upstream_status"] = last_error.upstream_status
        return DeadlineExceededError(
            "the request budget was exhausted before another attempt could complete",
            operation=operation,
            request_id=request_id,
            details=details,
        )

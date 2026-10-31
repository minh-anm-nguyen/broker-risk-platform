"""Deadlines, retry policy, Retry-After parsing and the circuit breaker.

Covers WF2-H-01 .. WF2-H-12, WF2-D-02, WF2-D-03, WF2-R-05 and INV-11.

Every timing assertion runs on :class:`ManualClock`, so a loaded machine
changes how long the test takes but never whether it passes - which is the
discipline section 11.4 asks for.
"""

from __future__ import annotations

import random

import pytest

from mt5_connector.deadline import Deadline, ManualClock
from mt5_connector.errors import (
    DataIntegrityError,
    DeadlineExceededError,
    PermissionDeniedError,
    RateLimitedError,
    SourceContractError,
    UpstreamUnavailableError,
)
from mt5_connector.retry import (
    BreakerState,
    CircuitBreaker,
    CircuitOpenError,
    RetryController,
    RetryStats,
    parse_retry_after,
)

pytestmark = pytest.mark.unit


def controller(
    clock: ManualClock,
    *,
    max_attempts: int = 3,
    base_ms: int = 100,
    max_ms: int = 1_000,
    retry_after_cap_ms: int = 2_000,
    minimum_ms: int = 50,
    breaker: CircuitBreaker | None = None,
    seed: int = 7,
) -> RetryController:
    return RetryController(
        max_attempts=max_attempts,
        base_backoff_ms=base_ms,
        max_backoff_ms=max_ms,
        retry_after_cap_ms=retry_after_cap_ms,
        minimum_attempt_budget_ms=minimum_ms,
        clock=clock,
        breaker=breaker
        or CircuitBreaker(failure_threshold=5, cooldown_ms=10_000, clock=clock),
        rng=random.Random(seed),
    )


class TestDeadline:
    def test_remaining_never_goes_negative(self) -> None:
        """WF2-H-10: a negative duration becomes an immortal request."""
        clock = ManualClock()
        deadline = Deadline.after(100, clock=clock)
        clock.advance(10.0)
        assert deadline.remaining_s == 0.0
        assert deadline.expired

    def test_check_raises_once_expired(self) -> None:
        clock = ManualClock()
        deadline = Deadline.after(100, clock=clock)
        deadline.check(operation="fetch_deals")
        clock.advance(0.2)
        with pytest.raises(DeadlineExceededError):
            deadline.check(operation="fetch_deals")

    def test_capped_can_only_move_the_instant_earlier(self) -> None:
        clock = ManualClock()
        deadline = Deadline.after(3_000, clock=clock)
        assert deadline.capped(500).at < deadline.at
        # A cap longer than what remains cannot extend the deadline.
        assert deadline.capped(10_000).at == pytest.approx(deadline.at)

    def test_budget_for_attempt_declines_a_doomed_attempt(self) -> None:
        clock = ManualClock()
        deadline = Deadline.after(1_000, clock=clock)
        assert deadline.budget_for_attempt(minimum_ms=50) == pytest.approx(1.0)
        clock.advance(0.98)
        assert deadline.budget_for_attempt(minimum_ms=50) is None

    def test_wall_clock_jumps_do_not_affect_a_monotonic_deadline(self) -> None:
        """WF2-H-10, WF2-O-08: durations never come from the wall clock."""
        clock = ManualClock(start=5_000.0)
        deadline = Deadline.after(1_000, clock=clock)
        # Simulate an NTP step by moving the *wall* clock only - the manual
        # clock here is the monotonic source and is untouched.
        import time as _time

        original = _time.time
        try:
            _time.time = lambda: 0.0  # type: ignore[assignment]
            assert deadline.remaining_s == pytest.approx(1.0)
        finally:
            _time.time = original  # type: ignore[assignment]


class TestRetryAfter:
    def test_delta_seconds_is_honoured(self) -> None:
        assert parse_retry_after("2", cap_s=10.0) == 2.0

    def test_absurd_value_is_capped(self) -> None:
        """WF2-D-02: obeying 999999 literally parks the request for days."""
        assert parse_retry_after("999999", cap_s=2.0) == 2.0

    def test_http_date_in_the_past_yields_zero_not_a_negative_sleep(self) -> None:
        """WF2-H-09."""
        value = parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT", cap_s=5.0)
        assert value == 0.0

    def test_http_date_in_the_future_is_capped(self) -> None:
        value = parse_retry_after("Fri, 01 Jan 2100 00:00:00 GMT", cap_s=1.5)
        assert value == 1.5

    @pytest.mark.parametrize("value", [None, "", "   ", "soon", "-", "x" * 100])
    def test_absent_or_malformed_yields_none_not_zero(self, value) -> None:
        """None tells the caller to use its own backoff; 0 would hammer."""
        assert parse_retry_after(value, cap_s=5.0) is None

    def test_negative_delta_seconds_is_clamped_to_zero(self) -> None:
        assert parse_retry_after("-5", cap_s=5.0) == 0.0


class TestRetryPolicy:
    async def test_success_on_first_attempt_makes_no_retry(self) -> None:
        clock = ManualClock()
        stats = RetryStats()
        calls: list[int] = []

        async def attempt(n: int, budget: float) -> str:
            calls.append(n)
            return "page"

        result = await controller(clock).run(
            attempt, operation="fetch_deals", deadline=Deadline.after(3_000, clock=clock),
            stats=stats,
        )
        assert result == "page"
        assert calls == [1]
        assert stats.attempts == 1
        assert stats.delays_s == []

    async def test_two_failures_then_success_uses_exactly_three_attempts(self) -> None:
        """WF2-H-04: the attempt count is asserted, not inferred."""
        clock = ManualClock()
        stats = RetryStats()
        attempts: list[int] = []

        async def attempt(n: int, budget: float) -> str:
            attempts.append(n)
            if n < 3:
                raise UpstreamUnavailableError("503", upstream_status=503)
            return "page"

        result = await controller(clock, base_ms=10, max_ms=40).run(
            attempt,
            operation="fetch_deals",
            deadline=Deadline.after(3_000, clock=clock),
            stats=stats,
        )
        assert result == "page"
        assert attempts == [1, 2, 3]
        assert stats.attempts == 3
        assert len(stats.delays_s) == 2

    async def test_attempts_are_bounded_by_policy(self) -> None:
        """INV-11."""
        clock = ManualClock()
        stats = RetryStats()
        attempts: list[int] = []

        async def attempt(n: int, budget: float) -> str:
            attempts.append(n)
            raise UpstreamUnavailableError("503", upstream_status=503)

        with pytest.raises(UpstreamUnavailableError) as excinfo:
            await controller(clock, max_attempts=3, base_ms=1, max_ms=2).run(
                attempt,
                operation="fetch_deals",
                deadline=Deadline.after(60_000, clock=clock),
                stats=stats,
            )
        assert attempts == [1, 2, 3]
        assert excinfo.value.details["attempts"] == 3
        assert excinfo.value.details["max_attempts"] == 3
        assert stats.gave_up_reason == "attempts"

    @pytest.mark.parametrize(
        "error",
        [
            SourceContractError("malformed page"),
            DataIntegrityError("duplicate deal id"),
            PermissionDeniedError("403"),
        ],
    )
    async def test_invalid_data_is_read_exactly_once(self, error) -> None:
        """WF2-H-05, WF2-R-05, ADR-205: retrying bad data cannot help."""
        clock = ManualClock()
        stats = RetryStats()
        attempts: list[int] = []

        async def attempt(n: int, budget: float) -> str:
            attempts.append(n)
            raise error

        with pytest.raises(type(error)):
            await controller(clock).run(
                attempt,
                operation="fetch_deals",
                deadline=Deadline.after(3_000, clock=clock),
                stats=stats,
            )
        assert attempts == [1]
        assert stats.gave_up_reason == "not_retryable"

    async def test_no_sleep_beyond_the_deadline(self) -> None:
        """WF2-D-02: the sleep must not consume the budget it precedes."""
        clock = ManualClock()
        deadline = Deadline.after(300, clock=clock)
        stats = RetryStats()

        async def attempt(n: int, budget: float) -> str:
            error = RateLimitedError("429", upstream_status=429)
            error.retry_after_s = 10.0
            raise error

        with pytest.raises(DeadlineExceededError) as excinfo:
            await controller(clock, retry_after_cap_ms=2_000, minimum_ms=50).run(
                attempt,
                operation="fetch_deals",
                deadline=deadline,
                stats=stats,
            )
        # No sleep was performed at all: the capped 2 s wait does not fit in a
        # 300 ms budget, so the attempt is abandoned rather than slept through.
        assert clock.sleeps == []
        assert stats.gave_up_reason == "deadline"
        assert excinfo.value.details["last_error_code"] == "RATE_LIMITED"
        assert excinfo.value.details["last_upstream_status"] == 429

    async def test_retry_after_is_honoured_up_to_the_cap(self) -> None:
        """WF2-D-02 with enough budget: the wait is the capped value."""
        clock = ManualClock()
        stats = RetryStats()
        attempts: list[int] = []

        async def attempt(n: int, budget: float) -> str:
            attempts.append(n)
            if n == 1:
                error = RateLimitedError("429", upstream_status=429)
                error.retry_after_s = 999_999.0
                raise error
            return "page"

        result = await controller(clock, retry_after_cap_ms=500, minimum_ms=10).run(
            attempt,
            operation="fetch_deals",
            deadline=Deadline.after(10_000, clock=clock),
            stats=stats,
        )
        assert result == "page"
        assert stats.retry_after_honoured_s == [0.5]
        assert clock.sleeps == [0.5]

    async def test_missing_retry_after_falls_back_to_jittered_backoff(self) -> None:
        """WF2-D-03: bounded fallback, not zero."""
        clock = ManualClock()
        stats = RetryStats()

        async def attempt(n: int, budget: float) -> str:
            if n == 1:
                raise RateLimitedError("429", upstream_status=429)
            return "page"

        await controller(clock, base_ms=100, max_ms=200, minimum_ms=10).run(
            attempt,
            operation="fetch_deals",
            deadline=Deadline.after(10_000, clock=clock),
            stats=stats,
        )
        assert stats.retry_after_honoured_s == []
        assert len(stats.delays_s) == 1
        assert 0.0 <= stats.delays_s[0] <= 0.1

    async def test_budget_is_recomputed_for_every_attempt(self) -> None:
        """Section 5.2: never hand out the original budget twice."""
        clock = ManualClock()
        budgets: list[float] = []

        async def attempt(n: int, budget: float) -> str:
            budgets.append(budget)
            clock.advance(0.2)
            if n < 3:
                raise UpstreamUnavailableError("503", upstream_status=503)
            return "page"

        await controller(clock, base_ms=1, max_ms=2, minimum_ms=10).run(
            attempt, operation="fetch_deals", deadline=Deadline.after(3_000, clock=clock)
        )
        assert len(budgets) == 3
        assert budgets[0] > budgets[1] > budgets[2]


class TestFullJitter:
    def test_backoff_is_uniform_over_the_whole_window(self) -> None:
        """WF2-H-12: lock-step retries knock a recovering source over again."""
        clock = ManualClock()
        ctrl = controller(clock, base_ms=100, max_ms=1_000, seed=1234)
        samples = [ctrl.backoff_for(3) for _ in range(400)]
        window = min(1.0, 0.1 * 2**2)
        assert all(0.0 <= value <= window for value in samples)
        # Spread across the window rather than clustered at the top, which is
        # what distinguishes full jitter from exponential-plus-a-little.
        assert min(samples) < window * 0.15
        assert max(samples) > window * 0.85
        assert len({round(value, 4) for value in samples}) > 100

    def test_backoff_is_capped(self) -> None:
        clock = ManualClock()
        ctrl = controller(clock, base_ms=100, max_ms=250)
        assert all(ctrl.backoff_for(20) <= 0.25 for _ in range(50))


class TestCircuitBreaker:
    def test_opens_after_the_threshold_of_transport_failures(self) -> None:
        clock = ManualClock()
        breaker = CircuitBreaker(failure_threshold=3, cooldown_ms=1_000, clock=clock)
        for _ in range(3):
            breaker.record_failure(UpstreamUnavailableError("503"))
        assert breaker.state is BreakerState.OPEN
        with pytest.raises(CircuitOpenError):
            breaker.allow(operation="fetch_deals")

    def test_a_contract_error_does_not_open_the_breaker(self) -> None:
        """Section 6.1: a caller's bad request is not an availability signal."""
        clock = ManualClock()
        breaker = CircuitBreaker(failure_threshold=2, cooldown_ms=1_000, clock=clock)
        for _ in range(10):
            breaker.record_failure(SourceContractError("malformed"))
        assert breaker.state is BreakerState.CLOSED
        assert breaker.allow(operation="fetch_deals") is False

    def test_half_open_admits_exactly_one_probe(self) -> None:
        """WF2-H-08: fifty waiting callers must not become fifty requests."""
        clock = ManualClock()
        breaker = CircuitBreaker(failure_threshold=1, cooldown_ms=1_000, clock=clock)
        breaker.record_failure(UpstreamUnavailableError("503"))
        clock.advance(1.5)
        assert breaker.state is BreakerState.HALF_OPEN

        assert breaker.allow(operation="fetch_deals") is True
        rejected = 0
        for _ in range(49):
            with pytest.raises(CircuitOpenError):
                breaker.allow(operation="fetch_deals")
            rejected += 1
        assert rejected == 49
        assert breaker.probes_total == 1

    def test_a_failed_probe_reopens_and_restarts_the_cooldown(self) -> None:
        clock = ManualClock()
        breaker = CircuitBreaker(failure_threshold=1, cooldown_ms=1_000, clock=clock)
        breaker.record_failure(UpstreamUnavailableError("503"))
        clock.advance(1.5)
        breaker.allow(operation="fetch_deals")
        breaker.record_failure(UpstreamUnavailableError("503"))
        assert breaker.state is BreakerState.OPEN
        clock.advance(0.5)
        assert breaker.state is BreakerState.OPEN

    def test_a_successful_probe_closes_the_breaker(self) -> None:
        clock = ManualClock()
        breaker = CircuitBreaker(failure_threshold=1, cooldown_ms=500, clock=clock)
        breaker.record_failure(UpstreamUnavailableError("503"))
        clock.advance(1.0)
        breaker.allow(operation="fetch_deals")
        breaker.record_success()
        assert breaker.state is BreakerState.CLOSED
        assert breaker.allow(operation="fetch_deals") is False

    async def test_a_failed_probe_is_not_retried_inside_one_operation(self) -> None:
        """One probe means one, including across a retry loop."""
        clock = ManualClock()
        breaker = CircuitBreaker(failure_threshold=1, cooldown_ms=100, clock=clock)
        breaker.record_failure(UpstreamUnavailableError("503"))
        clock.advance(0.5)
        attempts: list[int] = []
        stats = RetryStats()

        async def attempt(n: int, budget: float) -> str:
            attempts.append(n)
            raise UpstreamUnavailableError("503", upstream_status=503)

        with pytest.raises(UpstreamUnavailableError):
            await controller(clock, breaker=breaker, base_ms=1, max_ms=1).run(
                attempt,
                operation="fetch_deals",
                deadline=Deadline.after(5_000, clock=clock),
                stats=stats,
            )
        assert attempts == [1]
        assert stats.gave_up_reason == "probe_failed"

    async def test_a_fresh_operation_gets_a_fresh_budget(self) -> None:
        """WF2-H-11: a transient failure must not disable a source forever."""
        clock = ManualClock()
        ctrl = controller(clock, max_attempts=2, base_ms=1, max_ms=1)
        failing = 0

        async def attempt(n: int, budget: float) -> str:
            nonlocal failing
            failing += 1
            if failing <= 2:
                raise UpstreamUnavailableError("503", upstream_status=503)
            return "page"

        with pytest.raises(UpstreamUnavailableError):
            await ctrl.run(
                attempt, operation="fetch_deals", deadline=Deadline.after(5_000, clock=clock)
            )
        result = await ctrl.run(
            attempt, operation="fetch_deals", deadline=Deadline.after(5_000, clock=clock)
        )
        assert result == "page"

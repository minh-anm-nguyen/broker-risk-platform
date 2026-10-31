"""Retry policy: classified, bounded, and not nested across layers."""

from __future__ import annotations

import random

import pytest

from trading_sync.application.retry import (
    RetryBudget,
    ScanRetryPolicy,
    backoff_delay,
    with_db_retry,
)
from trading_sync.errors import (
    ImmutableDealConflict,
    OverloadedError,
    SnapshotExpiredError,
    TransientDatabaseError,
)

pytestmark = pytest.mark.unit


def policy(**overrides: object) -> ScanRetryPolicy:
    kwargs: dict[str, object] = {
        "max_attempts": 2,
        "base_seconds": 2.0,
        "cap_seconds": 60.0,
        "rng": random.Random(42),
    }
    kwargs.update(overrides)
    return ScanRetryPolicy(**kwargs)  # type: ignore[arg-type]


@pytest.mark.whatif("WF3-037", "WF3-135")
def test_expired_snapshot_is_retried() -> None:
    """A fresh scan from the durable watermark is the correct response."""
    decision = policy().decide(SnapshotExpiredError("410"), attempt=0)
    assert decision.retry
    assert decision.reason_code == "SNAPSHOT_EXPIRED"


@pytest.mark.whatif("WF3-028")
def test_money_conflict_is_never_retried() -> None:
    """Retrying would rediscover the same conflict and burn the budget.

    It is permanent until somebody investigates, which is a different state
    from "temporarily failing" and must not be treated as one (design 9.2).
    """
    decision = policy().decide(ImmutableDealConflict("conflict"), attempt=0)
    assert not decision.retry


def test_unclassified_errors_are_not_retried() -> None:
    """An error we cannot classify is not known to be safe to repeat."""
    assert not policy().decide(RuntimeError("boom"), attempt=0).retry


@pytest.mark.whatif("WF3-152")
def test_attempts_are_capped() -> None:
    assert policy(max_attempts=2).decide(SnapshotExpiredError(), attempt=2).reason_code == (
        "RETRY_ATTEMPTS_EXHAUSTED"
    )


@pytest.mark.whatif("WF3-152")
def test_budget_bounds_retries_over_time_not_just_in_a_row() -> None:
    """Repeated cursor expiry is the motivating case.

    Each scan looks like a fresh first attempt, so an attempt counter alone
    would permit an unbounded hot loop. The sliding window is what actually
    opens the breaker.
    """
    now = [1000.0]
    budget = RetryBudget(max_attempts=3, window_seconds=600, clock=lambda: now[0])
    retry_policy = policy(max_attempts=99, budget=budget)
    for _ in range(3):
        assert retry_policy.decide(SnapshotExpiredError(), attempt=0).retry
    exhausted = retry_policy.decide(SnapshotExpiredError(), attempt=0)
    assert not exhausted.retry
    assert exhausted.reason_code == "RETRY_BUDGET_EXHAUSTED"

    now[0] += 601  # the window slides
    assert retry_policy.decide(SnapshotExpiredError(), attempt=0).retry


@pytest.mark.whatif("WF3-136")
def test_retry_after_is_honoured_but_clamped() -> None:
    """Respected because upstream knows its load; clamped because a bad or
    hostile header could otherwise park a worker for hours."""
    decision = policy(cap_seconds=30.0).decide(
        OverloadedError("429", retry_after_seconds=3600), attempt=0
    )
    assert decision.retry
    assert decision.delay_seconds == 30.0


@pytest.mark.whatif("WF3-136")
def test_nonsense_retry_after_falls_back_to_backoff() -> None:
    decision = policy().decide(
        OverloadedError("429", retry_after_seconds="soon"), attempt=0
    )
    assert decision.retry
    assert 0.0 <= decision.delay_seconds <= 60.0


def test_backoff_grows_and_stays_capped() -> None:
    """Full jitter, so workers that failed together do not retry in lockstep."""
    rng = random.Random(7)
    for attempt in range(8):
        delay = backoff_delay(attempt, base_seconds=1.0, cap_seconds=10.0, rng=rng)
        assert 0.0 <= delay <= 10.0


@pytest.mark.whatif("WF3-114", "WF3-115")
async def test_db_retry_replays_the_whole_transaction() -> None:
    """Serialization failures retry; the operation is re-invoked from scratch.

    Resuming a partially applied transaction would not be idempotent, so the
    unit of retry is the whole callable.
    """
    attempts = 0
    slept: list[float] = []

    async def flaky() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise TransientDatabaseError("deadlock", sqlstate="40P01")
        return "committed"

    async def sleep(delay: float) -> None:
        slept.append(delay)

    result = await with_db_retry(flaky, attempts=3, sleep=sleep, rng=random.Random(1))
    assert result == "committed"
    assert attempts == 3
    assert len(slept) == 2


@pytest.mark.whatif("WF3-114")
async def test_db_retry_gives_up_and_reraises() -> None:
    async def always_deadlock() -> None:
        raise TransientDatabaseError("deadlock", sqlstate="40P01")

    async def sleep(_: float) -> None:
        return None

    with pytest.raises(TransientDatabaseError):
        await with_db_retry(always_deadlock, attempts=2, sleep=sleep)

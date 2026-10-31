"""Retry policy: bounded, classified, and separate at each layer.

Three rules from design 9.2, each of which exists because its opposite is a
common and expensive mistake.

**Layers do not nest their retries.** Transport retries belong to the
connector. If this service also wrapped each fetch in its own loop, the total
attempts would be the product of the two, and a brief upstream wobble would
become a minutes-long storm. What lives here is *scan-level* retry: a whole
new scan, from the durable watermark.

**Only a classified-retryable error is retried.** The decision reads the
error's ``retryable`` flag rather than its type or its message. A deal whose
money changed is permanent until somebody looks at it; retrying it would just
burn the budget rediscovering the same conflict.

**A budget bounds attempts over time, not just in a row.** Repeated cursor
expiry is the motivating case: each scan looks like a fresh first attempt, so
an attempt counter alone would allow an unbounded hot loop (WF3-152).
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Sequence, TypeVar

from trading_sync.errors import SyncError, TransientDatabaseError

__all__ = [
    "RetryBudget",
    "RetryDecision",
    "ScanRetryPolicy",
    "backoff_delay",
    "with_db_retry",
]

T = TypeVar("T")


def backoff_delay(
    attempt: int,
    *,
    base_seconds: float,
    cap_seconds: float,
    rng: random.Random | None = None,
) -> float:
    """Exponential backoff with full jitter, capped.

    Jitter is not decoration: without it, several workers that failed together
    retry together, and the retry traffic arrives in synchronised spikes. The
    generator is injectable so a test can assert exact delays.
    """
    if attempt < 0:
        raise ValueError("attempt must not be negative")
    ceiling = min(cap_seconds, base_seconds * (2**attempt))
    source = rng if rng is not None else random
    return source.uniform(0.0, ceiling)


@dataclass
class RetryBudget:
    """A sliding-window cap on attempts.

    ``max_attempts`` attempts per ``window_seconds``, judged by an injected
    clock so tests need no real waiting.
    """

    max_attempts: int
    window_seconds: float
    clock: Callable[[], float]
    _attempts: list[float] = field(default_factory=list, repr=False)

    def consume(self) -> bool:
        """Record an attempt; return ``False`` when the budget is exhausted."""
        now = self.clock()
        cutoff = now - self.window_seconds
        self._attempts = [stamp for stamp in self._attempts if stamp > cutoff]
        if len(self._attempts) >= self.max_attempts:
            return False
        self._attempts.append(now)
        return True

    @property
    def used(self) -> int:
        return len(self._attempts)


@dataclass(frozen=True, slots=True)
class RetryDecision:
    retry: bool
    delay_seconds: float
    reason_code: str


class ScanRetryPolicy:
    """Decides whether a failed scan is retried, and after how long."""

    def __init__(
        self,
        *,
        max_attempts: int,
        base_seconds: float,
        cap_seconds: float,
        budget: RetryBudget | None = None,
        rng: random.Random | None = None,
    ) -> None:
        self._max_attempts = max_attempts
        self._base = base_seconds
        self._cap = cap_seconds
        self._budget = budget
        self._rng = rng

    def decide(self, error: BaseException, *, attempt: int) -> RetryDecision:
        """``attempt`` is zero-based: the first failure is attempt 0."""
        if not isinstance(error, SyncError):
            # An unclassified failure is not known to be safe to repeat.
            return RetryDecision(False, 0.0, "UNCLASSIFIED")
        if not error.retryable:
            return RetryDecision(False, 0.0, error.reason_code)
        if attempt >= self._max_attempts:
            return RetryDecision(False, 0.0, "RETRY_ATTEMPTS_EXHAUSTED")
        if self._budget is not None and not self._budget.consume():
            return RetryDecision(False, 0.0, "RETRY_BUDGET_EXHAUSTED")
        delay = self._retry_after(error)
        if delay is None:
            delay = backoff_delay(
                attempt, base_seconds=self._base, cap_seconds=self._cap, rng=self._rng
            )
        return RetryDecision(True, delay, error.reason_code)

    def _retry_after(self, error: SyncError) -> float | None:
        """Honour an upstream ``Retry-After``, clamped to our own cap.

        Respected because the upstream knows its own load, clamped because a
        hostile or buggy header could otherwise park a worker for hours
        (WF3-136).
        """
        hint = getattr(error, "retry_after_seconds", None)
        if hint is None:
            return None
        try:
            value = float(hint)
        except (TypeError, ValueError):
            return None
        if value <= 0:
            return None
        return min(value, self._cap)


async def with_db_retry(
    operation: Callable[[], Awaitable[T]],
    *,
    attempts: int,
    sleep: Callable[[float], Awaitable[None]],
    base_seconds: float = 0.05,
    cap_seconds: float = 1.0,
    rng: random.Random | None = None,
) -> T:
    """Replay a whole database transaction on 40001 or 40P01.

    The *entire* transaction is re-run, which is the only form of retry that is
    sound here: resuming a partially applied transaction would not be
    idempotent. ``operation`` must therefore be a callable that starts its own
    transaction each time (design 9.2, WF3-114, WF3-115).
    """
    last: TransientDatabaseError | None = None
    for attempt in range(attempts + 1):
        try:
            return await operation()
        except TransientDatabaseError as exc:
            last = exc
            if attempt >= attempts:
                break
            await sleep(
                backoff_delay(
                    attempt, base_seconds=base_seconds, cap_seconds=cap_seconds, rng=rng
                )
            )
    assert last is not None
    raise last

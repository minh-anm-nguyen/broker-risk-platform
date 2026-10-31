"""Rate limiting and bounded admission.

These are the two places the service can say "no" cheaply, and saying no
cheaply is the whole defence against overload. The failure they prevent is not
a slow response - it is the unbounded one: a queue that keeps accepting work
grows memory until the kernel decides the outcome (WF-090, WF-092, WF-096).

Both are measured on the monotonic clock, so a wall-clock step cannot hand out
a burst of free capacity or stall a bucket for an hour.
"""

from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from contextlib import asynccontextmanager
from dataclasses import dataclass
from collections.abc import AsyncIterator

from ..domain.models import RateLimitedError, ServiceNotReadyError
from ..settings import Settings


@dataclass(slots=True)
class _Bucket:
    tokens: float
    updated_at: float


class RateLimiter:
    """Per-principal token bucket.

    The bucket table is bounded and evicts least-recently-used entries. Keys
    here are derived from tokens, so the set is naturally tiny; the cap exists
    so that a future per-client key cannot turn this into a memory leak.
    """

    def __init__(self, settings: Settings, *, max_keys: int = 1_024) -> None:
        self._enabled = settings.rate_limit_enabled
        self._rate = settings.rate_limit_rps
        self._capacity = float(settings.rate_limit_burst)
        self._max_keys = max_keys
        self._buckets: OrderedDict[str, _Bucket] = OrderedDict()

    @property
    def enabled(self) -> bool:
        return self._enabled

    def check(self, key: str, *, cost: float = 1.0) -> None:
        """Consume ``cost`` tokens or raise :class:`RateLimitedError`."""
        if not self._enabled:
            return
        now = time.monotonic()
        bucket = self._buckets.get(key)
        if bucket is None:
            bucket = _Bucket(tokens=self._capacity, updated_at=now)
            self._buckets[key] = bucket
            if len(self._buckets) > self._max_keys:
                self._buckets.popitem(last=False)
        else:
            self._buckets.move_to_end(key)
            elapsed = max(0.0, now - bucket.updated_at)
            bucket.tokens = min(self._capacity, bucket.tokens + elapsed * self._rate)
            bucket.updated_at = now

        if bucket.tokens < cost:
            deficit = cost - bucket.tokens
            retry_after = max(1, int(deficit / self._rate) + 1)
            raise RateLimitedError(
                "request rate limit exceeded",
                details={"retry_after_seconds": retry_after, "limit_rps": self._rate},
            )
        bucket.tokens -= cost

    def reset(self) -> None:
        self._buckets.clear()


class AdmissionController:
    """Bounded in-flight requests with a hard wait deadline.

    Waiting without a deadline is how a service dies quietly: every caller
    blocks, nothing is rejected, and the queue is invisible. A caller that
    cannot get a slot within the budget is told so, with a retryable error
    (section 9.3).
    """

    def __init__(self, settings: Settings) -> None:
        self._limit = settings.max_data_requests_in_flight
        self._timeout = settings.admission_timeout_ms / 1000.0
        self._semaphore = asyncio.Semaphore(self._limit)
        self._in_flight = 0
        self._peak = 0

    @property
    def limit(self) -> int:
        return self._limit

    @property
    def in_flight(self) -> int:
        return self._in_flight

    @property
    def peak_in_flight(self) -> int:
        return self._peak

    @asynccontextmanager
    async def slot(self) -> AsyncIterator[None]:
        try:
            if self._timeout <= 0:
                if self._semaphore.locked():
                    raise TimeoutError
                await self._semaphore.acquire()
            else:
                await asyncio.wait_for(self._semaphore.acquire(), timeout=self._timeout)
        except TimeoutError as exc:
            raise ServiceNotReadyError(
                "no request capacity available; retry after backoff",
                details={
                    "cause": "admission_timeout",
                    "max_in_flight": self._limit,
                    "waited_ms": int(self._timeout * 1000),
                },
            ) from exc
        self._in_flight += 1
        self._peak = max(self._peak, self._in_flight)
        try:
            yield
        finally:
            self._in_flight -= 1
            self._semaphore.release()

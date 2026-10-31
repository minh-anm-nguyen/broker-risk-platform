"""Per-principal rate limiting.

Rate and capacity are different failures and get different answers: exceeding
your own quota is ``429``, while the service running out of room is ``503``.
Conflating them makes a client retry the wrong thing.

Requests are weighted, so one expensive aggregate costs more quota than a
cheap single-account read. The counters are per process; a multi-replica
deployment needs a shared store (P1), and this class says so rather than
pretending local counters are global.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Callable, Mapping

__all__ = ["TokenBucket", "QuotaDecision", "PrincipalQuotas"]

Clock = Callable[[], float]


@dataclass(slots=True)
class TokenBucket:
    """Classic token bucket with a sustained rate and a burst ceiling."""

    rate_per_second: float
    burst: float
    tokens: float = field(default=0.0)
    # None rather than 0.0: a bucket first touched at t=0 must still refill.
    updated_at: float | None = None

    def __post_init__(self) -> None:
        if self.rate_per_second <= 0:
            raise ValueError("rate must be positive")
        if self.burst < self.rate_per_second:
            raise ValueError("burst must be at least the sustained rate")
        self.tokens = self.burst

    def _refill(self, now: float) -> None:
        if self.updated_at is None:
            self.updated_at = now
            return
        elapsed = max(0.0, now - self.updated_at)
        self.updated_at = now
        self.tokens = min(self.burst, self.tokens + elapsed * self.rate_per_second)

    def try_consume(self, weight: float, *, now: float) -> tuple[bool, float]:
        """Consume ``weight`` tokens. Returns success and a retry-after hint."""
        if weight <= 0:
            raise ValueError("request weight must be positive")
        if weight > self.burst:
            # A request that can never fit is refused immediately rather than
            # being told to retry forever.
            return False, math.inf
        self._refill(now)
        if self.tokens >= weight:
            self.tokens -= weight
            return True, 0.0
        deficit = weight - self.tokens
        return False, deficit / self.rate_per_second


@dataclass(frozen=True, slots=True)
class QuotaDecision:
    allowed: bool
    retry_after_seconds: int
    scope: str


class PrincipalQuotas:
    """Rate limits keyed by tenant, falling back to the principal."""

    def __init__(
        self,
        *,
        rate_per_second: float,
        burst: float,
        clock: Clock,
        overrides: Mapping[str, tuple[float, float]] | None = None,
    ) -> None:
        self._rate = rate_per_second
        self._burst = burst
        self._clock = clock
        self._overrides = dict(overrides or {})
        self._buckets: dict[str, TokenBucket] = {}

    def _bucket(self, key: str) -> TokenBucket:
        bucket = self._buckets.get(key)
        if bucket is None:
            rate, burst = self._overrides.get(key, (self._rate, self._burst))
            bucket = TokenBucket(rate_per_second=rate, burst=burst)
            self._buckets[key] = bucket
        return bucket

    def check(self, *, tenant_id: str, weight: float) -> QuotaDecision:
        """Charge ``weight`` against the tenant bucket."""
        bucket = self._bucket(tenant_id)
        allowed, retry_after = bucket.try_consume(weight, now=self._clock())
        if allowed:
            return QuotaDecision(True, 0, tenant_id)
        hint = 3600 if retry_after == math.inf else max(1, int(math.ceil(retry_after)))
        return QuotaDecision(False, hint, tenant_id)

    def tokens_left(self, tenant_id: str) -> float:
        return self._bucket(tenant_id).tokens

    def reset(self) -> None:
        self._buckets.clear()

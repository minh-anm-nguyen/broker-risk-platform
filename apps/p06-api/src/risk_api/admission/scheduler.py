"""Admission control.

Capacity is bounded in two independent dimensions (design doc section 7.3):
how many heavy reports may execute at once, and how many may wait. Both are
hard ceilings. When the queue is full the request is refused *before* any
projection read or engine call, which is the point: an overloaded service must
get cheaper under pressure, not more expensive.

A rejected request releases every resource it took. The counters here are what
the load tests assert against.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator, Literal

from ..contracts.problem_details import ApiProblem
from .budgets import Deadline
from .quotas import PrincipalQuotas

__all__ = ["WorkloadClass", "AdmissionController", "AdmissionStats"]

WorkloadClass = Literal["cheap", "heavy"]


@dataclass(slots=True)
class AdmissionStats:
    admitted_cheap: int = 0
    admitted_heavy: int = 0
    rejected_quota: int = 0
    rejected_capacity: int = 0
    executing_cheap: int = 0
    executing_heavy: int = 0
    queued_heavy: int = 0
    inflight: int = 0
    peak_executing_heavy: int = 0
    peak_queued_heavy: int = 0
    peak_inflight: int = 0

    def snapshot(self) -> dict[str, int]:
        return {
            "admitted_cheap": self.admitted_cheap,
            "admitted_heavy": self.admitted_heavy,
            "rejected_quota": self.rejected_quota,
            "rejected_capacity": self.rejected_capacity,
            "executing_cheap": self.executing_cheap,
            "executing_heavy": self.executing_heavy,
            "queued_heavy": self.queued_heavy,
            "inflight": self.inflight,
            "peak_executing_heavy": self.peak_executing_heavy,
            "peak_queued_heavy": self.peak_queued_heavy,
            "peak_inflight": self.peak_inflight,
        }


class AdmissionController:
    """Gate every request through quota, then capacity, then a bounded slot."""

    def __init__(
        self,
        *,
        quotas: PrincipalQuotas,
        max_heavy_executing: int,
        max_heavy_queued: int,
        max_cheap_concurrent: int,
        heavy_weight: float,
        cheap_weight: float,
        retry_after_seconds: int,
    ) -> None:
        if max_heavy_executing < 1 or max_cheap_concurrent < 1:
            raise ValueError("concurrency ceilings must be at least one")
        if max_heavy_queued < 0:
            raise ValueError("queue ceiling cannot be negative")
        self._quotas = quotas
        self._heavy_slots = asyncio.Semaphore(max_heavy_executing)
        self._cheap_slots = asyncio.Semaphore(max_cheap_concurrent)
        self._max_heavy_executing = max_heavy_executing
        self._max_heavy_queued = max_heavy_queued
        self._max_cheap_concurrent = max_cheap_concurrent
        self._heavy_weight = heavy_weight
        self._cheap_weight = cheap_weight
        self._retry_after = retry_after_seconds
        self.stats = AdmissionStats()

    # ----------------------------------------------------------------- quota

    def charge_quota(self, *, tenant_id: str, workload: WorkloadClass) -> None:
        """Charge the principal quota. Raises ``429`` when the bucket is empty."""
        weight = self._heavy_weight if workload == "heavy" else self._cheap_weight
        decision = self._quotas.check(tenant_id=tenant_id, weight=weight)
        if not decision.allowed:
            self.stats.rejected_quota += 1
            raise ApiProblem(
                "RATE_LIMITED",
                "the principal request quota is exhausted",
                headers={"Retry-After": str(decision.retry_after_seconds)},
            )

    # -------------------------------------------------------------- capacity

    @asynccontextmanager
    async def admit(
        self,
        *,
        tenant_id: str,
        workload: WorkloadClass,
        deadline: Deadline,
        charge_quota: bool = True,
    ) -> AsyncIterator[None]:
        """Reserve a bounded execution slot for the duration of the block."""
        if charge_quota:
            self.charge_quota(tenant_id=tenant_id, workload=workload)

        if workload == "cheap":
            async with self._reserve_cheap(deadline):
                yield
            return
        async with self._reserve_heavy(deadline):
            yield

    @asynccontextmanager
    async def _reserve_cheap(self, deadline: Deadline) -> AsyncIterator[None]:
        if self.stats.executing_cheap >= self._max_cheap_concurrent:
            # Refuse rather than queue: a cheap read is not worth waiting for.
            self.stats.rejected_capacity += 1
            raise ApiProblem(
                "OVERLOADED",
                "the service is at its concurrent read ceiling",
                headers={"Retry-After": str(self._retry_after)},
            )
        await self._cheap_slots.acquire()
        self.stats.admitted_cheap += 1
        self.stats.executing_cheap += 1
        self.stats.inflight += 1
        self.stats.peak_inflight = max(self.stats.peak_inflight, self.stats.inflight)
        try:
            yield
        finally:
            self.stats.executing_cheap -= 1
            self.stats.inflight -= 1
            self._cheap_slots.release()

    @asynccontextmanager
    async def _reserve_heavy(self, deadline: Deadline) -> AsyncIterator[None]:
        if self.stats.queued_heavy >= self._max_heavy_queued and self._no_slot_free():
            # The queue is full. Refuse now; do not grow an unbounded backlog.
            self.stats.rejected_capacity += 1
            raise ApiProblem(
                "OVERLOADED",
                "the heavy report queue is full",
                headers={"Retry-After": str(self._retry_after)},
            )

        self.stats.queued_heavy += 1
        self.stats.peak_queued_heavy = max(self.stats.peak_queued_heavy, self.stats.queued_heavy)
        wait_budget = deadline.slice(deadline.remaining())
        try:
            await asyncio.wait_for(self._heavy_slots.acquire(), timeout=max(0.001, wait_budget))
        except TimeoutError as exc:
            self.stats.queued_heavy -= 1
            self.stats.rejected_capacity += 1
            raise ApiProblem(
                "DEADLINE_EXCEEDED",
                "the request exceeded its deadline while queued for capacity",
                reason_codes=("DEADLINE_EXCEEDED",),
            ) from exc
        except asyncio.CancelledError:
            # A client that gave up must not leave a phantom queue entry.
            self.stats.queued_heavy -= 1
            raise

        self.stats.queued_heavy -= 1
        self.stats.admitted_heavy += 1
        self.stats.executing_heavy += 1
        self.stats.inflight += 1
        self.stats.peak_executing_heavy = max(
            self.stats.peak_executing_heavy, self.stats.executing_heavy
        )
        self.stats.peak_inflight = max(self.stats.peak_inflight, self.stats.inflight)
        try:
            yield
        finally:
            self.stats.executing_heavy -= 1
            self.stats.inflight -= 1
            self._heavy_slots.release()

    def _no_slot_free(self) -> bool:
        return self.stats.executing_heavy >= self._max_heavy_executing

    # --------------------------------------------------------------- limits

    @property
    def max_heavy_executing(self) -> int:
        return self._max_heavy_executing

    @property
    def max_heavy_queued(self) -> int:
        return self._max_heavy_queued

    @property
    def max_cheap_concurrent(self) -> int:
        return self._max_cheap_concurrent

    def healthy(self) -> bool:
        """Readiness view: queued work must be within its ceiling."""
        return self.stats.queued_heavy <= self._max_heavy_queued

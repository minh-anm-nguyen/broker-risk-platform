"""Admission control, deadlines and workload guards.

Overload has to produce an error, not a queue. An unbounded queue converts a
traffic spike into unbounded latency and then into an OOM kill, and the caller
learns about it last. So concurrency is capped, the waiting room is capped, and
anything past both is rejected in milliseconds with ``OVERLOADED`` (section 8.1,
WF5-211).

Deadlines are measured on a monotonic clock. Wall-clock time can jump backwards
through an NTP correction, which on a wall-clock deadline means a request that
never expires (WF5-182).
"""

from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from decimal import Decimal
from typing import AsyncIterator, Iterable, Iterator, TypeVar

from risk.errors import ReasonCode, ResourceError
from risk.policies.loader import LimitPolicy

__all__ = [
    "Deadline",
    "AdmissionController",
    "check_request_limits",
    "check_estimated_input_bytes",
    "batched",
    "BYTES_PER_ROW_ESTIMATE",
]

#: Conservative per-row footprint used by the pre-flight memory guard. A
#: declared estimate, not a measurement: the decoded size depends on the
#: driver, so the guard is coarse on purpose (section 15.3).
BYTES_PER_ROW_ESTIMATE = 512

T = TypeVar("T")


@dataclass
class Deadline:
    """A monotonic time budget for one request."""

    budget_seconds: Decimal
    _started: float = 0.0

    def __post_init__(self) -> None:
        if self.budget_seconds <= 0:
            raise ResourceError(ReasonCode.DEADLINE_EXCEEDED, "deadline budget must be positive")
        self._started = time.monotonic()

    @property
    def elapsed_seconds(self) -> Decimal:
        return Decimal(str(round(time.monotonic() - self._started, 6)))

    @property
    def remaining_seconds(self) -> Decimal:
        return self.budget_seconds - self.elapsed_seconds

    @property
    def expired(self) -> bool:
        return self.remaining_seconds <= 0

    def check(self, *, stage: str) -> None:
        """Raise if the budget is gone.

        Called at stage boundaries rather than inside hot loops: the point is to
        stop the next expensive thing, not to pay for a clock read per row.
        """
        if self.expired:
            raise ResourceError(
                ReasonCode.DEADLINE_EXCEEDED,
                "request exceeded its time budget",
                stage=stage,
                budget_seconds=str(self.budget_seconds),
            )


class AdmissionController:
    """Bounded concurrency with a bounded waiting room.

    Two counters, not one. The semaphore caps *running* work so the CPU is not
    oversubscribed; ``max_queued`` caps *waiting* work so that a backlog cannot
    grow without limit. Dropping either one reintroduces the failure mode the
    other prevents.
    """

    def __init__(self, limits: LimitPolicy) -> None:
        self._semaphore = asyncio.Semaphore(limits.max_concurrent_jobs)
        self._max_running = limits.max_concurrent_jobs
        self._max_queued = limits.max_queued_jobs
        self._running = 0
        self._queued = 0
        self._rejected = 0

    @property
    def running(self) -> int:
        return self._running

    @property
    def queued(self) -> int:
        return self._queued

    @property
    def rejected(self) -> int:
        return self._rejected

    @asynccontextmanager
    async def admit(self) -> AsyncIterator[None]:
        """Acquire a slot or refuse immediately."""
        if self._running >= self._max_running and self._queued >= self._max_queued:
            self._rejected += 1
            raise ResourceError(
                ReasonCode.OVERLOADED,
                "all worker slots and the queue are full; retry with backoff",
                running=self._running,
                queued=self._queued,
            )
        self._queued += 1
        try:
            await self._semaphore.acquire()
        finally:
            self._queued -= 1
        self._running += 1
        try:
            yield
        finally:
            self._running -= 1
            self._semaphore.release()


def check_request_limits(
    limits: LimitPolicy,
    *,
    account_count: int,
    position_count: int | None = None,
    deal_count: int | None = None,
) -> None:
    """Reject a workload that exceeds the declared envelope.

    Checked before the data is loaded where possible. Discovering at row
    900 000 that the limit was 10 000 means the memory has already been spent
    (WF5-243).
    """
    if account_count > limits.max_accounts_per_request:
        raise ResourceError(
            ReasonCode.LIMIT_EXCEEDED,
            "too many accounts in one request",
            requested=account_count,
            limit=limits.max_accounts_per_request,
        )
    if position_count is not None and position_count > limits.max_positions_per_request:
        raise ResourceError(
            ReasonCode.LIMIT_EXCEEDED,
            "too many positions in one request",
            requested=position_count,
            limit=limits.max_positions_per_request,
        )
    if deal_count is not None and deal_count > limits.max_deals_per_window:
        raise ResourceError(
            ReasonCode.LIMIT_EXCEEDED,
            "too many deals in the requested window",
            requested=deal_count,
            limit=limits.max_deals_per_window,
        )


def batched(items: Iterable[T], size: int) -> Iterator[tuple[T, ...]]:
    """Yield fixed-size tuples from an iterable.

    Used so that a one-million-row aggregation holds one batch in memory rather
    than the whole result set. Batch size must not change the answer, which is
    an explicit test rather than an assumption (WF5-319, WF5-320).
    """
    if size <= 0:
        raise ResourceError(ReasonCode.LIMIT_EXCEEDED, "batch size must be positive", size=size)
    batch: list[T] = []
    for item in items:
        batch.append(item)
        if len(batch) >= size:
            yield tuple(batch)
            batch = []
    if batch:
        yield tuple(batch)


def check_estimated_input_bytes(
    limits: LimitPolicy, *, row_count: int, bytes_per_row: int = BYTES_PER_ROW_ESTIMATE
) -> None:
    """Refuse a row set whose estimated footprint exceeds the memory budget.

    Checked from a row *count* before the rows are built, which is the only
    point at which refusing is still cheap. ``bytes_per_row`` is a declared
    estimate, not a measurement: the exact decoded size depends on the driver,
    so the guard is deliberately coarse and conservative rather than precise
    and late (section 15.3).
    """
    estimated = row_count * bytes_per_row
    if estimated > limits.max_input_bytes:
        raise ResourceError(
            ReasonCode.INPUT_TOO_LARGE,
            "estimated input size exceeds the per-request memory budget",
            row_count=row_count,
            estimated_bytes=estimated,
            limit=limits.max_input_bytes,
        )

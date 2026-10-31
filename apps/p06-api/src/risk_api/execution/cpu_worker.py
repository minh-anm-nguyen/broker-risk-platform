"""Bounded CPU isolation for engine work.

Decimal aggregation is CPU work. Running it inline would block the event loop
and stall every other request, so it goes to a bounded thread pool with:

* a hard ceiling on workers, so the pool cannot grow under load;
* a per-submission deadline that abandons the *wait*, never claiming to have
  stopped the computation;
* a generation fence so an abandoned computation's result is discarded;
* a heartbeat task that measures event-loop scheduling delay, which is the
  observable that proves the loop stayed responsive.

A truly wedged worker is an operational matter: the pool reports it and the
instance can be recycled. Nothing here pretends a timeout killed it.
"""

from __future__ import annotations

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Callable, TypeVar

from ..contracts.problem_details import ApiProblem
from .cancellation import FenceToken, GenerationFence, LateResultDiscarded

__all__ = ["CpuWorkerPool", "WorkerStats", "HeartbeatMonitor"]

T = TypeVar("T")


@dataclass(slots=True)
class WorkerStats:
    submitted: int = 0
    completed: int = 0
    timed_out: int = 0
    failed: int = 0
    late_discarded: int = 0
    in_flight: int = 0
    peak_in_flight: int = 0
    cpu_seconds: float = 0.0
    abandoned_still_running: int = 0

    def snapshot(self) -> dict[str, float]:
        return {
            "submitted": self.submitted,
            "completed": self.completed,
            "timed_out": self.timed_out,
            "failed": self.failed,
            "late_discarded": self.late_discarded,
            "in_flight": self.in_flight,
            "peak_in_flight": self.peak_in_flight,
            "cpu_seconds": round(self.cpu_seconds, 6),
            "abandoned_still_running": self.abandoned_still_running,
        }


@dataclass(slots=True)
class HeartbeatMonitor:
    """Measures how late the event loop runs a task that asked for 10 ms."""

    interval_seconds: float = 0.01
    samples: list[float] = field(default_factory=list)
    max_samples: int = 2000
    _task: asyncio.Task | None = None

    async def _run(self) -> None:
        while True:
            start = time.perf_counter()
            try:
                await asyncio.sleep(self.interval_seconds)
            except asyncio.CancelledError:
                return
            lag = (time.perf_counter() - start) - self.interval_seconds
            self.samples.append(max(0.0, lag))
            if len(self.samples) > self.max_samples:
                del self.samples[: len(self.samples) - self.max_samples]

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run(), name="event-loop-heartbeat")

    async def stop(self) -> None:
        task = self._task
        self._task = None
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    def reset(self) -> None:
        self.samples.clear()

    def percentile(self, fraction: float) -> float:
        if not self.samples:
            return 0.0
        ordered = sorted(self.samples)
        index = min(len(ordered) - 1, max(0, int(round(fraction * (len(ordered) - 1)))))
        return ordered[index]

    @property
    def p99_lag_seconds(self) -> float:
        return self.percentile(0.99)

    @property
    def max_lag_seconds(self) -> float:
        return max(self.samples) if self.samples else 0.0


class CpuWorkerPool:
    """A bounded executor for engine computations."""

    def __init__(
        self,
        *,
        max_workers: int,
        thread_name_prefix: str = "risk-cpu",
    ) -> None:
        if max_workers < 1:
            raise ValueError("at least one worker is required")
        self._max_workers = max_workers
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix=thread_name_prefix
        )
        self._fence: GenerationFence[object] = GenerationFence()
        self._admit = asyncio.Semaphore(max_workers)
        self.stats = WorkerStats()
        self.heartbeat = HeartbeatMonitor()

    # ------------------------------------------------------------- lifecycle

    def start(self) -> None:
        self.heartbeat.start()

    async def shutdown(self, *, drain_seconds: float = 2.0) -> None:
        """Stop accepting work and give in-flight work a bounded drain window."""
        await self.heartbeat.stop()
        deadline = time.monotonic() + drain_seconds
        while self.stats.in_flight > 0 and time.monotonic() < deadline:
            await asyncio.sleep(0.01)
        # ``cancel_futures`` only drops work that never started.
        self._executor.shutdown(wait=False, cancel_futures=True)

    # ---------------------------------------------------------------- submit

    async def run(
        self,
        func: Callable[[], T],
        *,
        timeout_seconds: float,
        request_id: str,
    ) -> T:
        """Run ``func`` off the event loop under a deadline.

        On timeout this raises ``504`` and marks the submission abandoned. The
        thread may still be running; that is recorded, not hidden.
        """
        if timeout_seconds <= 0:
            raise ApiProblem(
                "DEADLINE_EXCEEDED",
                "no compute budget remained for this request",
                reason_codes=("DEADLINE_EXCEEDED",),
            )

        token: FenceToken = self._fence.issue(request_id)
        self.stats.submitted += 1
        self.stats.in_flight += 1
        self.stats.peak_in_flight = max(self.stats.peak_in_flight, self.stats.in_flight)
        loop = asyncio.get_running_loop()
        started = time.perf_counter()
        future = loop.run_in_executor(self._executor, func)
        try:
            result = await asyncio.wait_for(asyncio.shield(future), timeout=timeout_seconds)
        except TimeoutError:
            self.stats.timed_out += 1
            self.stats.abandoned_still_running += 1
            self._fence.retire(token)
            # Keep accounting for the thread until it genuinely finishes.
            future.add_done_callback(lambda _f: self._release_abandoned())
            raise ApiProblem(
                "DEADLINE_EXCEEDED",
                "the computation exceeded its CPU budget",
                reason_codes=("DEADLINE_EXCEEDED",),
            ) from None
        except asyncio.CancelledError:
            self._fence.retire(token)
            self.stats.abandoned_still_running += 1
            future.add_done_callback(lambda _f: self._release_abandoned())
            raise
        except Exception:
            self.stats.failed += 1
            self.stats.in_flight -= 1
            self._fence.retire(token)
            raise
        else:
            self.stats.cpu_seconds += time.perf_counter() - started
            self.stats.completed += 1
            self.stats.in_flight -= 1
            try:
                return self._fence.accept(token, result)  # type: ignore[return-value]
            except LateResultDiscarded:
                self.stats.late_discarded += 1
                raise ApiProblem(
                    "DEADLINE_EXCEEDED",
                    "the computation finished after its request was abandoned",
                    reason_codes=("DEADLINE_EXCEEDED",),
                ) from None

    def _release_abandoned(self) -> None:
        self.stats.in_flight = max(0, self.stats.in_flight - 1)
        self.stats.abandoned_still_running = max(0, self.stats.abandoned_still_running - 1)

    # ---------------------------------------------------------------- status

    @property
    def max_workers(self) -> int:
        return self._max_workers

    @property
    def late_discarded(self) -> int:
        return self._fence.discarded

    def healthy(self) -> bool:
        """Readiness view: workers must not all be stuck on abandoned work."""
        return self.stats.abandoned_still_running < self._max_workers

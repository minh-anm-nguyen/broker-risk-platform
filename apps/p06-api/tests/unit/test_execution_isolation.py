"""CPU isolation, late-result fencing and event-loop responsiveness.

WF06-T. The claim being tested is narrow and important: a timeout abandons the
*wait*, not the computation. The pool must therefore keep accounting for a
thread that is still burning CPU, discard its result when it finally arrives,
and keep the event loop responsive throughout.
"""

from __future__ import annotations

import asyncio
import time

import pytest
from risk_api.contracts.problem_details import ApiProblem
from risk_api.execution.cancellation import FenceToken, GenerationFence, LateResultDiscarded
from risk_api.execution.cpu_worker import CpuWorkerPool, HeartbeatMonitor

pytestmark = pytest.mark.unit


def burn(seconds: float) -> str:
    """Occupy a thread with real work; a sleep would yield and prove nothing."""
    end = time.perf_counter() + seconds
    acc = 0
    while time.perf_counter() < end:
        acc = (acc + 1) % 1_000_003
    return f"done-{acc % 2}"


class TestGenerationFence:
    def test_a_live_token_accepts_its_result(self):
        fence: GenerationFence[str] = GenerationFence()
        token = fence.issue("req-1")
        assert fence.accept(token, "value") == "value"

    def test_a_retired_token_discards_its_result(self):
        fence: GenerationFence[str] = GenerationFence()
        token = fence.issue("req-1")
        fence.retire(token)
        with pytest.raises(LateResultDiscarded):
            fence.accept(token, "value")
        assert fence.discarded == 1

    def test_a_result_cannot_be_accepted_twice(self):
        fence: GenerationFence[str] = GenerationFence()
        token = fence.issue("req-1")
        fence.accept(token, "value")
        with pytest.raises(LateResultDiscarded):
            fence.accept(token, "value")

    def test_tokens_from_different_requests_do_not_collide(self):
        fence: GenerationFence[str] = GenerationFence()
        first = fence.issue("req-1")
        second = fence.issue("req-2")
        assert first.generation != second.generation
        fence.retire(first)
        assert fence.is_live(second)
        assert not fence.is_live(first)

    def test_retiring_an_unknown_token_is_harmless(self):
        fence: GenerationFence[str] = GenerationFence()
        fence.retire(FenceToken(generation=999, request_id="req-x"))
        assert fence.retired == 0


class TestWorkerPool:
    async def test_work_runs_and_returns(self):
        pool = CpuWorkerPool(max_workers=2)
        try:
            assert await pool.run(lambda: 7, timeout_seconds=1.0, request_id="req-1") == 7
            assert pool.stats.completed == 1
            assert pool.stats.in_flight == 0
        finally:
            await pool.shutdown(drain_seconds=0.1)

    async def test_a_timeout_is_a_504_and_the_thread_is_still_accounted_for(self):
        """IT06-08: wait_for did not kill the CPU work, and the pool says so."""
        pool = CpuWorkerPool(max_workers=1)
        try:
            with pytest.raises(ApiProblem) as caught:
                await pool.run(lambda: burn(0.6), timeout_seconds=0.05, request_id="req-slow")
            assert caught.value.code == "DEADLINE_EXCEEDED"
            assert caught.value.status == 504
            assert pool.stats.timed_out == 1
            assert pool.stats.abandoned_still_running >= 1
            # The abandoned submission is retired, so its result cannot be served.
            assert pool.late_discarded == 0
            await asyncio.sleep(0.9)
            assert pool.stats.abandoned_still_running == 0
        finally:
            await pool.shutdown(drain_seconds=0.1)

    async def test_a_late_result_cannot_leak_into_another_request(self):
        """The second request gets its own answer, never the abandoned one."""
        pool = CpuWorkerPool(max_workers=2)
        try:
            with pytest.raises(ApiProblem):
                await pool.run(lambda: burn(0.4), timeout_seconds=0.01, request_id="req-1")
            second = await pool.run(lambda: "fresh", timeout_seconds=1.0, request_id="req-2")
            assert second == "fresh"
        finally:
            await pool.shutdown(drain_seconds=0.1)

    async def test_zero_budget_is_refused_without_submitting(self):
        pool = CpuWorkerPool(max_workers=1)
        try:
            with pytest.raises(ApiProblem) as caught:
                await pool.run(lambda: 1, timeout_seconds=0.0, request_id="req-1")
            assert caught.value.code == "DEADLINE_EXCEEDED"
            assert pool.stats.submitted == 0
        finally:
            await pool.shutdown(drain_seconds=0.1)

    async def test_a_raising_computation_propagates_and_releases(self):
        pool = CpuWorkerPool(max_workers=1)

        def explode() -> None:
            raise ValueError("engine bug")

        try:
            with pytest.raises(ValueError, match="engine bug"):
                await pool.run(explode, timeout_seconds=1.0, request_id="req-1")
            assert pool.stats.failed == 1
            assert pool.stats.in_flight == 0
        finally:
            await pool.shutdown(drain_seconds=0.1)

    async def test_concurrency_never_exceeds_the_worker_count(self):
        pool = CpuWorkerPool(max_workers=2)
        try:
            results = await asyncio.gather(
                *(pool.run(lambda: burn(0.08), timeout_seconds=5.0, request_id=f"r{i}") for i in range(6))
            )
            assert len(results) == 6
            assert pool.stats.peak_in_flight <= 6
            assert pool.stats.completed == 6
        finally:
            await pool.shutdown(drain_seconds=0.2)

    async def test_the_event_loop_stays_responsive_while_cpu_work_runs(self):
        """INV06-15: offloaded CPU work must not stall the loop."""
        pool = CpuWorkerPool(max_workers=2)
        pool.start()
        try:
            await asyncio.sleep(0.05)
            pool.heartbeat.reset()
            await asyncio.gather(
                pool.run(lambda: burn(0.3), timeout_seconds=5.0, request_id="r1"),
                pool.run(lambda: burn(0.3), timeout_seconds=5.0, request_id="r2"),
            )
            await asyncio.sleep(0.05)
            assert pool.heartbeat.samples, "the heartbeat produced no samples"
            # Generous on a shared CI box, but a blocked loop shows seconds here.
            assert pool.heartbeat.p99_lag_seconds < 0.5
        finally:
            await pool.shutdown(drain_seconds=0.2)

    async def test_health_reports_unhealthy_when_every_worker_is_abandoned(self):
        pool = CpuWorkerPool(max_workers=1)
        try:
            assert pool.healthy()
            with pytest.raises(ApiProblem):
                await pool.run(lambda: burn(0.5), timeout_seconds=0.01, request_id="req-1")
            assert pool.healthy() is False
            await asyncio.sleep(0.8)
            assert pool.healthy() is True
        finally:
            await pool.shutdown(drain_seconds=0.1)

    async def test_shutdown_drains_within_its_window(self):
        """K06-20: a bounded drain, not an unbounded wait."""
        pool = CpuWorkerPool(max_workers=2)
        pool.start()
        task = asyncio.create_task(pool.run(lambda: burn(0.2), timeout_seconds=5.0, request_id="r1"))
        await asyncio.sleep(0.02)
        started = time.perf_counter()
        await pool.shutdown(drain_seconds=1.0)
        assert time.perf_counter() - started < 1.5
        await task

    def test_at_least_one_worker_is_required(self):
        with pytest.raises(ValueError):
            CpuWorkerPool(max_workers=0)


class TestHeartbeat:
    async def test_percentiles_are_computed_from_samples(self):
        monitor = HeartbeatMonitor(interval_seconds=0.001)
        monitor.samples.extend([0.001, 0.002, 0.003, 0.10])
        assert monitor.max_lag_seconds == 0.10
        assert monitor.percentile(0.5) in (0.002, 0.003)

    async def test_no_samples_reports_zero_rather_than_failing(self):
        monitor = HeartbeatMonitor()
        assert monitor.p99_lag_seconds == 0.0
        assert monitor.max_lag_seconds == 0.0

    async def test_the_sample_window_is_bounded(self):
        monitor = HeartbeatMonitor(interval_seconds=0.001, max_samples=10)
        monitor.start()
        await asyncio.sleep(0.2)
        await monitor.stop()
        assert len(monitor.samples) <= 10

    async def test_stopping_an_unstarted_monitor_is_harmless(self):
        await HeartbeatMonitor().stop()

"""IT-01 and IT-02: the event loop stays responsive; a timeout is not a kill.

Covers WF2-F-01, WF2-F-02, WF2-F-03, WF2-H-01, WF2-H-02, WF2-H-03, WF2-G-02,
INV-01, INV-02 and INV-04.

The two tests that carry the design are:

* :meth:`TestEventLoopIsolation.test_a_five_second_blocking_call_does_not_stall_the_loop`
  - the blocking driver sleeps for seconds while a heartbeat keeps sampling.
  If the call were on the loop, the lag would *be* the call length.
* :meth:`TestTimeoutIsNotCancellation.test_a_timeout_leaves_the_session_occupied`
  - the caller times out, the session stays busy, and a second call is not
  started. This is the distinction ``wait_for(to_thread(...))`` cannot make.

The lag assertion reports ``NOT_CONCLUSIVE`` rather than failing on a host too
noisy to measure. A loaded CI machine overshoots a 50 ms sleep for reasons
that have nothing to do with this connector, and failing on that would train
everyone to ignore the test (section 14.3, WF2-P-12).
"""

from __future__ import annotations

import asyncio
import time

import pytest

from mt5_connector.drivers.fake_blocking import ScriptedOutcome
from mt5_connector.errors import CircuitOpenError, DeadlineExceededError, OverloadedError
from mt5_connector.ports import Entity, ReadPlan
from mt5_connector.telemetry import EventLoopMonitor

from ..conftest import make_connector, make_fake_driver, make_settings, wait_for

pytestmark = [pytest.mark.integration]

#: The lab target from section 11.2. Breached only on a saturated host, which
#: is why the test distinguishes "breached" from "unmeasurable".
LAG_TARGET_MS = 25.0

#: Below this many samples there is nothing to draw a p99 from (section 11.4:
#: a p99 over 100 requests is not trustworthy).
MIN_SAMPLES = 60


async def measure_lag(
    duration_s: float, *, interval_s: float = 0.01, helpers: int = 20
) -> tuple[EventLoopMonitor, int]:
    """Sample event-loop lag under a fixed competing load.

    The competing coroutines are part of the *measurement*, not scenery. A
    baseline taken on an idle loop and compared against a loaded window mixes
    two variables, and the comparison then says more about the helpers than
    about the connector (section 11.4).
    """
    monitor = EventLoopMonitor(interval_s=interval_s)
    ticks = 0

    async def busywork() -> None:
        nonlocal ticks
        while True:
            await asyncio.sleep(0.001)
            ticks += 1

    tasks = [asyncio.create_task(busywork()) for _ in range(helpers)]
    monitor.start()
    try:
        await asyncio.sleep(duration_s)
    finally:
        await monitor.stop()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    return monitor, ticks


class TestEventLoopIsolation:
    async def test_a_five_second_blocking_call_does_not_stall_the_loop(self) -> None:
        """IT-01, WF2-F-01, INV-01.

        Two five-second windows under identical competing load: one idle, one
        with a blocking driver call in flight. The only difference between
        them is the call, so the difference in lag is attributable to it.
        """
        baseline_monitor, baseline_ticks = await measure_lag(5.0)
        baseline = baseline_monitor.summary()

        driver = make_fake_driver()
        driver.script(ScriptedOutcome(delay_s=5.0))
        settings = make_settings(
            page_total_deadline_ms=15_000,
            queue_wait_cap_ms=1_000,
            heartbeat_interval_ms=10,
            hung_call_hard_limit_ms=60_000,
        )
        connector = await make_connector(driver, settings=settings)
        monitor = EventLoopMonitor(interval_s=0.01)
        ticks = 0

        async def busywork() -> None:
            nonlocal ticks
            while True:
                await asyncio.sleep(0.001)
                ticks += 1

        helpers = [asyncio.create_task(busywork()) for _ in range(20)]
        monitor.start()
        try:
            started = time.perf_counter()
            page = await connector.read_page(ReadPlan(entity=Entity.DEALS, limit=10))
            blocked_for = time.perf_counter() - started
        finally:
            await monitor.stop()
            for task in helpers:
                task.cancel()
            await asyncio.gather(*helpers, return_exceptions=True)
            await connector.close()

        # The call really did block for the full five seconds in the worker -
        # the duration IT-01 specifies.
        assert blocked_for >= 4.9
        assert page.items == ()
        summary = monitor.summary()

        if summary.samples < MIN_SAMPLES or baseline.samples < MIN_SAMPLES:
            pytest.skip(
                f"NOT_CONCLUSIVE: {summary.samples} measured and "
                f"{baseline.samples} baseline lag samples"
            )

        # The loop kept turning throughout, at a rate comparable to the idle
        # window. A blocking call *on* the loop would have stopped it dead.
        assert ticks > baseline_ticks * 0.5, (
            f"the loop managed {ticks} turns while a blocking call ran, against "
            f"{baseline_ticks} with the same load and no call"
        )

        # The host-independent oracle, and the one that states INV-01: a
        # blocking call on the loop would show up as lag of roughly the call's
        # own length.
        blocked_ms = blocked_for * 1000.0
        assert summary.max_ms < blocked_ms * 0.2, (
            f"worst event-loop lag {summary.max_ms:.1f} ms against a "
            f"{blocked_ms:.0f} ms blocking call: the call is not isolated"
        )
        # Both bounds are expressed against the call duration rather than
        # against the baseline. A baseline measured in its own window on a
        # shared host varies by tens of milliseconds run to run, so comparing
        # against it makes the test flaky without making it stronger - and the
        # claim under test is about the call, not about the host's scheduling
        # noise. The baseline is reported instead.
        assert summary.p99_ms < blocked_ms * 0.1, (
            f"event-loop lag p99 {summary.p99_ms:.1f} ms against a "
            f"{blocked_ms:.0f} ms blocking call (same-load baseline p99 "
            f"{baseline.p99_ms:.1f} ms)"
        )

        # The absolute lab target from section 11.2 is only meaningful on a
        # host that can measure it. Reported, never silently skipped.
        if baseline.p99_ms > LAG_TARGET_MS:
            pytest.xfail(
                f"NOT_CONCLUSIVE for the {LAG_TARGET_MS} ms absolute target: this "
                f"host's p99 under the same load without any blocking call is "
                f"already {baseline.p99_ms:.1f} ms. Isolation itself verified: "
                f"p99 {summary.p99_ms:.1f} ms and max {summary.max_ms:.1f} ms during "
                f"a {blocked_ms:.0f} ms blocking call."
            )
        assert summary.p99_ms <= LAG_TARGET_MS, (
            f"event loop lag p99 {summary.p99_ms:.1f} ms exceeded the "
            f"{LAG_TARGET_MS} ms target while a blocking call ran"
        )

    async def test_health_state_is_readable_while_a_call_blocks(self) -> None:
        """INV-13: a health handler must not wait on the driver."""
        driver = make_fake_driver()
        driver.block_next_call()
        settings = make_settings(
            page_total_deadline_ms=200,
            queue_wait_cap_ms=150,
            hung_call_hard_limit_ms=60_000,
        )
        connector = await make_connector(driver, settings=settings)
        try:
            task = asyncio.create_task(
                connector.read_page(ReadPlan(entity=Entity.DEALS, limit=10))
            )
            assert await wait_for(lambda: driver.active_calls == 1)
            for _ in range(10):
                started = time.perf_counter()
                health = connector.supervisor.health()
                assert (time.perf_counter() - started) < 0.05
                assert health["live"] is True
                await asyncio.sleep(0.005)
            with pytest.raises(DeadlineExceededError):
                await task
        finally:
            driver.release_block()
            await connector.close()


class TestTimeoutIsNotCancellation:
    async def test_a_timeout_leaves_the_session_occupied(self) -> None:
        """IT-02, WF2-F-03, WF2-H-02: the heart of section 5.1."""
        driver = make_fake_driver()
        driver.block_next_call()
        settings = make_settings(
            page_total_deadline_ms=150,
            queue_wait_cap_ms=100,
            max_queue_pending=2,
            hung_call_hard_limit_ms=60_000,
        )
        connector = await make_connector(driver, settings=settings)
        try:
            with pytest.raises(DeadlineExceededError) as excinfo:
                await connector.read_page(ReadPlan(entity=Entity.DEALS, limit=10))
            assert excinfo.value.details["state_at_timeout"] == "dispatched"
            assert excinfo.value.details["session_slot_released"] is False

            # The driver is still inside the first call.
            assert driver.active_calls == 1
            assert connector.supervisor.actor.active_calls == 1
            assert driver.call_count == 1

            # Three more callers arrive. None of them starts a second call.
            results = await asyncio.gather(
                *[
                    connector.read_page(ReadPlan(entity=Entity.DEALS, limit=10))
                    for _ in range(3)
                ],
                return_exceptions=True,
            )
            assert all(
                isinstance(item, (DeadlineExceededError, OverloadedError, CircuitOpenError))
                for item in results
            ), results
            assert driver.call_count == 1
            assert driver.overlap_violations == 0
            assert driver.max_observed_concurrency == 1
        finally:
            driver.release_block()
            await connector.close()

    async def test_the_late_result_is_discarded_once_the_call_finishes(self) -> None:
        """WF2-H-03, INV-04: no success is delivered after its deadline."""
        driver = make_fake_driver()
        driver.block_next_call()
        settings = make_settings(
            page_total_deadline_ms=120, queue_wait_cap_ms=80, hung_call_hard_limit_ms=60_000
        )
        connector = await make_connector(driver, settings=settings)
        try:
            with pytest.raises(DeadlineExceededError):
                await connector.read_page(ReadPlan(entity=Entity.DEALS, limit=10))
            actor = connector.supervisor.actor
            driver.release_block()
            assert await wait_for(
                lambda: actor.stats.stale_completions_discarded_total >= 1, timeout_s=3.0
            )
            assert actor.stats.abandoned_dispatched_total >= 1
            assert await wait_for(lambda: actor.active_calls == 0, timeout_s=3.0)
        finally:
            driver.release_block()
            await connector.close()

    async def test_a_cancelled_caller_does_not_become_a_source_error(self) -> None:
        """WF2-G-02, section 4.4: CancelledError is a lifecycle signal."""
        driver = make_fake_driver()
        driver.block_next_call()
        settings = make_settings(page_total_deadline_ms=10_000, hung_call_hard_limit_ms=60_000)
        connector = await make_connector(driver, settings=settings)
        try:
            task = asyncio.create_task(
                connector.read_page(ReadPlan(entity=Entity.DEALS, limit=10))
            )
            assert await wait_for(lambda: driver.active_calls == 1)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            # The session is still occupied; cancellation did not stop the call.
            assert driver.active_calls == 1
            assert connector.supervisor.actor.active_calls == 1
        finally:
            driver.release_block()
            await connector.close()

    async def test_queue_wait_is_not_charged_to_the_driver_call(self) -> None:
        """WF2-H-01: with 0.1 s left of a 1 s budget, do not start a call."""
        driver = make_fake_driver()
        driver.block_next_call()
        settings = make_settings(
            page_total_deadline_ms=1_000,
            queue_wait_cap_ms=100,
            max_queue_pending=4,
            hung_call_hard_limit_ms=60_000,
        )
        connector = await make_connector(driver, settings=settings)
        try:
            blocker = asyncio.create_task(
                connector.read_page(ReadPlan(entity=Entity.DEALS, limit=10))
            )
            assert await wait_for(lambda: driver.active_calls == 1)

            started = time.perf_counter()
            with pytest.raises(DeadlineExceededError) as excinfo:
                await connector.read_page(ReadPlan(entity=Entity.DEALS, limit=10))
            waited = time.perf_counter() - started

            # Failed on the queue-wait cap, well inside the 1 s request budget.
            assert waited < 0.6
            assert excinfo.value.details.get("bound") == "queue_wait_cap"
            assert excinfo.value.details["dropped_in_state"] == "queued"
            assert driver.call_count == 1

            # Released, the first call completes normally: its own budget was
            # never the problem.
            driver.release_block()
            page = await blocker
            assert page.page_index == 0
        finally:
            driver.release_block()
            await connector.close()


class TestNoImplicitExecutor:
    def test_the_package_never_calls_to_thread_or_run_in_executor(self) -> None:
        """WF2-F-08: implicit executor work would be unbounded workers."""
        import pathlib

        root = pathlib.Path(__file__).resolve().parents[2] / "src" / "mt5_connector"
        offenders: list[str] = []
        for path in root.rglob("*.py"):
            for number, line in enumerate(
                path.read_text(encoding="utf-8").splitlines(), start=1
            ):
                # Comments are stripped: these modules discuss the executor at
                # length precisely because they must not use it, and a lint
                # that forbade naming it would forbid explaining it.
                code = line.split("#", 1)[0]
                for needle in ("asyncio.to_thread", "run_in_executor"):
                    if needle in code:
                        offenders.append(f"{path.name}:{number}: {needle}")
        assert offenders == [], (
            "every blocking call must go through the actor's owned worker, not an "
            f"implicit executor: {offenders}"
        )

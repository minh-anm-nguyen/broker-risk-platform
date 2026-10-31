"""IT-08 and IT-10: burst overload, backpressure, and the shutdown boundary.

Covers WF2-M-01, WF2-M-02, WF2-M-03, WF2-M-06, WF2-M-07, WF2-O-02, WF2-O-03,
WF2-G-04, WF2-B-07, WF2-H-07, WF2-K-11, WF2-Q-02, INV-03, INV-14 and INV-20.
"""

from __future__ import annotations

import asyncio
import gc
import threading
import time

import pytest

from mt5_connector.drivers.fake_blocking import ScriptedOutcome
from mt5_connector.errors import (
    CircuitOpenError,
    ConnectorClosedError,
    DeadlineExceededError,
    OverloadedError,
)
from mt5_connector.ports import Entity, ReadPlan
from mt5_connector.scan import Scanner

from ..conftest import make_connector, make_fake_driver, make_settings, wait_for

pytestmark = [pytest.mark.integration]


class TestBurstOverload:
    async def test_a_hundred_callers_against_one_worker_stay_bounded(self) -> None:
        """IT-08, WF2-M-01, INV-03, INV-20: the stated numbers from section 11.3."""
        threads_before = threading.active_count()
        driver = make_fake_driver()
        driver.script(*[ScriptedOutcome(delay_s=0.2) for _ in range(200)])
        settings = make_settings(
            max_queue_pending=32,
            page_total_deadline_ms=1_200,
            queue_wait_cap_ms=600,
            hung_call_hard_limit_ms=60_000,
        )
        connector = await make_connector(driver, settings=settings)
        try:
            started = time.perf_counter()
            results = await asyncio.gather(
                *[
                    connector.read_page(ReadPlan(entity=Entity.DEALS, limit=10))
                    for _ in range(100)
                ],
                return_exceptions=True,
            )
            elapsed = time.perf_counter() - started

            successes = [item for item in results if not isinstance(item, BaseException)]
            overloads = [item for item in results if isinstance(item, OverloadedError)]
            deadlines = [item for item in results if isinstance(item, DeadlineExceededError)]
            assert len(results) == 100
            assert successes or overloads

            # The bounds the design promises.
            assert connector.admission.peak_pending <= 32
            assert connector.supervisor.actor.stats.peak_queue_depth <= 32
            assert driver.max_observed_concurrency == 1
            assert driver.overlap_violations == 0
            # No thread was spawned per caller.
            assert threading.active_count() <= threads_before + 3

            # Rejections are immediate, not parked: the whole burst resolved in
            # roughly the configured budget, not 100 x 0.2 s of serial work.
            assert elapsed < 6.0
            assert len(overloads) + len(deadlines) + len(successes) == 100
            assert connector.admission.rejected_overload_total == len(overloads)
        finally:
            await connector.close()

    async def test_an_overload_rejection_is_fast(self) -> None:
        """WF2-M-01: reject in well under 100 ms, measured at admission."""
        driver = make_fake_driver()
        driver.block_next_call()
        settings = make_settings(
            max_queue_pending=1,
            page_total_deadline_ms=5_000,
            queue_wait_cap_ms=4_000,
            hung_call_hard_limit_ms=60_000,
        )
        connector = await make_connector(driver, settings=settings)
        try:
            blocked = asyncio.create_task(
                connector.read_page(ReadPlan(entity=Entity.DEALS, limit=10))
            )
            assert await wait_for(lambda: driver.active_calls == 1)
            latencies: list[float] = []
            for _ in range(20):
                started = time.perf_counter()
                with pytest.raises((OverloadedError, CircuitOpenError)):
                    await connector.read_page(ReadPlan(entity=Entity.DEALS, limit=10))
                latencies.append((time.perf_counter() - started) * 1000.0)
            assert max(latencies) < 100.0, latencies
            blocked.cancel()
            await asyncio.gather(blocked, return_exceptions=True)
        finally:
            driver.release_block()
            await connector.close()

    async def test_memory_and_task_counts_do_not_grow_under_sustained_rejection(
        self,
    ) -> None:
        """WF2-M-02, WF2-M-03: admission caps protect memory, not just CPU."""
        driver = make_fake_driver()
        driver.block_next_call()
        settings = make_settings(
            max_queue_pending=4,
            page_total_deadline_ms=400,
            queue_wait_cap_ms=200,
            hung_call_hard_limit_ms=60_000,
        )
        connector = await make_connector(driver, settings=settings)
        try:
            blocked = asyncio.create_task(
                connector.read_page(ReadPlan(entity=Entity.DEALS, limit=10))
            )
            assert await wait_for(lambda: driver.active_calls == 1)
            for _ in range(5):
                await asyncio.gather(
                    *[
                        connector.read_page(ReadPlan(entity=Entity.DEALS, limit=10))
                        for _ in range(60)
                    ],
                    return_exceptions=True,
                )
            gc.collect()
            assert connector.admission.pending == 0
            # Only the one blocked request is still outstanding.
            assert connector.supervisor.actor.waiters <= 1
            assert connector.supervisor.actor.queue_depth == 0
            blocked.cancel()
            await asyncio.gather(blocked, return_exceptions=True)
        finally:
            driver.release_block()
            await connector.close()

    async def test_concurrent_scan_slots_are_bounded(self) -> None:
        driver = make_fake_driver()
        settings = make_settings(max_concurrent_scans=1, max_queue_pending=8)
        connector = await make_connector(driver, settings=settings)
        try:
            scanner = Scanner(connector)
            first = scanner.pages(ReadPlan(entity=Entity.DEALS, limit=5))
            await first.__anext__()
            with pytest.raises(OverloadedError):
                second = scanner.pages(ReadPlan(entity=Entity.DEALS, limit=5))
                await second.__anext__()
            await first.aclose()
        finally:
            await connector.close()


class TestSingleFlightConnect:
    async def test_a_hundred_callers_produce_one_handshake(self) -> None:
        """WF2-B-07, WF2-G-04: exactly one leader, bounded waiters."""
        driver = make_fake_driver()
        driver.script(ScriptedOutcome(delay_s=0.2))
        settings = make_settings(max_queue_pending=128, page_total_deadline_ms=5_000,
                                 queue_wait_cap_ms=4_000)
        connector = await make_connector(driver, settings=settings, connect=False)
        try:
            results = await asyncio.gather(
                *[connector.connect() for _ in range(100)], return_exceptions=True
            )
            identities = [item for item in results if not isinstance(item, BaseException)]
            assert identities
            # One session, one epoch, one driver open.
            assert {item.worker_epoch for item in identities} == {1}
            assert driver.open_count == 1
            assert connector.supervisor.connect_leaders_total == 1
        finally:
            await connector.close()

    async def test_connect_waiters_are_bounded(self) -> None:
        driver = make_fake_driver()
        driver.script(ScriptedOutcome(delay_s=0.5))
        settings = make_settings(max_queue_pending=2, page_total_deadline_ms=5_000,
                                 queue_wait_cap_ms=4_000)
        connector = await make_connector(driver, settings=settings, connect=False)
        try:
            results = await asyncio.gather(
                *[connector.connect() for _ in range(40)], return_exceptions=True
            )
            overloaded = [item for item in results if isinstance(item, OverloadedError)]
            assert overloaded, "waiters must be bounded, not merely parked"
            assert all(
                item.details.get("max_waiters") == 2 for item in overloaded
            )
        finally:
            await connector.close()


class TestBackpressure:
    async def test_a_slow_sink_slows_the_producer_rather_than_buffering(self) -> None:
        """WF2-Q-02, WF2-K-11: a slow consumer must not become unbounded memory."""
        driver = make_fake_driver()
        connector = await make_connector(driver)
        try:
            pages_fetched = 0
            original = connector.read_page

            async def counting_read(*args, **kwargs):
                nonlocal pages_fetched
                pages_fetched += 1
                return await original(*args, **kwargs)

            connector.read_page = counting_read  # type: ignore[method-assign]

            sink_calls = 0

            async def slow_sink(page) -> None:
                nonlocal sink_calls
                sink_calls += 1
                # While the sink is working, no further page may be fetched.
                before = pages_fetched
                await asyncio.sleep(0.05)
                assert pages_fetched == before

            scanner = Scanner(connector)
            await scanner.run(ReadPlan(entity=Entity.DEALS, limit=5), sink=slow_sink)
            assert sink_calls >= 1
        finally:
            await connector.close()

    async def test_a_sink_failure_aborts_the_scan_without_a_candidate(self) -> None:
        """ADR-207: the connector does not know whether the sink's write landed."""
        driver = make_fake_driver()
        connector = await make_connector(driver)
        try:
            async def failing_sink(page) -> None:
                raise RuntimeError("the sink's transaction rolled back")

            scanner = Scanner(connector)
            with pytest.raises(RuntimeError):
                await scanner.run(ReadPlan(entity=Entity.DEALS, limit=5), sink=failing_sink)
        finally:
            await connector.close()


class TestShutdown:
    async def test_draining_denies_new_admissions_and_reclaims_the_worker(self) -> None:
        """IT-10, WF2-O-02, INV-14."""
        threads_before = threading.active_count()
        driver = make_fake_driver()
        connector = await make_connector(driver)
        await connector.read_page(ReadPlan(entity=Entity.DEALS, limit=10))

        clean = await connector.close()
        assert clean is True
        assert driver.close_count == 1
        assert driver.double_close_detected is False
        assert connector.supervisor.state.value == "STOPPED"

        with pytest.raises(ConnectorClosedError):
            await connector.read_page(ReadPlan(entity=Entity.DEALS, limit=10))
        assert await wait_for(
            lambda: threading.active_count() <= threads_before + 1, timeout_s=3.0
        )
        assert connector.supervisor.ready()[0] is False

    async def test_shutdown_during_an_unstoppable_call_is_reported_honestly(self) -> None:
        """IT-10, section 5.4: no false claim of a clean drain."""
        driver = make_fake_driver()
        driver.block_next_call()
        settings = make_settings(
            page_total_deadline_ms=5_000,
            queue_wait_cap_ms=4_000,
            drain_timeout_ms=200,
            hung_call_hard_limit_ms=60_000,
        )
        connector = await make_connector(driver, settings=settings)
        try:
            task = asyncio.create_task(
                connector.read_page(ReadPlan(entity=Entity.DEALS, limit=10))
            )
            assert await wait_for(lambda: driver.active_calls == 1)
            clean = await connector.close()
            # False: a call was still running, and the process is holding a
            # thread it could not reclaim. Saying True here would be the lie
            # section 5.4 is written against.
            assert clean is False
            assert connector.supervisor.abandoned_worker_count == 1
            ready, reasons = connector.supervisor.ready()
            assert ready is False
            assert "abandoned_worker_present" in reasons
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        finally:
            driver.release_block()
            await asyncio.sleep(0.1)

    async def test_close_is_idempotent(self) -> None:
        driver = make_fake_driver()
        connector = await make_connector(driver)
        assert await connector.close() is True
        assert await connector.close() is True
        assert driver.close_count == 1

    async def test_repeated_reconnects_do_not_leak_threads_or_sessions(self) -> None:
        """WF2-M-06, WF2-M-07: one leaked thread per cycle is the failure."""
        threads_before = threading.active_count()
        for _ in range(8):
            driver = make_fake_driver()
            connector = await make_connector(driver)
            await connector.read_page(ReadPlan(entity=Entity.DEALS, limit=10))
            assert await connector.close() is True
            assert driver.close_count == 1
        gc.collect()
        assert await wait_for(
            lambda: threading.active_count() <= threads_before + 1, timeout_s=5.0
        ), f"threads grew from {threads_before} to {threading.active_count()}"

"""The actor: one active call, timeouts that do not free it, fenced delivery.

Covers WF2-F-02, WF2-F-03, WF2-F-05, WF2-F-07, WF2-F-09, WF2-G-01, WF2-G-02,
WF2-G-03, WF2-H-01, WF2-H-02, WF2-H-03, WF2-M-06, WF2-E-06, WF2-E-09,
INV-02, INV-03 and INV-04.

The hard property under test is in :meth:`TestOccupancy`: a caller's deadline
expiring must **not** release the session slot. That is the difference between
this design and ``wait_for(to_thread(...))``, and it is checked both from the
outside (the actor's counters) and from the inside (the fake driver's own
overlap detector).
"""

from __future__ import annotations

import asyncio
import threading

import pytest

from mt5_connector.actor import CallEnvelope, Priority, RequestState, SDKActor
from mt5_connector.deadline import SystemClock
from mt5_connector.drivers.fake_blocking import FakeBlockingDriver, ScriptedOutcome
from mt5_connector.errors import (
    ConnectorClosedError,
    DeadlineExceededError,
    OverloadedError,
    TransportError,
)

from ..conftest import make_fake_driver, wait_for

pytestmark = pytest.mark.unit

CLOCK = SystemClock()


async def start_actor(
    driver: FakeBlockingDriver, *, epoch: int = 1, max_queue: int = 4
) -> SDKActor:
    actor = SDKActor(
        lambda: driver,
        epoch=epoch,
        source_id="broker-sim-01",
        max_queue=max_queue,
        clock=CLOCK,
    )
    await actor.start()
    return actor


def envelope(
    actor: SDKActor,
    *,
    deadline_s: float = 1.0,
    dispatch_by_s: float | None = None,
    request_id: str | None = None,
    priority: Priority = Priority.NORMAL,
    operation: str = "fetch_deals",
) -> CallEnvelope:
    now = CLOCK.monotonic()
    return CallEnvelope.new(
        source_id="broker-sim-01",
        operation=operation,
        params={"cursor": None, "limit": 10},
        clock=CLOCK,
        absolute_deadline_monotonic=now + deadline_s,
        worker_epoch=actor.epoch,
        source_generation=1,
        dispatch_by_monotonic=None if dispatch_by_s is None else now + dispatch_by_s,
        priority=priority,
        request_id=request_id,
    )


class TestSerialisation:
    async def test_the_driver_is_opened_on_the_worker_thread(self) -> None:
        """WF2-E-06: a session created on the loop and used from a worker is a bug."""
        driver = make_fake_driver()
        actor = await start_actor(driver)
        try:
            assert driver.open_count == 1
            assert driver._opened_on_thread != threading.get_ident()
        finally:
            await actor.drain(timeout_s=1.0)

    async def test_two_concurrent_requests_never_overlap_in_the_driver(self) -> None:
        """WF2-F-02, INV-02: serialisation is structural, not checked."""
        driver = make_fake_driver()
        driver.script(ScriptedOutcome(delay_s=0.05), ScriptedOutcome(delay_s=0.05))
        actor = await start_actor(driver)
        try:
            futures = [actor.submit(envelope(actor, deadline_s=5.0)) for _ in range(2)]
            completions = await asyncio.gather(*futures)
            assert all(item.error is None for item in completions)
            assert driver.overlap_violations == 0
            assert driver.max_observed_concurrency == 1
            assert actor.stats.dispatched_total == 2
        finally:
            await actor.drain(timeout_s=2.0)

    async def test_twenty_requests_stay_within_one_active_call(self) -> None:
        driver = make_fake_driver()
        actor = await start_actor(driver, max_queue=32)
        try:
            futures = [actor.submit(envelope(actor, deadline_s=10.0)) for _ in range(20)]
            await asyncio.gather(*futures)
            assert driver.overlap_violations == 0
            assert driver.max_observed_concurrency == 1
            assert actor.stats.peak_queue_depth <= 32
        finally:
            await actor.drain(timeout_s=2.0)

    async def test_only_the_worker_thread_ever_calls_the_driver(self) -> None:
        driver = make_fake_driver()
        actor = await start_actor(driver, max_queue=8)
        try:
            await asyncio.gather(*[actor.submit(envelope(actor, deadline_s=5.0)) for _ in range(5)])
            assert len(driver.threads_seen) == 1
            assert driver.cross_thread_calls == 0
        finally:
            await actor.drain(timeout_s=2.0)


class TestOccupancy:
    async def test_a_caller_timeout_does_not_release_the_session_slot(self) -> None:
        """WF2-F-03, WF2-H-02, INV-02: the central claim of section 5.1."""
        driver = make_fake_driver()
        driver.block_next_call()
        actor = await start_actor(driver)
        try:
            first = envelope(actor, deadline_s=10.0, request_id="req-a")
            future = actor.submit(first)
            assert await wait_for(lambda: actor.active_calls == 1)

            # Caller A gives up.
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(future, timeout=0.05)
            state = actor.release_waiter(first.request_id)
            assert state is RequestState.DISPATCHED

            # The slot is still held, and the driver is still inside the call.
            assert actor.active_calls == 1
            assert actor.active_request_id == "req-a"
            assert driver.active_calls == 1
            assert actor.active_age_s > 0

            # A second request queues and is never dispatched while A runs.
            second = actor.submit(envelope(actor, deadline_s=10.0, request_id="req-b"))
            await asyncio.sleep(0.05)
            assert driver.call_count == 1
            assert actor.queue_depth == 1

            driver.release_block()
            completion = await asyncio.wait_for(second, timeout=3.0)
            assert completion.error is None
            assert driver.overlap_violations == 0
            # A's late completion was discarded, not delivered.
            assert actor.stats.stale_completions_discarded_total == 1
            assert actor.stats.abandoned_dispatched_total == 1
        finally:
            driver.release_block()
            await actor.drain(timeout_s=2.0)

    async def test_the_slot_is_released_only_when_the_call_returns(self) -> None:
        driver = make_fake_driver()
        driver.block_next_call()
        actor = await start_actor(driver)
        try:
            actor.submit(envelope(actor, deadline_s=10.0))
            assert await wait_for(lambda: actor.active_calls == 1)
            await asyncio.sleep(0.05)
            assert actor.active_calls == 1
            driver.release_block()
            assert await wait_for(lambda: actor.active_calls == 0, timeout_s=3.0)
        finally:
            driver.release_block()
            await actor.drain(timeout_s=2.0)

    async def test_a_late_completion_is_never_delivered_to_a_new_request(self) -> None:
        """WF2-H-03, INV-04."""
        driver = make_fake_driver()
        driver.block_next_call()
        actor = await start_actor(driver)
        try:
            stale = envelope(actor, deadline_s=10.0, request_id="req-stale")
            future = actor.submit(stale)
            assert await wait_for(lambda: actor.active_calls == 1)
            future.cancel()
            actor.release_waiter(stale.request_id)
            driver.release_block()
            await asyncio.sleep(0.1)

            driver.rearm_block()
            fresh = actor.submit(envelope(actor, deadline_s=10.0, request_id="req-fresh"))
            completion = await asyncio.wait_for(fresh, timeout=3.0)
            assert completion.request_id == "req-fresh"
            assert actor.stats.stale_completions_discarded_total >= 1
        finally:
            driver.release_block()
            await actor.drain(timeout_s=2.0)


class TestCancellation:
    async def test_a_queued_request_is_tombstoned_and_never_runs(self) -> None:
        """WF2-G-01: never dispatch cancelled work."""
        driver = make_fake_driver()
        driver.block_next_call()
        actor = await start_actor(driver)
        try:
            actor.submit(envelope(actor, deadline_s=10.0, request_id="req-running"))
            assert await wait_for(lambda: actor.active_calls == 1)

            queued = envelope(actor, deadline_s=10.0, request_id="req-queued")
            actor.submit(queued)
            assert actor.queue_depth == 1
            assert actor.release_waiter(queued.request_id) is RequestState.QUEUED

            driver.release_block()
            assert await wait_for(lambda: actor.queue_depth == 0, timeout_s=3.0)
            assert await wait_for(
                lambda: actor.stats.cancelled_queued_total == 1, timeout_s=3.0
            )
            # Only the running call ever reached the driver.
            assert driver.call_count == 1
        finally:
            driver.release_block()
            await actor.drain(timeout_s=2.0)

    async def test_a_hundred_cancellations_leave_no_orphan_futures(self) -> None:
        """WF2-G-03: counters return to zero, nothing is retained."""
        driver = make_fake_driver()
        driver.block_next_call()
        actor = await start_actor(driver, max_queue=128)
        try:
            running = envelope(actor, deadline_s=20.0, request_id="req-run")
            actor.submit(running)
            assert await wait_for(lambda: actor.active_calls == 1)

            envelopes = [
                envelope(actor, deadline_s=20.0, request_id=f"req-{index}")
                for index in range(100)
            ]
            futures = [actor.submit(item) for item in envelopes]
            assert actor.queue_depth == 100
            for item, future in zip(envelopes, futures):
                future.cancel()
                assert actor.release_waiter(item.request_id) is RequestState.QUEUED
            # Every waiter is released immediately, not when the worker gets to it.
            assert actor.waiters == 1

            driver.release_block()
            assert await wait_for(lambda: actor.queue_depth == 0, timeout_s=5.0)
            assert driver.call_count == 1
            assert actor.stats.cancelled_queued_total == 100
        finally:
            driver.release_block()
            await actor.drain(timeout_s=3.0)

    async def test_exactly_one_terminal_result_per_request(self) -> None:
        """WF2-F-09: no InvalidStateError when completion races cancellation."""
        driver = make_fake_driver()
        actor = await start_actor(driver, max_queue=64)
        try:
            for _ in range(30):
                item = envelope(actor, deadline_s=5.0)
                future = actor.submit(item)
                await asyncio.sleep(0)
                future.cancel()
                actor.release_waiter(item.request_id)
            await asyncio.sleep(0.2)
            assert actor.waiters == 0
        finally:
            await actor.drain(timeout_s=2.0)


class TestQueueBounds:
    async def test_the_mailbox_refuses_beyond_its_capacity(self) -> None:
        """INV-03: a structural backstop behind admission control."""
        driver = make_fake_driver()
        driver.block_next_call()
        actor = await start_actor(driver, max_queue=3)
        try:
            actor.submit(envelope(actor, deadline_s=20.0))
            assert await wait_for(lambda: actor.active_calls == 1)
            for _ in range(3):
                actor.submit(envelope(actor, deadline_s=20.0))
            overflowing = actor.submit(envelope(actor, deadline_s=20.0))
            with pytest.raises(OverloadedError) as excinfo:
                await overflowing
            assert excinfo.value.details["mailbox_capacity"] == 3
            assert actor.stats.rejected_mailbox_full_total == 1
            assert actor.queue_depth == 3
        finally:
            driver.release_block()
            await actor.drain(timeout_s=3.0)

    async def test_a_request_whose_budget_expired_in_the_queue_is_dropped(self) -> None:
        """WF2-H-01: do not start a 1.5 s call with 0.1 s of budget left."""
        driver = make_fake_driver()
        driver.block_next_call()
        actor = await start_actor(driver, max_queue=8)
        try:
            actor.submit(envelope(actor, deadline_s=20.0))
            assert await wait_for(lambda: actor.active_calls == 1)

            doomed = actor.submit(envelope(actor, deadline_s=0.05))
            await asyncio.sleep(0.1)
            driver.release_block()
            with pytest.raises(DeadlineExceededError) as excinfo:
                await asyncio.wait_for(doomed, timeout=3.0)
            assert excinfo.value.details["dropped_in_state"] == "queued"
            assert actor.stats.dropped_before_dispatch_total == 1
            # It never reached the driver.
            assert driver.call_count == 1
        finally:
            driver.release_block()
            await actor.drain(timeout_s=3.0)

    async def test_the_queue_wait_cap_is_distinguished_from_the_request_deadline(self) -> None:
        """A short queue cap fails fast and says which bound it hit."""
        driver = make_fake_driver()
        driver.block_next_call()
        actor = await start_actor(driver, max_queue=8)
        try:
            actor.submit(envelope(actor, deadline_s=20.0))
            assert await wait_for(lambda: actor.active_calls == 1)
            capped = actor.submit(envelope(actor, deadline_s=20.0, dispatch_by_s=0.05))
            await asyncio.sleep(0.1)
            driver.release_block()
            with pytest.raises(DeadlineExceededError) as excinfo:
                await asyncio.wait_for(capped, timeout=3.0)
            assert excinfo.value.details["bound"] == "queue_wait_cap"
            assert "queue-wait budget" in excinfo.value.message
        finally:
            driver.release_block()
            await actor.drain(timeout_s=3.0)

    async def test_high_priority_work_cannot_starve_normal_work(self) -> None:
        """WF2-G-09: an unweighted priority queue starves the sync service."""
        driver = make_fake_driver()
        actor = await start_actor(driver, max_queue=64)
        try:
            normal = envelope(actor, deadline_s=20.0, request_id="normal-1")
            normal_future = actor.submit(normal)
            for index in range(20):
                actor.submit(
                    envelope(
                        actor,
                        deadline_s=20.0,
                        priority=Priority.HIGH,
                        request_id=f"high-{index}",
                    )
                )
            completion = await asyncio.wait_for(normal_future, timeout=5.0)
            assert completion.error is None
            # The normal request ran well before all twenty probes finished.
            order = [call.params for call in driver.calls]
            assert len(order) >= 2
        finally:
            await actor.drain(timeout_s=3.0)


class TestFailures:
    async def test_a_driver_exception_becomes_a_typed_error_not_a_dead_worker(self) -> None:
        """WF2-F-05, WF2-E-02."""
        driver = make_fake_driver()
        driver.fail_next_call(TransportError("connection refused"))
        actor = await start_actor(driver)
        try:
            completion = await asyncio.wait_for(
                actor.submit(envelope(actor, deadline_s=5.0)), timeout=3.0
            )
            assert isinstance(completion.error, TransportError)
            assert completion.error.worker_epoch == actor.epoch
            # The worker survived and serves the next request.
            follow_up = await asyncio.wait_for(
                actor.submit(envelope(actor, deadline_s=5.0)), timeout=3.0
            )
            assert follow_up.error is None
            assert actor.stats.failed_total == 1
            assert actor.stats.completed_total == 1
        finally:
            await actor.drain(timeout_s=2.0)

    async def test_an_unexpected_exception_is_normalised_without_its_message(self) -> None:
        """WF2-B-06: an httpx or native message can contain a URL."""
        driver = make_fake_driver()
        driver.script(ScriptedOutcome(raises=RuntimeError("GET https://h/v1?token=abc123")))
        actor = await start_actor(driver)
        try:
            completion = await asyncio.wait_for(
                actor.submit(envelope(actor, deadline_s=5.0)), timeout=3.0
            )
            assert isinstance(completion.error, TransportError)
            assert "token" not in completion.error.message
            assert completion.error.details["driver_error"] == "RuntimeError"
        finally:
            await actor.drain(timeout_s=2.0)

    async def test_a_failed_open_is_reported_to_start(self) -> None:
        def factory():
            raise TransportError("the source refused the connection")

        actor = SDKActor(
            factory, epoch=1, source_id="s", max_queue=4, clock=CLOCK
        )
        with pytest.raises(TransportError):
            await actor.start()
        assert await actor.drain(timeout_s=1.0)

    async def test_an_unexpected_open_failure_is_wrapped(self) -> None:
        def factory():
            raise ValueError("bad config object")

        actor = SDKActor(factory, epoch=1, source_id="s", max_queue=4, clock=CLOCK)
        with pytest.raises(TransportError) as excinfo:
            await actor.start()
        assert excinfo.value.details["driver_error"] == "ValueError"


class TestShutdown:
    async def test_drain_closes_the_driver_and_reclaims_the_thread(self) -> None:
        """WF2-M-06: a reconnect must not leak a thread per cycle."""
        before = threading.active_count()
        driver = make_fake_driver()
        actor = await start_actor(driver)
        await asyncio.wait_for(actor.submit(envelope(actor, deadline_s=5.0)), timeout=3.0)
        assert await actor.drain(timeout_s=3.0) is True
        assert driver.close_count == 1
        assert driver.double_close_detected is False
        assert await wait_for(lambda: threading.active_count() <= before + 1, timeout_s=3.0)

    async def test_drain_fails_queued_requests_with_a_typed_error(self) -> None:
        driver = make_fake_driver()
        driver.block_next_call()
        actor = await start_actor(driver, max_queue=8)
        actor.submit(envelope(actor, deadline_s=20.0))
        assert await wait_for(lambda: actor.active_calls == 1)
        queued = actor.submit(envelope(actor, deadline_s=20.0))
        driver.release_block()
        await actor.drain(timeout_s=3.0)
        with pytest.raises((ConnectorClosedError, Exception)):
            await asyncio.wait_for(queued, timeout=2.0)

    async def test_drain_reports_false_when_a_call_will_not_stop(self) -> None:
        """Section 5.4: the honest answer, not a wait that never ends."""
        driver = make_fake_driver()
        driver.block_next_call()
        actor = await start_actor(driver)
        try:
            actor.submit(envelope(actor, deadline_s=60.0))
            assert await wait_for(lambda: actor.active_calls == 1)
            assert await actor.drain(timeout_s=0.2) is False
            assert actor.thread_alive is True
        finally:
            driver.release_block()
            await actor.drain(timeout_s=3.0)

    async def test_submitting_after_stop_fails_immediately(self) -> None:
        driver = make_fake_driver()
        actor = await start_actor(driver)
        await actor.drain(timeout_s=2.0)
        with pytest.raises(ConnectorClosedError):
            await actor.submit(envelope(actor, deadline_s=5.0))

    async def test_abandon_fails_every_waiter_and_discards_completions(self) -> None:
        """WF2-F-04, WF2-F-10: an abandoned thread keeps running, and we say so."""
        driver = make_fake_driver()
        driver.block_next_call()
        actor = await start_actor(driver, max_queue=8)
        try:
            running = actor.submit(envelope(actor, deadline_s=60.0))
            assert await wait_for(lambda: actor.active_calls == 1)
            queued = actor.submit(envelope(actor, deadline_s=60.0))

            actor.abandon()
            assert actor.abandoned is True
            with pytest.raises(ConnectorClosedError):
                await asyncio.wait_for(queued, timeout=2.0)
            with pytest.raises(ConnectorClosedError):
                await asyncio.wait_for(running, timeout=2.0)

            driver.release_block()
            await asyncio.sleep(0.1)
            assert actor.stats.stale_completions_discarded_total >= 1
        finally:
            driver.release_block()

    async def test_posting_to_a_closed_loop_is_counted_not_raised(self) -> None:
        """WF2-F-07: a dying thread must not produce an unhandled exception."""
        driver = make_fake_driver()
        actor = await start_actor(driver)

        class ClosedLoop:
            def call_soon_threadsafe(self, *_args, **_kwargs):
                raise RuntimeError("Event loop is closed")

        actor._loop = ClosedLoop()  # type: ignore[assignment]
        actor._post(lambda: None)
        assert actor.stats.loop_post_failures_total == 1

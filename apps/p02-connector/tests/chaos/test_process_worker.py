"""IT-07: a hung call, a killed worker process, and a fenced old result.

Covers WF2-E-04, WF2-F-04, WF2-F-06, WF2-F-10, WF2-F-11, WF2-O-06, WF2-O-07,
WF2-R-04, WF2-R-09, INV-05 and ADR-204.

This is the suite that distinguishes the two worker modes, and the
distinction is the point:

* **Thread mode** detects a hung call, quarantines the session and says so.
  It cannot recover, because a Python thread cannot be stopped, and
  :meth:`TestThreadModeCannotRecover` asserts that it does not pretend
  otherwise.
* **Process mode** kills the child, reaps it, mints a new epoch and serves
  again. The mechanism is specific: the worker thread is blocked reading a
  pipe, and killing the far end makes that read return.

A test that claimed automatic recovery from thread mode would be the exact
dishonesty section 5.4 and release gate G1 are written against.
"""

from __future__ import annotations

import asyncio
import sys
import time

import pytest

from mt5_connector.connection_fsm import SessionState
from mt5_connector.errors import (
    CircuitOpenError,
    ConnectorClosedError,
    DeadlineExceededError,
)
from mt5_connector.ports import Entity, ReadPlan
from mt5_connector.windows_worker.protocol import (
    PROTOCOL_VERSION,
    Frame,
    FrameError,
    read_frame,
    write_frame,
)
from mt5_connector.windows_worker.watchdog import ProcessDriverProxy, ProcessWorkerSpec

from ..conftest import make_connector, make_fake_driver, make_settings, wait_for

pytestmark = [pytest.mark.chaos, pytest.mark.slow]


def process_spec(*, script: list[dict] | None = None, **kwargs) -> ProcessWorkerSpec:
    return ProcessWorkerSpec(
        driver="fake_blocking",
        script=script or [],
        python_executable=sys.executable,
        **kwargs,
    )


class TestFraming:
    def test_a_frame_round_trips(self) -> None:
        import io

        buffer = io.BytesIO()
        frame = Frame(kind="call", worker_epoch=3, request_id="r1", payload={"a": 1})
        write_frame(buffer, frame)
        buffer.seek(0)
        decoded = read_frame(buffer)
        assert decoded == frame

    def test_a_truncated_frame_is_an_error_not_a_short_read(self) -> None:
        """WF2-F-11: a half-written message must never parse."""
        import io

        buffer = io.BytesIO()
        write_frame(buffer, Frame(kind="call", worker_epoch=1, request_id="r", payload={"a": 1}))
        raw = buffer.getvalue()
        for cut in (4, 8, len(raw) - 1):
            with pytest.raises(FrameError):
                read_frame(io.BytesIO(raw[:cut]))

    def test_a_protocol_mismatch_fails_closed(self) -> None:
        """WF2-O-07: no partial decode of a payload whose shape is disputed."""
        import io
        import json
        import struct

        body = json.dumps(
            {
                "protocol": "worker-ipc-999",
                "kind": "call",
                "worker_epoch": 1,
                "request_id": "r",
                "payload": {},
            }
        ).encode()
        stream = io.BytesIO(struct.pack(">Q", len(body)) + body)
        with pytest.raises(FrameError) as excinfo:
            read_frame(stream)
        assert PROTOCOL_VERSION in str(excinfo.value)

    def test_an_absurd_length_prefix_is_refused_before_allocating(self) -> None:
        import io
        import struct

        stream = io.BytesIO(struct.pack(">Q", 1 << 40))
        with pytest.raises(FrameError) as excinfo:
            read_frame(stream)
        assert "cap" in str(excinfo.value)


class TestProcessWorker:
    def test_the_worker_starts_serves_and_exits_cleanly(self) -> None:
        proxy = ProcessDriverProxy(process_spec(), epoch=1)
        try:
            identity = proxy.open()
            assert proxy.pid is not None
            assert identity.contract_version == "v1"
            response = proxy.fetch_deals(cursor=None, limit=5)
            assert response.status == 200
        finally:
            proxy.close()
        assert proxy.alive is False

    def test_close_is_idempotent(self) -> None:
        proxy = ProcessDriverProxy(process_spec(), epoch=1)
        proxy.open()
        proxy.close()
        proxy.close()
        assert proxy.alive is False

    def test_a_hung_call_is_released_by_killing_the_worker(self) -> None:
        """IT-07, WF2-E-04, ADR-204: the kill is what unblocks the read."""
        import threading

        proxy = ProcessDriverProxy(
            process_spec(script=[{"block": True}]), epoch=7
        )
        try:
            proxy.open()
            outcome: list[object] = []

            def call() -> None:
                try:
                    outcome.append(proxy.fetch_deals(cursor=None, limit=5))
                except BaseException as exc:  # noqa: BLE001 - recorded for the assert
                    outcome.append(exc)

            caller = threading.Thread(target=call, name="blocked-reader", daemon=True)
            caller.start()
            # The call is now parked inside the child, and the parent thread is
            # parked on the pipe.
            time.sleep(0.5)
            assert not outcome
            assert proxy.alive

            proxy.emergency_stop()
            caller.join(5.0)

            assert not caller.is_alive(), "killing the worker must release the reader"
            assert len(outcome) == 1
            assert isinstance(outcome[0], Exception)
            assert outcome[0].code == "TRANSPORT_ERROR"
            assert outcome[0].details["reason"] == "emergency_stop"
            assert proxy.kills_requested == 1
            assert proxy.alive is False
        finally:
            proxy.close()

    def test_a_new_epoch_serves_after_the_old_worker_is_reaped(self) -> None:
        """IT-07, WF2-F-06, WF2-O-06: a new PID, a new epoch, no mixed result."""
        first = ProcessDriverProxy(process_spec(script=[{"block": True}]), epoch=7)
        first.open()
        first_pid = first.pid
        first.emergency_stop()
        assert first.alive is False

        second = ProcessDriverProxy(process_spec(), epoch=8)
        try:
            second.open()
            assert second.pid != first_pid
            response = second.fetch_deals(cursor=None, limit=5)
            assert response.status == 200
        finally:
            second.close()

    def test_a_frame_naming_the_wrong_epoch_is_rejected_by_the_worker(self) -> None:
        """WF2-R-09: an IPC buffer can hold a message across a restart."""
        proxy = ProcessDriverProxy(process_spec(), epoch=5)
        try:
            proxy.open()
            process = proxy._process  # type: ignore[attr-defined]
            assert process is not None and process.stdin is not None
            write_frame(
                process.stdin,
                Frame(
                    kind="call",
                    worker_epoch=99,
                    request_id="stale-1",
                    payload={"operation": "fetch_deals", "params": {"limit": 5}},
                ),
            )
            # The worker answers with a rejection naming its own epoch, and the
            # proxy discards it because the request id does not match either.
            response = proxy.fetch_deals(cursor=None, limit=5)
            assert response.status == 200
            assert proxy.stale_frames_discarded >= 1
        finally:
            proxy.close()

    def test_a_dead_worker_is_reported_not_waited_on(self) -> None:
        proxy = ProcessDriverProxy(process_spec(), epoch=1)
        proxy.open()
        proxy.emergency_stop()
        with pytest.raises(Exception) as excinfo:
            proxy.fetch_deals(cursor=None, limit=5)
        assert excinfo.value.code == "TRANSPORT_ERROR"


class TestThreadModeCannotRecover:
    async def test_a_hung_call_quarantines_the_session_honestly(self) -> None:
        """WF2-F-10, WF2-R-04: readiness false, no claim of recovery."""
        driver = make_fake_driver()
        driver.block_next_call()
        settings = make_settings(
            worker_mode="thread",
            allow_thread_abandon=False,
            hung_call_hard_limit_ms=150,
            page_total_deadline_ms=400,
            queue_wait_cap_ms=200,
            heartbeat_interval_ms=20,
        )
        connector = await make_connector(driver, settings=settings)
        try:
            with pytest.raises(DeadlineExceededError):
                await connector.read_page(ReadPlan(entity=Entity.DEALS, limit=10))

            assert await wait_for(
                lambda: connector.supervisor.state is SessionState.QUARANTINED,
                timeout_s=3.0,
            ), connector.supervisor.state

            health = connector.supervisor.health()
            assert health["live"] is True
            assert health["ready"] is False
            assert "session_state_quarantined" in health["reasons"]

            # Every further caller is refused, and told why, rather than being
            # queued behind a call that will never end.
            with pytest.raises(CircuitOpenError) as excinfo:
                await connector.read_page(ReadPlan(entity=Entity.DEALS, limit=10))
            assert excinfo.value.details["reason"] == "session_quarantined"
            assert excinfo.value.details["worker_mode"] == "thread"
            # The honest field: thread mode cannot force its way out of this.
            assert excinfo.value.details["can_force_recover"] is False
            assert driver.call_count == 1
        finally:
            driver.release_block()
            await connector.close()

    async def test_abandoning_a_thread_is_reported_as_a_leak_not_a_recovery(
        self,
    ) -> None:
        """Section 5.4: with abandon enabled, the thread still runs - and counts."""
        driver = make_fake_driver()
        driver.block_next_call()
        settings = make_settings(
            worker_mode="thread",
            allow_thread_abandon=True,
            hung_call_hard_limit_ms=150,
            page_total_deadline_ms=400,
            queue_wait_cap_ms=200,
            drain_timeout_ms=200,
            heartbeat_interval_ms=20,
        )
        connector = await make_connector(driver, settings=settings)
        try:
            # Deadline or retired session - both typed, neither a success.
            with pytest.raises((DeadlineExceededError, ConnectorClosedError)):
                await connector.read_page(ReadPlan(entity=Entity.DEALS, limit=10))
            assert await wait_for(
                lambda: connector.supervisor.abandoned_worker_count == 1, timeout_s=5.0
            )
            ready, reasons = connector.supervisor.ready()
            assert ready is False
            assert "abandoned_worker_present" in reasons
            # The old session is gone from the supervisor's view, and the
            # thread is still alive - which is exactly what is reported.
            assert connector.supervisor.worker_epoch >= 1
        finally:
            driver.release_block()
            await asyncio.sleep(0.2)
            await connector.close()

    async def test_a_stale_completion_after_a_new_epoch_is_fenced(self) -> None:
        """WF2-F-04, INV-05: the old session's result is never delivered."""
        driver = make_fake_driver()
        driver.block_next_call()
        settings = make_settings(
            worker_mode="thread",
            allow_thread_abandon=True,
            hung_call_hard_limit_ms=150,
            page_total_deadline_ms=400,
            queue_wait_cap_ms=200,
            drain_timeout_ms=150,
            heartbeat_interval_ms=20,
        )
        connector = await make_connector(driver, settings=settings)
        try:
            # Either outcome is correct and both are typed: the caller's
            # deadline expires, or the session it was using is retired
            # underneath it. What must never happen is a success.
            with pytest.raises((DeadlineExceededError, ConnectorClosedError)):
                await connector.read_page(ReadPlan(entity=Entity.DEALS, limit=10))
            assert await wait_for(
                lambda: connector.supervisor.abandoned_worker_count == 1, timeout_s=5.0
            )
            abandoned = connector.supervisor._abandoned_actors[0]  # type: ignore[attr-defined]
            epoch_before = abandoned.epoch

            # The hung call finally returns, long after its session was retired.
            driver.release_block()
            assert await wait_for(
                lambda: abandoned.stats.stale_completions_discarded_total >= 1,
                timeout_s=5.0,
            )
            assert abandoned.abandoned is True
            assert epoch_before == abandoned.epoch
        finally:
            driver.release_block()
            await asyncio.sleep(0.2)
            await connector.close()

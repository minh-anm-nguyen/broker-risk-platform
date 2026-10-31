"""A programmable blocking driver: the test double for the worker model.

This is the only driver that can be made to *hang*, and that is why it is
part of the shipped package rather than the test tree - the chaos scripts and
the what-if runner drive it too.

It also acts as an oracle on the actor's central invariant. Every call
increments an overlap counter on entry and decrements it on exit, and a second
concurrent entry records a violation rather than quietly succeeding. So
"``active_native_calls <= 1``" is checked from *inside* the session as well as
observed from outside it: a bug in the actor that let two calls through would
be caught here even if the metric sampling happened to miss it (INV-02,
WF2-F-02, WF2-E-06).
"""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable

from ..errors import ConnectorError, TransportError
from .base import DriverIdentity, DriverResponse

__all__ = ["FakeBlockingDriver", "FakeCall", "ScriptedOutcome"]


@dataclass(slots=True)
class FakeCall:
    """A record of one call, for assertions after the fact."""

    operation: str
    thread_ident: int
    started_monotonic: float
    finished_monotonic: float | None = None
    outcome: str = "pending"
    params: dict[str, Any] = field(default_factory=dict)

    @property
    def duration_s(self) -> float | None:
        if self.finished_monotonic is None:
            return None
        return self.finished_monotonic - self.started_monotonic


@dataclass(slots=True)
class ScriptedOutcome:
    """One queued behaviour for the next call.

    Exactly one of the fields is acted on, checked in the order below:
    ``raises``, then ``block``, then ``response``.
    """

    #: Seconds to block the worker thread before doing anything else.
    delay_s: float = 0.0
    #: Raise this error instead of returning.
    raises: ConnectorError | BaseException | None = None
    #: Block until :meth:`FakeBlockingDriver.release_block` is called. With
    #: ``None`` the block has no timeout at all, which is the hard-hang case
    #: a thread-mode worker cannot recover from (WF2-F-03, IT-07).
    block: bool = False
    block_timeout_s: float | None = None
    #: The response to return.
    response: DriverResponse | None = None


class FakeBlockingDriver:
    """A driver whose every call is scripted.

    Scripted outcomes are consumed in order. When the script runs out, the
    ``default_response`` is returned, so a test only has to describe the part
    it cares about.
    """

    source_kind = "fake"

    def __init__(
        self,
        *,
        identity: DriverIdentity | None = None,
        default_response: DriverResponse | None = None,
        response_factory: Callable[[str, dict[str, Any]], DriverResponse] | None = None,
    ) -> None:
        self._identity = identity or DriverIdentity(
            source_kind=self.source_kind,
            contract_version="v1",
            run_id="run_fake",
            generation=1,
            raw_meta={},
        )
        self._default = default_response or DriverResponse(
            status=200,
            payload={
                "run_id": "run_fake",
                "generation": 1,
                "snapshot_seq": 1,
                "next_cursor": None,
                "items": [],
            },
            byte_count=64,
            content_type="application/json",
        )
        self._response_factory = response_factory
        self._script: deque[ScriptedOutcome] = deque()
        self._lock = threading.Lock()
        self._release = threading.Event()

        self.calls: list[FakeCall] = []
        self.call_count = 0
        self.open_count = 0
        self.close_count = 0
        #: Set if ``close`` is called on an already-closed session. A real SDK
        #: double-free is undefined behaviour, so the wrapper must not do it
        #: (WF2-E-09).
        self.double_close_detected = False
        #: Incremented whenever two calls are inside the driver at once.
        #: Must stay 0.
        self.overlap_violations = 0
        self.max_observed_concurrency = 0
        self._active = 0
        self.threads_seen: set[int] = set()
        self._opened_on_thread: int | None = None
        self.cross_thread_calls = 0
        self._closed = False

    # -- scripting ----------------------------------------------------------

    def script(self, *outcomes: ScriptedOutcome) -> "FakeBlockingDriver":
        with self._lock:
            self._script.extend(outcomes)
        return self

    def block_next_call(self, *, timeout_s: float | None = None) -> "FakeBlockingDriver":
        """Make the next call block until released (or forever)."""
        return self.script(ScriptedOutcome(block=True, block_timeout_s=timeout_s))

    def delay_next_call(self, seconds: float) -> "FakeBlockingDriver":
        return self.script(ScriptedOutcome(delay_s=seconds))

    def fail_next_call(self, error: ConnectorError) -> "FakeBlockingDriver":
        return self.script(ScriptedOutcome(raises=error))

    def release_block(self) -> None:
        """Let every blocked call finish."""
        self._release.set()

    def rearm_block(self) -> None:
        self._release.clear()

    @property
    def active_calls(self) -> int:
        return self._active

    @property
    def script_remaining(self) -> int:
        with self._lock:
            return len(self._script)

    # -- driver protocol ----------------------------------------------------

    def open(self) -> DriverIdentity:
        self.open_count += 1
        self._opened_on_thread = threading.get_ident()
        self._closed = False
        return self._identity

    def close(self) -> None:
        self.close_count += 1
        if self._closed:
            self.double_close_detected = True
            return
        self._closed = True

    def fetch_meta(self) -> DriverResponse:
        return self._call("fetch_meta", {})

    def fetch_accounts(self, *, cursor: str | None, limit: int) -> DriverResponse:
        return self._call("fetch_accounts", {"cursor": cursor, "limit": limit})

    def fetch_positions(
        self,
        *,
        cursor: str | None,
        limit: int,
        account_id: str | None = None,
        symbol: str | None = None,
    ) -> DriverResponse:
        return self._call(
            "fetch_positions",
            {"cursor": cursor, "limit": limit, "account_id": account_id, "symbol": symbol},
        )

    def fetch_deals(
        self,
        *,
        cursor: str | None,
        limit: int,
        after_seq: int | None = None,
        account_id: str | None = None,
        symbol: str | None = None,
    ) -> DriverResponse:
        return self._call(
            "fetch_deals",
            {
                "cursor": cursor,
                "limit": limit,
                "after_seq": after_seq,
                "account_id": account_id,
                "symbol": symbol,
            },
        )

    # -- the one implementation every fetch shares --------------------------

    def _call(self, operation: str, params: dict[str, Any]) -> DriverResponse:
        ident = threading.get_ident()
        record = FakeCall(
            operation=operation,
            thread_ident=ident,
            started_monotonic=time.monotonic(),
            params=dict(params),
        )
        with self._lock:
            self.calls.append(record)
            self.call_count += 1
            self.threads_seen.add(ident)
            if self._opened_on_thread is not None and ident != self._opened_on_thread:
                self.cross_thread_calls += 1
            self._active += 1
            if self._active > 1:
                self.overlap_violations += 1
            self.max_observed_concurrency = max(self.max_observed_concurrency, self._active)
            outcome = self._script.popleft() if self._script else None

        try:
            if outcome is not None:
                if outcome.delay_s > 0:
                    time.sleep(outcome.delay_s)
                if outcome.raises is not None:
                    record.outcome = "raised"
                    raise outcome.raises
                if outcome.block:
                    # The hard part of the model: this thread is now parked
                    # and nothing the event loop does can retrieve it.
                    released = self._release.wait(outcome.block_timeout_s)
                    if not released:
                        record.outcome = "block_timeout"
                        raise TransportError(
                            "the fake driver's blocking call timed out",
                            details={"operation": operation, "reason": "fake_block_timeout"},
                        )
                if outcome.response is not None:
                    record.outcome = "ok"
                    return outcome.response
            if self._response_factory is not None:
                record.outcome = "ok"
                return self._response_factory(operation, params)
            record.outcome = "ok"
            return self._default
        finally:
            record.finished_monotonic = time.monotonic()
            with self._lock:
                self._active -= 1

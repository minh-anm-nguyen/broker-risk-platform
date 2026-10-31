"""The SDK actor: a bounded mailbox and exactly one worker (section 5.3).

This is the module the whole design exists for. Its contract:

* **One active call per session, always.** The worker is a single thread
  consuming a queue, so serialisation is structural rather than enforced by a
  check that could be forgotten (INV-02, ADR-202).
* **A caller giving up does not free the session.** ``_active`` is set and
  cleared *only by the worker thread*, when the driver call actually enters
  and actually returns. A timeout on the async side cannot clear it, which is
  the single most important difference between this and
  ``wait_for(to_thread(...))`` (section 5.1, WF2-F-03, WF2-H-02).
* **A late completion is discarded, counted and never delivered.** The waiter
  is looked up by ``request_id``, the epoch is compared, and the deadline is
  re-checked. Three independent reasons to drop a result, because a success
  delivered after its deadline is indistinguishable from fresh data to the
  consumer (INV-04, INV-05, WF2-H-03).

What this module explicitly does **not** claim: that it can stop a hung call.
There is no safe way to kill a Python thread, and pretending otherwise is the
failure mode section 5.4 is written against. When a call exceeds the hard
limit, the honest outcome is a quarantined session - and recovering
automatically from that requires the process worker in
:mod:`windows_worker.watchdog`, not this file.

Cancellation is handled in three places, matching the three states a request
can be in (section 5.4): queued requests are tombstoned and removed,
dispatched requests leave a marker so their completion can be recognised as
discarded, and the session slot is untouched by either.
"""

from __future__ import annotations

import asyncio
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Callable, Final

from .deadline import Clock
from .drivers.base import BlockingDriverProtocol, DriverIdentity, DriverResponse
from .errors import (
    ConnectorClosedError,
    ConnectorError,
    DeadlineExceededError,
    OverloadedError,
    TransportError,
)

__all__ = [
    "Priority",
    "CallEnvelope",
    "Completion",
    "RequestState",
    "SDKActor",
    "ActorStats",
]

#: At most this many high-priority calls run back to back while normal work is
#: waiting. A health probe must not be able to starve the sync service, and an
#: unweighted priority queue does exactly that (WF2-G-09).
_HIGH_PRIORITY_STREAK_CAP: Final[int] = 2


class Priority(StrEnum):
    HIGH = "high"
    NORMAL = "normal"


class RequestState(StrEnum):
    QUEUED = "queued"
    DISPATCHED = "dispatched"
    COMPLETED = "completed"
    DROPPED = "dropped"


@dataclass(frozen=True, slots=True)
class CallEnvelope:
    """An immutable request as handed to the worker (section 5.2).

    Immutable so that a retry builds a new envelope rather than mutating the
    one a worker may still be reading, and so a completion can be matched
    against exactly what was dispatched.
    """

    request_id: str
    source_id: str
    operation: str
    params: dict[str, Any]
    created_monotonic: float
    absolute_deadline_monotonic: float
    worker_epoch_at_admission: int
    source_generation_at_admission: int | None
    #: The latest instant at which starting this call is still worthwhile.
    #: Separate from the request deadline so a queue-wait budget can be
    #: shorter than the whole request budget, while the *drop* decision stays
    #: in one place - the worker, immediately before dispatch. Splitting it
    #: across two timers would leave a window where both fire.
    dispatch_by_monotonic: float = float("inf")
    attempt: int = 1
    priority: Priority = Priority.NORMAL
    bytes_budget: int = 1_048_576

    @property
    def effective_dispatch_bound(self) -> float:
        return min(self.absolute_deadline_monotonic, self.dispatch_by_monotonic)

    @staticmethod
    def new(
        *,
        source_id: str,
        operation: str,
        params: dict[str, Any],
        clock: Clock,
        absolute_deadline_monotonic: float,
        worker_epoch: int,
        source_generation: int | None,
        dispatch_by_monotonic: float | None = None,
        attempt: int = 1,
        priority: Priority = Priority.NORMAL,
        bytes_budget: int = 1_048_576,
        request_id: str | None = None,
    ) -> "CallEnvelope":
        return CallEnvelope(
            request_id=request_id or f"req-{uuid.uuid4().hex[:16]}",
            source_id=source_id,
            operation=operation,
            params=dict(params),
            created_monotonic=clock.monotonic(),
            absolute_deadline_monotonic=absolute_deadline_monotonic,
            worker_epoch_at_admission=worker_epoch,
            source_generation_at_admission=source_generation,
            dispatch_by_monotonic=(
                float("inf") if dispatch_by_monotonic is None else dispatch_by_monotonic
            ),
            attempt=attempt,
            priority=priority,
            bytes_budget=bytes_budget,
        )


@dataclass(frozen=True, slots=True)
class Completion:
    """The outcome of one dispatched call, as the worker saw it."""

    request_id: str
    worker_epoch: int
    operation: str
    source_generation_at_admission: int | None
    response: DriverResponse | None
    error: ConnectorError | None
    dispatched_monotonic: float
    finished_monotonic: float

    @property
    def occupancy_s(self) -> float:
        """How long the call held the session slot."""
        return self.finished_monotonic - self.dispatched_monotonic


@dataclass(slots=True)
class _Item:
    envelope: CallEnvelope
    tombstoned: bool = False


@dataclass(slots=True)
class ActorStats:
    """Counters an oracle can assert on."""

    submitted_total: int = 0
    dispatched_total: int = 0
    completed_total: int = 0
    failed_total: int = 0
    dropped_before_dispatch_total: int = 0
    cancelled_queued_total: int = 0
    abandoned_dispatched_total: int = 0
    stale_completions_discarded_total: int = 0
    rejected_mailbox_full_total: int = 0
    #: Highest value ``active_calls`` ever reached. Must never exceed 1.
    peak_active_calls: int = 0
    peak_queue_depth: int = 0
    loop_post_failures_total: int = 0
    occupancy_s_total: float = 0.0

    def as_dict(self) -> dict[str, float | int]:
        return {
            "submitted_total": self.submitted_total,
            "dispatched_total": self.dispatched_total,
            "completed_total": self.completed_total,
            "failed_total": self.failed_total,
            "dropped_before_dispatch_total": self.dropped_before_dispatch_total,
            "cancelled_queued_total": self.cancelled_queued_total,
            "abandoned_dispatched_total": self.abandoned_dispatched_total,
            "stale_completions_discarded_total": self.stale_completions_discarded_total,
            "rejected_mailbox_full_total": self.rejected_mailbox_full_total,
            "peak_active_calls": self.peak_active_calls,
            "peak_queue_depth": self.peak_queue_depth,
            "loop_post_failures_total": self.loop_post_failures_total,
            "occupancy_s_total": self.occupancy_s_total,
        }


class SDKActor:
    """One session, one worker thread, one active call.

    An actor instance *is* an epoch. Reconnecting builds a new actor with a
    new epoch rather than resetting this one, so there is no window in which a
    half-reset actor could deliver a result from the session it used to own.
    """

    def __init__(
        self,
        driver_factory: Callable[[], BlockingDriverProtocol],
        *,
        epoch: int,
        source_id: str,
        max_queue: int,
        clock: Clock,
        loop: asyncio.AbstractEventLoop | None = None,
        thread_name: str | None = None,
    ) -> None:
        if max_queue < 1:
            raise ValueError("max_queue must be at least 1")
        self._driver_factory = driver_factory
        self._epoch = epoch
        self._source_id = source_id
        self._max_queue = max_queue
        self._clock = clock
        self._loop = loop
        self._thread_name = thread_name or f"sdk-actor-{source_id}-epoch{epoch}"

        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._normal: deque[_Item] = deque()
        self._high: deque[_Item] = deque()
        self._index: dict[str, _Item] = {}
        self._high_streak = 0

        self._active: CallEnvelope | None = None
        self._active_since: float | None = None

        self._waiters: dict[str, asyncio.Future[Completion]] = {}
        #: Request ids whose caller gave up while the call was dispatched.
        #: Bounded by the single active call plus in-flight bookkeeping.
        self._abandoned_requests: set[str] = set()

        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._started = threading.Event()
        self._open_error: ConnectorError | None = None
        self._identity: DriverIdentity | None = None
        self._driver: BlockingDriverProtocol | None = None
        self._abandoned = False
        self._exited = threading.Event()
        self._last_progress_monotonic: float = clock.monotonic()

        self.stats = ActorStats()

    # -- identity and state -------------------------------------------------

    @property
    def epoch(self) -> int:
        return self._epoch

    @property
    def identity(self) -> DriverIdentity | None:
        return self._identity

    @property
    def driver(self) -> BlockingDriverProtocol | None:
        """The driver this session owns, for out-of-band control only.

        The supervisor uses it to call ``emergency_stop`` on a process-backed
        driver while the worker thread is blocked. Nothing may issue a *read*
        through this: that would be a second call on the session, from the
        wrong thread, and both drivers refuse it (WF2-E-06).
        """
        return self._driver

    @property
    def active_calls(self) -> int:
        """0 or 1. Cleared only when the driver call actually returns."""
        return 1 if self._active is not None else 0

    @property
    def active_request_id(self) -> str | None:
        envelope = self._active
        return None if envelope is None else envelope.request_id

    @property
    def active_age_s(self) -> float:
        since = self._active_since
        if since is None:
            return 0.0
        return max(0.0, self._clock.monotonic() - since)

    @property
    def queue_depth(self) -> int:
        with self._lock:
            return len(self._normal) + len(self._high)

    @property
    def waiters(self) -> int:
        return len(self._waiters)

    @property
    def abandoned(self) -> bool:
        return self._abandoned

    @property
    def thread_alive(self) -> bool:
        thread = self._thread
        return thread is not None and thread.is_alive()

    @property
    def last_progress_monotonic(self) -> float:
        """When the worker last finished a call. Fuels the watchdog."""
        return self._last_progress_monotonic

    # -- lifecycle ----------------------------------------------------------

    async def start(self) -> DriverIdentity:
        """Spawn the worker and open the session inside it.

        The driver is constructed *on the worker thread*, never here. A
        session created on the event loop and then used from a worker would
        break the ownership rule this actor depends on, and some native
        libraries bind thread-local state at construction (WF2-E-06).
        """
        if self._loop is None:
            self._loop = asyncio.get_running_loop()
        opened: asyncio.Future[DriverIdentity] = self._loop.create_future()

        def _report(identity: DriverIdentity | None, error: ConnectorError | None) -> None:
            if opened.done():  # pragma: no cover - single report by construction
                return
            if error is not None:
                opened.set_exception(error)
            else:
                assert identity is not None
                opened.set_result(identity)

        self._thread = threading.Thread(
            target=self._run,
            args=(_report,),
            name=self._thread_name,
            daemon=True,
        )
        self._thread.start()
        identity = await opened
        self._identity = identity
        return identity

    def _post(self, fn: Callable[..., None], *args: Any) -> None:
        """Hand work back to the event loop from the worker thread.

        ``call_soon_threadsafe`` is the only safe way across this boundary -
        touching a ``Future`` directly from a thread is a data race. A closed
        loop raises, which happens during shutdown and is counted rather than
        allowed to become an unhandled exception in a dying thread
        (WF2-F-07).
        """
        loop = self._loop
        if loop is None:  # pragma: no cover - start() sets it
            return
        try:
            loop.call_soon_threadsafe(fn, *args)
        except RuntimeError:
            self.stats.loop_post_failures_total += 1

    def _run(self, report: Callable[[DriverIdentity | None, ConnectorError | None], None]) -> None:
        try:
            driver = self._driver_factory()
            identity = driver.open()
        except ConnectorError as error:
            self._post(report, None, error)
            self._exited.set()
            return
        except Exception as exc:  # noqa: BLE001 - any driver failure is a transport failure
            self._post(
                report,
                None,
                TransportError(
                    "the driver session could not be opened",
                    source_id=self._source_id,
                    details={"driver_error": type(exc).__name__},
                ),
            )
            self._exited.set()
            return

        self._driver = driver
        self._started.set()
        self._post(report, identity, None)

        try:
            self._consume(driver)
        finally:
            try:
                driver.close()
            except Exception:  # noqa: BLE001 - close must not mask the exit
                pass
            self._exited.set()

    def _consume(self, driver: BlockingDriverProtocol) -> None:
        pending: list[CallEnvelope] = []
        while True:
            with self._cond:
                while not self._normal and not self._high and not self._stop.is_set():
                    self._cond.wait(timeout=0.05)
                if self._stop.is_set():
                    pending = self._drain_queues_locked()
                    break
                item = self._take_locked()
                if item is None:
                    continue
                envelope = item.envelope
                if item.tombstoned:
                    self.stats.cancelled_queued_total += 1
                    self._index.pop(envelope.request_id, None)
                    continue
                now = self._clock.monotonic()
                if now >= envelope.effective_dispatch_bound:
                    # Out of budget while queued. Dropped *before* dispatch, so
                    # the session is never occupied by work nobody wants - the
                    # case where a request has spent 2.9 s of a 3 s budget
                    # queueing and a 1.5 s call would answer far too late
                    # (section 5.3, WF2-H-01).
                    self.stats.dropped_before_dispatch_total += 1
                    self._index.pop(envelope.request_id, None)
                    queue_capped = now >= envelope.dispatch_by_monotonic and (
                        envelope.dispatch_by_monotonic < envelope.absolute_deadline_monotonic
                    )
                    self._post(
                        self._deliver_drop,
                        envelope,
                        DeadlineExceededError(
                            "the queue-wait budget expired before this request could be "
                            "dispatched, so no driver call was started"
                            if queue_capped
                            else "the request budget was exhausted while it waited in "
                            "the mailbox, so it was never dispatched",
                            source_id=envelope.source_id,
                            operation=envelope.operation,
                            request_id=envelope.request_id,
                            worker_epoch=self._epoch,
                            details={
                                "dropped_in_state": RequestState.QUEUED.value,
                                "bound": "queue_wait_cap" if queue_capped else "request_deadline",
                                "queued_ms": round(
                                    (now - envelope.created_monotonic) * 1000.0, 3
                                ),
                            },
                        ),
                    )
                    continue
                self._index.pop(envelope.request_id, None)
                self._active = envelope
                self._active_since = self._clock.monotonic()
                self.stats.dispatched_total += 1
                self.stats.peak_active_calls = max(self.stats.peak_active_calls, 1)

            completion = self._execute(driver, envelope)

            with self._cond:
                self._active = None
                self._active_since = None
                self._last_progress_monotonic = self._clock.monotonic()
                self.stats.occupancy_s_total += completion.occupancy_s
                if completion.error is None:
                    self.stats.completed_total += 1
                else:
                    self.stats.failed_total += 1
            self._post(self._deliver, completion)

        for envelope in pending:
            self.stats.dropped_before_dispatch_total += 1
            self._post(
                self._deliver_drop,
                envelope,
                ConnectorClosedError(
                    "the session shut down before this request was dispatched",
                    source_id=envelope.source_id,
                    operation=envelope.operation,
                    request_id=envelope.request_id,
                    worker_epoch=self._epoch,
                    details={"dropped_in_state": RequestState.QUEUED.value},
                ),
            )

    def _drain_queues_locked(self) -> list[CallEnvelope]:
        pending = [item.envelope for item in self._high if not item.tombstoned]
        pending += [item.envelope for item in self._normal if not item.tombstoned]
        self._high.clear()
        self._normal.clear()
        self._index.clear()
        return pending

    def _take_locked(self) -> _Item | None:
        if self._high and self._high_streak < _HIGH_PRIORITY_STREAK_CAP:
            self._high_streak += 1
            return self._high.popleft()
        if self._normal:
            self._high_streak = 0
            return self._normal.popleft()
        if self._high:
            # Only high-priority work left; the streak cap exists to protect
            # normal work, not to idle the worker.
            self._high_streak += 1
            return self._high.popleft()
        return None

    def _execute(self, driver: BlockingDriverProtocol, envelope: CallEnvelope) -> Completion:
        dispatched = self._clock.monotonic()
        response: DriverResponse | None = None
        error: ConnectorError | None = None
        try:
            response = self._invoke(driver, envelope)
        except ConnectorError as exc:
            error = exc.with_context(
                source_id=envelope.source_id,
                operation=envelope.operation,
                request_id=envelope.request_id,
                attempt=envelope.attempt,
                worker_epoch=self._epoch,
            )
        except Exception as exc:  # noqa: BLE001
            # An unexpected driver exception is normalised rather than allowed
            # to kill the worker thread. The type is recorded; the message is
            # not, because a native or httpx message can contain a URL
            # (WF2-E-02, WF2-B-06).
            error = TransportError(
                "the driver raised an unexpected error",
                source_id=envelope.source_id,
                operation=envelope.operation,
                request_id=envelope.request_id,
                attempt=envelope.attempt,
                worker_epoch=self._epoch,
                details={"driver_error": type(exc).__name__},
            )
        finished = self._clock.monotonic()
        return Completion(
            request_id=envelope.request_id,
            worker_epoch=self._epoch,
            operation=envelope.operation,
            source_generation_at_admission=envelope.source_generation_at_admission,
            response=response,
            error=error,
            dispatched_monotonic=dispatched,
            finished_monotonic=finished,
        )

    @staticmethod
    def _invoke(driver: BlockingDriverProtocol, envelope: CallEnvelope) -> DriverResponse:
        operation = envelope.operation
        params = envelope.params
        if operation == "fetch_meta":
            return driver.fetch_meta()
        if operation == "fetch_accounts":
            return driver.fetch_accounts(cursor=params.get("cursor"), limit=params["limit"])
        if operation == "fetch_positions":
            return driver.fetch_positions(
                cursor=params.get("cursor"),
                limit=params["limit"],
                account_id=params.get("account_id"),
                symbol=params.get("symbol"),
            )
        if operation == "fetch_deals":
            return driver.fetch_deals(
                cursor=params.get("cursor"),
                limit=params["limit"],
                after_seq=params.get("after_seq"),
                account_id=params.get("account_id"),
                symbol=params.get("symbol"),
            )
        raise ConnectorClosedError(
            "unknown driver operation", details={"operation": operation}
        )

    # -- submission ---------------------------------------------------------

    def submit(self, envelope: CallEnvelope) -> asyncio.Future[Completion]:
        """Enqueue a call and return the waiter.

        Rejects rather than blocks when the mailbox is full. Admission control
        normally prevents that, and this is the structural backstop: the
        mailbox cannot grow past its cap even if a caller bypasses admission
        (INV-03).
        """
        loop = self._loop
        if loop is None:  # pragma: no cover - start() sets it
            raise ConnectorClosedError("the actor has not been started")
        future: asyncio.Future[Completion] = loop.create_future()
        if self._stop.is_set() or self._abandoned:
            future.set_exception(
                ConnectorClosedError(
                    "the session is shutting down and is not accepting calls",
                    source_id=envelope.source_id,
                    operation=envelope.operation,
                    request_id=envelope.request_id,
                )
            )
            return future

        item = _Item(envelope=envelope)
        with self._cond:
            if len(self._normal) + len(self._high) >= self._max_queue:
                self.stats.rejected_mailbox_full_total += 1
                future.set_exception(
                    OverloadedError(
                        "the session mailbox is full",
                        source_id=envelope.source_id,
                        operation=envelope.operation,
                        request_id=envelope.request_id,
                        details={"mailbox_capacity": self._max_queue},
                    )
                )
                return future
            queue = self._high if envelope.priority is Priority.HIGH else self._normal
            queue.append(item)
            self._index[envelope.request_id] = item
            self.stats.submitted_total += 1
            self.stats.peak_queue_depth = max(
                self.stats.peak_queue_depth, len(self._normal) + len(self._high)
            )
            self._cond.notify()
        self._waiters[envelope.request_id] = future
        return future

    def state_of(self, request_id: str) -> RequestState:
        """Where a request is right now, without changing anything.

        The async side needs this to enforce a queue-wait cap. The worker can
        only drop an over-budget request when it *pops* it, and while the
        worker is parked inside a long call it never gets there - so a request
        queued behind one would wait out the whole request deadline rather
        than its much shorter queue budget (WF2-H-01).
        """
        with self._cond:
            if request_id in self._index:
                return RequestState.QUEUED
            if self._active is not None and self._active.request_id == request_id:
                return RequestState.DISPATCHED
        return RequestState.COMPLETED

    def release_waiter(self, request_id: str) -> RequestState:
        """Called when the caller stops waiting - timeout or cancellation.

        Returns the state the request was in, which decides what happens to
        the session slot:

        * ``QUEUED`` - tombstoned and forgotten. It never ran, so there is
          nothing to account for (WF2-G-01).
        * ``DISPATCHED`` - the future is dropped but the id is remembered, so
          the completion that eventually arrives is recognised as discarded
          rather than counted as an unknown stale result. **The session slot
          is not released**: only the worker can do that, and only by
          returning (WF2-G-02, WF2-F-03).
        """
        self._waiters.pop(request_id, None)
        with self._cond:
            item = self._index.pop(request_id, None)
            if item is not None:
                item.tombstoned = True
                # Removed from the mailbox now rather than left for the worker
                # to skip. The worker cannot reach it while it is parked
                # inside a call, so leaving it there would keep a cancelled
                # request occupying mailbox capacity - and reporting a queue
                # depth made of work nobody is waiting for (INV-03, WF2-G-03).
                queue = self._high if item.envelope.priority is Priority.HIGH else self._normal
                try:
                    queue.remove(item)
                except ValueError:  # pragma: no cover - already taken by the worker
                    pass
                self.stats.cancelled_queued_total += 1
                return RequestState.QUEUED
            if self._active is not None and self._active.request_id == request_id:
                self._abandoned_requests.add(request_id)
                self.stats.abandoned_dispatched_total += 1
                return RequestState.DISPATCHED
        return RequestState.COMPLETED

    # -- delivery (event loop) ----------------------------------------------

    def _deliver(self, completion: Completion) -> None:
        if self._abandoned:
            # Results from an abandoned session are never usable: a new epoch
            # owns the source now (WF2-F-04).
            self.stats.stale_completions_discarded_total += 1
            return
        if completion.request_id in self._abandoned_requests:
            self._abandoned_requests.discard(completion.request_id)
            self.stats.stale_completions_discarded_total += 1
            return
        future = self._waiters.pop(completion.request_id, None)
        if future is None or future.done():
            self.stats.stale_completions_discarded_total += 1
            return
        future.set_result(completion)

    def _deliver_drop(self, envelope: CallEnvelope, error: ConnectorError) -> None:
        self._abandoned_requests.discard(envelope.request_id)
        future = self._waiters.pop(envelope.request_id, None)
        if future is None or future.done():
            return
        future.set_exception(error)

    # -- shutdown -----------------------------------------------------------

    async def drain(self, *, timeout_s: float) -> bool:
        """Stop the worker and wait, bounded, for it to exit.

        Returns whether the thread actually exited. ``False`` means a call is
        still running and this process is now holding a thread it cannot
        reclaim - which is reported, not hidden (WF2-O-02, INV-14).
        """
        self._stop.set()
        with self._cond:
            self._cond.notify_all()
        thread = self._thread
        if thread is None:
            return True
        # Polled rather than handed to an executor. ``run_in_executor``
        # would put this wait on the default thread pool, and "no implicit
        # executor work outside the actor" is a property this package is
        # tested for (WF2-F-08); a shutdown path is no reason to break it.
        # Real time, not the injectable clock: this waits on an actual
        # operating-system thread, and a test's simulated clock has no say in
        # how long that takes.
        deadline = time.monotonic() + timeout_s
        while not self._exited.is_set() and time.monotonic() < deadline:
            await asyncio.sleep(0.005)
        if not self._exited.is_set():
            return False
        # The event is set inside the thread's ``finally``, so what remains is
        # only the interpreter tearing the thread down.
        thread.join(1.0)
        return not thread.is_alive()

    def abandon(self) -> None:
        """Give up on this session without claiming it was stopped.

        Used when a call has exceeded the hard limit and the deployment has
        opted into abandoning a thread. Every pending waiter fails, every
        future completion is discarded, and the thread **keeps running** -
        which is exactly why readiness must report it (section 5.4).
        """
        self._abandoned = True
        self._stop.set()
        with self._cond:
            pending = self._drain_queues_locked()
            self._cond.notify_all()
        for envelope in pending:
            self._deliver_drop(
                envelope,
                ConnectorClosedError(
                    "the session was abandoned before this request was dispatched",
                    source_id=envelope.source_id,
                    operation=envelope.operation,
                    request_id=envelope.request_id,
                    worker_epoch=self._epoch,
                ),
            )
        for request_id in list(self._waiters):
            future = self._waiters.pop(request_id, None)
            if future is not None and not future.done():
                future.set_exception(
                    ConnectorClosedError(
                        "the session that owned this request was abandoned",
                        source_id=self._source_id,
                        request_id=request_id,
                        worker_epoch=self._epoch,
                    )
                )

    def snapshot(self) -> dict[str, Any]:
        return {
            "epoch": self._epoch,
            "active_calls": self.active_calls,
            "active_request_id": self.active_request_id,
            "active_age_s": self.active_age_s,
            "queue_depth": self.queue_depth,
            "waiters": self.waiters,
            "mailbox_capacity": self._max_queue,
            "thread_alive": self.thread_alive,
            "abandoned": self._abandoned,
            **self.stats.as_dict(),
        }

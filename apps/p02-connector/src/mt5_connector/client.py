"""The connector: the async port callers actually hold.

One read walks this path, and the order is the design's (section 16.1):

    admission -> breaker -> session -> mailbox -> worker -> fence -> validate

The two subtleties that make this file worth reading slowly:

* **Admission and the session slot are released at different times.** When a
  caller's deadline expires, its admission ticket is released in a ``finally``
  - that resource is about the caller. The *session slot* is not released,
  because the driver call is still running and only the worker can say when it
  stops. Conflating the two is the bug that makes a timeout look like capacity
  freed up, and then dispatches a second call onto a session already in use
  (section 16.1, WF2-F-03, WF2-H-02).
* **The fence runs before the payload is looked at.** A completion is checked
  against its request id, its session epoch and the source generation
  *before* anything parses the body. Validating first and fencing afterwards
  would mean a stale page's contents had already been decoded, and a later
  code change could easily let it through (INV-05).

``CancelledError`` is re-raised untouched, after tombstoning the request. It is
the caller's lifecycle signal; translating it into a source error would tell a
consumer the broker failed when in fact its own task was cancelled
(section 4.4).
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Callable

from .actor import CallEnvelope, Priority, RequestState, SDKActor
from .admission import AdmissionController
from .capability import SUPPORTED_SIMULATOR_CONTRACTS
from .config import Settings
from .deadline import Clock, Deadline, SystemClock
from .drivers.base import BlockingDriverProtocol
from .drivers.fake_blocking import FakeBlockingDriver
from .drivers.simulator_sync_http import SimulatorSyncHttpDriver, warm_tls_context
from .errors import (
    CapabilityUnsupportedError,
    ConfigurationError,
    ConnectorClosedError,
    ConnectorError,
    DeadlineExceededError,
    SourceContractError,
)
from .ports import (
    Capabilities,
    Entity,
    Page,
    Provenance,
    ReadPlan,
    SourceIdentity,
)
from .retry import RetryController, RetryStats
from .supervisor import SessionSupervisor
from .telemetry import SECRETS, EventLoopMonitor, Metrics, sanitize_token_text
from .validation import (
    build_items,
    check_deal_ordering,
    check_provenance,
    cursor_fingerprint,
    filter_digest,
    map_error_response,
    parse_envelope,
)

__all__ = ["Connector", "ScanExpectation", "build_driver_factory"]

logger = logging.getLogger("mt5_connector.client")

_OPERATION_FOR: dict[Entity, str] = {
    Entity.ACCOUNTS: "fetch_accounts",
    Entity.POSITIONS: "fetch_positions",
    Entity.DEALS: "fetch_deals",
}


@dataclass(frozen=True, slots=True)
class ScanExpectation:
    """What a continuing page must match to belong to its scan.

    Passed in by :mod:`mt5_connector.scan` rather than held here: the
    connector serves one page at a time and must not accumulate scan state,
    or two concurrent scans would contaminate each other (WF2-G-08).
    """

    run_id: str | None
    generation: int | None
    snapshot_seq: int | None
    page_index: int
    previous_upper_bound: int | None = None
    requested_after_seq: int | None = None


def build_driver_factory(
    settings: Settings, *, token: str | None = None
) -> Callable[[], BlockingDriverProtocol]:
    """Pick a driver for the configured source kind.

    The token is read **once**, here, and captured by the closure. Reading it
    per call would let a mid-scan rotation produce two different credentials
    inside one session, and the factory is also where it gets registered for
    redaction so it can never reach a log line (WF2-B-04, INV-15).
    """
    if settings.driver == "manager_sdk_windows":
        from .drivers.manager_sdk_windows import ManagerSdkWindowsDriver

        return lambda: ManagerSdkWindowsDriver(settings, token="")
    resolved = settings.token() if token is None else token
    SECRETS.add(resolved)
    if settings.driver == "fake_blocking":
        return lambda: FakeBlockingDriver()
    # Built here, before any deadline is running: loading the CA bundle is
    # local start-up work and must not be charged to a connect budget.
    warm_tls_context(settings.tls_verify)
    return lambda: SimulatorSyncHttpDriver(settings, token=resolved)


class Connector:
    """Implements :class:`mt5_connector.ports.MT5Client`."""

    def __init__(
        self,
        settings: Settings,
        *,
        driver_factory: Callable[[], BlockingDriverProtocol] | None = None,
        clock: Clock | None = None,
        metrics: Metrics | None = None,
        rng_seed: int | None = None,
    ) -> None:
        self._settings = settings
        self._clock = clock or SystemClock()
        self._metrics = metrics or Metrics(source_id=settings.source_id)
        self._admission = AdmissionController(
            max_pending=settings.max_queue_pending,
            max_concurrent_scans=settings.max_concurrent_scans,
        )
        self._supervisor = SessionSupervisor(
            settings,
            driver_factory=driver_factory or build_driver_factory(settings),
            clock=self._clock,
            metrics=self._metrics,
            admission=self._admission,
        )
        import random

        self._retry = RetryController(
            max_attempts=settings.max_attempts_per_read,
            base_backoff_ms=settings.retry_base_backoff_ms,
            max_backoff_ms=settings.retry_max_backoff_ms,
            retry_after_cap_ms=settings.retry_after_cap_ms,
            minimum_attempt_budget_ms=settings.minimum_attempt_budget_ms,
            clock=self._clock,
            breaker=self._supervisor.breaker,
            rng=random.Random(rng_seed) if rng_seed is not None else None,
        )
        self._loop_monitor = EventLoopMonitor(
            interval_s=settings.heartbeat_interval_ms / 1000.0, metrics=self._metrics
        )
        self._closed = False
        #: Per-operation retry statistics from the most recent call, for
        #: evidence files and attempt-count assertions.
        self.last_retry_stats: RetryStats | None = None

    # -- accessors ----------------------------------------------------------

    @property
    def settings(self) -> Settings:
        return self._settings

    @property
    def supervisor(self) -> SessionSupervisor:
        return self._supervisor

    @property
    def admission(self) -> AdmissionController:
        return self._admission

    @property
    def metrics(self) -> Metrics:
        return self._metrics

    @property
    def loop_monitor(self) -> EventLoopMonitor:
        return self._loop_monitor

    @property
    def retry(self) -> RetryController:
        return self._retry

    # -- port ---------------------------------------------------------------

    async def connect(self) -> SourceIdentity:
        self._loop_monitor.start()
        identity = await self._supervisor.start()
        capabilities = self._supervisor.capabilities
        assert capabilities is not None
        if capabilities.contract_version not in SUPPORTED_SIMULATOR_CONTRACTS:
            # Belt and braces: the capability mapper already refuses an
            # unknown version. Repeating it here keeps a future driver from
            # bypassing the check by constructing Capabilities directly.
            raise ConfigurationError(
                "negotiated contract version is not supported",
                source_id=self._settings.source_id,
                details={"contract_version": capabilities.contract_version},
            )
        logger.info(
            "source session established",
            extra={
                "source_id": identity.source_id,
                "worker_epoch": identity.worker_epoch,
                "run_id": identity.run_id,
                "source_generation": identity.generation,
                "contract_version": capabilities.contract_version,
            },
        )
        return identity

    async def capabilities(self) -> Capabilities:
        capabilities = self._supervisor.capabilities
        if capabilities is None:
            await self.connect()
            capabilities = self._supervisor.capabilities
        assert capabilities is not None
        return capabilities

    async def read_page(
        self,
        plan: ReadPlan,
        *,
        deadline_ms: int | None = None,
        client_id: str = "default",
        expect: ScanExpectation | None = None,
        priority: Priority = Priority.NORMAL,
    ) -> Page[Any]:
        """Read one validated page, or raise a typed error."""
        if self._closed:
            raise ConnectorClosedError(
                "this connector instance is closed",
                source_id=self._settings.source_id,
                operation=_OPERATION_FOR[plan.entity],
            )
        operation = _OPERATION_FOR[plan.entity]
        total_ms = deadline_ms if deadline_ms is not None else self._settings.page_total_deadline_ms
        if total_ms < 1:
            raise ConfigurationError(
                "a request deadline must be at least 1 ms; an unbounded request is "
                "not something a caller may ask for",
                operation=operation,
                details={"field": "deadline_ms"},
            )
        deadline = Deadline.after(total_ms, clock=self._clock)
        self._check_plan(plan, operation=operation)

        request_id = f"req-{_short_id()}"
        started = self._clock.monotonic()
        ticket = self._admission.acquire(
            request_id=request_id,
            deadline=deadline,
            operation=operation,
            client_id=client_id,
        )
        stats = RetryStats()
        self.last_retry_stats = stats
        outcome = "error"
        error_class = "none"
        try:
            page = await self._retry.run(
                lambda attempt, budget: self._attempt(
                    plan,
                    deadline=deadline,
                    operation=operation,
                    request_id=request_id,
                    attempt=attempt,
                    expect=expect,
                    priority=priority,
                ),
                operation=operation,
                deadline=deadline,
                request_id=request_id,
                stats=stats,
            )
        except ConnectorError as error:
            error_class = error.code
            raise
        except asyncio.CancelledError:
            outcome = "cancelled"
            raise
        else:
            outcome = "success"
            return page
        finally:
            # Admission is released here even if a driver call is still
            # running: it bounds *callers*, not the session (section 16.1).
            self._admission.release(ticket)
            duration = max(0.0, self._clock.monotonic() - started)
            self._metrics.observe_request(
                operation=operation,
                outcome=outcome,
                error_class=error_class,
                duration_s=duration,
            )
            for attempt_index, _ in enumerate(stats.delays_s, start=1):
                self._metrics.observe_retry(
                    error_class=stats.final_code or "unknown", attempt=attempt_index
                )

    async def close(self) -> bool:
        """Drain. Returns whether everything was reclaimed."""
        if self._closed:
            return True
        self._closed = True
        await self._loop_monitor.stop()
        return await self._supervisor.close()

    # -- one attempt --------------------------------------------------------

    def _check_plan(self, plan: ReadPlan, *, operation: str) -> None:
        capabilities = self._supervisor.capabilities
        self._settings.resolve_limit(plan.limit)
        if capabilities is None:
            return
        if plan.limit > capabilities.max_page_size:
            raise ConfigurationError(
                "the requested page size exceeds what this source declared; it is "
                "refused before any upstream request is made",
                operation=operation,
                details={"limit": plan.limit, "source_max_page_size": capabilities.max_page_size},
            )
        if not capabilities.supports(plan.entity):
            raise CapabilityUnsupportedError(
                "the source does not declare support for this entity",
                operation=operation,
                details={"entity": plan.entity.value},
            )
        if plan.cursor is not None and not capabilities.snapshot_cursor:
            raise CapabilityUnsupportedError(
                "the source does not declare a snapshot cursor, so a multi-page scan "
                "over it cannot be claimed to come from one snapshot",
                operation=operation,
                details={"capability": "snapshot_cursor"},
            )
        if plan.after_seq is not None and not capabilities.incremental_deals_after_seq:
            raise CapabilityUnsupportedError(
                "the source does not declare an exclusive after_seq filter with a "
                "per-deal sequence, so incremental sync is not available on it",
                operation=operation,
                details={"capability": "incremental_deals_after_seq"},
            )

    async def _attempt(
        self,
        plan: ReadPlan,
        *,
        deadline: Deadline,
        operation: str,
        request_id: str,
        attempt: int,
        expect: ScanExpectation | None,
        priority: Priority,
    ) -> Page[Any]:
        actor = await self._supervisor.ensure_serving(deadline=deadline, operation=operation)
        capabilities = self._supervisor.capabilities
        identity = self._supervisor.identity
        if capabilities is None or identity is None:  # pragma: no cover - defensive
            raise ConnectorClosedError(
                "no negotiated session is available",
                source_id=self._settings.source_id,
                operation=operation,
            )

        envelope = CallEnvelope.new(
            source_id=self._settings.source_id,
            operation=operation,
            params=self._params_for(plan),
            clock=self._clock,
            absolute_deadline_monotonic=deadline.at,
            worker_epoch=actor.epoch,
            source_generation=identity.generation,
            dispatch_by_monotonic=self._clock.monotonic()
            + self._settings.queue_wait_cap_ms / 1000.0,
            attempt=attempt,
            priority=priority,
            bytes_budget=self._settings.max_response_bytes,
            request_id=f"{request_id}.{attempt}",
        )
        future = actor.submit(envelope)
        queued_at = self._clock.monotonic()

        try:
            completion = await self._await_completion(
                future, actor=actor, envelope=envelope, deadline=deadline
            )
        except asyncio.TimeoutError as exc:
            state = actor.release_waiter(envelope.request_id)
            # The distinction in ``details`` is the operationally important
            # part: a request that timed out while queued consumed nothing,
            # while one that timed out while dispatched left a driver call
            # running and the session occupied.
            queued = state is RequestState.QUEUED
            raise DeadlineExceededError(
                "the queue-wait budget expired while this request was still waiting "
                "its turn, so no driver call was started"
                if queued
                else "the request deadline expired before the driver call returned; a "
                "call already dispatched keeps the session slot until it actually "
                "returns",
                source_id=self._settings.source_id,
                operation=operation,
                request_id=envelope.request_id,
                attempt=attempt,
                worker_epoch=actor.epoch,
                details={
                    "state_at_timeout": state.value,
                    "dropped_in_state": state.value,
                    "bound": "queue_wait_cap" if queued else "request_deadline",
                    "session_slot_released": state is not RequestState.DISPATCHED,
                    "inflight_age_s": round(actor.active_age_s, 3),
                },
            ) from exc
        except asyncio.CancelledError:
            # Tombstoned, then re-raised unchanged. The caller cancelled; that
            # is not a source failure (section 4.4, WF2-G-01, WF2-G-02).
            actor.release_waiter(envelope.request_id)
            raise

        self._metrics.queue_wait.labels(
            sanitize_token_text(self._settings.source_id), operation
        ).observe(max(0.0, completion.dispatched_monotonic - queued_at))
        self._metrics.sdk_call_duration.labels(
            sanitize_token_text(self._settings.source_id), operation
        ).observe(completion.occupancy_s)

        # Fence first. Nothing below this line may look at the payload before
        # the session and generation have been confirmed current.
        completion = self._supervisor.accept_completion(completion, envelope)
        deadline.check(operation=operation, request_id=envelope.request_id)

        if completion.error is not None:
            await self._supervisor.note_operation_failure(completion.error)
            raise completion.error
        response = completion.response
        if response is None:  # pragma: no cover - defensive
            raise SourceContractError(
                "the driver returned neither a response nor an error",
                source_id=self._settings.source_id,
                operation=operation,
                request_id=envelope.request_id,
            )
        self._metrics.response_bytes.labels(
            sanitize_token_text(self._settings.source_id), operation
        ).observe(response.byte_count)

        # Every error leaving this frame carries the correlation fields
        # section 10.2 requires - request id, attempt, worker epoch, source
        # generation. Without them an incident cannot be traced from the
        # caller to the worker call that failed, and a mapped status or a
        # validation failure is exactly where they were being lost
        # (WF2-P-07).
        context = {
            "request_id": envelope.request_id,
            "attempt": attempt,
            "worker_epoch": actor.epoch,
            "source_generation": identity.generation,
        }
        try:
            if not 200 <= response.status < 300:
                mapped = map_error_response(
                    response,
                    source_id=self._settings.source_id,
                    operation=operation,
                    request_id=envelope.request_id,
                    retry_after_cap_s=self._settings.retry_after_cap_ms / 1000.0,
                )
                await self._supervisor.note_operation_failure(mapped)
                raise mapped

            return self._validate(
                response,
                plan=plan,
                expect=expect,
                operation=operation,
                request_id=envelope.request_id,
                capabilities=capabilities,
                identity=identity,
            )
        except ConnectorError as error:
            raise error.with_context(**context)

    async def _await_completion(
        self,
        future: "asyncio.Future[Any]",
        *,
        actor: SDKActor,
        envelope: CallEnvelope,
        deadline: Deadline,
    ) -> Any:
        """Wait in two phases: the queue budget, then the rest of the deadline.

        The queue budget has to be enforced here as well as in the worker.
        The worker checks it when it pops a request, which it cannot do while
        it is parked inside a long call - so a request queued behind one would
        sit for the full request deadline instead of its much shorter queue
        budget (WF2-H-01).

        Both waits use :func:`asyncio.wait` rather than
        :func:`asyncio.wait_for`. ``wait_for`` cancels the future it is
        waiting on when it times out, and that future is the actor's record of
        a call that is *still running*: cancelling it destroys the bookkeeping
        that later recognises the completion as discarded, and
        :func:`asyncio.shield` is no better here - a shielded future whose
        outer waiter was cancelled makes asyncio log the eventual failure as
        an unretrieved exception, which looks like a bug and buries real ones.
        ``wait`` leaves the future alone, which is exactly the semantics this
        design needs.
        """
        # The completion may arrive long after every waiter has gone. The
        # callback retrieves it so it is never reported as unretrieved; the
        # discard itself is already counted by the actor.
        future.add_done_callback(_consume_abandoned_result)

        queue_budget = min(
            self._settings.queue_wait_cap_ms / 1000.0, max(0.0, deadline.remaining_s)
        )
        done, _ = await asyncio.wait({future}, timeout=queue_budget)
        if not done:
            if actor.state_of(envelope.request_id) is RequestState.QUEUED:
                # Still waiting its turn with no queue budget left. The raise
                # is handled by the caller, which tombstones it.
                raise asyncio.TimeoutError
            done, _ = await asyncio.wait(
                {future}, timeout=max(0.0, deadline.remaining_s)
            )
            if not done:
                raise asyncio.TimeoutError
        return future.result()

    def _validate(
        self,
        response: Any,
        *,
        plan: ReadPlan,
        expect: ScanExpectation | None,
        operation: str,
        request_id: str,
        capabilities: Capabilities,
        identity: SourceIdentity,
    ) -> Page[Any]:
        page_index = 0 if expect is None else expect.page_index
        try:
            facts = parse_envelope(
                response,
                source_id=self._settings.source_id,
                operation=operation,
                request_id=request_id,
            )
            check_provenance(
                facts,
                source_id=self._settings.source_id,
                operation=operation,
                request_id=request_id,
                expected_run_id=expect.run_id if expect else None,
                expected_generation=expect.generation if expect else None,
                expected_snapshot_seq=expect.snapshot_seq if expect else None,
                page_index=page_index,
            )
            if len(facts.raw_items) > plan.limit:
                # More records than were asked for. Either the limit was
                # ignored or the page was assembled from somewhere else;
                # either way the page size cap is a memory bound and this
                # breached it (WF2-M-05).
                raise SourceContractError(
                    "the page carries more records than the requested limit",
                    source_id=self._settings.source_id,
                    operation=operation,
                    request_id=request_id,
                    details={"limit": plan.limit, "items_on_page": len(facts.raw_items)},
                )
            items = build_items(
                facts,
                entity=plan.entity,
                capabilities=capabilities,
                source_id=self._settings.source_id,
                operation=operation,
                request_id=request_id,
                page_index=page_index,
            )
            upper_bound: int | None = None
            if plan.entity is Entity.DEALS:
                upper_bound = check_deal_ordering(
                    items,
                    facts=facts,
                    requested_after_seq=(
                        expect.requested_after_seq if expect else plan.after_seq
                    ),
                    previous_upper_bound=expect.previous_upper_bound if expect else None,
                    capabilities=capabilities,
                    source_id=self._settings.source_id,
                    operation=operation,
                    request_id=request_id,
                    page_index=page_index,
                )
        except ConnectorError as error:
            field_name = str(error.details.get("field", "envelope"))
            self._metrics.observe_validation_error(
                entity=plan.entity.value, field_name=field_name
            )
            raise

        provenance = Provenance(
            source_id=self._settings.source_id,
            entity=plan.entity,
            run_id=facts.run_id,
            generation=facts.generation,
            snapshot_seq=facts.snapshot_seq,
            worker_epoch=identity.worker_epoch,
            schema_version=capabilities.contract_version,
            filter_digest=filter_digest(plan.filter_fields()),
        )
        page_identity = SourceIdentity(
            source_kind=identity.source_kind,
            source_id=identity.source_id,
            worker_epoch=identity.worker_epoch,
            run_id=facts.run_id,
            generation=facts.generation,
            contract_version=capabilities.contract_version,
        )
        if facts.next_cursor is not None:
            logger.debug(
                "page validated",
                extra={
                    "source_id": self._settings.source_id,
                    "operation": operation,
                    "request_id": request_id,
                    "page_index": page_index,
                    "items": len(items),
                    "cursor_fingerprint": cursor_fingerprint(facts.next_cursor),
                },
            )
        return Page(
            identity=page_identity,
            provenance=provenance,
            items=items,
            next_cursor=facts.next_cursor,
            snapshot_seq=facts.snapshot_seq,
            requested_after_seq=(expect.requested_after_seq if expect else plan.after_seq),
            observed_upper_bound=upper_bound,
            page_index=page_index,
            response_bytes=facts.byte_count,
        )

    @staticmethod
    def _params_for(plan: ReadPlan) -> dict[str, Any]:
        params: dict[str, Any] = {"cursor": plan.cursor, "limit": plan.limit}
        if plan.cursor is None:
            if plan.account_id is not None:
                params["account_id"] = plan.account_id
            if plan.symbol is not None:
                params["symbol"] = plan.symbol
            if plan.after_seq is not None:
                params["after_seq"] = plan.after_seq
        return params


def _short_id() -> str:
    import uuid

    return uuid.uuid4().hex[:12]


def _consume_abandoned_result(future: "asyncio.Future[Any]") -> None:
    """Retrieve a result nobody is waiting for, so asyncio stays quiet."""
    if future.cancelled():
        return
    future.exception()

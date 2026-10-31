"""The session supervisor: lifecycle, epochs, fencing and the watchdog.

One supervisor owns one source. It is the only thing that creates an actor,
and therefore the only thing that mints an epoch - which is what makes
fencing trustworthy. An epoch is not a counter that gets bumped; it *is* a
session incarnation, because reconnecting constructs a new
:class:`~mt5_connector.actor.SDKActor` and the old one keeps its own epoch
until it dies. A completion from the old actor can never be mistaken for the
new one's, because they are different objects with different numbers
(ADR-206).

Three behaviours are worth reading closely:

* **Single-flight connect.** 100 callers arriving during an outage produce one
  handshake, not 100. The shared task is awaited through
  ``asyncio.shield``, so a waiter whose own deadline expires does not cancel
  the connect that the other 99 are waiting on - a bug that turns a
  single-flight design back into a stampede (WF2-B-07, WF2-G-04).
* **Permanent failure disables.** A 401, a 403, a configuration error or an
  incompatible driver moves the session to ``DISABLED`` and *stores the
  error*. Every subsequent caller gets that stored error without a single
  upstream request, until configuration changes. This is what prevents the
  retry storm in section 15.4 (WF2-B-01, WF2-H-06).
* **The watchdog does not claim to kill anything.** It detects a call past the
  hard limit and quarantines the session. In thread mode that is where things
  stay, and readiness says so, because there is no safe way to stop a running
  Python thread. Recovering automatically requires the process worker
  (section 5.4, WF2-F-10, WF2-R-04).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable

from .actor import CallEnvelope, Completion, SDKActor
from .admission import AdmissionController
from .capability import map_fake_capabilities, map_simulator_capabilities
from .config import Settings
from .connection_fsm import SessionState, SessionStateMachine
from .deadline import Clock, Deadline, SystemClock
from .drivers.base import BlockingDriverProtocol, DriverIdentity
from .errors import (
    AuthenticationError,
    CircuitOpenError,
    ConfigurationError,
    ConnectorClosedError,
    ConnectorError,
    IncompatibleSdkError,
    OverloadedError,
    PermissionDeniedError,
    SourceEpochChangedError,
)
from .ports import Capabilities, SourceIdentity
from .retry import BreakerState, CircuitBreaker
from .telemetry import Metrics, sanitize_token_text

__all__ = ["SessionSupervisor", "StaleCompletion"]

logger = logging.getLogger("mt5_connector.supervisor")

#: Errors that disable a source rather than being retried.
_PERMANENT = (
    AuthenticationError,
    PermissionDeniedError,
    ConfigurationError,
    IncompatibleSdkError,
)


class StaleCompletion(ConnectorError):
    """A completion arrived for a session that no longer owns the source.

    Never returned to a caller as data - raised so the caller's request fails
    rather than succeeding with a result from a replaced session (INV-05).
    """

    code = "STALE_COMPLETION_FENCED"
    category = "consistency"
    resync_required = True
    safe_action = (
        "the session was replaced while this call was in flight; retry on the new "
        "session and reconcile anything the old one may have reported"
    )


class SessionSupervisor:
    """Owns the FSM, the current actor, the breaker and the watchdog."""

    def __init__(
        self,
        settings: Settings,
        *,
        driver_factory: Callable[[], BlockingDriverProtocol],
        clock: Clock | None = None,
        metrics: Metrics | None = None,
        admission: AdmissionController | None = None,
    ) -> None:
        self._settings = settings
        self._driver_factory = driver_factory
        self._clock = clock or SystemClock()
        self._metrics = metrics or Metrics(source_id=settings.source_id)
        self._admission = admission or AdmissionController(
            max_pending=settings.max_queue_pending,
            max_concurrent_scans=settings.max_concurrent_scans,
        )
        self._fsm = SessionStateMachine()
        self._breaker = CircuitBreaker(
            failure_threshold=settings.breaker_failure_threshold,
            cooldown_ms=settings.breaker_cooldown_ms,
            clock=self._clock,
        )

        self._epoch = 0
        self._actor: SDKActor | None = None
        self._identity: SourceIdentity | None = None
        self._capabilities: Capabilities | None = None
        self._disabled_error: ConnectorError | None = None
        self._resync_reason: str | None = None

        self._connect_task: asyncio.Task[None] | None = None
        self._connect_waiters = 0
        self._watchdog_task: asyncio.Task[None] | None = None
        self._closing = False

        #: Actors this process could not reclaim. Each one is a live thread
        #: running a call nobody can stop, so the count is reported rather
        #: than forgotten (section 5.4).
        self._abandoned_actors: list[SDKActor] = []

        self.connect_attempts_total = 0
        self.connect_leaders_total = 0
        self.quarantine_total = 0
        self.stale_fenced_total = 0

    # -- accessors ----------------------------------------------------------

    @property
    def settings(self) -> Settings:
        return self._settings

    @property
    def state(self) -> SessionState:
        return self._fsm.state

    @property
    def fsm(self) -> SessionStateMachine:
        return self._fsm

    @property
    def worker_epoch(self) -> int:
        return self._epoch

    @property
    def identity(self) -> SourceIdentity | None:
        return self._identity

    @property
    def capabilities(self) -> Capabilities | None:
        return self._capabilities

    @property
    def admission(self) -> AdmissionController:
        return self._admission

    @property
    def breaker(self) -> CircuitBreaker:
        return self._breaker

    @property
    def metrics(self) -> Metrics:
        return self._metrics

    @property
    def clock(self) -> Clock:
        return self._clock

    @property
    def actor(self) -> SDKActor | None:
        return self._actor

    @property
    def source_generation(self) -> int | None:
        return None if self._identity is None else self._identity.generation

    @property
    def abandoned_worker_count(self) -> int:
        # Recounted each time: an abandoned thread that finally exits stops
        # counting against readiness, which is the only way out of that state
        # in thread mode.
        self._abandoned_actors = [a for a in self._abandoned_actors if a.thread_alive]
        return len(self._abandoned_actors)

    # -- lifecycle ----------------------------------------------------------

    async def start(self) -> SourceIdentity:
        """Connect and begin the watchdog. Raises if the source is unusable."""
        if self._watchdog_task is None:
            self._watchdog_task = asyncio.get_running_loop().create_task(
                self._watchdog(), name=f"session-watchdog-{self._settings.source_id}"
            )
        await self.ensure_serving(
            deadline=Deadline.after(self._connect_budget_ms(), clock=self._clock),
            operation="connect",
        )
        assert self._identity is not None
        return self._identity

    def _connect_budget_ms(self) -> int:
        """How long a handshake may take in total.

        A handshake is a TCP connect *and* a ``/v1/meta`` read, so both
        timeouts count, once per permitted attempt, plus the backoff between
        them. Sizing this from the connect timeout alone was wrong: it made a
        healthy source look unreachable whenever the metadata read took
        longer than the socket handshake.
        """
        per_attempt = (
            self._settings.connect_timeout_ms + self._settings.upstream_read_timeout_ms
        )
        return (
            per_attempt + self._settings.retry_max_backoff_ms
        ) * self._settings.max_connect_attempts

    async def ensure_serving(self, *, deadline: Deadline, operation: str) -> SDKActor:
        """Return an actor able to serve, or raise a typed refusal.

        Called before every attempt rather than once per request, so a
        reconnect can happen between two attempts of the same read.
        """
        self._refuse_if_not_servable(operation)
        if self._fsm.serving and self._actor is not None:
            return self._actor

        await self._join_connect(deadline=deadline, operation=operation)
        # Re-checked after the handshake, not only before it. A connect that
        # ended in DISABLED must surface its own stored error - the auth
        # failure the operator has to fix - rather than a generic refusal that
        # says nothing about why (WF2-B-01).
        self._refuse_if_not_servable(operation)
        if not self._fsm.serving or self._actor is None:  # pragma: no cover - defensive
            raise CircuitOpenError(
                "the session did not reach a serving state",
                source_id=self._settings.source_id,
                operation=operation,
                details={"state": self._fsm.state.value},
            )
        return self._actor

    def _refuse_if_not_servable(self, operation: str) -> None:
        """Raise the specific refusal that belongs to the current state."""
        if self._closing:
            raise ConnectorClosedError(
                "the connector is shutting down",
                source_id=self._settings.source_id,
                operation=operation,
            )
        state = self._fsm.state
        if state is SessionState.DISABLED:
            # No upstream request at all. The stored error is replayed as-is,
            # which is what keeps 100 callers from becoming 300 auth attempts.
            raise self._disabled_error or AuthenticationError(
                "the source is disabled pending a configuration change",
                source_id=self._settings.source_id,
                operation=operation,
            )
        if state is SessionState.RESYNC_REQUIRED:
            raise SourceEpochChangedError(
                "this source needs an explicit re-sync before it will serve reads again",
                source_id=self._settings.source_id,
                operation=operation,
                details={"reason": self._resync_reason or "source identity changed"},
            )
        if state in (SessionState.SUSPECT, SessionState.QUARANTINED):
            actor = self._actor
            raise CircuitOpenError(
                "the session is quarantined: a driver call has run past the hard limit "
                "and cannot be stopped, so no further call will be dispatched on it",
                source_id=self._settings.source_id,
                operation=operation,
                worker_epoch=self._epoch,
                details={
                    "reason": "session_quarantined",
                    "state": state.value,
                    "inflight_age_s": round(actor.active_age_s, 3) if actor else 0.0,
                    "worker_mode": self._settings.worker_mode,
                    "can_force_recover": self._settings.process_isolation
                    or self._settings.allow_thread_abandon,
                },
            )

    async def _join_connect(self, *, deadline: Deadline, operation: str) -> None:
        task = self._connect_task
        if task is None or task.done():
            self.connect_leaders_total += 1
            task = asyncio.get_running_loop().create_task(
                self._connect_sequence(), name=f"connect-{self._settings.source_id}"
            )
            self._connect_task = task
        elif self._connect_waiters >= self._settings.max_queue_pending:
            # Waiters are bounded like everything else. Without this, an
            # outage turns every arriving caller into a retained coroutine.
            raise OverloadedError(
                "too many callers are already waiting for this source to reconnect",
                source_id=self._settings.source_id,
                operation=operation,
                details={"max_waiters": self._settings.max_queue_pending},
            )

        self._connect_waiters += 1
        try:
            # Shielded: this caller's deadline must not cancel the shared
            # handshake that other callers are also waiting on.
            await asyncio.wait_for(asyncio.shield(task), timeout=max(0.0, deadline.remaining_s))
        except asyncio.TimeoutError as exc:
            deadline.check(operation=operation)
            raise CircuitOpenError(
                "the source did not become available inside this request's deadline",
                source_id=self._settings.source_id,
                operation=operation,
                details={"reason": "connect_timeout", "state": self._fsm.state.value},
            ) from exc
        finally:
            self._connect_waiters -= 1

    async def _connect_sequence(self) -> None:
        """One handshake, with bounded attempts. Runs as the shared task."""
        attempt = 0
        while True:
            attempt += 1
            self.connect_attempts_total += 1
            if self._fsm.state is SessionState.DISABLED:
                return
            if self._fsm.state in (SessionState.STOPPED, SessionState.BACKOFF):
                self._transition(SessionState.CONNECTING, reason=f"connect attempt {attempt}")
            elif self._fsm.state is not SessionState.CONNECTING:
                return
            try:
                await self._open_session()
            except _PERMANENT as error:
                self._disable(error)
                return
            except ConnectorError as error:
                self._breaker.record_failure(error)
                self._metrics.reconnect_total.labels(
                    sanitize_token_text(self._settings.source_id),
                    sanitize_token_text(error.code),
                ).inc()
                if attempt >= self._settings.max_connect_attempts:
                    self._transition(
                        SessionState.BACKOFF, reason=f"connect failed: {error.code}"
                    )
                    raise
                self._transition(SessionState.BACKOFF, reason=f"connect retry: {error.code}")
                delay = min(
                    self._settings.retry_max_backoff_ms / 1000.0,
                    (self._settings.retry_base_backoff_ms / 1000.0) * (2 ** (attempt - 1)),
                )
                await self._clock.sleep(delay)
                continue
            else:
                self._breaker.record_success()
                return

    async def _open_session(self) -> None:
        """Mint a new epoch, start an actor, verify identity."""
        previous = self._identity
        self._epoch += 1
        self._metrics.set_gauge(self._metrics.worker_epoch, self._epoch)
        actor = SDKActor(
            self._driver_factory,
            epoch=self._epoch,
            source_id=self._settings.source_id,
            max_queue=self._settings.max_queue_pending,
            clock=self._clock,
        )
        try:
            driver_identity = await actor.start()
        except ConnectorError:
            await actor.drain(timeout_s=self._settings.drain_timeout_ms / 1000.0)
            raise

        try:
            capabilities = self._map_capabilities(driver_identity)
        except ConnectorError:
            # A source whose capability document we cannot understand is not a
            # source we read from. The actor is torn down so no session is
            # left open against it (WF2-A-04).
            await actor.drain(timeout_s=self._settings.drain_timeout_ms / 1000.0)
            raise

        identity = SourceIdentity(
            source_kind=self._settings.source_kind,
            source_id=self._settings.source_id,
            worker_epoch=self._epoch,
            run_id=driver_identity.run_id,
            generation=driver_identity.generation,
            contract_version=capabilities.contract_version,
        )

        if previous is not None and previous.source_fingerprint != identity.source_fingerprint:
            # The source we reconnected to is not the source we were reading.
            # Serving reads now would mix two datasets under one source_id, so
            # the session is marked for an explicit re-sync instead
            # (WF2-B-05, WF2-J-05).
            self._actor = actor
            self._identity = identity
            self._capabilities = capabilities
            self._transition(
                SessionState.READY, reason="reconnected", _force_from_connecting=True
            )
            self.mark_resync_required(
                "the source identity changed across the reconnect: "
                f"{previous.source_fingerprint} -> {identity.source_fingerprint}"
            )
            return

        old_actor = self._actor
        self._actor = actor
        self._identity = identity
        self._capabilities = capabilities
        self._transition(SessionState.READY, reason="handshake complete", _force_from_connecting=True)
        if old_actor is not None and old_actor is not actor:
            await self._retire(old_actor)

    def _map_capabilities(self, driver_identity: DriverIdentity) -> Capabilities:
        kind = self._settings.source_kind
        if kind == "simulator":
            return map_simulator_capabilities(
                driver_identity.raw_meta, declared_max_page_size=self._settings.max_page_size
            )
        if kind == "fake":
            return map_fake_capabilities(
                driver_identity.raw_meta,
                declared_max_page_size=self._settings.max_page_size,
                source_kind="fake",
            )
        raise IncompatibleSdkError(
            "no capability mapping exists for this source kind",
            source_id=self._settings.source_id,
            details={"source_kind": kind},
        )

    async def _retire(self, actor: SDKActor) -> None:
        exited = await actor.drain(timeout_s=self._settings.drain_timeout_ms / 1000.0)
        if not exited:
            actor.abandon()
            self._abandoned_actors.append(actor)
        self._metrics.set_gauge(self._metrics.abandoned_workers, self.abandoned_worker_count)

    # -- fencing ------------------------------------------------------------

    def accept_completion(self, completion: Completion, envelope: CallEnvelope) -> Completion:
        """Fence a completion before anything looks at its payload.

        Four independent checks, each closing a different way a stale result
        reaches a consumer: the wrong request, the wrong session, the wrong
        source generation, or the right everything but too late. All four are
        cheap, and the cost of missing one is a financial record attributed to
        a dataset it did not come from (INV-04, INV-05, INV-06, WF2-R-01).
        """
        if completion.request_id != envelope.request_id:
            self.stale_fenced_total += 1
            self._record_stale("request_id_mismatch")
            raise StaleCompletion(
                "a completion arrived for a different request",
                source_id=self._settings.source_id,
                operation=envelope.operation,
                request_id=envelope.request_id,
                worker_epoch=self._epoch,
            )
        if completion.worker_epoch != self._epoch:
            self.stale_fenced_total += 1
            self._record_stale("worker_epoch_changed")
            raise StaleCompletion(
                "a completion arrived from a session that has since been replaced",
                source_id=self._settings.source_id,
                operation=envelope.operation,
                request_id=envelope.request_id,
                worker_epoch=self._epoch,
                details={
                    "completion_worker_epoch": completion.worker_epoch,
                    "current_worker_epoch": self._epoch,
                },
            )
        current_generation = self.source_generation
        if (
            envelope.source_generation_at_admission is not None
            and current_generation is not None
            and envelope.source_generation_at_admission != current_generation
        ):
            self.stale_fenced_total += 1
            self._record_stale("source_generation_changed")
            raise SourceEpochChangedError(
                "the source generation changed while this call was in flight",
                source_id=self._settings.source_id,
                operation=envelope.operation,
                request_id=envelope.request_id,
                worker_epoch=self._epoch,
                details={
                    "generation_at_admission": envelope.source_generation_at_admission,
                    "current_generation": current_generation,
                },
            )
        return completion

    def _record_stale(self, reason: str) -> None:
        self._metrics.stale_completions_total.labels(
            sanitize_token_text(self._settings.source_id), reason
        ).inc()

    # -- state signals ------------------------------------------------------

    def _transition(
        self, target: SessionState, *, reason: str, _force_from_connecting: bool = False
    ) -> None:
        if _force_from_connecting and self._fsm.state not in (
            SessionState.CONNECTING,
            target,
        ):
            # Reaching READY is only legal from CONNECTING. If the state moved
            # underneath the handshake - a drain, say - the handshake does not
            # get to overrule it.
            logger.warning(
                "discarding a handshake result because the session state moved",
                extra={"state": self._fsm.state.value, "attempted": target.value},
            )
            return
        self._fsm.transition(
            target,
            reason=reason,
            at_monotonic=self._clock.monotonic(),
            worker_epoch=self._epoch,
        )
        self._metrics.set_gauge(
            self._metrics.readiness, 1.0 if self._fsm.serving else 0.0
        )

    def _disable(self, error: ConnectorError) -> None:
        self._disabled_error = error
        self._transition(SessionState.DISABLED, reason=f"permanent failure: {error.code}")
        logger.error(
            "source disabled pending configuration change",
            extra={
                "source_id": self._settings.source_id,
                "error_code": error.code,
                "worker_epoch": self._epoch,
            },
        )

    async def note_operation_failure(self, error: ConnectorError) -> None:
        """Let a failed *read* disable the source, not just a failed connect.

        A credential revoked mid-session fails every read with 401 while the
        state machine still says READY, so readiness lies and every caller
        retries. Disabling here closes that: the stored error is replayed to
        subsequent callers and the session is torn down, so nothing holds an
        authenticated handle that is no longer authenticated
        (WF2-B-09, WF2-B-12).
        """
        if not isinstance(error, _PERMANENT):
            return
        if self._fsm.state not in (SessionState.READY, SessionState.DEGRADED):
            return
        self._disable(error)
        old_actor, self._actor = self._actor, None
        if old_actor is not None:
            await self._retire(old_actor)

    def mark_degraded(self, reason: str) -> None:
        if self._fsm.state is SessionState.READY:
            self._transition(SessionState.DEGRADED, reason=reason)

    def mark_healthy(self, reason: str = "probe succeeded") -> None:
        if self._fsm.state is SessionState.DEGRADED:
            self._transition(SessionState.READY, reason=reason)

    def mark_resync_required(self, reason: str) -> None:
        """Signal that the consumer must abandon its scan.

        Reachable from ``READY`` and ``DEGRADED``. From anywhere else the
        session already has a more specific problem and overwriting it would
        lose that.
        """
        self._resync_reason = reason
        if self._fsm.state in (SessionState.READY, SessionState.DEGRADED):
            self._transition(SessionState.RESYNC_REQUIRED, reason=reason)
            self._metrics.snapshot_restart_total.labels(
                sanitize_token_text(self._settings.source_id), "source_epoch_changed"
            ).inc()

    async def acknowledge_resync(self) -> SourceIdentity:
        """The consumer has planned its re-sync; renegotiate identity.

        A fresh handshake rather than a flag flip: the point of
        ``RESYNC_REQUIRED`` is that the source may be a different dataset now,
        and only a handshake establishes which (WF2-J-01).
        """
        if self._fsm.state is not SessionState.RESYNC_REQUIRED:
            if self._identity is None:
                raise ConnectorClosedError(
                    "no session identity is established",
                    source_id=self._settings.source_id,
                )
            return self._identity
        # Dropped so the handshake cannot compare the new identity against the
        # stale one and declare a second identity change.
        self._identity = None
        old_actor, self._actor = self._actor, None
        self._transition(SessionState.DRAINING, reason="resync acknowledged; recycling session")
        if old_actor is not None:
            await self._retire(old_actor)
        self._transition(SessionState.STOPPED, reason="session recycled for resync")
        self._resync_reason = None
        await self.ensure_serving(
            deadline=Deadline.after(self._connect_budget_ms(), clock=self._clock),
            operation="acknowledge_resync",
        )
        assert self._identity is not None
        return self._identity

    def reconfigured(self) -> None:
        """Leave ``DISABLED`` because the credential or configuration changed."""
        self._disabled_error = None
        self._fsm.reconfigured(
            at_monotonic=self._clock.monotonic(), worker_epoch=self._epoch
        )
        if self._fsm.state is SessionState.CONNECTING:
            # Put the machine back in a state the connect sequence accepts.
            self._fsm.transition(
                SessionState.BACKOFF,
                reason="awaiting reconnect after reconfiguration",
                at_monotonic=self._clock.monotonic(),
                worker_epoch=self._epoch,
            )

    # -- watchdog -----------------------------------------------------------

    async def _watchdog(self) -> None:
        interval = max(0.01, self._settings.heartbeat_interval_ms / 1000.0)
        hard_limit_s = self._settings.hung_call_hard_limit_ms / 1000.0
        while not self._closing:
            await asyncio.sleep(interval)
            self._publish_gauges()
            actor = self._actor
            if actor is None or actor.active_calls == 0:
                continue
            if actor.active_age_s <= hard_limit_s:
                continue
            await self._quarantine(actor, age_s=actor.active_age_s)

    async def _quarantine(self, actor: SDKActor, *, age_s: float) -> None:
        if self._fsm.state in (SessionState.SUSPECT, SessionState.QUARANTINED):
            return
        source = sanitize_token_text(self._settings.source_id)
        self._transition(
            SessionState.SUSPECT, reason=f"driver call exceeded the hard limit ({age_s:.1f}s)"
        )
        self._transition(
            SessionState.QUARANTINED, reason="the active driver call cannot be stopped"
        )
        self.quarantine_total += 1
        self._metrics.quarantine_total.labels(source, "hung_driver_call").inc()
        logger.error(
            "session quarantined: a driver call has run past the hard limit",
            extra={
                "source_id": self._settings.source_id,
                "worker_epoch": self._epoch,
                "inflight_age_s": round(age_s, 3),
                "worker_mode": self._settings.worker_mode,
                "recoverable_without_operator": self._settings.process_isolation
                or self._settings.allow_thread_abandon,
            },
        )
        if self._settings.allow_thread_abandon or self._settings.process_isolation:
            await self.force_recycle(reason="hung driver call")

    async def force_recycle(self, *, reason: str) -> None:
        """Replace the session, abandoning the old one if it will not stop.

        In thread mode "abandoning" means exactly that: the thread keeps
        running and this process has leaked it. That is reported through
        :attr:`abandoned_worker_count` and readiness rather than described as
        a recovery (section 5.4, WF2-F-10).
        """
        actor = self._actor
        if actor is not None:
            # A process-backed driver can be stopped for real: killing the
            # child closes the pipe its worker thread is blocked on, so the
            # thread unblocks and the drain below succeeds. A thread-backed
            # driver has no such hook, and the drain will time out - which is
            # the honest difference between the two modes (ADR-204).
            stop = getattr(actor.driver, "emergency_stop", None)
            if callable(stop):
                try:
                    stop()
                except Exception:  # noqa: BLE001 - a failed kill must not mask the recycle
                    logger.exception(
                        "the worker could not be stopped out of band",
                        extra={"source_id": self._settings.source_id},
                    )
            exited = await actor.drain(timeout_s=self._settings.drain_timeout_ms / 1000.0)
            if not exited:
                actor.abandon()
                self._abandoned_actors.append(actor)
                logger.error(
                    "a worker could not be reclaimed and was abandoned; this process "
                    "is holding a thread it cannot stop",
                    extra={
                        "source_id": self._settings.source_id,
                        "worker_epoch": actor.epoch,
                        "reason": reason,
                    },
                )
        self._actor = None
        # The state must end somewhere the connect sequence will act on,
        # otherwise the supervisor is left holding a serving state with no
        # session: every caller then falls through to a reconnect that
        # declines to run because the machine still says READY.
        if self._fsm.state in (SessionState.SUSPECT, SessionState.QUARANTINED):
            self._transition(SessionState.BACKOFF, reason=f"session recycled: {reason}")
        elif self._fsm.state not in (SessionState.STOPPED, SessionState.DISABLED):
            self._transition(SessionState.DRAINING, reason=f"session recycled: {reason}")
            self._transition(SessionState.STOPPED, reason=f"session recycled: {reason}")
        self._metrics.set_gauge(self._metrics.abandoned_workers, self.abandoned_worker_count)
        self._metrics.reconnect_total.labels(
            sanitize_token_text(self._settings.source_id), "force_recycle"
        ).inc()

    def _publish_gauges(self) -> None:
        actor = self._actor
        self._metrics.set_gauge(
            self._metrics.sdk_active_calls, actor.active_calls if actor else 0
        )
        self._metrics.set_gauge(self._metrics.queue_depth, actor.queue_depth if actor else 0)
        self._metrics.set_gauge(
            self._metrics.inflight_oldest_age, actor.active_age_s if actor else 0.0
        )
        self._metrics.set_gauge(
            self._metrics.circuit_state,
            {BreakerState.CLOSED: 0.0, BreakerState.HALF_OPEN: 1.0, BreakerState.OPEN: 2.0}[
                self._breaker.state
            ],
        )
        self._metrics.set_gauge(self._metrics.readiness, 1.0 if self.ready()[0] else 0.0)
        self._metrics.set_gauge(self._metrics.abandoned_workers, self.abandoned_worker_count)

    # -- health -------------------------------------------------------------

    def live(self) -> bool:
        """Process liveness only.

        Never touches the driver. Calling upstream from a liveness probe makes
        an orchestrator restart the connector because the *source* is down,
        which is the wrong remediation and multiplies the outage (INV-13).
        """
        return not self._closing

    def ready(self) -> tuple[bool, list[str]]:
        """Whether this connector will serve a read, and why not if it will not."""
        reasons: list[str] = []
        if self._closing:
            reasons.append("draining")
        state = self._fsm.state
        if not self._fsm.serving:
            reasons.append(f"session_state_{state.value.lower()}")
        if self._actor is None:
            reasons.append("no_session")
        elif not self._actor.thread_alive:
            reasons.append("worker_not_running")
        if self._admission.closed:
            reasons.append("admission_closed")
        if self._breaker.state is BreakerState.OPEN:
            reasons.append("circuit_open")
        if self.abandoned_worker_count:
            # An unreclaimed worker is a real degradation: the process is
            # holding a thread and a session it cannot account for.
            reasons.append("abandoned_worker_present")
        if self._capabilities is None:
            reasons.append("capabilities_unknown")
        return (not reasons, reasons)

    def health(self) -> dict[str, Any]:
        ready, reasons = self.ready()
        actor = self._actor
        return {
            "live": self.live(),
            "ready": ready,
            "reasons": reasons,
            "source_id": self._settings.source_id,
            "source_kind": self._settings.source_kind,
            "state": self._fsm.state.value,
            "worker_mode": self._settings.worker_mode,
            "worker_epoch": self._epoch,
            "run_id": self._identity.run_id if self._identity else None,
            "generation": self._identity.generation if self._identity else None,
            "contract_version": self._capabilities.contract_version
            if self._capabilities
            else None,
            "breaker": self._breaker.snapshot(),
            "admission": self._admission.snapshot(),
            "actor": actor.snapshot() if actor else None,
            "abandoned_workers": self.abandoned_worker_count,
            "quarantine_total": self.quarantine_total,
            "stale_fenced_total": self.stale_fenced_total,
            "connect_leaders_total": self.connect_leaders_total,
            "connect_attempts_total": self.connect_attempts_total,
            "resync_reason": self._resync_reason,
            "disabled_error_code": self._disabled_error.code if self._disabled_error else None,
        }

    # -- shutdown -----------------------------------------------------------

    async def close(self) -> bool:
        """Drain: shut admission, finish or report in-flight work, stop.

        Returns whether everything was reclaimed. ``False`` means a call was
        still running when the drain timeout expired, which is reported rather
        than waited out indefinitely (WF2-O-02, INV-14).
        """
        if self._closing:
            return True
        self._closing = True
        self._admission.close()
        if self._fsm.state not in (SessionState.DRAINING, SessionState.STOPPED):
            try:
                self._fsm.transition(
                    SessionState.DRAINING,
                    reason="shutdown requested",
                    at_monotonic=self._clock.monotonic(),
                    worker_epoch=self._epoch,
                )
            except ConnectorError:  # pragma: no cover - every state can drain
                pass

        watchdog, self._watchdog_task = self._watchdog_task, None
        if watchdog is not None:
            watchdog.cancel()
            try:
                await watchdog
            except asyncio.CancelledError:
                pass

        connect, self._connect_task = self._connect_task, None
        if connect is not None and not connect.done():
            connect.cancel()
            try:
                await connect
            except (asyncio.CancelledError, ConnectorError):
                pass

        clean = True
        actor = self._actor
        if actor is not None:
            clean = await actor.drain(timeout_s=self._settings.drain_timeout_ms / 1000.0)
            if not clean:
                actor.abandon()
                self._abandoned_actors.append(actor)
        self._actor = None
        if self._fsm.state is SessionState.DRAINING:
            self._fsm.transition(
                SessionState.STOPPED,
                reason="drain complete" if clean else "drain timed out with work in flight",
                at_monotonic=self._clock.monotonic(),
                worker_epoch=self._epoch,
            )
        self._metrics.set_gauge(self._metrics.readiness, 0.0)
        return clean and self.abandoned_worker_count == 0

"""One publish cycle: admit, claim, enqueue, poll, then write the ACK back.

The ordering of those five steps is the design, and each boundary is a place
where a shortcut would break something specific.

**Admit before claiming.** A claimed row is leased and its attempt counter has
already been incremented, so claiming first and discovering the buffer is full
afterwards punishes an event for our resource state.

**The DB transaction ends before the produce begins.** Design section 7.2 point
4: a thirty-second broker timeout must not hold a PostgreSQL row lock for
thirty seconds.

**``PUBLISHED`` requires a delivery callback *and* a successful CAS.** Not an
enqueue, not a poll returning, not a lack of error. This is INV07-06, and it is
the invariant the mutation suite attacks hardest.

**A failed CAS is not a delivery failure.** If the broker acknowledged the
record and the row has since been taken over by a standby, the event *is* in
Kafka. Rescheduling it as undelivered would be a lie in the metrics and would
hand the row back for another publish; the correct action is to count a stale
ACK, leave the row to its new owner, and let the consumer's dedup absorb the
copy that is already out there. Design section 7.3 says this in a comment that
is worth more than the code around it: "Do NOT reclassify as not-delivered."
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field

from ..clock import Clock, RealClock
from ..errors import (
    BrokerError,
    Classification,
    Disposition,
    ErrorCode,
    ProducerQueueFull,
    classify,
)
from ..events.routing import authorize_source
from ..faults.hooks import HOOKS, HookRegistry, KillPoint
from ..observability.logging import get_logger
from ..observability.metrics import Metrics
from ..persistence.ports import AttemptOutcome, Claim
from ..settings import Settings
from .backoff import CircuitBreaker, compute_delay
from .limits import Admission, InFlightLimits, StoragePolicy
from ..broker.ports import DeliveryResult, DeliveryState, Producer

_log = get_logger("relay.publisher")


@dataclass(slots=True)
class CycleReport:
    """What one cycle did. Returned so a caller can drive the loop and assert."""

    admission: Admission
    claimed: int = 0
    acked: int = 0
    stale_acks: int = 0
    ambiguous: int = 0
    rescheduled: int = 0
    quarantined: int = 0
    released: int = 0
    callbacks_served: int = 0
    errors: list[ErrorCode] = field(default_factory=list)

    @property
    def made_progress(self) -> bool:
        return bool(self.acked or self.quarantined or self.stale_acks)


class Publisher:
    """Moves committed outbox rows to the broker, once each if it can."""

    def __init__(
        self,
        store: object,
        producer: Producer,
        *,
        settings: Settings,
        clock: Clock | None = None,
        metrics: Metrics | None = None,
        hooks: HookRegistry | None = None,
        rng: random.Random | None = None,
        owner: str | None = None,
    ) -> None:
        self._store = store
        self._producer = producer
        self._settings = settings
        self._clock = clock or RealClock()
        self._metrics = metrics or Metrics.build()
        self._hooks = hooks or HOOKS
        self._rng = rng or random.Random(20261030)
        self._owner = owner or settings.relay_owner
        self._limits = InFlightLimits(
            max_inflight=settings.max_inflight,
            max_buffer_bytes=settings.producer_buffer_max_bytes,
            max_event_bytes=settings.max_event_wire_bytes,
        )
        self._circuit = CircuitBreaker(
            failure_threshold=settings.circuit_failure_threshold,
            cooldown_seconds=settings.circuit_cooldown_seconds,
        )
        self._storage = StoragePolicy(
            budget_bytes=settings.storage_budget_bytes,
            warn_ratio=settings.storage_warn_ratio,
            throttle_ratio=settings.storage_throttle_ratio,
            fail_closed_ratio=settings.storage_fail_closed_ratio,
        )
        self._draining = False
        self._storage_bytes = 0
        self._storage_checked_at: float | None = None
        self._storage_check_interval = 5.0
        # Delivery callbacks append here while the producer is polled, and the
        # cycle drains it afterwards. Doing the database write inside the
        # callback would put it on librdkafka's poll thread in the real adapter.
        self._pending_results: list[tuple[Claim, DeliveryResult]] = []

    # --- introspection ------------------------------------------------------
    @property
    def owner(self) -> str:
        return self._owner

    @property
    def limits(self) -> InFlightLimits:
        return self._limits

    @property
    def circuit(self) -> CircuitBreaker:
        return self._circuit

    @property
    def metrics(self) -> Metrics:
        return self._metrics

    def begin_drain(self) -> None:
        """Stop claiming new rows; keep serving callbacks."""
        self._draining = True

    # --- the cycle ----------------------------------------------------------
    def publish_cycle(self) -> CycleReport:
        self._hooks.fire(KillPoint.RELAY_BEFORE_CLAIM, owner=self._owner)
        admission = self._admission()
        report = CycleReport(admission=admission)

        if admission is Admission.OK and not self._draining:
            headroom = min(self._settings.claim_batch_size, self._limits.headroom())
            if headroom > 0:
                claims = self._store.claim_due(  # type: ignore[attr-defined]
                    owner=self._owner,
                    batch_size=headroom,
                    lease_seconds=self._settings.lease_seconds,
                    now=self._clock.now(),
                )
                report.claimed = len(claims)
                for claim in claims:
                    self._enqueue(claim, report)
        elif admission is not Admission.OK:
            self._metrics.relay_backpressure_total.inc()

        # Callbacks only run while the producer is polled. Everything after
        # this line depends on it having been called.
        report.callbacks_served = self._producer.poll(0.0)
        settled, self._pending_results = self._pending_results, []
        for claim, result in settled:
            self._settle(claim, result, report)

        self._publish_gauges()
        return report

    def drain(self, *, max_cycles: int = 1000) -> CycleReport:
        """Serve outstanding callbacks without claiming anything new (KP07-17).

        Claims that remain unresolved when the deadline passes are *released*,
        not marked published. A relay that guessed on the way out would record
        an ACK it never received.
        """
        self.begin_drain()
        total = CycleReport(admission=Admission.OK)
        for _ in range(max_cycles):
            self._hooks.fire(KillPoint.RELAY_DURING_GRACEFUL_FLUSH, owner=self._owner)
            report = self.publish_cycle()
            total.acked += report.acked
            total.stale_acks += report.stale_acks
            total.ambiguous += report.ambiguous
            total.rescheduled += report.rescheduled
            total.quarantined += report.quarantined
            total.callbacks_served += report.callbacks_served
            total.errors.extend(report.errors)
            if self._producer.in_flight() == 0:
                break
        return total

    # --- internals ----------------------------------------------------------
    def _admission(self) -> Admission:
        if self._circuit.is_open(monotonic=self._clock.monotonic()):
            return Admission.CIRCUIT_OPEN
        storage_bytes = self._storage_bytes_throttled()
        if not self._storage.accepts_new_business_writes(storage_bytes):
            # Publishing continues regardless -- draining the backlog is how
            # storage pressure is relieved. What this signals is that *new
            # business imports* must stop; the relay keeps claiming.
            _log.warning(
                "outbox storage at fail-closed threshold; new business imports must stop",
                storage_bytes=storage_bytes,
                budget_bytes=self._storage.budget_bytes,
            )
        return self._limits.admit()

    def _storage_bytes_throttled(self) -> int:
        """Read the storage figure at most once per interval.

        Storage grows at the speed of a backlog, not of a poll loop, so reading
        it every cycle costs a ``PRAGMA page_count`` pair (or a
        ``pg_total_relation_size``) for a number that cannot have moved
        meaningfully. The backlog *counts* are still read every cycle -- those
        do change per cycle, and an index on ``state`` makes them cheap.
        """
        now = self._clock.monotonic()
        if (
            self._storage_checked_at is None
            or now - self._storage_checked_at >= self._storage_check_interval
        ):
            self._storage_bytes = int(self._store.storage_bytes())  # type: ignore[attr-defined]
            self._storage_checked_at = now
        return self._storage_bytes

    def _enqueue(self, claim: Claim, report: CycleReport) -> None:
        self._hooks.fire(
            KillPoint.RELAY_AFTER_CLAIM_BEFORE_PRODUCE,
            event_id=str(claim.event_id),
            owner=self._owner,
        )
        # Re-check tenancy at publish time. The row was authorized when it was
        # written, but the allowlist may have changed since, and routing a
        # retired tenant's events is not something to discover downstream.
        try:
            authorize_source(claim.source_id, self._settings.source_allowlist)
        except Exception as exc:  # noqa: BLE001 - classified, then parked
            self._handle_failure(claim, classify(exc), ErrorCode.TENANT_FORBIDDEN, str(exc), report)
            return

        self._limits.acquire(aggregate_key=claim.stream_key, size_bytes=claim.wire_size)
        try:
            self._producer.produce(
                topic=claim.topic,
                key=claim.partition_key.encode("utf-8"),
                value=claim.wire_bytes,
                headers={
                    "event_id": str(claim.event_id),
                    "event_type": claim.event_type,
                    "schema_version": str(claim.schema_version),
                    "source_id": claim.source_id,
                },
                on_delivery=lambda result, claim=claim: self._pending_results.append((claim, result)),
            )
        except ProducerQueueFull as exc:
            # Nothing was sent. Give the row straight back so it stays visible
            # as PENDING rather than sitting leased behind a full buffer.
            self._limits.release(aggregate_key=claim.stream_key, size_bytes=claim.wire_size)
            self._store.release_if_owner(claim=claim, now=self._clock.now())  # type: ignore[attr-defined]
            report.released += 1
            report.errors.append(exc.code)
            self._metrics.record_producer_error(exc.code)
            self._metrics.relay_backpressure_total.inc()
            return
        except BrokerError as exc:
            self._limits.release(aggregate_key=claim.stream_key, size_bytes=claim.wire_size)
            self._handle_failure(claim, classify(exc), exc.code, str(exc), report)
            return
        self._hooks.fire(
            KillPoint.RELAY_AFTER_ENQUEUE_BEFORE_CALLBACK, event_id=str(claim.event_id)
        )

    def _settle(self, claim: Claim, result: DeliveryResult, report: CycleReport) -> None:
        self._limits.release(aggregate_key=claim.stream_key, size_bytes=claim.wire_size)

        if result.state is DeliveryState.ACKED:
            self._circuit.record_success()
            self._hooks.fire(
                KillPoint.RELAY_AFTER_ACK_BEFORE_CAS,
                event_id=str(claim.event_id),
                partition=result.partition,
                offset=result.offset,
            )
            now = self._clock.now()
            assert result.partition is not None and result.offset is not None
            changed = self._store.mark_published_if_owner(  # type: ignore[attr-defined]
                claim=claim,
                kafka_partition=result.partition,
                kafka_offset=result.offset,
                now=now,
            )
            self._hooks.fire(
                KillPoint.RELAY_AFTER_CAS_BEFORE_OBSERVE,
                event_id=str(claim.event_id),
                changed=changed,
            )
            if changed:
                report.acked += 1
                self._metrics.record_attempt(AttemptOutcome.ACK)
                self._metrics.observe_ack_latency((now - claim.created_at).total_seconds())
                self._record_attempt(claim, AttemptOutcome.ACK, None)
            else:
                # The broker holds the record; we simply no longer own the row.
                report.stale_acks += 1
                self._metrics.record_attempt(AttemptOutcome.STALE_ACK)
                self._record_attempt(claim, AttemptOutcome.STALE_ACK, ErrorCode.INTEGRITY_CONFLICT)
                _log.warning(
                    "broker acknowledged a record whose claim we no longer hold; "
                    "the event is in Kafka and consumer dedup will absorb the copy",
                    event_id=str(claim.event_id),
                    claim_version=claim.claim_version,
                )
            return

        if result.state is DeliveryState.AMBIGUOUS:
            # The broker may hold it. Retrying the same event_id is correct and
            # a duplicate is the accepted cost (design section 3.3).
            report.ambiguous += 1
            self._metrics.outbox_ack_ambiguous_total.inc()
            code = result.error_code or ErrorCode.BROKER_ACK_AMBIGUOUS
            self._handle_failure(claim, Classification(code, Disposition.RETRY, True), code, result.detail, report)
            return

        code = result.error_code or ErrorCode.UNKNOWN
        self._handle_failure(claim, Classification(code, _disposition(code), True), code, result.detail, report)

    def _handle_failure(
        self,
        claim: Claim,
        classification: Classification,
        code: ErrorCode,
        detail: str,
        report: CycleReport,
    ) -> None:
        report.errors.append(code)
        self._metrics.record_producer_error(code)
        now = self._clock.now()

        if classification.disposition is Disposition.QUARANTINE:
            self._store.quarantine_if_owner(  # type: ignore[attr-defined]
                claim=claim, reason=detail or str(code), error_code=str(code), now=now
            )
            report.quarantined += 1
            self._metrics.record_attempt(AttemptOutcome.QUARANTINE)
            self._record_attempt(claim, AttemptOutcome.QUARANTINE, code)
            return

        if classification.disposition is Disposition.CIRCUIT_OPEN:
            if self._circuit.record_failure(monotonic=self._clock.monotonic()):
                self._metrics.relay_circuit_open_total.inc()
                _log.error("publish circuit opened", code=str(code), owner=self._owner)

        if claim.attempt_count >= self._settings.max_attempts:
            # Bounded attempts, then a human. Retrying forever is how an outage
            # becomes a storm; dropping the event is never the alternative.
            self._store.quarantine_if_owner(  # type: ignore[attr-defined]
                claim=claim,
                reason=f"exhausted {claim.attempt_count} attempts; last error {code}",
                error_code=str(code),
                now=now,
            )
            report.quarantined += 1
            self._metrics.record_attempt(AttemptOutcome.QUARANTINE)
            self._record_attempt(claim, AttemptOutcome.QUARANTINE, code)
            return

        delay = compute_delay(
            attempt=claim.attempt_count,
            base_seconds=self._settings.backoff_base_seconds,
            max_seconds=self._settings.backoff_max_seconds,
            jitter_ratio=self._settings.backoff_jitter_ratio,
            rng=self._rng,
        )
        self._store.reschedule_if_owner(  # type: ignore[attr-defined]
            claim=claim, delay_seconds=delay, error_code=str(code), now=now
        )
        report.rescheduled += 1
        self._metrics.record_attempt(AttemptOutcome.RETRY)
        self._record_attempt(claim, AttemptOutcome.RETRY, code)

    def _record_attempt(self, claim: Claim, outcome: AttemptOutcome, code: ErrorCode | None) -> None:
        self._store.record_attempt(  # type: ignore[attr-defined]
            claim=claim,
            outcome=outcome,
            error_code=str(code) if code else None,
            now=self._clock.now(),
        )

    def _publish_gauges(self) -> None:
        # Counts every cycle (they change every cycle, and the state index makes
        # them cheap); storage on the throttled cadence.
        stats = self._store.stats(now=self._clock.now(), include_storage=False)  # type: ignore[attr-defined]
        self._metrics.outbox_pending_total.set(stats.pending_total)
        self._metrics.outbox_claimed_total.set(stats.claimed_total)
        self._metrics.outbox_quarantined_total.set(stats.quarantined_total)
        self._metrics.outbox_oldest_pending_age_seconds.set(stats.oldest_pending_age_seconds)
        self._metrics.pg_outbox_storage_bytes.set(self._storage_bytes_throttled())
        self._metrics.producer_buffer_used_bytes.set(self._producer.buffer_used_bytes())
        self._metrics.producer_inflight_records.set(self._limits.inflight)


def _disposition(code: ErrorCode) -> Disposition:
    from ..errors import DISPOSITIONS

    return DISPOSITIONS.get(code, Disposition.QUARANTINE)

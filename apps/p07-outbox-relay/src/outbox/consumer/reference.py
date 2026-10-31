"""The reference idempotent consumer.

This is the sink design section 8.1 specifies, and its whole value is in the
order of operations:

1. validate everything *before* any side effect,
2. open the consumer's own transaction,
3. claim the ``event_id`` in the inbox,
4. check the aggregate version under that same transaction,
5. apply the projection,
6. COMMIT the database,
7. and only then commit the Kafka offset.

Step 7 after step 6 is the difference between a duplicate and a loss. Crash
between them and the record is redelivered, the inbox recognises it, nothing is
applied twice. Commit the offset first and a crash loses the record for good --
which is why that ordering is a P0 test rather than a style preference (design
section 8.2).

**Offsets advance contiguously, per partition.** A record that cannot be applied
-- poison payload, unknown schema, a version gap, an unauthorised source -- stops
its partition where it is. Committing past it would be the "silent skip" the
design forbids, and committing *around* it is not available: an offset is a
watermark, not a set. So a blocked partition stays blocked and visible in the
lag metric until an operator acts, and unrelated partitions keep moving.

**A version gap pauses; it does not buffer indefinitely.** If version ``n+2``
arrives while ``n+1`` has not, the transaction rolls back entirely (no inbox
row, no projection change) and the aggregate is marked blocked. Buffering out-of
-order events in memory up to some bound is the alternative the design allows,
but it trades a visible stop for an invisible one, and the visible stop is what
an operator can act on.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from datetime import datetime

from ..broker.ports import Consumer, Record
from ..clock import Clock, RealClock
from ..errors import ErrorCode, IntegrityIncident, OutboxError, ValidationError, classify
from ..events.envelope import Envelope
from ..events.keys import EventType
from ..events.routing import CONSUMABLE_TOPICS
from ..faults.hooks import HOOKS, HookRegistry, KillPoint
from ..observability.logging import get_logger
from ..observability.metrics import Metrics
from ..settings import Settings
from .inbox import ConsumerStore, ProcessedOutcome

_log = get_logger("consumer.reference")


#: Per-record outcomes. The first three allow the partition's offset to
#: advance; the last three stop it where it is.
APPLIED = "APPLIED"
DUPLICATE = "DUPLICATE"
STALE_VERSION = "STALE_VERSION"
GAP_BLOCKED = "GAP_BLOCKED"
QUARANTINED = "QUARANTINED"
ALREADY_BLOCKED = "ALREADY_BLOCKED"


@dataclass(slots=True)
class ProcessReport:
    polled: int = 0
    applied: int = 0
    duplicates: int = 0
    stale_versions: int = 0
    gaps: int = 0
    quarantined: int = 0
    halted_partitions: set[tuple[str, int]] = field(default_factory=set)
    committed: dict[tuple[str, int], int] = field(default_factory=dict)
    outcomes: list[tuple[str, str]] = field(default_factory=list)

    @property
    def progressed(self) -> bool:
        return bool(self.applied or self.duplicates or self.stale_versions)


class ReferenceConsumer:
    """Applies broker events to a PostgreSQL-shaped projection, once each."""

    def __init__(
        self,
        consumer: Consumer,
        store: ConsumerStore,
        *,
        settings: Settings,
        clock: Clock | None = None,
        metrics: Metrics | None = None,
        hooks: HookRegistry | None = None,
        allowed_sources: tuple[str, ...] | None = None,
    ) -> None:
        self._consumer = consumer
        self._store = store
        self._settings = settings
        self._clock = clock or RealClock()
        self._metrics = metrics or Metrics.build()
        self._hooks = hooks or HOOKS
        # Defaults to the publisher's allowlist, but is a separate argument so
        # a canary consumer can be scoped to one tenant (IT07-14).
        self._allowed_sources = allowed_sources or settings.source_allowlist
        self._name = settings.consumer_name

    @property
    def name(self) -> str:
        return self._name

    @property
    def store(self) -> ConsumerStore:
        return self._store

    @property
    def metrics(self) -> Metrics:
        return self._metrics

    def subscribe(self, topics: tuple[str, ...] | None = None) -> None:
        chosen = topics or CONSUMABLE_TOPICS
        forbidden = [topic for topic in chosen if topic not in CONSUMABLE_TOPICS]
        if forbidden:
            # The poison topic carries sanitised quarantine metadata. A sink
            # that consumed it would be re-applying the thing that was parked.
            raise ValidationError(f"topics {forbidden} are not consumable by a reference sink")
        self._consumer.subscribe(list(chosen))

    # --- the loop -----------------------------------------------------------
    def process_once(self, *, max_records: int | None = None) -> ProcessReport:
        """Poll once, process in arrival order, then commit what is contiguous."""
        limit = max_records or self._settings.consumer_max_poll_records
        records = self._consumer.poll(max_records=limit)
        report = ProcessReport(polled=len(records))
        self._hooks.fire(KillPoint.CONSUMER_AFTER_POLL_BEFORE_BEGIN, count=len(records))

        # Highest offset per partition that is safe to commit, plus the set of
        # partitions where advancement stopped.
        ready: dict[tuple[str, int], int] = {}
        for record in records:
            tp = record.topic_partition
            if tp in report.halted_partitions:
                # Earlier record in this partition could not be applied. Do not
                # look at later ones: they are behind the blockage.
                continue
            outcome = self._handle(record, report)
            report.outcomes.append((f"{record.topic}/{record.partition}@{record.offset}", outcome))
            if outcome in (APPLIED, DUPLICATE, STALE_VERSION):
                ready[tp] = record.offset + 1
            else:
                report.halted_partitions.add(tp)

        self._hooks.fire(KillPoint.CONSUMER_AFTER_DB_COMMIT_BEFORE_OFFSET, offsets=len(ready))
        if ready:
            self._consumer.commit(ready)
            report.committed = dict(ready)
        self._hooks.fire(KillPoint.CONSUMER_AFTER_OFFSET_COMMIT, offsets=len(ready))
        self._metrics.consumer_applied_total.inc(report.applied)
        return report

    def run_until_idle(self, *, max_rounds: int = 1000) -> ProcessReport:
        """Poll until a round yields nothing. Aggregates the reports."""
        total = ProcessReport()
        for _ in range(max_rounds):
            report = self.process_once()
            total.polled += report.polled
            total.applied += report.applied
            total.duplicates += report.duplicates
            total.stale_versions += report.stale_versions
            total.gaps += report.gaps
            total.quarantined += report.quarantined
            total.halted_partitions |= report.halted_partitions
            total.committed.update(report.committed)
            total.outcomes.extend(report.outcomes)
            if report.polled == 0:
                return total
        raise RuntimeError(f"consumer did not settle within {max_rounds} rounds")

    # --- per record ---------------------------------------------------------
    def _handle(self, record: Record, report: ProcessReport) -> str:
        now = self._clock.now()

        # --- validation, before any side effect ---------------------------
        if record.topic not in CONSUMABLE_TOPICS:
            return self._quarantine(
                record, ErrorCode.SCHEMA_UNSUPPORTED, f"topic {record.topic!r} is not consumable", report
            )
        try:
            envelope = Envelope.parse_wire(
                record.value, supported_schema_versions=self._settings.supported_schema_versions
            )
        except OutboxError as exc:
            # Covers unsupported schema version, hash mismatch, malformed
            # envelope and money-policy violations. All park the record. When
            # the failure carried identity context -- a hash mismatch does --
            # the affected aggregate is blocked by name, so the blast radius is
            # one account rather than every account on this partition.
            return self._quarantine(
                record, classify(exc).code, str(exc), report, identity=exc.context or None
            )

        if envelope.source_id not in self._allowed_sources:
            # Tenant isolation: a record from a source this sink is not scoped
            # to must never reach the projection, even on a shared topic.
            return self._quarantine(
                record,
                ErrorCode.TENANT_FORBIDDEN,
                f"source {envelope.source_id!r} is outside this consumer's scope",
                report,
                envelope=envelope,
            )

        header_event_id = record.headers.get("event_id")
        if header_event_id is not None and header_event_id != str(envelope.event_id):
            return self._quarantine(
                record,
                ErrorCode.INTEGRITY_CONFLICT,
                "record header event_id disagrees with the envelope",
                report,
                envelope=envelope,
            )

        # --- side effects, in one transaction ------------------------------
        try:
            return self._apply(record, envelope, report, now)
        except IntegrityIncident as exc:
            return self._quarantine(
                record, ErrorCode.INTEGRITY_CONFLICT, str(exc), report, envelope=envelope, block=True
            )

    def _apply(self, record: Record, envelope: Envelope, report: ProcessReport, now: datetime) -> str:
        """Run the inbox/version/projection transaction and classify the result.

        The commit/rollback split is the thing to get right, and it is not the
        same for every non-applied outcome:

        * a *duplicate* and a *stale version* both COMMIT. The inbox row is the
          record that this ``event_id`` has been seen, and keeping it is what
          makes the next redelivery cheap and the oracle's "distinct processed
          ids" count meaningful.
        * a *version gap* ROLLS BACK everything. No inbox row, no projection
          change -- because the event has not been processed, it has been
          deferred, and an inbox row would later make it look applied.
        """
        store = self._store
        gap_expected: int | None = None
        blocked_reason: str | None = None
        result = APPLIED

        try:
            with store.transaction() as conn:
                state = store.version_state(
                    conn,
                    consumer_name=self._name,
                    source_id=envelope.source_id,
                    epoch_id=envelope.epoch_id,
                    aggregate_key=envelope.aggregate_key,
                )
                if state.blocked:
                    blocked_reason = state.blocked_reason
                    raise _Deferred

                outcome = store.insert_processed(
                    conn,
                    consumer_name=self._name,
                    event_id=envelope.event_id,
                    payload_sha256=envelope.payload_sha256,
                    source_id=envelope.source_id,
                    epoch_id=envelope.epoch_id,
                    aggregate_key=envelope.aggregate_key,
                    aggregate_version=envelope.aggregate_version,
                    topic=record.topic,
                    partition=record.partition,
                    offset=record.offset,
                    now=now,
                )
                self._hooks.fire(
                    KillPoint.CONSUMER_AFTER_INBOX_INSERT,
                    event_id=str(envelope.event_id),
                    outcome=str(outcome),
                )

                if outcome is ProcessedOutcome.DUPLICATE_DIFFERENT_HASH:
                    raise IntegrityIncident(
                        f"event {envelope.event_id} was already applied with a different payload "
                        "hash; two records claim to be the same event and disagree about its "
                        "content"
                    )

                expected = state.last_applied_version + 1
                if outcome is ProcessedOutcome.DUPLICATE_SAME_HASH:
                    # A redelivery. The projection already reflects it.
                    result = DUPLICATE
                elif envelope.aggregate_version > expected:
                    gap_expected = expected
                    raise _Deferred
                elif envelope.aggregate_version < expected:
                    # Older than what we hold, but a new event_id: a replayed
                    # record from before a rebootstrap. Remember that we saw
                    # it; do not re-apply its money.
                    result = STALE_VERSION
                else:
                    self._project(conn, envelope, now)
                    self._hooks.fire(
                        KillPoint.CONSUMER_AFTER_PROJECTION_BEFORE_COMMIT,
                        event_id=str(envelope.event_id),
                    )
                    store.advance_version(
                        conn,
                        consumer_name=self._name,
                        source_id=envelope.source_id,
                        epoch_id=envelope.epoch_id,
                        aggregate_key=envelope.aggregate_key,
                        version=envelope.aggregate_version,
                    )
        except _Deferred:
            if blocked_reason is not None:
                return ALREADY_BLOCKED
            assert gap_expected is not None
            report.gaps += 1
            self._metrics.consumer_version_gap_total.inc()
            reason = f"version gap: expected {gap_expected}, received {envelope.aggregate_version}"
            with store.transaction() as conn:
                store.block_aggregate(
                    conn,
                    consumer_name=self._name,
                    source_id=envelope.source_id,
                    epoch_id=envelope.epoch_id,
                    aggregate_key=envelope.aggregate_key,
                    reason=reason,
                )
            _log.warning(
                "aggregate paused on a version gap",
                event_id=str(envelope.event_id),
                aggregate_key=envelope.aggregate_key,
                expected=gap_expected,
                received=envelope.aggregate_version,
            )
            return GAP_BLOCKED

        if result is DUPLICATE:
            report.duplicates += 1
            self._metrics.consumer_duplicates_total.inc()
        elif result is STALE_VERSION:
            report.stale_versions += 1
        else:
            report.applied += 1
        return result

    def _project(self, conn: sqlite3.Connection, envelope: Envelope, now: datetime) -> None:
        """Dispatch by event type. Markers are notifications, not data."""
        match envelope.event_type:
            case EventType.DEAL_RECORDED:
                self._store.apply_deal(
                    conn,
                    consumer_name=self._name,
                    source_id=envelope.source_id,
                    epoch_id=envelope.epoch_id,
                    event_id=envelope.event_id,
                    payload=envelope.payload,
                    now=now,
                )
            case EventType.ACCOUNT_SNAPSHOT_PUBLISHED | EventType.POSITION_SNAPSHOT_PUBLISHED:
                # ADR07-06: record that a refresh is due. Do not pretend the
                # marker contained the accounts or the positions.
                self._store.record_snapshot_notice(
                    conn,
                    consumer_name=self._name,
                    source_id=envelope.source_id,
                    epoch_id=envelope.epoch_id,
                    payload=envelope.payload,
                    now=now,
                )
            case EventType.SOURCE_EPOCH_CHANGED | EventType.RECONCILIATION_VERIFIED:
                # Lifecycle notifications: the version ledger advancing is the
                # whole effect. Nothing financial follows from them here.
                pass

    def _quarantine(
        self,
        record: Record,
        code: ErrorCode,
        reason: str,
        report: ProcessReport,
        *,
        envelope: Envelope | None = None,
        identity: dict[str, object] | None = None,
        block: bool = True,
    ) -> str:
        """Park the record, and block its aggregate when we know which one it is.

        Identity comes either from a fully parsed ``envelope`` or from the
        ``context`` an integrity failure carried. Both are accepted because a
        record can be unusable and still be identifiable, and in that case the
        right response is narrower than halting the partition.
        """
        known = dict(identity or {})
        if envelope is not None:
            known = {
                "event_id": str(envelope.event_id),
                "source_id": envelope.source_id,
                "epoch_id": envelope.epoch_id,
                "aggregate_key": envelope.aggregate_key,
                "aggregate_version": envelope.aggregate_version,
                "payload_sha256": envelope.payload_sha256,
            }

        self._store.quarantine(
            consumer_name=self._name,
            topic=record.topic,
            partition=record.partition,
            offset=record.offset,
            error_code=str(code),
            reason=reason,
            now=self._clock.now(),
            event_id=_as_text(known.get("event_id")),
            source_id=_as_text(known.get("source_id")),
            epoch_id=_as_text(known.get("epoch_id")),
            aggregate_key=_as_text(known.get("aggregate_key")),
            aggregate_version=_as_int(known.get("aggregate_version")),
            payload_sha256=_as_text(known.get("payload_sha256")),
        )
        blockable = all(
            isinstance(known.get(field), str)
            for field in ("source_id", "epoch_id", "aggregate_key")
        )
        if block and blockable:
            with self._store.transaction() as conn:
                self._store.block_aggregate(
                    conn,
                    consumer_name=self._name,
                    source_id=str(known["source_id"]),
                    epoch_id=str(known["epoch_id"]),
                    aggregate_key=str(known["aggregate_key"]),
                    reason=f"{code}: {reason[:200]}",
                )
        report.quarantined += 1
        self._metrics.record_consumer_quarantine(code)
        _log.error(
            "record quarantined; partition halted at this offset",
            topic=record.topic,
            partition=record.partition,
            offset=record.offset,
            code=str(code),
            event_id=_as_text(known.get("event_id")),
            aggregate_blocked=blockable,
        )
        return QUARANTINED

    def handle_revocation(self, topic_partitions: list[tuple[str, int]]) -> None:
        """Give up partitions on a rebalance (KP07-23).

        Nothing to flush: every applied record was committed to the database
        before its offset was committed, so there is no in-memory state whose
        loss could matter. What must *not* happen is a commit of offsets for
        records this member never finished, and the contiguous-commit rule in
        :meth:`process_once` already guarantees that.
        """
        self._hooks.fire(
            KillPoint.CONSUMER_DURING_REVOCATION, partitions=len(topic_partitions)
        )
        revoke = getattr(self._consumer, "revoke", None)
        if callable(revoke):
            revoke(topic_partitions)

    def lag(self, broker: object, topic: str) -> int:
        consumer_lag = getattr(broker, "consumer_lag", None)
        if not callable(consumer_lag):
            return 0
        group = getattr(self._consumer, "group", self._settings.consumer_group)
        value = int(consumer_lag(group, topic))
        self._metrics.kafka_consumer_lag_records.labels(group=group, topic=topic).set(value)
        return value


class _Deferred(Exception):
    """Roll the transaction back: this event is not processed, only postponed."""


def _as_text(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _as_int(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None

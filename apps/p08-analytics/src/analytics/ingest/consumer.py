"""The ingest worker: poll, validate, quarantine, insert, *then* commit.

The order above is the entire safety argument, and three details make it hold:

**Contiguity.** A commit names the highest offset such that every offset below
it in that partition has been either durably inserted or durably quarantined.
Offset 55 succeeding does not license skipping an unresolved offset 54
(section 6.1 step 5, P08-E-05).

**Per-partition independence.** Pausing partition 0 on poison must not stop
partitions 1-5 (P08-F-04), so progress and pausing are tracked per partition
and a stall in one does not block the others.

**Ownership re-checked at the commit.** The assignment can change while the
insert is in flight. The epoch is captured at poll time and re-asserted
immediately before the commit, because a worker that lost its partitions must
not advance anyone's offsets (INV08-11, P08-F-03).

On an ambiguous insert the worker commits nothing and lets Kafka replay. Raw
observations may then duplicate, which is expected and harmless: dedup happens
at business identity in the build, not here (ADR08-09).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Sequence

from ..clock import Clock
from ..errors import (
    AmbiguousWriteError,
    AnalyticsError,
    FencedError,
    PoisonError,
    Reason,
    RetriableError,
)
from ..faults.killpoints import FaultPlan, KillPoint, NullFaultPlan
from ..observability.logging import StructuredLogger, structured_logger
from ..observability.metrics import Metrics
from ..ports.clickhouse import ClickHousePort
from ..ports.control import ControlPlanePort
from ..ports.kafka import ConsumerPort, ConsumerRecord, PartitionKey
from ..settings import Settings
from .quarantine import QuarantineLedger
from .validator import ValidatedBatch, ValidationFailure, validate_records


@dataclass(slots=True)
class PartitionProgress:
    """What this worker proved about one partition in one batch."""

    partition: PartitionKey
    #: Offsets whose rows are durably in ClickHouse.
    inserted: set[int] = field(default_factory=set)
    #: Offsets with a durable quarantine receipt.
    quarantined: set[int] = field(default_factory=set)
    #: Offsets that are neither, and therefore block every higher offset.
    unresolved: set[int] = field(default_factory=set)
    first_offset: int | None = None

    def resolved(self) -> set[int]:
        return self.inserted | self.quarantined

    def contiguous_next_offset(self, start: int) -> int | None:
        """Highest commitable "next offset", or ``None`` if nothing is safe.

        Walks up from ``start`` while each offset is resolved. The first gap or
        unresolved record stops the walk; the walk does not resume above it.
        """
        resolved = self.resolved()
        if not resolved:
            return None
        cursor = start
        while cursor in resolved and cursor not in self.unresolved:
            cursor += 1
        return cursor if cursor > start else None


@dataclass(slots=True)
class BatchOutcome:
    """Result of one ``run_once``, for assertions and evidence."""

    polled: int
    accepted: int
    raw_rows_written: int
    quarantined: int
    committed: dict[PartitionKey, int] = field(default_factory=dict)
    paused: dict[PartitionKey, str] = field(default_factory=dict)
    blocked: dict[PartitionKey, str] = field(default_factory=dict)
    ambiguous: bool = False
    markers: int = 0
    assignment_epoch: int = 0

    @property
    def made_progress(self) -> bool:
        return bool(self.committed)


class IngestWorker:
    """Drives one consumer through the section 6.1 protocol."""

    def __init__(
        self,
        *,
        consumer: ConsumerPort,
        clickhouse: ClickHousePort,
        control: ControlPlanePort,
        settings: Settings,
        clock: Clock,
        metrics: Metrics | None = None,
        faults: FaultPlan | None = None,
        logger: StructuredLogger | None = None,
    ) -> None:
        self._consumer = consumer
        self._ch = clickhouse
        self._control = control
        self._settings = settings
        self._clock = clock
        self._metrics = metrics or Metrics()
        self._faults = faults or NullFaultPlan()
        self._log = logger or structured_logger("ingest", redact_pii=settings.metrics_redact_pii)
        self._quarantine = QuarantineLedger(control, clock=clock)
        self._tenant_resolver = settings.tenant_resolver()
        self._seen_event_ids: set[str] = set()

    # -- main loop ---------------------------------------------------------

    def run_once(self) -> BatchOutcome:
        """One poll-validate-insert-commit cycle."""
        batch = self._consumer.poll(
            max_records=self._settings.kafka_max_batch_records,
            timeout_ms=self._settings.kafka_poll_timeout_ms,
        )
        epoch = batch.assignment_epoch
        outcome = BatchOutcome(
            polled=len(batch.records),
            accepted=0,
            raw_rows_written=0,
            quarantined=0,
            assignment_epoch=epoch,
        )
        if not batch.records:
            self._refresh_lag()
            return outcome

        self._faults.fire(
            KillPoint.AFTER_POLL_BEFORE_VALIDATE, count=len(batch.records), epoch=epoch
        )

        validated = validate_records(
            batch.records,
            tenant_resolver=self._tenant_resolver,
            allowed_topics=self._settings.allowed_topics,
            max_envelope_bytes=self._settings.kafka_max_envelope_bytes,
            ingested_at=self._clock.now(),
        )
        outcome.accepted = len(validated.accepted)
        outcome.markers = len(validated.markers)

        progress = {
            key: PartitionProgress(partition=key, first_offset=min(r.offset for r in records))
            for key, records in batch.by_partition().items()
        }

        self._quarantine_failures(validated, progress, outcome)
        self._insert_accepted(validated, progress, outcome, epoch)
        self._commit_contiguous(progress, outcome, epoch)
        self._rewind_unconfirmed(progress, outcome, epoch)
        self._refresh_lag()
        return outcome

    def run_until_idle(self, *, max_cycles: int = 1_000) -> list[BatchOutcome]:
        """Drain the assignment, stopping on an empty poll or a stall.

        ``max_cycles`` is a bound, not a formality: without it a paused
        partition that keeps returning zero records would spin forever, which
        is the sort of unbounded retry loop section 8.4 forbids.
        """
        outcomes: list[BatchOutcome] = []
        for _ in range(max_cycles):
            outcome = self.run_once()
            outcomes.append(outcome)
            if outcome.polled == 0:
                break
            if not outcome.made_progress and not outcome.quarantined:
                break
        return outcomes

    # -- steps -------------------------------------------------------------

    def _quarantine_failures(
        self,
        validated: ValidatedBatch,
        progress: Mapping[PartitionKey, PartitionProgress],
        outcome: BatchOutcome,
    ) -> None:
        """Write a durable receipt per poison record, or pause its partition."""
        for key, failures in validated.failures_by_partition().items():
            tracker = progress[key]
            for failure in failures:
                self._metrics.records_rejected_total.labels(reason=str(failure.reason)).inc()
                self._faults.fire(
                    KillPoint.AFTER_POISON_DETECT_BEFORE_RECEIPT,
                    topic=failure.record.topic,
                    partition=failure.record.partition,
                    offset=failure.record.offset,
                    reason=str(failure.reason),
                )
                try:
                    self._quarantine.record(failure)
                except Exception as exc:
                    # The receipt is not durable, so this offset is unresolved
                    # and everything above it in the partition is blocked.
                    tracker.unresolved.add(failure.record.offset)
                    self._pause(key, f"quarantine write failed: {exc}", outcome)
                    self._log.error(
                        "quarantine receipt failed",
                        topic=failure.record.topic,
                        partition=failure.record.partition,
                        offset=failure.record.offset,
                        error=str(exc),
                    )
                    break
                tracker.quarantined.add(failure.record.offset)
                outcome.quarantined += 1
                self._faults.fire(
                    KillPoint.AFTER_QUARANTINE_COMMIT_BEFORE_OFFSET_ACK,
                    offset=failure.record.offset,
                )
                self._log.warning(
                    "record quarantined",
                    topic=failure.record.topic,
                    partition=failure.record.partition,
                    offset=failure.record.offset,
                    reason=str(failure.reason),
                )
        self._metrics.quarantine_pending.set(self._quarantine.pending_count())

    def _insert_accepted(
        self,
        validated: ValidatedBatch,
        progress: Mapping[PartitionKey, PartitionProgress],
        outcome: BatchOutcome,
        epoch: int,
    ) -> None:
        """Insert raw rows; mark their offsets resolved only on a proven ACK."""
        if not validated.raw_rows and not validated.markers:
            return

        self._faults.fire(
            KillPoint.AFTER_VALIDATE_BEFORE_INSERT, rows=len(validated.raw_rows), epoch=epoch
        )

        if validated.raw_rows:
            try:
                result = self._ch.insert_raw(
                    validated.raw_rows, deadline_ms=self._settings.ch_insert_deadline_ms
                )
            except AmbiguousWriteError:
                # Section 3.2: the rows may well be durable. Committing would
                # be the only unrecoverable mistake available here.
                outcome.ambiguous = True
                self._metrics.raw_insert_ambiguous_total.inc()
                for key in progress:
                    self._block(key, "insert acknowledgement unproven", outcome)
                self._log.error("raw insert ambiguous; offsets withheld", rows=len(validated.raw_rows))
                return
            except PoisonError as exc:
                # The server refused the batch for a structural reason. Rows
                # are not stored, so each offset becomes poison in its own
                # right rather than an endless retry (P08-H-01).
                self._reject_batch_as_poison(validated, progress, outcome, exc)
                return
            except RetriableError as exc:
                outcome.ambiguous = False
                for key in progress:
                    self._block(key, f"retriable insert failure: {exc.reason}", outcome)
                self._log.warning("raw insert failed; will replay", reason=str(exc.reason))
                return

            if result.rows_written != len(validated.raw_rows):
                # A partial write means we cannot say which rows landed, so it
                # is treated exactly like ambiguity (P08-E-07).
                outcome.ambiguous = True
                self._metrics.raw_insert_ambiguous_total.inc()
                for key in progress:
                    self._block(key, "partial insert; batch outcome unknown", outcome)
                self._log.error(
                    "raw insert partially applied",
                    expected=len(validated.raw_rows),
                    written=result.rows_written,
                )
                return

            outcome.raw_rows_written = result.rows_written
            for row in validated.raw_rows:
                topic, partition, offset = row.coordinate
                progress[(topic, partition)].inserted.add(offset)
                self._metrics.raw_rows_total.labels(topic=topic).inc()
                if row.event_id not in self._seen_event_ids:
                    self._seen_event_ids.add(row.event_id)
                    self._metrics.unique_event_ids_total.labels(topic=topic).inc()

        for marker in validated.markers:
            # Markers are accepted and recorded as observed, but they create
            # no Deal fact and no account metric (INV08-16).
            progress[(marker.topic, marker.partition)].inserted.add(marker.offset)

        self._faults.fire(
            KillPoint.AFTER_INSERT_ACK_BEFORE_OFFSET_COMMIT,
            rows=outcome.raw_rows_written,
            epoch=epoch,
        )

    def _reject_batch_as_poison(
        self,
        validated: ValidatedBatch,
        progress: Mapping[PartitionKey, PartitionProgress],
        outcome: BatchOutcome,
        exc: PoisonError,
    ) -> None:
        for row in validated.raw_rows:
            topic, partition, offset = row.coordinate
            record = ConsumerRecord(
                topic=topic,
                partition=partition,
                offset=offset,
                key=row.kafka_record_key or None,
                value=row.payload_json.encode("utf-8"),
                timestamp_ms=0,
            )
            failure = ValidationFailure(
                record=record,
                reason=exc.reason,
                message=f"clickhouse rejected the row: {exc}",
                detail={"source_id": row.source_id, "epoch_id": row.epoch_id},
                event_id=row.event_id,
                payload_sha256=row.payload_sha256,
            )
            try:
                self._quarantine.record(failure, tenant_hint=row.tenant_id)
            except Exception as write_error:
                progress[(topic, partition)].unresolved.add(offset)
                self._pause((topic, partition), f"quarantine write failed: {write_error}", outcome)
                continue
            progress[(topic, partition)].quarantined.add(offset)
            outcome.quarantined += 1
        self._metrics.quarantine_pending.set(self._quarantine.pending_count())
        self._log.error("clickhouse refused a raw batch", reason=str(exc.reason))

    def _commit_contiguous(
        self,
        progress: Mapping[PartitionKey, PartitionProgress],
        outcome: BatchOutcome,
        epoch: int,
    ) -> None:
        """Commit the highest contiguous eligible offset per partition."""
        if self._consumer.assignment_epoch != epoch:
            # P08-F-03: the slow worker finished after revocation. Its rows
            # may be durable - that is fine - but it may not move offsets.
            for key in progress:
                self._block(key, "assignment generation changed before commit", outcome)
            self._metrics.offset_commit_blocked_total.labels(reason="PARTITION_REVOKED").inc()
            self._log.warning("assignment changed before commit; skipping", held=epoch)
            return

        commits: dict[PartitionKey, int] = {}
        for key, tracker in progress.items():
            if key in outcome.blocked or key in self._consumer.paused():
                continue
            start = tracker.first_offset
            if start is None:
                continue
            next_offset = tracker.contiguous_next_offset(start)
            if next_offset is None:
                self._metrics.offset_commit_blocked_total.labels(reason="UNRESOLVED_LOWER").inc()
                continue
            if tracker.unresolved and min(tracker.unresolved) < next_offset:  # pragma: no cover
                # Defensive: ``contiguous_next_offset`` already stops at the
                # first unresolved offset. Kept because the cost of this being
                # wrong is silent data loss.
                next_offset = min(tracker.unresolved)
            commits[key] = next_offset

        if not commits:
            return

        self._faults.fire(KillPoint.DURING_OFFSET_COMMIT, offsets=len(commits), epoch=epoch)
        try:
            self._consumer.commit(commits, assignment_epoch=epoch)
        except FencedError as exc:
            for key in commits:
                self._block(key, f"commit fenced: {exc.reason}", outcome)
            self._metrics.offset_commit_blocked_total.labels(reason=str(exc.reason)).inc()
            return
        except AnalyticsError as exc:
            for key in commits:
                self._block(key, f"commit failed: {exc.reason}", outcome)
            self._metrics.offset_commit_blocked_total.labels(reason=str(exc.reason)).inc()
            return

        outcome.committed = dict(commits)
        for (topic, _partition) in commits:
            self._metrics.offset_commits_total.labels(topic=topic).inc()
        self._faults.fire(
            KillPoint.AFTER_OFFSET_ACK_BEFORE_LOCAL_RECEIPT, offsets=len(commits), epoch=epoch
        )

    def _rewind_unconfirmed(
        self,
        progress: Mapping[PartitionKey, PartitionProgress],
        outcome: BatchOutcome,
        epoch: int,
    ) -> None:
        """Seek back to the committed position on any partition we did not
        fully confirm, so the records are re-delivered to this process.

        A consumer's read position advances at poll time, so without this the
        uncommitted tail of a failed batch would simply never be seen again by
        a long-running worker - it would only come back after a restart or
        rebalance. That turns a transient insert failure into permanent data
        loss at the sink, which is the opposite of what withholding the commit
        was supposed to achieve.
        """
        if self._consumer.assignment_epoch != epoch:
            return
        for key, tracker in progress.items():
            committed_next = outcome.committed.get(key)
            if committed_next is None:
                committed_next = self._consumer.committed([key]).get(key)
            if committed_next is None:
                committed_next = tracker.first_offset
            if committed_next is None:
                continue
            if self._consumer.position(key) <= committed_next:
                continue
            try:
                self._consumer.seek(key, committed_next)
            except AnalyticsError as exc:  # pragma: no cover - partition changed
                self._log.warning(
                    "could not rewind partition", partition=str(key), reason=str(exc.reason)
                )

    # -- helpers -----------------------------------------------------------

    def _pause(self, key: PartitionKey, reason: str, outcome: BatchOutcome) -> None:
        try:
            self._consumer.pause(key, reason=reason)
        except FencedError:  # pragma: no cover - partition already gone
            pass
        outcome.paused[key] = reason
        outcome.blocked[key] = reason
        self._metrics.partitions_paused.set(len(self._consumer.paused()))

    def _block(self, key: PartitionKey, reason: str, outcome: BatchOutcome) -> None:
        outcome.blocked[key] = reason

    def _refresh_lag(self) -> None:
        for key in sorted(self._consumer.assignment()):
            topic, partition = key
            try:
                _low, high = self._consumer.watermarks(key)
            except AnalyticsError:  # pragma: no cover - partition vanished
                continue
            committed = self._consumer.committed([key]).get(key, 0)
            self._metrics.observe_lag(topic, partition, max(0, high - committed))

    def paused_partitions(self) -> Mapping[PartitionKey, str]:
        return self._consumer.paused()

    def resume_after_quarantine(self, key: PartitionKey) -> None:
        """Resume a partition once an operator resolved its blocking poison."""
        unresolved = self._quarantine.unresolved()
        blocking = [
            record
            for record in unresolved
            if (record.topic, record.partition) == key
        ]
        if blocking:
            raise AnalyticsError(
                Reason.QUARANTINE_PENDING_IN_CUT,
                "cannot resume a partition with unresolved poison",
                partition=str(key),
                receipts=[record.receipt_id for record in blocking[:10]],
            )
        self._consumer.resume(key)
        self._metrics.partitions_paused.set(len(self._consumer.paused()))



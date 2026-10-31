"""Deterministic in-process Kafka: partition logs, groups, generations.

The model covers the behaviours P08's offset discipline has to survive, and
nothing else (no replication protocol, no transactions):

* **Offsets are "next to read"**, as in a real client, so an off-by-one in the
  contiguous-commit logic shows up as skipped data rather than as a tidy test
  failure.
* **Generations fence commits.** Every assignment change bumps a generation,
  and a commit naming a stale one is refused. That is what makes the
  "slow worker finishes after revocation" case (P08-F-03) a deterministic
  assertion instead of a timing accident.
* **Retention truncates.** ``truncate_to`` drops the head of a log, so a
  committed offset can legitimately fall below the low watermark - the
  ``OFFSET_OUT_OF_RANGE`` condition of INV08-13 that must trigger a verified
  rebootstrap rather than an ``auto.offset.reset``.
* **Duplicate delivery is a first-class operation**, because at-least-once is
  the contract (ADR08-09), not an anomaly.

A broker-side commit is a dict write; what matters is *when* P08 performs it.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Iterable, Mapping, Sequence

from ..errors import AnalyticsError, FencedError, Reason, RetriableError
from ..faults.killpoints import FaultPlan, FaultSite, NullFaultPlan
from ..ports.kafka import ConsumerRecord, PartitionKey, PolledBatch


@dataclass(slots=True)
class _LogEntry:
    offset: int
    key: str | None
    value: bytes
    timestamp_ms: int


@dataclass(slots=True)
class _PartitionLog:
    """One partition: a list plus a low watermark that retention can raise."""

    entries: list[_LogEntry] = field(default_factory=list)
    low_watermark: int = 0
    next_offset: int = 0

    @property
    def high_watermark(self) -> int:
        return self.next_offset


class InMemoryBroker:
    """A tiny Kafka with explicit retention and group metadata."""

    def __init__(self, *, cluster_id: str = "lab-cluster-1", faults: FaultPlan | None = None) -> None:
        self.cluster_id = cluster_id
        self._faults = faults or NullFaultPlan()
        self._lock = threading.RLock()
        self._logs: dict[PartitionKey, _PartitionLog] = {}
        self._committed: dict[tuple[str, PartitionKey], int] = {}
        self._generation: dict[str, int] = {}
        self._topics: dict[str, int] = {}

    # -- topic admin -------------------------------------------------------

    def create_topic(self, topic: str, partitions: int = 1) -> None:
        if partitions < 1:
            raise ValueError("a topic needs at least one partition")
        with self._lock:
            self._topics[topic] = partitions
            for index in range(partitions):
                self._logs.setdefault((topic, index), _PartitionLog())

    def partitions_of(self, topic: str) -> list[PartitionKey]:
        with self._lock:
            return [(topic, index) for index in range(self._topics.get(topic, 0))]

    def _log(self, partition: PartitionKey) -> _PartitionLog:
        log = self._logs.get(partition)
        if log is None:
            raise AnalyticsError(Reason.FIELD_INVALID, f"unknown partition {partition}")
        return log

    # -- produce -----------------------------------------------------------

    def produce(
        self,
        topic: str,
        value: bytes,
        *,
        key: str | None = None,
        partition: int | None = None,
        timestamp_ms: int = 0,
    ) -> int:
        """Append one record, returning its offset.

        Partition selection mirrors a keyed producer: the same key always
        lands on the same partition, which is what gives per-account ordering
        and what P08-D exercises when the partition count changes.
        """
        with self._lock:
            self._faults.fire(FaultSite.KAFKA_UNAVAILABLE, topic=topic)
            count = self._topics.get(topic)
            if count is None:
                raise AnalyticsError(Reason.TOPIC_NOT_ALLOWED, f"topic {topic!r} does not exist")
            index = partition if partition is not None else (_hash_key(key) % count if key else 0)
            if not 0 <= index < count:
                raise AnalyticsError(Reason.FIELD_INVALID, f"partition {index} out of range")
            log = self._log((topic, index))
            offset = log.next_offset
            log.entries.append(_LogEntry(offset, key, value, timestamp_ms))
            log.next_offset += 1
            return offset

    def redeliver(self, partition: PartitionKey, offset: int) -> int:
        """Append a byte-identical copy of an existing record.

        This is the duplicate *delivery* of at-least-once: the same
        ``event_id`` arriving at a new offset (G08-02, P08-G-02). It is a
        broker-level operation precisely because P08 must not be able to tell
        the difference.
        """
        with self._lock:
            log = self._log(partition)
            source = next((e for e in log.entries if e.offset == offset), None)
            if source is None:
                raise AnalyticsError(Reason.OFFSET_OUT_OF_RANGE, f"offset {offset} is not retained")
            new_offset = log.next_offset
            log.entries.append(_LogEntry(new_offset, source.key, source.value, source.timestamp_ms))
            log.next_offset += 1
            return new_offset

    # -- retention ---------------------------------------------------------

    def truncate_to(self, partition: PartitionKey, new_low: int) -> int:
        """Expire history below ``new_low``, as retention would."""
        with self._lock:
            log = self._log(partition)
            if new_low <= log.low_watermark:
                return log.low_watermark
            log.entries = [e for e in log.entries if e.offset >= new_low]
            log.low_watermark = min(new_low, log.next_offset)
            return log.low_watermark

    def watermarks(self, partition: PartitionKey) -> tuple[int, int]:
        with self._lock:
            log = self._log(partition)
            return (log.low_watermark, log.high_watermark)

    # -- groups ------------------------------------------------------------

    def join_group(self, group_id: str) -> int:
        """Bump and return the group generation."""
        with self._lock:
            self._generation[group_id] = self._generation.get(group_id, 0) + 1
            return self._generation[group_id]

    def generation(self, group_id: str) -> int:
        with self._lock:
            return self._generation.get(group_id, 0)

    def committed(self, group_id: str, partition: PartitionKey) -> int | None:
        with self._lock:
            return self._committed.get((group_id, partition))

    def commit(self, group_id: str, partition: PartitionKey, next_offset: int) -> None:
        with self._lock:
            self._faults.fire(
                FaultSite.KAFKA_COMMIT_FAIL, group=group_id, partition=partition, offset=next_offset
            )
            current = self._committed.get((group_id, partition))
            if current is not None and next_offset < current:
                # A real broker accepts a backwards commit; P08 only ever
                # performs one through the audited operator path, so an
                # accidental rewind is a bug worth refusing here.
                raise AnalyticsError(
                    Reason.OFFSET_INVALID,
                    "refusing a non-monotonic commit; use reset_offsets for operator rewinds",
                    partition=str(partition),
                    current=current,
                    requested=next_offset,
                )
            self._committed[(group_id, partition)] = next_offset
            # Fired after the broker-side write: the commit succeeded and the
            # client never found out (P08-E-06).
            self._faults.fire(
                FaultSite.KAFKA_COMMIT_ACK_LOST,
                group=group_id,
                partition=partition,
                offset=next_offset,
            )

    def reset_offsets(
        self, group_id: str, offsets: Mapping[PartitionKey, int], *, actor: str
    ) -> None:
        """Audited operator rewind. P08-E-11."""
        if not actor:
            raise AnalyticsError(Reason.UNAUTHENTICATED, "offset reset requires an actor")
        with self._lock:
            for partition, offset in offsets.items():
                self._committed[(group_id, partition)] = offset

    def records(self, partition: PartitionKey) -> list[ConsumerRecord]:
        with self._lock:
            log = self._log(partition)
            topic, index = partition
            return [
                ConsumerRecord(topic, index, e.offset, e.key, e.value, e.timestamp_ms)
                for e in log.entries
            ]

    def total_records(self) -> int:
        with self._lock:
            return sum(len(log.entries) for log in self._logs.values())


class InMemoryConsumer:
    """Implements :class:`~analytics.ports.kafka.ConsumerPort`."""

    def __init__(
        self,
        broker: InMemoryBroker,
        *,
        group_id: str,
        assignment: Sequence[PartitionKey],
        faults: FaultPlan | None = None,
        auto_offset_reset: str = "none",
    ) -> None:
        if auto_offset_reset != "none":
            # Section 16.2 pins ``auto.offset.reset: none`` so a retention
            # gap fails loudly instead of silently skipping to ``latest``.
            raise AnalyticsError(
                Reason.FIELD_INVALID,
                "auto_offset_reset must be 'none'; see INV08-13",
                value=auto_offset_reset,
            )
        self._broker = broker
        self._group_id = group_id
        self._faults = faults or NullFaultPlan()
        self._assignment: frozenset[PartitionKey] = frozenset(assignment)
        self._epoch = broker.join_group(group_id)
        self._paused: dict[PartitionKey, str] = {}
        self._positions: dict[PartitionKey, int] = {}
        self._closed = False
        self._seek_to_committed()

    # -- assignment --------------------------------------------------------

    @property
    def assignment_epoch(self) -> int:
        return self._epoch

    def assignment(self) -> frozenset[PartitionKey]:
        return self._assignment

    def _seek_to_committed(self) -> None:
        for partition in self._assignment:
            committed = self._broker.committed(self._group_id, partition)
            low, _high = self._broker.watermarks(partition)
            if committed is None:
                if low > 0:
                    # No trusted offset *and* the head of the log is already
                    # gone: records expired before this group ever read them.
                    # Starting at the low watermark here would be precisely
                    # the silent skip that ``auto.offset.reset=none`` is
                    # pinned to prevent (INV08-13, P08-E-08). A cold start on
                    # an untruncated partition (low == 0) is still legitimate.
                    raise RetriableError(
                        Reason.OFFSET_OUT_OF_RANGE,
                        "no committed offset and the log head has expired; "
                        "a verified rebootstrap is required",
                        partition=str(partition),
                        low_watermark=low,
                    )
                self._positions[partition] = low
            elif committed < low:
                # INV08-13: history needed to resume is gone. Refusing here is
                # the whole point; resetting to ``latest`` would hide the gap.
                raise RetriableError(
                    Reason.OFFSET_OUT_OF_RANGE,
                    "committed offset predates retention; verified rebootstrap required",
                    partition=str(partition),
                    committed=committed,
                    low_watermark=low,
                )
            else:
                self._positions[partition] = committed

    def rebalance(self, assignment: Sequence[PartitionKey]) -> int:
        """Simulate a rebalance: new generation, re-seek to committed."""
        self._assignment = frozenset(assignment)
        self._epoch = self._broker.join_group(self._group_id)
        self._paused = {p: r for p, r in self._paused.items() if p in self._assignment}
        self._positions = {p: o for p, o in self._positions.items() if p in self._assignment}
        self._seek_to_committed()
        return self._epoch

    def revoke_all(self) -> int:
        """Lose every partition, as a group eviction would. P08-F-06."""
        return self.rebalance(())

    # -- poll / commit -----------------------------------------------------

    def poll(self, *, max_records: int = 500, timeout_ms: int = 1_000) -> PolledBatch:
        if self._closed:
            raise AnalyticsError(Reason.BACKEND_UNAVAILABLE, "consumer is closed")
        if max_records < 1:
            raise AnalyticsError(Reason.FIELD_INVALID, "max_records must be positive")
        self._faults.fire(FaultSite.KAFKA_UNAVAILABLE, operation="poll")
        collected: list[ConsumerRecord] = []
        # Deterministic partition order, so a fault targeting "the batch that
        # contains offset 10" always hits the same batch.
        for partition in sorted(self._assignment):
            if partition in self._paused:
                continue
            position = self._positions.get(partition, 0)
            for record in self._broker.records(partition):
                if record.offset < position:
                    continue
                if len(collected) >= max_records:
                    break
                collected.append(record)
            if collected:
                last = max(
                    (r.offset for r in collected if r.partition_key == partition), default=None
                )
                if last is not None:
                    self._positions[partition] = last + 1
            if len(collected) >= max_records:
                break
        return PolledBatch(
            records=tuple(collected), assignment_epoch=self._epoch, assignment=self._assignment
        )

    def commit(self, offsets: Mapping[PartitionKey, int], *, assignment_epoch: int) -> None:
        if assignment_epoch != self._epoch:
            raise FencedError(
                Reason.PARTITION_REVOKED,
                "refusing to commit under a stale assignment generation",
                held=assignment_epoch,
                current=self._epoch,
            )
        if self._broker.generation(self._group_id) != self._epoch:
            raise FencedError(
                Reason.PARTITION_REVOKED,
                "group generation advanced; this member no longer owns its partitions",
                held=self._epoch,
                broker=self._broker.generation(self._group_id),
            )
        for partition in offsets:
            if partition not in self._assignment:
                raise FencedError(
                    Reason.PARTITION_REVOKED,
                    f"partition {partition} is not assigned to this consumer",
                    partition=str(partition),
                )
        for partition, next_offset in offsets.items():
            self._broker.commit(self._group_id, partition, next_offset)

    def committed(self, partitions: Sequence[PartitionKey]) -> dict[PartitionKey, int]:
        out: dict[PartitionKey, int] = {}
        for partition in partitions:
            value = self._broker.committed(self._group_id, partition)
            if value is not None:
                out[partition] = value
        return out

    # -- flow control ------------------------------------------------------

    def pause(self, partition: PartitionKey, *, reason: str) -> None:
        if partition not in self._assignment:
            raise FencedError(Reason.PARTITION_REVOKED, f"cannot pause unowned {partition}")
        self._paused[partition] = reason

    def resume(self, partition: PartitionKey) -> None:
        self._paused.pop(partition, None)

    def paused(self) -> Mapping[PartitionKey, str]:
        return dict(self._paused)

    def position(self, partition: PartitionKey) -> int:
        return self._positions.get(partition, 0)

    def seek(self, partition: PartitionKey, offset: int) -> None:
        if partition not in self._assignment:
            raise FencedError(Reason.PARTITION_REVOKED, f"cannot seek unowned {partition}")
        low, high = self._broker.watermarks(partition)
        if offset < low or offset > high:
            raise RetriableError(
                Reason.OFFSET_OUT_OF_RANGE,
                f"offset {offset} outside [{low}, {high}]",
                partition=str(partition),
            )
        self._positions[partition] = offset

    def watermarks(self, partition: PartitionKey) -> tuple[int, int]:
        return self._broker.watermarks(partition)

    def close(self) -> None:
        self._closed = True


def _hash_key(key: str | None) -> int:
    """Stable key hash. ``hash()`` is salted per process and unusable here."""
    if key is None:
        return 0
    total = 0
    for char in key.encode("utf-8"):
        total = (total * 31 + char) & 0xFFFFFFFF
    return total

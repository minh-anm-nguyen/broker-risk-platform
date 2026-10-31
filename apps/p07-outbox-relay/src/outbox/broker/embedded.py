"""An in-process Kafka model: append-only logs, manual offsets, injectable faults.

Be clear about what this is. It is a *model* of the broker semantics the relay
and the consumer must survive -- enqueue-then-callback, per-partition append
order, manual offset commit, retention truncation, and a set of failures that
includes the genuinely ambiguous one. It is not Kafka, and nothing measured
here says anything about replication, ISR behaviour under leader election, or
durability of an acknowledged record. Design section 16.1 is explicit that a
single broker proves nothing about durability; a model in the same process
proves even less. The ``HA-LIKE`` three-broker lab is where those claims get
made, and :mod:`outbox.broker.confluent` is the adapter for it.

What the model does buy is every ordering and crash interleaving in the
kill-point inventory, deterministically and in milliseconds. Three behaviours
are worth calling out because they are easy to get wrong in a fake and then
test nothing:

**Appends happen during ``poll``, not during ``produce``.** A record sits in a
local queue until the producer is polled. That is what makes "the callback was
never served" a reachable state (fixture G07-11) and what makes the relay's
poll loop load-bearing rather than decorative.

**``drop_ack`` appends the record and reports failure.** This is the ambiguous
window from design section 3.3: the broker holds the event, the producer does
not know it. A fake that only ever failed *before* appending would make
duplicates untestable and would quietly validate a relay that treats "no ACK"
as "not delivered".

**Order within a partition is never shuffled.** Kafka guarantees it, so the
model guarantees it. The late-duplicate scenario is reproduced the way it
really happens -- two producers, and the test chooses whose record is appended
first -- rather than by reordering delivery, which would prove the consumer
handles something that cannot occur and skip the thing that can.
"""

from __future__ import annotations

import enum
import threading
from collections import deque
from collections.abc import Sequence
from dataclasses import dataclass, field

from ..errors import BrokerError, ErrorCode, MessageTooLarge, ProducerQueueFull
from ..events.routing import ALL_TOPICS, partition_for_key
from .ports import DeliveryCallback, DeliveryResult, DeliveryState, Record


class Fault(enum.StrEnum):
    """Injectable broker conditions."""

    NONE = "NONE"
    DOWN = "DOWN"
    """Unreachable: nothing is appended, every attempt fails."""

    LEADER_UNAVAILABLE = "LEADER_UNAVAILABLE"
    NOT_ENOUGH_REPLICAS = "NOT_ENOUGH_REPLICAS"
    """ISR below ``min.insync.replicas`` with ``acks=all``: refuse the write."""

    AUTH_DENIED = "AUTH_DENIED"
    TIMEOUT = "TIMEOUT"
    DROP_ACK = "DROP_ACK"
    """Append, then lose the acknowledgement. The ambiguous window."""

    DUPLICATE_APPEND = "DUPLICATE_APPEND"
    """Append twice and ACK once, as a retrying non-idempotent producer would."""


class OffsetOutOfRange(BrokerError):
    """The requested offset has been truncated away by retention.

    A distinct exception rather than a silent reset. Design section 14.4
    (IT07-16) requires that a consumer returning after retention expiry must
    not skip the lost history and declare success, and a fake that silently
    jumped to the latest offset would hide exactly that bug.
    """

    code = ErrorCode.BROKER_UNREACHABLE


@dataclass(slots=True)
class _Partition:
    records: list[Record] = field(default_factory=list)
    log_start_offset: int = 0

    @property
    def high_watermark(self) -> int:
        return self.log_start_offset + len(self.records)

    def get(self, offset: int) -> Record | None:
        if offset < self.log_start_offset:
            raise OffsetOutOfRange(
                f"offset {offset} is below log start {self.log_start_offset}: "
                "the records between them have been removed by retention and cannot be read"
            )
        index = offset - self.log_start_offset
        if index >= len(self.records):
            return None
        return self.records[index]


@dataclass(slots=True)
class _Queued:
    topic: str
    key: bytes
    value: bytes
    headers: dict[str, str]
    callback: DeliveryCallback


class EmbeddedKafka:
    """The broker: topics, partitions, group offsets and fault state."""

    def __init__(
        self,
        *,
        topics: Sequence[tuple[str, int]] | None = None,
        max_message_bytes: int = 1 << 20,
        min_insync_replicas: int = 2,
        replication_factor: int = 3,
    ) -> None:
        self._lock = threading.RLock()
        self._partitions: dict[str, list[_Partition]] = {}
        self._group_offsets: dict[tuple[str, str, int], int] = {}
        self.max_message_bytes = max_message_bytes
        self.min_insync_replicas = min_insync_replicas
        self.replication_factor = replication_factor
        self.fault: Fault = Fault.NONE
        #: Topics the current producer identity may write to. ``None`` means all.
        self.producer_acl: set[str] | None = None
        #: Topics the current consumer identity may read. ``None`` means all.
        self.consumer_acl: set[str] | None = None
        for name, partitions in topics or [(spec.name, spec.partitions) for spec in ALL_TOPICS]:
            self.create_topic(name, partitions)

    # --- admin --------------------------------------------------------------
    def create_topic(self, name: str, partitions: int) -> None:
        with self._lock:
            self._partitions.setdefault(name, [_Partition() for _ in range(partitions)])

    def partition_count(self, topic: str) -> int:
        with self._lock:
            if topic not in self._partitions:
                raise BrokerError(f"unknown topic {topic!r}", code=ErrorCode.BROKER_UNREACHABLE)
            return len(self._partitions[topic])

    def expire_retention(self, topic: str, partition: int, *, keep_from_offset: int) -> int:
        """Truncate a partition's prefix, as segment expiry would.

        Returns how many records were removed.
        """
        with self._lock:
            part = self._partitions[topic][partition]
            drop = max(0, min(keep_from_offset - part.log_start_offset, len(part.records)))
            del part.records[:drop]
            part.log_start_offset += drop
            return drop

    # --- producer path ------------------------------------------------------
    def _append(self, topic: str, key: bytes, value: bytes, headers: dict[str, str]) -> tuple[int, int]:
        partition = partition_for_key(key.decode("utf-8"), self.partition_count(topic))
        part = self._partitions[topic][partition]
        offset = part.high_watermark
        part.records.append(
            Record(topic=topic, partition=partition, offset=offset, key=key, value=value, headers=headers)
        )
        return partition, offset

    def _deliver(self, item: _Queued) -> DeliveryResult:
        """Decide what happens to one queued record, under the current fault."""
        with self._lock:
            if self.producer_acl is not None and item.topic not in self.producer_acl:
                return DeliveryResult(
                    state=DeliveryState.FAILED,
                    topic=item.topic,
                    error_code=ErrorCode.AUTH_DENIED,
                    detail="producer identity is not authorized for this topic",
                )
            if item.topic not in self._partitions:
                return DeliveryResult(
                    state=DeliveryState.FAILED,
                    topic=item.topic,
                    error_code=ErrorCode.BROKER_UNREACHABLE,
                    detail="unknown topic",
                )
            if len(item.value) > self.max_message_bytes:
                return DeliveryResult(
                    state=DeliveryState.FAILED,
                    topic=item.topic,
                    error_code=ErrorCode.MESSAGE_TOO_LARGE,
                    detail=f"{len(item.value)} bytes exceeds max.message.bytes",
                )

            match self.fault:
                case Fault.DOWN:
                    return DeliveryResult(
                        state=DeliveryState.FAILED,
                        topic=item.topic,
                        error_code=ErrorCode.BROKER_UNREACHABLE,
                    )
                case Fault.LEADER_UNAVAILABLE:
                    return DeliveryResult(
                        state=DeliveryState.FAILED,
                        topic=item.topic,
                        error_code=ErrorCode.BROKER_LEADER_UNAVAILABLE,
                    )
                case Fault.NOT_ENOUGH_REPLICAS:
                    # acks=all with ISR below minimum: the write is refused, so
                    # nothing is appended and the event stays durable in the outbox.
                    return DeliveryResult(
                        state=DeliveryState.FAILED,
                        topic=item.topic,
                        error_code=ErrorCode.BROKER_NOT_ENOUGH_REPLICAS,
                        detail=f"ISR below min.insync.replicas={self.min_insync_replicas}",
                    )
                case Fault.AUTH_DENIED:
                    return DeliveryResult(
                        state=DeliveryState.FAILED, topic=item.topic, error_code=ErrorCode.AUTH_DENIED
                    )
                case Fault.TIMEOUT:
                    return DeliveryResult(
                        state=DeliveryState.FAILED, topic=item.topic, error_code=ErrorCode.BROKER_TIMEOUT
                    )
                case Fault.DROP_ACK:
                    self._append(item.topic, item.key, item.value, item.headers)
                    return DeliveryResult(
                        state=DeliveryState.AMBIGUOUS,
                        topic=item.topic,
                        error_code=ErrorCode.BROKER_ACK_AMBIGUOUS,
                        detail="record was appended but the acknowledgement was lost",
                    )
                case Fault.DUPLICATE_APPEND:
                    self._append(item.topic, item.key, item.value, item.headers)
                    partition, offset = self._append(item.topic, item.key, item.value, item.headers)
                    return DeliveryResult(
                        state=DeliveryState.ACKED,
                        topic=item.topic,
                        partition=partition,
                        offset=offset,
                    )
                case _:
                    partition, offset = self._append(item.topic, item.key, item.value, item.headers)
                    return DeliveryResult(
                        state=DeliveryState.ACKED,
                        topic=item.topic,
                        partition=partition,
                        offset=offset,
                    )

    # --- reads for oracles and tests ---------------------------------------
    def records(self, topic: str | None = None) -> list[Record]:
        with self._lock:
            topics = [topic] if topic else sorted(self._partitions)
            out: list[Record] = []
            for name in topics:
                for part in self._partitions[name]:
                    out.extend(part.records)
            return out

    def record_count(self, topic: str | None = None) -> int:
        return len(self.records(topic))

    def high_watermark(self, topic: str, partition: int) -> int:
        with self._lock:
            return self._partitions[topic][partition].high_watermark

    def log_start_offset(self, topic: str, partition: int) -> int:
        with self._lock:
            return self._partitions[topic][partition].log_start_offset

    def committed(self, group: str, topic: str, partition: int) -> int:
        with self._lock:
            return self._group_offsets.get((group, topic, partition), 0)

    def set_committed(self, group: str, topic: str, partition: int, offset: int) -> None:
        with self._lock:
            self._group_offsets[(group, topic, partition)] = offset

    def consumer_lag(self, group: str, topic: str) -> int:
        with self._lock:
            total = 0
            for index, part in enumerate(self._partitions[topic]):
                committed = self._group_offsets.get((group, topic, index), part.log_start_offset)
                total += max(0, part.high_watermark - committed)
            return total

    def producer(self, *, queue_max_bytes: int = 8 << 20) -> EmbeddedProducer:
        return EmbeddedProducer(self, queue_max_bytes=queue_max_bytes)

    def consumer(self, *, group: str) -> EmbeddedConsumer:
        return EmbeddedConsumer(self, group=group)


class EmbeddedProducer:
    """A producer whose ``produce`` only enqueues."""

    def __init__(self, broker: EmbeddedKafka, *, queue_max_bytes: int = 8 << 20) -> None:
        self._broker = broker
        self._queue: deque[_Queued] = deque()
        self._queued_bytes = 0
        self._queue_max_bytes = queue_max_bytes
        self._lock = threading.Lock()
        self.closed = False

    def produce(
        self,
        *,
        topic: str,
        key: bytes,
        value: bytes,
        headers: dict[str, str] | None = None,
        on_delivery: DeliveryCallback,
    ) -> None:
        if self.closed:
            raise BrokerError("producer is closed", code=ErrorCode.BROKER_UNREACHABLE)
        if len(value) > self._broker.max_message_bytes:
            # Refused at enqueue, as librdkafka does: there is no point queueing
            # a record the broker will certainly reject.
            raise MessageTooLarge(f"{len(value)} bytes exceeds max.message.bytes")
        with self._lock:
            if self._queued_bytes + len(value) > self._queue_max_bytes:
                # The relay must treat this as backpressure and stop claiming,
                # not as a delivery failure: nothing was sent and nothing is lost.
                raise ProducerQueueFull(
                    f"local queue holds {self._queued_bytes} bytes, limit is {self._queue_max_bytes}"
                )
            self._queue.append(
                _Queued(topic=topic, key=key, value=value, headers=dict(headers or {}), callback=on_delivery)
            )
            self._queued_bytes += len(value)

    def poll(self, timeout: float = 0.0) -> int:
        """Attempt delivery of everything queued and fire the callbacks.

        ``timeout`` is accepted for interface parity and ignored: there is no
        network to wait for, and sleeping would only slow the tests down.
        """
        del timeout
        served = 0
        while True:
            with self._lock:
                if not self._queue:
                    break
                item = self._queue.popleft()
                self._queued_bytes -= len(item.value)
            result = self._broker._deliver(item)
            item.callback(result)
            served += 1
        return served

    def flush(self, timeout: float = 10.0) -> int:
        self.poll(timeout)
        with self._lock:
            return len(self._queue)

    def buffer_used_bytes(self) -> int:
        with self._lock:
            return self._queued_bytes

    def in_flight(self) -> int:
        with self._lock:
            return len(self._queue)

    def close(self) -> None:
        self.closed = True


class EmbeddedConsumer:
    """A consumer with manual offset commits and a simulated rebalance."""

    def __init__(self, broker: EmbeddedKafka, *, group: str) -> None:
        self._broker = broker
        self._group = group
        self._topics: list[str] = []
        self._assignment: list[tuple[str, int]] = []
        self._positions: dict[tuple[str, int], int] = {}
        self.closed = False

    @property
    def group(self) -> str:
        return self._group

    def subscribe(self, topics: Sequence[str]) -> None:
        if self._broker.consumer_acl is not None:
            forbidden = [topic for topic in topics if topic not in self._broker.consumer_acl]
            if forbidden:
                raise BrokerError(
                    f"consumer group {self._group!r} is not authorized for {forbidden}",
                    code=ErrorCode.AUTH_DENIED,
                )
        self._topics = list(topics)
        self._assignment = [
            (topic, index)
            for topic in self._topics
            for index in range(self._broker.partition_count(topic))
        ]
        self._reset_positions()

    def _reset_positions(self) -> None:
        """Start from the committed offset, which is the only safe start.

        Not from the log start, and not from the latest: both would be a policy
        decision this consumer is not entitled to make. If the committed offset
        has been truncated away, the read raises and an operator decides.
        """
        self._positions = {
            tp: self._broker.committed(self._group, tp[0], tp[1]) for tp in self._assignment
        }

    def assignment(self) -> list[tuple[str, int]]:
        return list(self._assignment)

    def revoke(self, topic_partitions: Sequence[tuple[str, int]]) -> None:
        """Simulate a rebalance taking partitions away from this member."""
        revoked = set(topic_partitions)
        self._assignment = [tp for tp in self._assignment if tp not in revoked]
        for tp in revoked:
            self._positions.pop(tp, None)

    def assign(self, topic_partitions: Sequence[tuple[str, int]]) -> None:
        for tp in topic_partitions:
            if tp not in self._assignment:
                self._assignment.append(tp)
            self._positions[tp] = self._broker.committed(self._group, tp[0], tp[1])

    def poll(self, max_records: int = 1, timeout: float = 0.0) -> list[Record]:
        """Fetch up to ``max_records``, round-robin across assigned partitions.

        Round-robin rather than partition-at-a-time: both are legal, and
        interleaving means a test exercising cross-partition arrival order gets
        it for free instead of having to force it.
        """
        del timeout
        if self.closed:
            return []
        out: list[Record] = []
        progressed = True
        while len(out) < max_records and progressed:
            progressed = False
            for tp in list(self._assignment):
                if len(out) >= max_records:
                    break
                topic, partition = tp
                position = self._positions.get(tp, 0)
                record = self._broker._partitions[topic][partition].get(position)
                if record is None:
                    continue
                out.append(record)
                self._positions[tp] = position + 1
                progressed = True
        return out

    def position(self, topic: str, partition: int) -> int:
        return self._positions.get((topic, partition), 0)

    def seek(self, topic: str, partition: int, offset: int) -> None:
        self._positions[(topic, partition)] = offset

    def commit(self, offsets: dict[tuple[str, int], int]) -> None:
        """Commit the *next* offset to read for each partition."""
        for (topic, partition), offset in offsets.items():
            current = self._broker.committed(self._group, topic, partition)
            if offset < current:
                # Moving a committed offset backwards is replay, which is an
                # operator action with an audit trail, not something the
                # consumer loop may do on its own.
                raise BrokerError(
                    f"refusing to move committed offset backwards for {topic}/{partition}: "
                    f"{current} -> {offset}",
                    code=ErrorCode.INTEGRITY_CONFLICT,
                )
            self._broker.set_committed(self._group, topic, partition, offset)

    def committed(self, topic: str, partition: int) -> int:
        return self._broker.committed(self._group, topic, partition)

    def close(self) -> None:
        self.closed = True

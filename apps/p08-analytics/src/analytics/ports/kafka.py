"""The Kafka consumer port, with assignment generations made explicit.

Section 6.1 step 7 and INV08-11 both turn on one question: *does this worker
still own the partition it is about to commit?* A real client answers that
through rebalance callbacks and a generation id, and the usual bug is code
that checks ownership once at poll time and commits minutes later.

So ``assignment_epoch`` is part of the port rather than an implementation
detail. Every commit names the generation it believes it holds, and the broker
refuses a commit from a stale one. That turns a race that normally needs luck
to reproduce into a deterministic assertion (P08-F-03).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Protocol, Sequence, runtime_checkable

PartitionKey = tuple[str, int]
"""``(topic, partition)``."""


@dataclass(frozen=True, slots=True)
class ConsumerRecord:
    """One delivered record. ``value`` stays bytes until validation."""

    topic: str
    partition: int
    offset: int
    key: str | None
    value: bytes
    timestamp_ms: int

    @property
    def partition_key(self) -> PartitionKey:
        return (self.topic, self.partition)

    @property
    def size_bytes(self) -> int:
        return len(self.value)


@dataclass(frozen=True, slots=True)
class PolledBatch:
    """Records plus the generation they were delivered under."""

    records: tuple[ConsumerRecord, ...]
    assignment_epoch: int
    assignment: frozenset[PartitionKey]

    def by_partition(self) -> dict[PartitionKey, list[ConsumerRecord]]:
        grouped: dict[PartitionKey, list[ConsumerRecord]] = {}
        for record in self.records:
            grouped.setdefault(record.partition_key, []).append(record)
        for records in grouped.values():
            records.sort(key=lambda r: r.offset)
        return grouped


@runtime_checkable
class ConsumerPort(Protocol):
    """A bounded consumer with manual, fenced offset commits."""

    @property
    def assignment_epoch(self) -> int:
        """Generation of the current assignment; changes on every rebalance."""

    def assignment(self) -> frozenset[PartitionKey]: ...

    def poll(self, *, max_records: int, timeout_ms: int) -> PolledBatch:
        """Fetch up to ``max_records``, skipping paused partitions."""

    def commit(
        self, offsets: Mapping[PartitionKey, int], *, assignment_epoch: int
    ) -> None:
        """Commit *next* offsets for owned partitions.

        Raises :class:`FencedError` if ``assignment_epoch`` is stale and
        :class:`AnalyticsError` on a non-monotonic commit. Offsets are
        "next to read", matching Kafka, so committing 60 means 0..59 are done.
        """

    def committed(self, partitions: Sequence[PartitionKey]) -> dict[PartitionKey, int]: ...

    def pause(self, partition: PartitionKey, *, reason: str) -> None:
        """Stop fetching one partition without affecting the others.

        The per-partition granularity is required by P08-F-04: unresolved
        poison must stop its own partition, not the whole consumer.
        """

    def resume(self, partition: PartitionKey) -> None: ...

    def paused(self) -> Mapping[PartitionKey, str]: ...

    def position(self, partition: PartitionKey) -> int: ...

    def seek(self, partition: PartitionKey, offset: int) -> None: ...

    def watermarks(self, partition: PartitionKey) -> tuple[int, int]:
        """``(low, high)``. A committed offset below ``low`` means history is
        gone and a verified rebootstrap is required (INV08-13)."""

    def close(self) -> None: ...

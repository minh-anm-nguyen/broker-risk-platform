"""Broker interface, modelled on the one mistake that matters.

``produce()`` enqueues. It does not deliver, and its return is not an ACK. The
design says this twice (sections 7.3 and 19.1) because
``confluent_kafka.Producer.produce()`` really does return immediately after
putting the record in a local queue, and treating that as success is the single
most dangerous available misreading: it loses committed events during a broker
outage while reporting a healthy publish rate.

So the interface has no call that both sends and confirms. A record is enqueued
with a callback, and the callback only runs while :meth:`Producer.poll` is
being called. A relay that forgets to poll will never mark anything
``PUBLISHED`` -- it will stall, visibly, which is the correct failure
(fixture G07-11).
"""

from __future__ import annotations

import enum
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Protocol

from ..errors import ErrorCode


class DeliveryState(enum.StrEnum):
    ACKED = "ACKED"
    FAILED = "FAILED"
    AMBIGUOUS = "AMBIGUOUS"
    """The broker may or may not hold the record. Treat as not-published, retry."""


@dataclass(frozen=True, slots=True)
class DeliveryResult:
    """The outcome a delivery callback reports."""

    state: DeliveryState
    topic: str
    partition: int | None = None
    offset: int | None = None
    error_code: ErrorCode | None = None
    detail: str = ""

    @property
    def broker_acked(self) -> bool:
        return self.state is DeliveryState.ACKED

    def __post_init__(self) -> None:
        if self.state is DeliveryState.ACKED and (self.partition is None or self.offset is None):
            # An ACK without a position is not an ACK we can record, and
            # writing PUBLISHED without one would make the row unverifiable.
            raise ValueError("an ACKED delivery must carry a partition and an offset")
        if self.state is not DeliveryState.ACKED and self.error_code is None:
            raise ValueError("a non-ACKED delivery must carry an error code")


@dataclass(frozen=True, slots=True)
class Record:
    """One record as a consumer sees it."""

    topic: str
    partition: int
    offset: int
    key: bytes
    value: bytes
    headers: dict[str, str] = field(default_factory=dict)

    @property
    def topic_partition(self) -> tuple[str, int]:
        return (self.topic, self.partition)


DeliveryCallback = Callable[[DeliveryResult], None]


class Producer(Protocol):
    def produce(
        self,
        *,
        topic: str,
        key: bytes,
        value: bytes,
        headers: dict[str, str] | None = None,
        on_delivery: DeliveryCallback,
    ) -> None:
        """Enqueue locally. Raises :class:`~outbox.errors.ProducerQueueFull`."""

    def poll(self, timeout: float = 0.0) -> int:
        """Serve delivery callbacks. Returns how many fired."""

    def flush(self, timeout: float = 10.0) -> int:
        """Serve callbacks until the queue drains. Returns what is still queued."""

    def buffer_used_bytes(self) -> int: ...

    def in_flight(self) -> int: ...

    def close(self) -> None: ...


class Consumer(Protocol):
    def subscribe(self, topics: Sequence[str]) -> None: ...

    def poll(self, max_records: int = 1, timeout: float = 0.0) -> list[Record]:
        """Fetch records at the current position without committing anything."""

    def commit(self, offsets: dict[tuple[str, int], int]) -> None:
        """Commit *next* offsets, explicitly. Never automatic."""

    def committed(self, topic: str, partition: int) -> int: ...

    def assignment(self) -> list[tuple[str, int]]: ...

    def close(self) -> None: ...

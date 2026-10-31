"""Row shapes and the store interface.

The vocabulary here is the design's state machine (section 6.5) made explicit.
Two details carry most of the correctness weight:

**A claim is a capability, not a note.** :class:`Claim` carries the
``claim_token`` and ``claim_version`` that the row had when this worker took it.
Every subsequent write is conditional on both still matching, so a worker that
was paused past its lease cannot write to the row after a standby has taken it
(INV07-06). The token is per row, not per batch, so one stale callback cannot
fence a sibling row that is still legitimately owned.

**The published bytes are frozen at write time.** ``wire_bytes`` is serialised
inside the business transaction and stored, so a retry republishes the exact
bytes that were hashed, not a re-serialisation that happens to be equal today.
``payload`` is stored alongside only so SQL oracles can read the fields; the
two are written together and neither may be mutated afterwards.
"""

from __future__ import annotations

import enum
import uuid
from collections.abc import Iterator, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from ..events.envelope import Envelope


class EventState(enum.StrEnum):
    """The outbox lifecycle. ``QUARANTINED`` is not success."""

    PENDING = "PENDING"
    CLAIMED = "CLAIMED"
    PUBLISHED = "PUBLISHED"
    QUARANTINED = "QUARANTINED"


class AttemptOutcome(enum.StrEnum):
    ACK = "ACK"
    RETRY = "RETRY"
    QUARANTINE = "QUARANTINE"
    STALE_ACK = "STALE_ACK"
    """Broker accepted the record but this worker no longer owned the row."""


@dataclass(frozen=True, slots=True)
class OutboxRecord:
    """A persisted outbox row, as read back."""

    outbox_id: int
    event_id: uuid.UUID
    source_id: str
    epoch_id: str
    dedupe_key: str
    aggregate_type: str
    aggregate_key: str
    aggregate_version: int
    event_type: str
    schema_version: int
    topic: str
    partition_key: str
    payload: dict[str, Any]
    payload_sha256: str
    wire_bytes: bytes
    state: EventState
    created_at: datetime
    next_attempt_at: datetime
    attempt_count: int
    claim_owner: str | None
    claim_token: uuid.UUID | None
    claim_version: int
    lease_until: datetime | None
    published_at: datetime | None
    kafka_partition: int | None
    kafka_offset: int | None
    last_error_code: str | None
    quarantine_reason: str | None


@dataclass(frozen=True, slots=True)
class Claim:
    """A leased row plus the fencing credentials needed to write it back."""

    outbox_id: int
    event_id: uuid.UUID
    source_id: str
    epoch_id: str
    dedupe_key: str
    aggregate_key: str
    aggregate_version: int
    event_type: str
    schema_version: int
    topic: str
    partition_key: str
    wire_bytes: bytes
    payload_sha256: str
    attempt_count: int
    claim_owner: str
    claim_token: uuid.UUID
    claim_version: int
    lease_until: datetime
    created_at: datetime
    """Row creation time, carried so the relay can observe created-to-ACK
    latency without a second read. Not the COMMIT instant -- see
    :mod:`outbox.clock`."""

    @property
    def wire_size(self) -> int:
        return len(self.wire_bytes)

    @property
    def stream_key(self) -> str:
        """The ordering stream: source, epoch and aggregate together.

        ``aggregate_key`` alone is not the stream. The claim query serialises
        versions within ``(source_id, epoch_id, aggregate_key)``, so account
        ``A-42`` in two epochs -- or in two tenants -- is two independent
        streams whose events may be in flight simultaneously. Keying
        single-flight on the aggregate alone would refuse that legitimate
        concurrency after a source reset.

        NUL is the separator because every component is validated to exclude
        it, which makes the composition unambiguous.
        """
        return f"{self.source_id}\x00{self.epoch_id}\x00{self.aggregate_key}"


@dataclass(frozen=True, slots=True)
class OutboxStats:
    """The gauges design section 11.1 requires, read in one pass."""

    pending_total: int
    claimed_total: int
    published_total: int
    quarantined_total: int
    oldest_pending_age_seconds: float
    storage_bytes: int


class Transaction(Protocol):
    """A business transaction that the outbox writer joins.

    The writer never opens its own transaction. It is handed one that already
    contains the Deal insert or the snapshot publish, which is the whole point:
    an outbox writer that could begin its own transaction could not provide
    atomicity with the business change (design section 6.3).
    """

    def reserve_aggregate_head(self, source_id: str, epoch_id: str, aggregate_key: str) -> int:
        """Lock (creating if absent) the aggregate head and return last_version."""

    def bump_aggregate_head(self, source_id: str, epoch_id: str, aggregate_key: str) -> int:
        """Advance the locked head and return the new version."""

    def find_event_by_dedupe(self, source_id: str, epoch_id: str, dedupe_key: str) -> OutboxRecord | None:
        """Look up an existing logical event by its semantic key."""

    def insert_event(self, envelope: Envelope, *, topic: str, partition_key: str, now: datetime) -> int:
        """Insert the outbox row. Returns ``outbox_id``."""

    def epoch_status(self, source_id: str, epoch_id: str) -> str:
        """Read the P03 epoch status, for the publication gate."""

    def execute(self, sql: str, params: Sequence[Any] = ()) -> Any:
        """Escape hatch for P03-owned statements and oracle SQL."""


class OutboxStore(Protocol):
    """Publisher-side persistence."""

    def migrate(self) -> None: ...

    def transaction(self) -> AbstractContextManager[Transaction]:
        """A read-write transaction. Commits on clean exit, rolls back on error."""

    def claim_due(self, *, owner: str, batch_size: int, lease_seconds: float, now: datetime) -> list[Claim]: ...

    def mark_published_if_owner(
        self, *, claim: Claim, kafka_partition: int, kafka_offset: int, now: datetime
    ) -> bool:
        """CAS to PUBLISHED. False means this worker no longer owned the row."""

    def reschedule_if_owner(
        self, *, claim: Claim, delay_seconds: float, error_code: str, now: datetime
    ) -> bool: ...

    def quarantine_if_owner(
        self, *, claim: Claim, reason: str, error_code: str, now: datetime
    ) -> bool: ...

    def record_attempt(
        self, *, claim: Claim, outcome: AttemptOutcome, error_code: str | None, now: datetime
    ) -> None: ...

    def get(self, event_id: uuid.UUID) -> OutboxRecord | None: ...

    def stats(self, *, now: datetime, include_storage: bool = True) -> OutboxStats: ...

    def storage_bytes(self) -> int:
        """The storage figure on its own, so a caller can refresh it separately."""

    def iter_records(self, *, state: EventState | None = None) -> Iterator[OutboxRecord]: ...

    def close(self) -> None: ...

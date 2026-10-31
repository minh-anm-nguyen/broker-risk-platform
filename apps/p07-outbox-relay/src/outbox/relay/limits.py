"""The hard caps: in-flight records, queued bytes, one record per aggregate.

INV07-13 asks for relay queue, in-flight, DB pool and payload size to enforce
hard caps, and the FMEA entry it defends against is "claim 100k rows into memory
then wait for Kafka". The admission decision therefore has to happen *before*
the claim transaction, not after: a row that has been claimed is already leased
and counted, so discovering the buffer is full at that point means releasing a
claim you should never have taken.

``admit`` is the single predicate for that, and it returns a reason rather than
a bool so the caller can emit the right metric and the operator can tell
"waiting for the broker" apart from "my own buffer is full".

The per-aggregate single-flight set is belt to the claim query's braces. The SQL
already refuses to hand out version ``n+1`` while ``n`` is unpublished, so two
records of one aggregate should never be in flight together; this set makes that
an assertion in the process rather than a property you have to re-derive from
the SQL every time you read it.
"""

from __future__ import annotations

import enum
import threading
from dataclasses import dataclass, field


class Admission(enum.StrEnum):
    OK = "OK"
    INFLIGHT_FULL = "INFLIGHT_FULL"
    BUFFER_FULL = "BUFFER_FULL"
    CIRCUIT_OPEN = "CIRCUIT_OPEN"
    STORAGE_FAIL_CLOSED = "STORAGE_FAIL_CLOSED"
    """The outbox is near its storage budget. Publishing continues; new
    business imports are what must stop (design section 16.4)."""


@dataclass(slots=True)
class InFlightLimits:
    """Counts what has been enqueued and not yet acknowledged."""

    max_inflight: int
    max_buffer_bytes: int
    max_event_bytes: int
    _inflight: int = 0
    _bytes: int = 0
    _keys: set[str] = field(default_factory=set)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    @property
    def inflight(self) -> int:
        with self._lock:
            return self._inflight

    @property
    def buffered_bytes(self) -> int:
        with self._lock:
            return self._bytes

    def headroom(self) -> int:
        """How many more records could be claimed right now."""
        with self._lock:
            by_count = self.max_inflight - self._inflight
            by_bytes = (self.max_buffer_bytes - self._bytes) // max(1, self.max_event_bytes)
            return max(0, min(by_count, by_bytes))

    def admit(self) -> Admission:
        with self._lock:
            if self._inflight >= self.max_inflight:
                return Admission.INFLIGHT_FULL
            if self._bytes + self.max_event_bytes > self.max_buffer_bytes:
                # Judged against the worst case, not the average: admitting on
                # average size lets a run of large events overshoot the budget.
                return Admission.BUFFER_FULL
        return Admission.OK

    def acquire(self, *, aggregate_key: str, size_bytes: int) -> None:
        with self._lock:
            if aggregate_key in self._keys:
                raise AssertionError(
                    f"aggregate {aggregate_key!r} already has a record in flight; the claim "
                    "query must not release a second version of one aggregate"
                )
            self._keys.add(aggregate_key)
            self._inflight += 1
            self._bytes += size_bytes

    def release(self, *, aggregate_key: str, size_bytes: int) -> None:
        with self._lock:
            self._keys.discard(aggregate_key)
            # Clamped: a double release would otherwise drive the counters
            # negative and make the caps stop working without any error.
            self._inflight = max(0, self._inflight - 1)
            self._bytes = max(0, self._bytes - size_bytes)

    def in_flight_keys(self) -> frozenset[str]:
        with self._lock:
            return frozenset(self._keys)


@dataclass(frozen=True, slots=True)
class StoragePolicy:
    """Outbox storage pressure thresholds from design section 16.4.

    Illustrative policy candidates, not validated thresholds. Note what the
    actions are: warn, throttle *upstream*, then fail closed on *new business
    imports*. Deleting PENDING rows to reclaim space is never an option.
    """

    budget_bytes: int
    warn_ratio: float
    throttle_ratio: float
    fail_closed_ratio: float

    def level(self, used_bytes: int) -> str:
        ratio = used_bytes / self.budget_bytes if self.budget_bytes else 0.0
        if ratio >= self.fail_closed_ratio:
            return "FAIL_CLOSED"
        if ratio >= self.throttle_ratio:
            return "THROTTLE"
        if ratio >= self.warn_ratio:
            return "WARN"
        return "OK"

    def accepts_new_business_writes(self, used_bytes: int) -> bool:
        return self.level(used_bytes) != "FAIL_CLOSED"

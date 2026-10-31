"""Admission control: the only place a request is allowed in (section 5.3).

Admission is a **counting gate that fails fast**, not a waiting room. When the
connector is at capacity a caller is refused immediately with
:class:`~mt5_connector.errors.OverloadedError` rather than parked on a future.

That choice is the whole point of the component. The alternative - letting
callers queue until their deadline - looks friendlier and is strictly worse:
100 callers against one serial worker produce 100 pending futures, 100
envelopes retained in memory and 100 timeouts that all fire later, and the
service has spent memory to deliver failures slowly instead of spending
nothing to deliver them quickly. Little's Law says the queue must grow once
arrival rate exceeds service rate; a bounded refusal is the designed response
to that, not a degradation of it (section 11.2, WF2-M-01, WF2-M-02).

Two counters are tracked separately because they bound different resources:

* ``pending`` - requests admitted and not yet finished. Bounds memory and
  future count.
* ``scans`` - concurrent long scans. Bounds how many independent
  :class:`~mt5_connector.scan.ScanContext` objects, each with its own
  duplicate-tracking set, can exist at once.

Neither counter is the native call slot. That slot lives in the actor and is
released only when the call actually returns, which is why a caller timeout
can release admission while the session stays busy (section 16.1).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from .errors import ConnectorClosedError, OverloadedError

if TYPE_CHECKING:
    from .deadline import Deadline

__all__ = ["AdmissionTicket", "AdmissionController", "ScanSlot"]


@dataclass(slots=True)
class AdmissionTicket:
    """Proof that one request was admitted.

    Released exactly once. Double release would corrupt the counter and
    eventually let the gate admit beyond its cap, so the ticket tracks its own
    state rather than trusting call sites (WF2-G-03).
    """

    request_id: str
    client_id: str
    released: bool = False


@dataclass(slots=True)
class ScanSlot:
    scan_id: str
    released: bool = False


class AdmissionController:
    """Bounded admission for one source.

    Not thread-safe by design: every method runs on the event loop, and the
    counters it guards are only ever touched from there. Introducing a lock
    would imply worker threads mutate admission state, which they must not -
    a thread that could release admission could also release it for a call
    that is still running.
    """

    def __init__(
        self,
        *,
        max_pending: int,
        max_concurrent_scans: int,
        max_pending_per_client: int | None = None,
    ) -> None:
        if max_pending < 1:
            raise ValueError("max_pending must be at least 1")
        if max_concurrent_scans < 1:
            raise ValueError("max_concurrent_scans must be at least 1")
        self._max_pending = max_pending
        self._max_scans = max_concurrent_scans
        #: Per-caller share of the queue. Without it one hostile or buggy
        #: caller can fill the whole queue and starve the sync service, which
        #: is a fairness failure rather than an overload (WF2-G-09, FR-18).
        self._max_per_client = max_pending_per_client
        self._pending: int = 0
        self._per_client: dict[str, int] = {}
        self._scans: int = 0
        self._closed = False

        self.admitted_total: int = 0
        self.rejected_overload_total: int = 0
        self.rejected_closed_total: int = 0
        self.rejected_client_quota_total: int = 0
        #: High-water mark, so a test can assert the cap was never exceeded
        #: even momentarily rather than only at the moment it sampled.
        self.peak_pending: int = 0

    # -- state --------------------------------------------------------------

    @property
    def pending(self) -> int:
        return self._pending

    @property
    def capacity(self) -> int:
        return self._max_pending

    @property
    def scans(self) -> int:
        return self._scans

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def saturated(self) -> bool:
        return self._pending >= self._max_pending

    def pending_for(self, client_id: str) -> int:
        return self._per_client.get(client_id, 0)

    # -- request admission --------------------------------------------------

    def acquire(
        self,
        *,
        request_id: str,
        deadline: "Deadline",
        operation: str,
        client_id: str = "default",
    ) -> AdmissionTicket:
        """Admit one request or refuse it now.

        Synchronous on purpose: there is no ``await`` in this method, so a
        refusal costs one function call and cannot itself be delayed by load.
        """
        if self._closed:
            raise ConnectorClosedError(
                "the connector is draining and is not accepting new requests",
                operation=operation,
                request_id=request_id,
            )
        # Checked before the capacity test: a request that has already run out
        # of budget should not consume a slot that a live request could use.
        deadline.check(operation=operation, request_id=request_id)

        if self._max_per_client is not None:
            used = self._per_client.get(client_id, 0)
            if used >= self._max_per_client:
                self.rejected_client_quota_total += 1
                raise OverloadedError(
                    "this caller is already using its share of the admission queue",
                    operation=operation,
                    request_id=request_id,
                    details={
                        "client_quota": self._max_per_client,
                        "client_pending": used,
                    },
                )
        if self._pending >= self._max_pending:
            self.rejected_overload_total += 1
            raise OverloadedError(
                "the admission queue is full; the request is refused immediately rather "
                "than parked until its deadline",
                operation=operation,
                request_id=request_id,
                details={"queue_capacity": self._max_pending, "queue_depth": self._pending},
            )

        self._pending += 1
        self._per_client[client_id] = self._per_client.get(client_id, 0) + 1
        self.peak_pending = max(self.peak_pending, self._pending)
        self.admitted_total += 1
        return AdmissionTicket(request_id=request_id, client_id=client_id)

    def release(self, ticket: AdmissionTicket) -> None:
        """Release a ticket. Idempotent, because call sites use ``finally``."""
        if ticket.released:
            return
        ticket.released = True
        self._pending -= 1
        remaining = self._per_client.get(ticket.client_id, 0) - 1
        if remaining > 0:
            self._per_client[ticket.client_id] = remaining
        else:
            # Dropped rather than left at zero so an unbounded number of
            # distinct client ids cannot grow this dict without bound.
            self._per_client.pop(ticket.client_id, None)
        if self._pending < 0:  # pragma: no cover - guarded by the ticket flag
            raise AssertionError("admission pending counter went negative")

    # -- scan admission -----------------------------------------------------

    def acquire_scan(self, *, scan_id: str) -> ScanSlot:
        if self._closed:
            raise ConnectorClosedError(
                "the connector is draining and is not starting new scans",
                details={"scan_id": scan_id},
            )
        if self._scans >= self._max_scans:
            self.rejected_overload_total += 1
            raise OverloadedError(
                "too many concurrent scans for this source",
                details={"max_concurrent_scans": self._max_scans, "active_scans": self._scans},
            )
        self._scans += 1
        return ScanSlot(scan_id=scan_id)

    def release_scan(self, slot: ScanSlot) -> None:
        if slot.released:
            return
        slot.released = True
        self._scans -= 1

    # -- lifecycle ----------------------------------------------------------

    def close(self) -> None:
        """Stop admitting. Does not touch requests already in flight.

        Separating "no new work" from "existing work finished" is what makes a
        bounded drain possible: the gate shuts instantly, and the actor then
        has until the drain timeout to finish what it already holds
        (WF2-O-02, INV-14).
        """
        self._closed = True

    def snapshot(self) -> dict[str, int | bool]:
        return {
            "pending": self._pending,
            "capacity": self._max_pending,
            "peak_pending": self.peak_pending,
            "scans": self._scans,
            "max_scans": self._max_scans,
            "admitted_total": self.admitted_total,
            "rejected_overload_total": self.rejected_overload_total,
            "rejected_closed_total": self.rejected_closed_total,
            "rejected_client_quota_total": self.rejected_client_quota_total,
            "closed": self._closed,
        }

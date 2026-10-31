"""Liveness, readiness and drain, kept honest about what each one means.

Design section 11.2 separates these three deliberately, and the separation is
operational, not cosmetic:

* **live** asks only whether this process is working. It must not depend on
  PostgreSQL or Kafka, because a process that reports itself dead during a
  broker outage gets restarted by the orchestrator -- repeatedly -- and a
  restart loop is the worst possible response to a dependency being down.
* **ready** asks whether this relay can usefully claim and publish right now.
  During a Kafka outage the answer is no, so the relay is taken out of
  rotation while staying alive to recover.
* **drain** is a state, not a question: stop claiming, keep serving callbacks,
  and above all persist no false ``PUBLISHED`` on the way out.

The last line of that section is the one most worth encoding: P06's readiness
and P07's are separate. A Kafka failure must not retroactively turn an already
verified read-only risk result false, so nothing here reports on anything but
this worker.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any

from .clock import Clock, RealClock
from .errors import OutboxError
from .settings import Settings


class Readiness(enum.StrEnum):
    READY = "READY"
    DEGRADED = "DEGRADED"
    """Serving, but a dependency is impaired. Operational policy decides
    whether this keeps the worker in rotation."""

    NOT_READY = "NOT_READY"


@dataclass(slots=True)
class HealthReport:
    live: bool
    readiness: Readiness
    draining: bool
    checks: dict[str, str] = field(default_factory=dict)
    reasons: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "live": self.live,
            "readiness": str(self.readiness),
            "draining": self.draining,
            "checks": dict(self.checks),
            "reasons": list(self.reasons),
        }


class HealthMonitor:
    """Answers the three questions for one relay worker."""

    def __init__(
        self,
        store: Any,
        producer: Any,
        *,
        settings: Settings,
        clock: Clock | None = None,
    ) -> None:
        self._store = store
        self._producer = producer
        self._settings = settings
        self._clock = clock or RealClock()
        self._draining = False

    def begin_drain(self) -> None:
        self._draining = True

    def live(self) -> bool:
        """True while this process can still do work. No dependencies consulted."""
        return True

    def report(self, *, circuit_open: bool = False) -> HealthReport:
        checks: dict[str, str] = {}
        reasons: list[str] = []
        readiness = Readiness.READY

        # The store must be readable *and* writable: a relay that can only read
        # can claim nothing, and a read-only replica would otherwise look fine.
        try:
            stats = self._store.stats(now=self._clock.now())
            checks["store"] = "ok"
            checks["pending"] = str(stats.pending_total)
            checks["storage_bytes"] = str(stats.storage_bytes)
            ratio = (
                stats.storage_bytes / self._settings.storage_budget_bytes
                if self._settings.storage_budget_bytes
                else 0.0
            )
            if ratio >= self._settings.storage_fail_closed_ratio:
                checks["storage"] = "fail_closed"
                reasons.append(
                    f"outbox storage at {ratio:.0%} of budget: new business imports must stop"
                )
                readiness = Readiness.DEGRADED
            elif ratio >= self._settings.storage_warn_ratio:
                checks["storage"] = "warn"
            else:
                checks["storage"] = "ok"
        except OutboxError as exc:
            checks["store"] = f"error: {type(exc).__name__}"
            reasons.append("authoritative store is not usable for claims")
            readiness = Readiness.NOT_READY

        try:
            buffered = int(self._producer.buffer_used_bytes())
            checks["producer_buffer_bytes"] = str(buffered)
            if buffered >= self._settings.producer_buffer_max_bytes:
                checks["producer"] = "buffer_full"
                reasons.append("producer buffer is full; claiming is paused")
                readiness = Readiness.DEGRADED
            else:
                checks["producer"] = "ok"
        except Exception as exc:  # noqa: BLE001 - a health check never raises
            checks["producer"] = f"error: {type(exc).__name__}"
            reasons.append("producer is not usable")
            readiness = Readiness.NOT_READY

        if circuit_open:
            checks["circuit"] = "open"
            reasons.append("publish circuit is open; the broker is unreachable or refusing us")
            # Not ready to take work, but deliberately still live so that the
            # process survives to drain the backlog when the broker returns.
            readiness = Readiness.NOT_READY
        else:
            checks["circuit"] = "closed"

        if self._draining:
            reasons.append("draining: not claiming new rows")
            readiness = Readiness.NOT_READY

        return HealthReport(
            live=self.live(),
            readiness=readiness,
            draining=self._draining,
            checks=checks,
            reasons=reasons,
        )

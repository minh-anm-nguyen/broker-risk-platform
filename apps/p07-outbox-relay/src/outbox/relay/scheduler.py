"""The relay loop around the publish cycle.

Thin on purpose. All the correctness lives in :class:`~outbox.relay.publisher.Publisher`;
this adds only the three things a long-running worker needs and a single cycle
does not: when to sleep, when to stop, and how to stop without lying about what
was published.

``run_until_idle`` is the bounded form the tests use. It stops when a cycle
claims nothing, serves no callbacks and leaves nothing in flight -- which is a
real quiescence check rather than a fixed iteration count, so a test cannot pass
by running out of loops while work remains.

Shutdown goes through :meth:`Publisher.drain`: stop claiming, keep polling until
callbacks are served or the deadline passes, and release whatever is still
unresolved. Design section 11.2 is the rule being honoured -- "persist no false
PUBLISHED status on timeout".
"""

from __future__ import annotations

import signal
import threading
import time
from dataclasses import dataclass

from ..clock import Clock, RealClock
from ..observability.logging import get_logger
from ..settings import Settings
from .limits import Admission
from .publisher import CycleReport, Publisher

_log = get_logger("relay.scheduler")


@dataclass(slots=True)
class RunSummary:
    cycles: int = 0
    claimed: int = 0
    acked: int = 0
    stale_acks: int = 0
    ambiguous: int = 0
    rescheduled: int = 0
    quarantined: int = 0
    released: int = 0
    backpressure_cycles: int = 0
    circuit_open_cycles: int = 0

    def absorb(self, report: CycleReport) -> None:
        self.cycles += 1
        self.claimed += report.claimed
        self.acked += report.acked
        self.stale_acks += report.stale_acks
        self.ambiguous += report.ambiguous
        self.rescheduled += report.rescheduled
        self.quarantined += report.quarantined
        self.released += report.released
        if report.admission is Admission.CIRCUIT_OPEN:
            self.circuit_open_cycles += 1
        elif report.admission is not Admission.OK:
            self.backpressure_cycles += 1


class RelayWorker:
    """A single relay owner, run as a standalone worker process."""

    def __init__(
        self,
        publisher: Publisher,
        *,
        settings: Settings,
        clock: Clock | None = None,
        sleep: object | None = None,
    ) -> None:
        self._publisher = publisher
        self._settings = settings
        self._clock = clock or RealClock()
        self._sleep = sleep or time.sleep
        self._stopping = threading.Event()

    @property
    def publisher(self) -> Publisher:
        return self._publisher

    def stop(self) -> None:
        self._stopping.set()

    def run_until_idle(self, *, max_cycles: int = 10_000) -> RunSummary:
        """Cycle until there is demonstrably nothing left to do.

        Raises if ``max_cycles`` is reached, because silently returning from a
        loop that never settled would let a test assert on a half-finished run.
        """
        summary = RunSummary()
        for _ in range(max_cycles):
            report = self._publisher.publish_cycle()
            summary.absorb(report)
            if self._is_idle(report):
                return summary
        raise RuntimeError(
            f"relay did not reach an idle state within {max_cycles} cycles; "
            f"last summary {summary}"
        )

    def run_forever(self, *, install_signal_handlers: bool = True) -> RunSummary:
        """Run until stopped, then drain."""
        if install_signal_handlers:
            self._install_signals()
        summary = RunSummary()
        while not self._stopping.is_set():
            report = self._publisher.publish_cycle()
            summary.absorb(report)
            self._pace(report)
        _log.info("relay draining", owner=self._publisher.owner)
        drain = self._publisher.drain()
        summary.absorb(drain)
        return summary

    def _pace(self, report: CycleReport) -> None:
        """Sleep as long as the situation calls for, and no longer."""
        if report.admission is Admission.CIRCUIT_OPEN:
            # Wait out the cooldown rather than spinning a cycle per idle tick.
            wait = self._publisher.circuit.seconds_until_probe(monotonic=self._clock.monotonic())
            self._sleep(max(self._settings.poll_idle_sleep_seconds, wait))
            return
        if report.claimed == 0 and report.callbacks_served == 0:
            self._sleep(self._settings.poll_idle_sleep_seconds)

    def _is_idle(self, report: CycleReport) -> bool:
        if report.claimed or report.callbacks_served:
            return False
        if self._publisher.limits.inflight:
            return False
        # A circuit-open cycle is not idle: there is work, we are just refusing
        # to take it. Treating it as idle would end a drain with a full backlog.
        return report.admission is Admission.OK

    def _install_signals(self) -> None:
        def _handler(signum: int, _frame: object) -> None:
            _log.info("shutdown signal received", signal=signum)
            self.stop()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                signal.signal(sig, _handler)
            except (ValueError, OSError):  # pragma: no cover - non-main thread
                pass

"""Named kill points, and the evidence that one actually fired.

Design section 15 lists 24 fault sites and then adds the rule that makes them
worth having: "A test that did not reach its hook is INCONCLUSIVE." So this
registry does two jobs. It lets a test attach a crash, a delay or an observer to
a named point in production code, and it *counts* how often each point was
reached, which is what oracle O8 reads to decide whether a PASS is honest.

:class:`CrashSignal` derives from ``BaseException`` on purpose. A simulated
``SIGKILL`` must not be swallowed by an ``except Exception`` somewhere in the
middle of the relay; the only handlers that may see it are the ones that exist
to roll a transaction back, which is exactly what the operating system would do
to an uncommitted transaction anyway.

The production cost of a disarmed hook is one dict lookup against an empty dict.
That is cheap enough to leave the calls in place permanently, which matters:
hooks that are compiled out of release builds are hooks that drift away from the
code they are supposed to instrument.
"""

from __future__ import annotations

import enum
import threading
from collections import Counter
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any


class KillPoint(enum.StrEnum):
    """The deterministic fault sites from design section 15."""

    # P03 business transaction (KP07-01..10)
    P03_BEFORE_BEGIN = "KP07-01"
    P03_AFTER_BEGIN = "KP07-02"
    P03_AFTER_DEAL_INSERT = "KP07-03"
    P03_AFTER_HEAD_BUMP = "KP07-04"
    P03_AFTER_OUTBOX_INSERT = "KP07-05"
    P03_BEFORE_COMMIT = "KP07-06"
    P03_AFTER_COMMIT_BEFORE_ACK = "KP07-07"
    P03_SNAPSHOT_AFTER_STAGING = "KP07-08"
    P03_SNAPSHOT_AFTER_PUBLISH_UPDATE = "KP07-09"
    P03_SNAPSHOT_AFTER_MARKER_INSERT = "KP07-10"

    # Relay (KP07-11..17)
    RELAY_BEFORE_CLAIM = "KP07-11"
    RELAY_AFTER_CLAIM_BEFORE_PRODUCE = "KP07-12"
    RELAY_AFTER_ENQUEUE_BEFORE_CALLBACK = "KP07-13"
    RELAY_AFTER_ACK_BEFORE_CAS = "KP07-14"
    RELAY_AFTER_CAS_BEFORE_OBSERVE = "KP07-15"
    RELAY_LEASE_EXPIRED_WITH_INFLIGHT = "KP07-16"
    RELAY_DURING_GRACEFUL_FLUSH = "KP07-17"

    # Consumer (KP07-18..23)
    CONSUMER_AFTER_POLL_BEFORE_BEGIN = "KP07-18"
    CONSUMER_AFTER_INBOX_INSERT = "KP07-19"
    CONSUMER_AFTER_PROJECTION_BEFORE_COMMIT = "KP07-20"
    CONSUMER_AFTER_DB_COMMIT_BEFORE_OFFSET = "KP07-21"
    CONSUMER_AFTER_OFFSET_COMMIT = "KP07-22"
    CONSUMER_DURING_REVOCATION = "KP07-23"

    # Source lifecycle (KP07-24)
    EPOCH_TRANSITION_MID_FLIGHT = "KP07-24"


class CrashSignal(BaseException):
    """A simulated process kill. Deliberately not an ``Exception``."""

    def __init__(self, kill_point: KillPoint | str, detail: str = "") -> None:
        self.kill_point = str(kill_point)
        super().__init__(f"simulated crash at {self.kill_point}{f': {detail}' if detail else ''}")


@dataclass(slots=True)
class HookRegistry:
    """Handlers and fire counts for every kill point."""

    _handlers: dict[str, list[Callable[..., None]]] = field(default_factory=dict)
    _counts: Counter[str] = field(default_factory=Counter)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def fire(self, kill_point: KillPoint | str, /, **context: Any) -> None:
        """Record that execution reached this point, then run any handlers.

        The count is incremented *before* the handlers run so that a handler
        which raises ``CrashSignal`` still leaves evidence that the point was
        reached -- otherwise every crash test would report its own fault as
        never having fired.
        """
        name = str(kill_point)
        with self._lock:
            self._counts[name] += 1
            handlers = list(self._handlers.get(name, ()))
        for handler in handlers:
            handler(**context)

    def fired(self, kill_point: KillPoint | str) -> int:
        with self._lock:
            return self._counts[str(kill_point)]

    def any_fired(self) -> dict[str, int]:
        with self._lock:
            return dict(self._counts)

    def reset(self) -> None:
        with self._lock:
            self._handlers.clear()
            self._counts.clear()

    @contextmanager
    def handler(self, kill_point: KillPoint | str, fn: Callable[..., None]) -> Iterator[None]:
        """Attach a handler for the duration of the block."""
        name = str(kill_point)
        with self._lock:
            self._handlers.setdefault(name, []).append(fn)
        try:
            yield
        finally:
            with self._lock:
                handlers = self._handlers.get(name, [])
                if fn in handlers:
                    handlers.remove(fn)

    @contextmanager
    def crash_at(self, kill_point: KillPoint | str, *, after: int = 1, detail: str = "") -> Iterator[None]:
        """Raise :class:`CrashSignal` the ``after``-th time the point is hit.

        ``after`` exists because several kill points are inside a loop, and
        "crash on the third event" is a different test from "crash on the
        first".
        """
        hits = {"n": 0}

        def _crash(**_: Any) -> None:
            hits["n"] += 1
            if hits["n"] == after:
                raise CrashSignal(kill_point, detail)

        with self.handler(kill_point, _crash):
            yield

    @contextmanager
    def run_at(self, kill_point: KillPoint | str, fn: Callable[[], None], *, after: int = 1) -> Iterator[None]:
        """Run an arbitrary action when the point is reached.

        This is how a takeover race is built without threads: at the moment the
        relay has an ACK but has not yet written it back, let the test expire
        the lease and run a standby.
        """
        hits = {"n": 0}

        def _run(**_: Any) -> None:
            hits["n"] += 1
            if hits["n"] == after:
                fn()

        with self.handler(kill_point, _run):
            yield


#: The process-wide registry. Production code fires against this; tests swap or
#: reset it through the ``hooks`` fixture rather than reaching in directly.
HOOKS = HookRegistry()


def fire(kill_point: KillPoint | str, /, **context: Any) -> None:
    HOOKS.fire(kill_point, **context)

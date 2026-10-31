"""Deterministic crash and fault injection points inside the writer.

Why this exists: ``sleep(0.5)`` followed by ``kill -9`` does not prove the
process died at a transaction boundary. It proves the process died somewhere.
IT-03 and WF-076/WF-077 require knowing *which side of COMMIT* the crash
happened on, and the only way to know that is to put a named barrier in the
code and fire it on request (section 10.4).

Nothing here is enabled by default. ``SIM_FAILPOINT`` must name a failpoint
explicitly, which is why the kill mode is safe to ship: an operator who has
not set the variable cannot trip it.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

#: Everything is written and validated; ``COMMIT`` has not been issued. After a
#: crash here the event must be absent and ``committed_seq`` unchanged.
BEFORE_COMMIT = "just_before_commit"

#: ``COMMIT`` returned; the caller has not been told. After a crash here the
#: event must be present exactly once, and a retry with the same idempotency
#: key must return the original outcome rather than apply a second effect.
AFTER_COMMIT = "just_after_commit"

#: Simulates ``ENOSPC`` from the filesystem during a batch (WF-080).
STORAGE_FULL = "storage_full"

#: Simulates a read-only mount or permission failure (WF-081).
STORAGE_READONLY = "storage_readonly"

KNOWN_FAILPOINTS = frozenset({BEFORE_COMMIT, AFTER_COMMIT, STORAGE_FULL, STORAGE_READONLY})

#: Exit code used for a simulated hard kill. 137 is what a SIGKILL victim
#: reports, which keeps test assertions honest about what was simulated.
KILL_EXIT_CODE = 137


class InjectedStorageFailure(RuntimeError):
    """Raised by a failpoint configured to fail the transaction, not the process.

    Carries the failpoint ``name`` so a test can assert *which* fault fired
    rather than merely that something failed. A chaos test that cannot tell
    the difference is the failure mode WF-119 describes.
    """

    def __init__(self, name: str, seq: int) -> None:
        super().__init__(f"failpoint {name} fired at seq={seq}")
        self.name = name
        self.seq = seq


@dataclass(frozen=True, slots=True)
class FailpointConfig:
    name: str = ""
    at_seq: int = 0
    #: ``kill`` terminates the process without unwinding (the only honest way
    #: to emulate SIGKILL in-process); ``raise`` fails the transaction instead.
    mode: str = "kill"


class FailpointController:
    """Evaluates named failpoints. A no-op unless explicitly configured."""

    __slots__ = ("_config", "_fired")

    def __init__(self, config: FailpointConfig | None = None) -> None:
        self._config = config or FailpointConfig()
        self._fired = 0

    @property
    def enabled(self) -> bool:
        return bool(self._config.name)

    @property
    def fired(self) -> int:
        return self._fired

    def check(self, name: str, seq: int = 0) -> None:
        """Fire the failpoint if ``name``/``seq`` match the configuration."""
        config = self._config
        if not config.name or config.name != name:
            return
        if config.at_seq and seq != config.at_seq:
            return
        self._fired += 1
        if config.mode == "kill":
            # Deliberately os._exit: no atexit handlers, no buffer flush, no
            # SQLite cleanup. A graceful shutdown here would prove nothing
            # about WAL recovery.
            os._exit(KILL_EXIT_CODE)
        raise InjectedStorageFailure(name, seq)


def controller_from_settings(failpoint: str, at_seq: int, mode: str = "kill") -> FailpointController:
    """Build a controller, refusing a misspelled failpoint name.

    A typo would otherwise produce a test that passes because nothing was
    injected at all, which WF-119 exists to prevent.
    """
    if not failpoint:
        return FailpointController()
    if failpoint not in KNOWN_FAILPOINTS:
        raise ValueError(
            f"Unknown failpoint {failpoint!r}; expected one of {sorted(KNOWN_FAILPOINTS)}"
        )
    return FailpointController(FailpointConfig(name=failpoint, at_seq=at_seq, mode=mode))

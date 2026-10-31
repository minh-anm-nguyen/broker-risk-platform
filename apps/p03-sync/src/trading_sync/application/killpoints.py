"""Named crash barriers for deterministic crash-window tests.

Design 17.3 asks for seventeen named points around the commit boundaries, and
for one specific discipline: *a test must assert the barrier was actually
crossed*, not infer it from a log line that may have been written before the
transaction it describes. So every barrier counts its crossings, and the test
helpers read that counter. A crash test whose injector never fired is
``NOT_CONCLUSIVE``, never ``PASS`` (design 15.6, WF3-241).

:class:`KillPointCrash` derives from ``BaseException`` deliberately. A plain
``Exception`` would be caught by the pipeline's own error handling and turned
into an orderly abort, which is the opposite of the abrupt stop being
simulated.

Barriers are inert unless explicitly enabled. In a deployed process
``Barriers.enabled`` is ``False``, :meth:`Barriers.reach` is a cheap no-op, and
``arm`` refuses to do anything -- a barrier that could be armed at runtime
would be a way to terminate a transaction on demand.
"""

from __future__ import annotations

import asyncio
from collections import Counter
from dataclasses import dataclass, field
from typing import Awaitable, Callable

__all__ = ["KILL_POINTS", "Barriers", "KillPointCrash"]

#: The barrier names from design 17.3, in pipeline order.
KILL_POINTS: tuple[str, ...] = (
    "K00_before_source_call",
    "K01_after_source_response",
    "K02_after_page_validate",
    "K03_before_db_begin",
    "K04_after_deal_insert_before_receipt",
    "K05_after_receipt_before_commit",
    "K06_after_commit_before_ack",
    "K07_after_last_page_commit",
    "K08_before_checkpoint_select_for_update",
    "K09_after_checkpoint_update_before_commit",
    "K10_after_checkpoint_commit_before_status_response",
    "K11_before_snapshot_stage",
    "K12_after_snapshot_stage_page",
    "K13_before_snapshot_promote",
    "K14_mid_snapshot_promote_transaction",
    "K15_after_snapshot_commit_before_ack",
    "K16_after_lease_expiry_before_old_worker_publish",
)


class KillPointCrash(BaseException):
    """Simulates abrupt termination at a barrier.

    ``BaseException`` so ordinary ``except Exception`` handlers do not convert
    an abrupt stop into a graceful one.
    """

    def __init__(self, name: str) -> None:
        super().__init__(f"killpoint {name}")
        self.name = name


@dataclass
class _Armed:
    #: How many more times this barrier should fire before going quiet.
    remaining: int
    action: Callable[[], Awaitable[None]] | None
    crash: bool


@dataclass
class Barriers:
    """A registry of armed barriers plus a crossing counter."""

    enabled: bool = False
    _armed: dict[str, _Armed] = field(default_factory=dict, repr=False)
    _crossings: Counter[str] = field(default_factory=Counter, repr=False)

    def arm_crash(self, name: str, *, times: int = 1) -> None:
        """Raise :class:`KillPointCrash` the next ``times`` times ``name`` is reached."""
        self._arm(name, _Armed(remaining=times, action=None, crash=True))

    def arm_action(
        self, name: str, action: Callable[[], Awaitable[None]], *, times: int = 1
    ) -> None:
        """Run ``action`` when the barrier is reached, then continue.

        Used to interleave another actor at an exact point -- for instance,
        taking the lease over while the original worker is mid-transaction
        (WF3-123).
        """
        self._arm(name, _Armed(remaining=times, action=action, crash=False))

    def arm_pause(self, name: str, seconds: float, *, times: int = 1) -> None:
        async def _pause() -> None:
            await asyncio.sleep(seconds)

        self.arm_action(name, _pause, times=times)

    def _arm(self, name: str, armed: _Armed) -> None:
        if not self.enabled:
            raise RuntimeError(
                "barriers are disabled; enable them only in a test process"
            )
        if name not in KILL_POINTS:
            raise KeyError(f"unknown kill point {name!r}")
        self._armed[name] = armed

    async def reach(self, name: str) -> None:
        """Called from production code paths; a no-op unless armed."""
        if not self.enabled:
            return
        self._crossings[name] += 1
        armed = self._armed.get(name)
        if armed is None or armed.remaining <= 0:
            return
        armed.remaining -= 1
        if armed.crash:
            raise KillPointCrash(name)
        if armed.action is not None:
            await armed.action()

    def crossed(self, name: str) -> int:
        """How many times a barrier was reached. The evidence a test asserts on."""
        return self._crossings[name]

    def assert_crossed(self, name: str) -> None:
        if self._crossings[name] == 0:
            raise AssertionError(
                f"kill point {name} was never reached, so this run proves nothing"
            )

    def clear(self) -> None:
        """Disarm everything.

        The harness calls this between cases: a rule left armed would leak into
        the next case and make it unreproducible (WF3-248).
        """
        self._armed.clear()
        self._crossings.clear()

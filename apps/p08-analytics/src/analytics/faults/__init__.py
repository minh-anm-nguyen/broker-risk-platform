"""Deterministic fault injection.

Section 11 is explicit that a scenario where the fault never fired is
``NOT_RUN``, not ``PASS``. That single rule shapes this package: a
:class:`~analytics.faults.killpoints.FaultPlan` records every site it was
asked about and every fault it actually delivered, and the test helper
:func:`~analytics.faults.killpoints.FaultPlan.assert_fired` turns a silently
missed injection into a failure.
"""

from .killpoints import (
    ALL_KILL_POINTS,
    CrashError,
    FaultAction,
    FaultPlan,
    KillPoint,
    NullFaultPlan,
)

__all__ = [
    "ALL_KILL_POINTS",
    "CrashError",
    "FaultAction",
    "FaultPlan",
    "KillPoint",
    "NullFaultPlan",
]
